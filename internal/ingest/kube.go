package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"

	"github.com/datafocal/datafocal/internal/identity"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/tail"
)

const kindKube = "kube"

// kube follows the cluster's Events and its Pods' container state transitions as JSON
// lines, each cursor the latest time a line so far carried.
type kube struct {
	lines  chan kubeLine
	pods   cache.Store
	since  time.Time // the saved cursor; a line no later was collected before a restart
	latest time.Time
}

type kubeLine struct {
	at   time.Time
	text string
}

// openKube starts informers on client, in-cluster config's when nil, and returns once
// they have synced, so every line after an object's first listing is one it changed.
func openKube(ctx context.Context, client kubernetes.Interface, from tail.Position) (tail.Follower, error) {
	if client == nil {
		cfg, err := rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("ingest: kube config: %w", err)
		}
		if client, err = kubernetes.NewForConfig(cfg); err != nil {
			return nil, fmt.Errorf("ingest: kube client: %w", err)
		}
	}
	since, _ := time.Parse(time.RFC3339Nano, from.Cursor) // zero when none is saved
	k := &kube{lines: make(chan kubeLine, batchLines), since: since, latest: since}
	factory := informers.NewSharedInformerFactory(client, 0)
	pods, events := factory.Core().V1().Pods().Informer(), factory.Core().V1().Events().Informer()
	k.pods = pods.GetStore()
	_, podErr := pods.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(old, cur any) { k.podUpdated(ctx, old.(*corev1.Pod), cur.(*corev1.Pod)) },
	})
	_, eventErr := events.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(e any) { k.event(ctx, e.(*corev1.Event)) },
		UpdateFunc: func(old, cur any) {
			if !eventTime(old.(*corev1.Event)).Equal(eventTime(cur.(*corev1.Event))) {
				k.event(ctx, cur.(*corev1.Event))
			}
		},
	})
	if err := errors.Join(podErr, eventErr); err != nil {
		return nil, fmt.Errorf("ingest: kube handlers: %w", err)
	}
	factory.Start(ctx.Done())
	for informer, synced := range factory.WaitForCacheSync(ctx.Done()) {
		if !synced {
			return nil, fmt.Errorf("ingest: kube %v did not sync: %w", informer, ctx.Err())
		}
	}
	return k, nil
}

func (k *kube) Next(ctx context.Context) (tail.Line, error) {
	select {
	case l := <-k.lines:
		if l.at.After(k.latest) {
			k.latest = l.at
		}
		return tail.Line{Text: l.text, Position: tail.Position{Cursor: k.latest.Format(time.RFC3339Nano)}}, nil
	case <-ctx.Done():
		return tail.Line{}, ctx.Err()
	}
}

// send queues a line unless it is no later than the saved cursor.
func (k *kube) send(ctx context.Context, at time.Time, columns map[string]any) {
	if !at.After(k.since) {
		return
	}
	columns["time"] = at.Format(time.RFC3339Nano)
	text, _ := json.Marshal(columns) // strings, numbers and times only
	select {
	case k.lines <- kubeLine{at: at, text: string(text)}:
	case <-ctx.Done():
	}
}

func eventTime(e *corev1.Event) time.Time {
	if e.Series != nil {
		return e.Series.LastObservedTime.Time
	}
	for _, t := range []time.Time{e.LastTimestamp.Time, e.EventTime.Time, e.FirstTimestamp.Time} {
		if !t.IsZero() {
			return t
		}
	}
	return e.CreationTimestamp.Time
}

func (k *kube) event(ctx context.Context, e *corev1.Event) {
	obj, count := e.InvolvedObject, e.Count
	if e.Series != nil {
		count = e.Series.Count
	}
	columns := map[string]any{segment.ColMessage: e.Message, segment.ColLevel: "info", "reason": e.Reason,
		"kind": obj.Kind, "object": obj.Name, segment.ColNamespace: obj.Namespace, "count": count}
	if e.Type == corev1.EventTypeWarning {
		columns[segment.ColLevel] = "warn"
	}
	if obj.Kind != "Pod" {
		columns[segment.ColService] = "kube-" + strings.ToLower(obj.Kind)
		k.send(ctx, eventTime(e), columns)
		return
	}
	// A container's events name it in the field path, as spec.containers{name}.
	if _, rest, ok := strings.Cut(obj.FieldPath, "{"); ok {
		columns[segment.ColContainer] = strings.TrimSuffix(rest, "}")
	}
	var node string
	if pod, ok, _ := k.pods.GetByKey(obj.Namespace + "/" + obj.Name); ok {
		node = pod.(*corev1.Pod).Spec.NodeName
	}
	k.send(ctx, eventTime(e), podColumns(columns, obj.Name, node))
}

// podColumns files a line under its pod as the pod's container logs are: the node is
// their host, which a static pod's name ends with.
func podColumns(columns map[string]any, pod, node string) map[string]any {
	columns[segment.ColPod] = pod
	columns[segment.ColService] = identity.PodService(pod, node)
	if node != "" {
		columns[segment.ColHost] = node
	}
	return columns
}

// podUpdated sends a line for each container state cur reports that old did not.
func (k *kube) podUpdated(ctx context.Context, old, cur *corev1.Pod) {
	before := slices.Concat(old.Status.InitContainerStatuses, old.Status.ContainerStatuses)
	for _, s := range slices.Concat(cur.Status.InitContainerStatuses, cur.Status.ContainerStatuses) {
		var was corev1.ContainerStatus
		if i := slices.IndexFunc(before, func(b corev1.ContainerStatus) bool { return b.Name == s.Name }); i >= 0 {
			was = before[i]
		}
		// A container restarted between updates shows its termination only as its last state.
		if t := s.LastTerminationState.Terminated; t != nil && !terminatedIn(was, t) {
			k.send(ctx, t.FinishedAt.Time, lifecycle(cur, s, s.LastTerminationState))
		}
		if t := s.State.Terminated; t != nil && !terminatedIn(was, t) {
			k.send(ctx, t.FinishedAt.Time, lifecycle(cur, s, s.State))
		}
		if s.State.Terminated == nil && stateName(s.State) != stateName(was.State) {
			at := time.Now() // a waiting state carries no time
			if r := s.State.Running; r != nil {
				at = r.StartedAt.Time
			}
			k.send(ctx, at, lifecycle(cur, s, s.State))
		}
	}
}

func terminatedIn(s corev1.ContainerStatus, t *corev1.ContainerStateTerminated) bool {
	return slices.ContainsFunc([]*corev1.ContainerStateTerminated{s.State.Terminated, s.LastTerminationState.Terminated}, func(o *corev1.ContainerStateTerminated) bool {
		return o != nil && o.ContainerID == t.ContainerID && o.FinishedAt.Equal(&t.FinishedAt)
	})
}

// stateName tells a running container from a waiting one and one waiting for another reason.
func stateName(state corev1.ContainerState) string {
	switch {
	case state.Running != nil:
		return "running"
	case state.Waiting != nil:
		return "waiting: " + state.Waiting.Reason
	}
	return ""
}

func lifecycle(pod *corev1.Pod, s corev1.ContainerStatus, state corev1.ContainerState) map[string]any {
	columns := map[string]any{segment.ColLevel: "info", segment.ColNamespace: pod.Namespace, segment.ColContainer: s.Name, "restart_count": s.RestartCount}
	desc := stateName(state)
	switch {
	case state.Terminated != nil:
		t := state.Terminated
		desc = fmt.Sprintf("terminated: %s (exit %d)", t.Reason, t.ExitCode)
		columns["reason"], columns["exit_code"] = t.Reason, t.ExitCode
		if t.ExitCode != 0 {
			columns[segment.ColLevel] = "error"
		}
	case state.Waiting != nil:
		columns["reason"] = state.Waiting.Reason
		if state.Waiting.Reason != "ContainerCreating" {
			columns[segment.ColLevel] = "warn"
		}
	}
	columns[segment.ColMessage] = fmt.Sprintf("container %s %s, restarts %d", s.Name, desc, s.RestartCount)
	return podColumns(columns, pod.Name, pod.Spec.NodeName)
}
