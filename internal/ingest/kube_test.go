package ingest

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/datafocal/datafocal/internal/collector"
	"github.com/datafocal/datafocal/internal/identity"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/value"
)

func TestKubeFilesEventsAndLifecycleUnderTheirPod(t *testing.T) {
	at := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	event := func(name, typ, reason, note string, at time.Time) *corev1.Event {
		return &corev1.Event{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "datafocal"}, Type: typ, Reason: reason, Message: note, Count: 1,
			InvolvedObject: corev1.ObjectReference{Kind: "Node", Name: "node-1"}, LastTimestamp: metav1.NewTime(at)}
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "compactor-29887695-pwfbq", Namespace: "datafocal"}, Spec: corev1.PodSpec{NodeName: "node-1"},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "compactor",
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(at.Add(-time.Hour))}}}}}}
	client := fake.NewClientset(event("pressure", corev1.EventTypeWarning, "SystemOOM", "System OOM encountered", at), pod)
	watching := make(chan struct{}, 4)
	client.PrependWatchReactor("*", func(k8stesting.Action) (bool, watch.Interface, error) {
		watching <- struct{}{}
		return false, nil, nil
	})
	dir := t.TempDir()
	// run collects from a fresh source over client until it has want lines, after changing
	// the cluster with change once the pod and event watches have started.
	run := func(want int, change func(ctx context.Context)) []map[string]string {
		collected := make(chan map[string]string, 8)
		stop := start(t, Config{Sources: []Source{{Kind: kindKube}}, StateDir: dir, FlushInterval: time.Hour, Kube: client, Host: "cluster-events"},
			collector.Fake{CollectFn: func(r segment.Record) (collector.CollectResult, error) {
				collected <- columns(r)
				return collector.CollectResult{}, nil
			}})
		<-watching
		<-watching
		change(context.Background())
		var got []map[string]string
		for len(got) < want {
			select {
			case c := <-collected:
				got = append(got, c)
			case <-time.After(5 * time.Second):
				require.Fail(t, "a line was not collected", "got %v", got)
			}
		}
		require.NoError(t, stop())
		assert.Empty(t, collected)
		return got
	}

	got := run(2, func(ctx context.Context) {
		oom := pod.DeepCopy()
		oom.Status.ContainerStatuses[0].RestartCount = 1
		oom.Status.ContainerStatuses[0].State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			Reason: "OOMKilled", ExitCode: 137, ContainerID: "containerd://1", FinishedAt: metav1.NewTime(at.Add(time.Second))}}
		_, err := client.CoreV1().Pods("datafocal").UpdateStatus(ctx, oom, metav1.UpdateOptions{})
		require.NoError(t, err)
	})
	logFile := identity.New("node-1").Resolve("/var/log/containers/compactor-29887695-pwfbq_datafocal_compactor-"+strings.Repeat("ab", 32)+".log", segment.Record{})
	assert.ElementsMatch(t, []map[string]string{
		{"ts": at.Format(time.RFC3339Nano), "level": "warn", "message": "System OOM encountered", "reason": "SystemOOM",
			"kind": "Node", "object": "node-1", "namespace": "", "count": "1", "service": "kube-node", "host": "cluster-events"},
		{"ts": at.Add(time.Second).Format(time.RFC3339Nano), "level": "error", "message": "container compactor terminated: OOMKilled (exit 137), restarts 1",
			"reason": "OOMKilled", "exit_code": "137", "restart_count": "1", "pod": "compactor-29887695-pwfbq", "namespace": "datafocal",
			"container": "compactor", "service": "compactor", "host": "node-1"},
	}, got)
	assert.Equal(t, columns(logFile), map[string]string{"ts": "0001-01-01T00:00:00Z", "pod": got[1]["pod"], "namespace": got[1]["namespace"],
		"container": got[1]["container"], "service": got[1]["service"], "host": got[1]["host"]}, "filed as the container's log file is")

	got = run(1, func(ctx context.Context) {
		_, err := client.CoreV1().Events("datafocal").Create(ctx, event("ready", corev1.EventTypeNormal, "NodeReady", "Node is ready", at.Add(2*time.Second)), metav1.CreateOptions{})
		require.NoError(t, err)
	})
	assert.Equal(t, "Node is ready", got[0]["message"], "a restarted source sends only what it has not")
}

// columns is a record's time and each column's text, by name.
func columns(r segment.Record) map[string]string {
	m := map[string]string{"ts": r.Timestamp.UTC().Format(time.RFC3339Nano)}
	for _, c := range r.Columns {
		m[c.Name] = value.Format(c.Type, c.Value)
	}
	return m
}
