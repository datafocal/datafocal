package identity

import (
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

// place is where a container runs: its pod, the pod's namespace, and its name in the pod,
// empty for a pod's sandbox; or, for a container no pod runs, service is what runs in it and
// container its runtime's name.
type place struct{ pod, namespace, container, service string }

// containers maps a container id to its place, from the kubelet's log names and, for the
// containers that log nothing such as a pod's sandbox, the cgroup tree; and a Docker
// container's id and name to what runs in it, from Docker's state where it can be read and
// from the journal lines Docker's journald driver writes, which name the image.
type containers struct {
	logs, pods, cgroups, docker string
	mu                          sync.Mutex
	known                       map[string]place
	read                        time.Time
	// configs are the Docker containers read, by id, as a container's config is read once.
	configs map[string]place
	// learned are the Docker containers the journal named, by id and name.
	learned map[string]place
}

// maxLearned bounds the containers learned from the journal, as a test suite starts one a run.
const maxLearned = 4096

// reread bounds how often an id the node does not know reads its directories again, as a
// sampler names each container every few seconds and a sandbox never gets a log.
const reread = 15 * time.Second

func newContainers(logs, pods, cgroups, docker string) *containers {
	return &containers{logs: logs, pods: pods, cgroups: cgroups, docker: docker,
		configs: map[string]place{}, learned: map[string]place{}}
}

// learn remembers what runs in a Docker container by its keys, its id and name, as a journal
// line names it, for the samples about it that name only one of them. Docker's state, where
// it was read, says so already.
func (c *containers) learn(p place, keys ...string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.known[keys[0]]; ok {
		return
	}
	if c.learned[keys[0]] == p {
		return
	}
	if len(c.learned) >= maxLearned {
		c.learned = map[string]place{}
	}
	for _, k := range keys {
		if k != "" {
			c.learned[k] = p
		}
	}
}

func (c *containers) lookup(id string) (place, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if p, ok := c.known[id]; ok {
		return p, true
	}
	if p, ok := c.learned[id]; ok {
		return p, true
	}
	if time.Since(c.read) < reread {
		return place{}, false
	}
	c.read = time.Now()
	c.known = c.scan()
	p, ok := c.known[id]
	return p, ok
}

// podLog is the kubelet's <namespace>_<pod>_<uid> directory of a pod's logs, a static pod's
// uid being its manifest's 32-hex hash.
var podLog = regexp.MustCompile(`^([^_]+)_([^_]+)_([0-9a-f-]{36}|[0-9a-f]{32})$`)

// scan reads the node's containers; a directory it cannot read, as off a Kubernetes node,
// adds none.
func (c *containers) scan() map[string]place {
	known := map[string]place{}
	entries, _ := os.ReadDir(c.logs)
	for _, e := range entries {
		if m := containerLog.FindStringSubmatch(e.Name()); m != nil {
			known[m[4]] = place{pod: m[1], namespace: m[2], container: m[3]}
		}
	}
	pods := map[string]place{}
	entries, _ = os.ReadDir(c.pods)
	for _, e := range entries {
		if m := podLog.FindStringSubmatch(e.Name()); m != nil {
			pods[m[3]] = place{pod: m[2], namespace: m[1]}
		}
	}
	for id, uid := range c.podCgroups() {
		if p, ok := pods[uid]; ok && known[id] == (place{}) {
			known[id] = p
		}
	}
	c.dockerContainers(known)
	return known
}

// dockerConfig is what Docker's state says of a container: its name, image and labels.
type dockerConfig struct {
	Name   string `json:"Name"`
	Config struct {
		Image  string            `json:"Image"`
		Labels map[string]string `json:"Labels"`
	} `json:"Config"`
}

// dockerContainers adds each container in Docker's state, <id>/config.v2.json, by its id and
// its name, reading a container's config once. Docker's state is root's, so a collector that
// cannot read it adds none.
func (c *containers) dockerContainers(known map[string]place) {
	entries, _ := os.ReadDir(c.docker)
	for _, e := range entries {
		id := e.Name()
		p, ok := c.configs[id]
		if !ok {
			b, err := os.ReadFile(filepath.Join(c.docker, id, "config.v2.json"))
			var cfg dockerConfig
			if err != nil || json.Unmarshal(b, &cfg) != nil {
				continue
			}
			name := strings.TrimPrefix(cfg.Name, "/")
			p = place{container: name, service: dockerService(name, cfg.Config.Image, cfg.Config.Labels)}
			c.configs[id] = p
		}
		known[id], known[p.container] = p, p
	}
}

// composeService is the label Docker Compose gives a container: its service in the project.
const composeService = "com.docker.compose.service"

// dockerService is what runs in a Docker container: its Compose service, or its image's name
// less its registry, tag and digest, cgr.dev/chainguard/minio:latest as minio, or without
// either its name.
func dockerService(name, image string, labels map[string]string) string {
	if s := labels[composeService]; s != "" {
		return s
	}
	image, _, _ = strings.Cut(image, "@")
	base := filepath.Base(image)
	if repo, _, ok := strings.Cut(base, ":"); ok {
		base = repo
	}
	if image == "" || strings.HasPrefix(image, "sha256:") {
		return name
	}
	return base
}

// podCgroup is a pod's cgroup, kubepods-burstable-pod<uid>.slice under systemd or
// pod<uid> under cgroupfs, and containerCgroup a container's in it, such as
// cri-containerd-<id>.scope.
var (
	podCgroup       = regexp.MustCompile(`pod([0-9a-f]{8}[-_][0-9a-f]{4}[-_][0-9a-f]{4}[-_][0-9a-f]{4}[-_][0-9a-f]{12}|[0-9a-f]{32})(?:\.slice)?$`)
	containerCgroup = regexp.MustCompile(`(?:^|-)([0-9a-f]{64})(?:\.scope)?$`)
)

// podCgroups maps each container id in the kubelet's cgroups, kubepods.slice or kind's
// kubelet.slice, to its pod's uid.
func (c *containers) podCgroups() map[string]string {
	uids := map[string]string{}
	_ = filepath.WalkDir(c.cgroups, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return fs.SkipDir // an unreadable cgroup names no container
		}
		if !d.IsDir() {
			return nil
		}
		if filepath.Dir(path) == c.cgroups && !strings.Contains(d.Name(), "kube") {
			return fs.SkipDir
		}
		m := containerCgroup.FindStringSubmatch(d.Name())
		if m == nil {
			return nil
		}
		if pod := podCgroup.FindStringSubmatch(filepath.Base(filepath.Dir(path))); pod != nil {
			uids[m[1]] = strings.ReplaceAll(pod[1], "_", "-")
		}
		return fs.SkipDir
	})
	return uids
}
