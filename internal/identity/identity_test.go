package identity

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/segment"
)

// etcd is a static pod's container, found only in the cgroup tree.
var etcd = strings.Repeat("e7", 32)

// node lays out a kubelet's log and cgroup directories under a temporary root: checkout's
// app container logs, its sandbox and the etcd static pod are only in the cgroup tree.
func node(t *testing.T, app, sandbox string) *containers {
	t.Helper()
	root := t.TempDir()
	uid := "6f1c2a3b-0d4e-4f5a-8b6c-7d8e9f0a1b2c"
	// A static pod's uid is its manifest's hash, with no dashes.
	static := strings.Repeat("c3", 16)
	pod := filepath.Join(root, "cgroup/kubelet.slice/kubelet-kubepods.slice/kubelet-kubepods-burstable.slice",
		"kubelet-kubepods-burstable-pod"+strings.ReplaceAll(uid, "-", "_")+".slice")
	for _, dir := range []string{
		filepath.Join(root, "containers"),
		filepath.Join(root, "pods", "shop_checkout-7d9f8b6c4-x2x9z_"+uid),
		filepath.Join(pod, "cri-containerd-"+app+".scope"),
		filepath.Join(pod, "cri-containerd-"+sandbox+".scope"),
		filepath.Join(root, "pods", "kube-system_etcd-node-1_"+static),
		filepath.Join(root, "cgroup/kubelet.slice/kubelet-kubepods.slice/kubelet-kubepods-burstable.slice",
			"kubelet-kubepods-burstable-pod"+static+".slice", "cri-containerd-"+etcd+".scope"),
		filepath.Join(root, "cgroup/system.slice/docker-"+strings.Repeat("ee", 32)+".scope"),
	} {
		require.NoError(t, os.MkdirAll(dir, 0o755))
	}
	log := filepath.Join(root, "containers", "checkout-7d9f8b6c4-x2x9z_shop_app-"+app+".log")
	require.NoError(t, os.WriteFile(log, nil, 0o644))
	// Docker's state: a container Docker named, which says only its image, and one of a
	// Compose project, whose label names its service.
	for id, config := range map[string]string{
		strings.Repeat("d1", 32): `{"Name": "/silly_chatelet", "Config": {"Image": "cgr.dev/chainguard/minio:latest@sha256:00", "Labels": {}}}`,
		strings.Repeat("d2", 32): `{"Name": "/shop-web-1", "Config": {"Image": "nginx:1.29", "Labels": {"com.docker.compose.service": "web"}}}`,
	} {
		dir := filepath.Join(root, "docker", id)
		require.NoError(t, os.MkdirAll(dir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "config.v2.json"), []byte(config), 0o644))
	}
	return newContainers(filepath.Join(root, "containers"), filepath.Join(root, "pods"), filepath.Join(root, "cgroup"), filepath.Join(root, "docker"))
}

func TestResolve(t *testing.T) {
	id := strings.Repeat("0a", 32)
	app, sandbox := strings.Repeat("ab", 32), strings.Repeat("cd", 32)
	sampler := "/var/log/containers/cadvisor-x2x9z_datafocal_cadvisor-" + id + ".log"
	for _, tc := range []struct {
		name, path string
		// before is a journal line the resolver reads first, as the journal interleaves a
		// container's own lines with the samples about it.
		before map[string]string
		attrs  map[string]string
		want   map[string]string
	}{
		{name: "a deployment's pod names its workload", path: "/var/log/containers/checkout-7d9f8b6c4-x2x9z_shop_app-" + id + ".log",
			want: map[string]string{"service": "checkout", "pod": "checkout-7d9f8b6c4-x2x9z", "namespace": "shop", "container": "app"}},
		{name: "a statefulset's pod", path: "/var/log/containers/postgres-0_db_pg-" + id + ".log",
			want: map[string]string{"service": "postgres", "pod": "postgres-0", "namespace": "db", "container": "pg"}},
		{name: "a daemonset's pod", path: "/var/log/containers/fluent-bit-x2x9z_logging_fluent-bit-" + id + ".log",
			want: map[string]string{"service": "fluent-bit", "pod": "fluent-bit-x2x9z", "namespace": "logging", "container": "fluent-bit"}},
		{name: "a bare pod", path: "/var/log/containers/sandbox-s-" + id[:32] + "_factory_sandbox-" + id + ".log",
			want: map[string]string{"service": "sandbox-s", "pod": "sandbox-s-" + id[:32], "namespace": "factory", "container": "sandbox"}},
		{name: "a static pod", path: "/var/log/containers/kube-apiserver-node-1_kube-system_api-" + id + ".log",
			want: map[string]string{"service": "kube-apiserver", "pod": "kube-apiserver-node-1", "namespace": "kube-system", "container": "api"}},
		{name: "a named pod", path: "/var/log/containers/web-frontend_shop_app-" + id + ".log",
			want: map[string]string{"service": "web-frontend", "pod": "web-frontend", "namespace": "shop", "container": "app"}},
		{name: "a file under a directory", path: "/var/log/nginx/access.log", want: map[string]string{"service": "nginx"}},
		{name: "a rotated file", path: "/var/log/syslog.1", want: map[string]string{"service": "syslog"}},
		{name: "a file elsewhere", path: "/srv/app/worker.log.2.gz", want: map[string]string{"service": "worker"}},
		{name: "the record's own service wins", path: "/var/log/nginx/access.log", attrs: map[string]string{"service": "billing"},
			want: map[string]string{"service": "billing"}},
		{name: "the record's own host wins", path: "/var/log/nginx/access.log", attrs: map[string]string{"host": "host-7"},
			want: map[string]string{"service": "nginx", "host": "host-7"}},
		{name: "service.name names the service", path: "/var/log/nginx/access.log", attrs: map[string]string{"service.name": "billing"},
			want: map[string]string{"service": "billing", "service.name": "billing"}},
		{name: "a sample about a pod's container is the pod's, not the sampler's", path: sampler, attrs: map[string]string{"container.id": app},
			want: map[string]string{"service": "checkout", "pod": "checkout-7d9f8b6c4-x2x9z", "namespace": "shop", "container": "app", "container.id": app}},
		{name: "a pod's sandbox, which logs nothing, is found in the cgroup tree", path: sampler, attrs: map[string]string{"container.id": sandbox},
			want: map[string]string{"service": "checkout", "pod": "checkout-7d9f8b6c4-x2x9z", "namespace": "shop", "container.id": sandbox}},
		{name: "a static pod's container, whose uid is a hash", path: sampler, attrs: map[string]string{"container.id": etcd},
			want: map[string]string{"service": "etcd", "pod": "etcd-node-1", "namespace": "kube-system", "container.id": etcd}},
		{name: "a container no pod runs is its short id", path: sampler, attrs: map[string]string{"container.id": strings.Repeat("ee", 32)},
			want: map[string]string{"service": "eeeeeeeeeeee", "container.id": strings.Repeat("ee", 32)}},
		{name: "a container's name is its service", path: sampler, attrs: map[string]string{"container.id": "postgres"},
			want: map[string]string{"service": "postgres", "container.id": "postgres"}},
		{name: "a cgroup is its unit, over the journal's service of the sampler", path: "journalctl",
			attrs: map[string]string{"service": "cadvisor", "container.id": "/system.slice/getty@tty1.service"},
			want:  map[string]string{"service": "getty", "container.id": "/system.slice/getty@tty1.service"}},
		{name: "a desktop app's scope is the app", path: "journalctl",
			attrs: map[string]string{"container.id": "/user.slice/user-1000.slice/user@1000.service/app.slice/app-gnome-firefox-4211.scope"},
			want:  map[string]string{"service": "gnome-firefox", "container.id": "/user.slice/user-1000.slice/user@1000.service/app.slice/app-gnome-firefox-4211.scope"}},
		{name: "a scope nested in an app's is the app's, as a terminal's tab", path: "journalctl",
			attrs: map[string]string{"container.id": "/user.slice/user-1000.slice/user@1000.service/app.slice/app-org.kde.konsole-4946.scope/tab(120464).scope"},
			want:  map[string]string{"service": "org.kde.konsole", "container.id": "/user.slice/user-1000.slice/user@1000.service/app.slice/app-org.kde.konsole-4946.scope/tab(120464).scope"}},
		{name: "a unit's escapes are undone", path: "journalctl",
			attrs: map[string]string{"container.id": `/user.slice/user-1000.slice/user@1000.service/app.slice/app-google\x2dchrome@7e1e6d19.service`},
			want:  map[string]string{"service": "google-chrome", "container.id": `/user.slice/user-1000.slice/user@1000.service/app.slice/app-google\x2dchrome@7e1e6d19.service`}},
		{name: "a service D-Bus started is the service", path: "journalctl",
			attrs: map[string]string{"container.id": "/user.slice/user-1000.slice/user@1000.service/app.slice/dbus-:1.2-org.kde.kdeconnect@0.service"},
			want:  map[string]string{"service": "org.kde.kdeconnect", "container.id": "/user.slice/user-1000.slice/user@1000.service/app.slice/dbus-:1.2-org.kde.kdeconnect@0.service"}},
		{name: "a journal line is the unit its cgroup nests in, as the samples about it are", path: "journalctl",
			attrs: map[string]string{"service": "user@1000", "_SYSTEMD_CGROUP": "/user.slice/user-1000.slice/user@1000.service/app.slice/app-slack-60114.scope"},
			want:  map[string]string{"service": "slack", "_SYSTEMD_CGROUP": "/user.slice/user-1000.slice/user@1000.service/app.slice/app-slack-60114.scope"}},
		{name: "a container's journal line is the container's", path: "journalctl",
			attrs: map[string]string{"service": "docker", "_SYSTEMD_CGROUP": "/system.slice/docker.service", "CONTAINER_NAME": "datafocal-kind-minio"},
			want:  map[string]string{"service": "datafocal-kind-minio", "_SYSTEMD_CGROUP": "/system.slice/docker.service", "CONTAINER_NAME": "datafocal-kind-minio"}},
		{name: "a file in a pod's container is the pod's, its service the file's",
			path: "/run/containerd/io.containerd.runtime.v2.task/k8s.io/" + app + "/rootfs/home/agent/.claude/projects/-workspace/842e33af-9917-f20e-e46e-5bec950ddc23.jsonl",
			want: map[string]string{"service": "workspace", "pod": "checkout-7d9f8b6c4-x2x9z", "namespace": "shop", "container": "app"}},
		{name: "an OpenTelemetry service.name wins over the container it names", path: "/var/log/app.log",
			attrs: map[string]string{"service.name": "billing", "container.id": strings.Repeat("ee", 32)},
			want:  map[string]string{"service": "billing", "service.name": "billing", "container.id": strings.Repeat("ee", 32)}},
		{name: "a Docker container is what runs in it, its image, as Docker's state says", path: "journalctl",
			attrs: map[string]string{"container.id": "silly_chatelet"},
			want:  map[string]string{"service": "minio", "container": "silly_chatelet", "container.id": "silly_chatelet"}},
		{name: "a Compose project's container is its service", path: "journalctl",
			attrs: map[string]string{"container.id": "shop-web-1"},
			want:  map[string]string{"service": "web", "container": "shop-web-1", "container.id": "shop-web-1"}},
		{name: "a container's journal line is its image, as the journald driver names it", path: "journalctl",
			attrs: map[string]string{"service": "docker", "CONTAINER_NAME": "datafocal-kind-minio", "IMAGE_NAME": "alpine/socat:1.8.0.3"},
			want:  map[string]string{"service": "socat", "CONTAINER_NAME": "datafocal-kind-minio", "IMAGE_NAME": "alpine/socat:1.8.0.3"}},
		{name: "a sample about a container the journal named is its image too", path: "journalctl",
			before: map[string]string{"CONTAINER_NAME": "datafocal-kind-minio", "IMAGE_NAME": "alpine/socat:1.8.0.3"},
			attrs:  map[string]string{"container.id": "datafocal-kind-minio"},
			want:   map[string]string{"service": "socat", "container": "datafocal-kind-minio", "container.id": "datafocal-kind-minio"}},
		{name: "a sampler's own lines teach its container, as the journald driver names it", path: "journalctl",
			before: map[string]string{"container.id": "silly_chatelet", "CONTAINER_NAME": "datafocal-cadvisor", "IMAGE_NAME": "ghcr.io/google/cadvisor:0.60.6"},
			attrs:  map[string]string{"container.id": "datafocal-cadvisor"},
			want:   map[string]string{"service": "cadvisor", "container": "datafocal-cadvisor", "container.id": "datafocal-cadvisor"}},
		{name: "the root cgroup is the host", path: "journalctl", attrs: map[string]string{"container.id": "/"},
			want: map[string]string{"service": "node-1", "container.id": "/"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := resolver{host: "node-1", containers: node(t, app, sandbox)}
			if tc.before != nil {
				res.Resolve("journalctl", segment.LogRecord(time.Time{}, "", "", tc.before))
			}
			r := res.Resolve(tc.path, segment.LogRecord(time.Time{}, "", "", tc.attrs))
			got := map[string]string{}
			for _, c := range r.Columns {
				got[c.Name] = c.Value.String
			}
			if _, own := tc.want["host"]; !own {
				tc.want["host"] = "node-1"
			}
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestPodServiceWithoutHost(t *testing.T) {
	assert.Equal(t, "kube-apiserver-node", PodService("kube-apiserver-node-1", ""))
	assert.Equal(t, "kube-apiserver-node-name", PodService("kube-apiserver-node-name", ""))
}

func TestFileService(t *testing.T) {
	id := "842e33af-9917-f20e-e46e-5bec950ddc23"
	for _, tc := range []struct{ path, want string }{
		{"/run/containerd/k8s.io/" + strings.Repeat("0a", 32) + "/rootfs/home/agent/.claude/projects/-workspace/" + id + ".jsonl", "workspace"},
		{"/home/agent/.claude/projects/-workspace/" + id + "/subagents/agent-0123456789abcdef.jsonl", "workspace"},
		{"/home/agent/.codex/sessions/2026/10/20/rollout-2026-10-20T10-11-12-" + id + ".jsonl", "sessions"},
		{"/srv/app/worker.txt.2.gz", "worker"},
		{"/var/log/worker.jsonl", "worker"},
		{"/" + id + ".jsonl", id},
		{"/srv/-worker_.jsonl", "worker"},
	} {
		t.Run(tc.path, func(t *testing.T) { assert.Equal(t, tc.want, fileService(tc.path)) })
	}
}

func TestIsID(t *testing.T) {
	for _, name := range []string{"123", "2026", "10", "20", "agent-0123456789abcdef", "842e33af-9917-f20e-e46e-5bec950ddc23", strings.Repeat("0a", 32)} {
		assert.True(t, isID(name), name)
	}
	for _, name := range []string{"workspace", "s3-proxy", "k8s.io", "abcdefabcdef", "abc123", "web-frontend"} {
		assert.False(t, isID(name), name)
	}
}
