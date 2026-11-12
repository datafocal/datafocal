// Package identity names the producer of a record from what the record and the file it
// was read from already say, so a collector needs no per-source configuration.
package identity

import (
	"cmp"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/value"
)

// LogRoot is where a plain file's directory names its service.
const LogRoot = "/var/log"

// containerID names the container a record is about, in OpenTelemetry's spelling: its id,
// or where the runtime gives none, its name or cgroup path, as cAdvisor's samples do.
const containerID = "container.id"

// Resolver adds the service, host and any path columns to a record.
type Resolver interface {
	Resolve(path string, r segment.Record) segment.Record
}

type resolver struct {
	host       string
	containers *containers
}

// New returns a Resolver that names the host of records that do not name their own,
// os.Hostname for an empty host, and the pod of a container the node's kubelet runs.
func New(host string) Resolver {
	if host == "" {
		host, _ = os.Hostname() // a host it cannot name is left off the records
	}
	return resolver{host: host, containers: newContainers("/var/log/containers", "/var/log/pods", "/sys/fs/cgroup", "/var/lib/docker/containers")}
}

// containerLog is the kubelet's <pod>_<namespace>_<container>-<id>.log.
var containerLog = regexp.MustCompile(`^([^_]+)_([^_]+)_(.+)-([0-9a-f]{64})\.log$`)

// podSuffix is what a controller appends to its workload's name: a Deployment's
// -<replicaset hash>-<pod hash>, a StatefulSet's -<ordinal> or a DaemonSet's -<suffix>.
var podSuffix = regexp.MustCompile(`-(?:[bcdfghjklmnpqrstvwxz2456789]{6,10}-[bcdfghjklmnpqrstvwxz2456789]{5}|\d+|[bcdfghjklmnpqrstvwxz2456789]{5})$`)

// rotation is a rotated file's suffix, such as .1 or .2.gz.
var rotation = regexp.MustCompile(`(\.\d+)?(\.gz)?$`)

// journalCgroup is the journal's cgroup of the process that logged a line; journalContainer,
// journalContainerID and journalImage are, through Docker's journald driver, its container's
// name, id and image.
const (
	journalCgroup      = "_SYSTEMD_CGROUP"
	journalContainer   = "CONTAINER_NAME"
	journalContainerID = "CONTAINER_ID_FULL"
	journalImage       = "IMAGE_NAME"
)

// journalService names a journal line as a sampler's line about its cgroup is named: after
// what runs in its container, or the unit its cgroup is or nests in, so a desktop app's lines,
// which the journal files under user@1000.service, are the app's, as its resource samples are.
func (res resolver) journalService(r segment.Record) (string, bool) {
	if name, ok := lookup(r, journalContainer); ok && name != "" {
		return res.journalContainer(r, name), true
	}
	if cg, ok := lookup(r, journalCgroup); ok && cg != "" && cg != "/" {
		return cgroupService(cg), true
	}
	return "", false
}

// Resolve takes the service from the first of: the record's service.name, the container
// it names, a journal line's cgroup, its own service, the workload a container log's pod
// belongs to, and the path.
func (res resolver) Resolve(path string, r segment.Record) segment.Record {
	res.learnContainer(r)
	if id, ok := lookup(r, containerID); ok && !has(r, "service.name") {
		return res.withHost(res.contained(id, r))
	}
	if s, ok := res.journalService(r); ok && !has(r, "service.name") {
		r = set(r, segment.ColService, s)
	}
	service, named := lookup(r, segment.ColService)
	if !named {
		service, _ = lookup(r, "service.name")
	}
	if m := containerLog.FindStringSubmatch(filepath.Base(path)); m != nil {
		r.Columns = append(r.Columns, column(segment.ColPod, m[1]), column(segment.ColNamespace, m[2]), column(segment.ColContainer, m[3]))
		service = cmp.Or(service, PodService(m[1], res.host))
	} else if m := rootfs.FindStringSubmatch(path); m != nil {
		r = res.inPod(m[1], r)
	}
	if !named {
		r.Columns = append(r.Columns, column(segment.ColService, cmp.Or(service, fileService(path))))
	}
	return res.withHost(r)
}

// withHost names the node's host unless the record names its own: a line relayed from
// host-7 is about host-7, not the node whose collector read it.
func (res resolver) withHost(r segment.Record) segment.Record {
	if _, own := lookup(r, segment.ColHost); res.host != "" && !own {
		r.Columns = append(r.Columns, column(segment.ColHost, res.host))
	}
	return r
}

// journalContainer is what runs in the Docker container a journal line came from, as
// Docker's state says or, as learnContainer learnt, the image the line names.
func (res resolver) journalContainer(r segment.Record, name string) string {
	if p, ok := res.containers.lookup(name); ok && p.service != "" {
		return p.service
	}
	image, _ := lookup(r, journalImage)
	return dockerService(name, image, nil)
}

// learnContainer remembers what runs in the container a line Docker's journald driver wrote
// came from, its image, so the samples about the container, naming it alone, are named the
// same. A sampler's own lines teach its container too, as they carry its envelope.
func (res resolver) learnContainer(r segment.Record) {
	name, _ := lookup(r, journalContainer)
	image, _ := lookup(r, journalImage)
	if name == "" || image == "" {
		return
	}
	id, _ := lookup(r, journalContainerID)
	res.containers.learn(place{container: name, service: dockerService(name, image, nil)}, name, id)
}

// contained names a record about container id after it, not after the file or the process
// that wrote it: a sampler's line about a pod's container is the pod's, and about a Docker
// container, what runs in it, its name the container.
func (res resolver) contained(id string, r segment.Record) segment.Record {
	p, ok := res.containers.lookup(id)
	if !ok {
		return set(r, segment.ColService, containerService(id, res.host))
	}
	if p.pod == "" && p.service != "" {
		return set(set(r, segment.ColContainer, p.container), segment.ColService, p.service)
	}
	r = set(r, segment.ColPod, p.pod)
	r = set(r, segment.ColNamespace, p.namespace)
	if p.container != "" {
		r = set(r, segment.ColContainer, p.container)
	}
	return set(r, segment.ColService, PodService(p.pod, res.host))
}

// rootfs is a file read through the node's view of a running container's root, as the
// transcripts an agent's sandbox writes and no log holds.
var rootfs = regexp.MustCompile(`/([0-9a-f]{64})/rootfs/`)

// inPod adds the pod of container id to a record about its own files, leaving its service
// the files', as a pod's agent sessions are not the pod.
func (res resolver) inPod(id string, r segment.Record) segment.Record {
	p, ok := res.containers.lookup(id)
	if !ok {
		return r
	}
	r.Columns = append(r.Columns, column(segment.ColPod, p.pod), column(segment.ColNamespace, p.namespace))
	if p.container != "" {
		r.Columns = append(r.Columns, column(segment.ColContainer, p.container))
	}
	return r
}

// unit is a systemd unit's name less its type, an instance, a launcher's app- prefix and a
// counter: sshd.service, getty@tty1.service and app-gnome-firefox-4211.scope.
var unit = regexp.MustCompile(`^(?:app-)?(.+?)(?:@[^.]*)?(?:-\d+)?\.(?:service|scope|slice)$`)

// longID is a runtime's 64-hex container id, alone or in a unit such as docker-<id>.scope.
var longID = regexp.MustCompile(`[0-9a-f]{64}`)

// escaped is a byte systemd escapes in a unit's name, as \x2d for a "-" a name holds.
var escaped = regexp.MustCompile(`\\x([0-9a-f]{2})`)

// dbusActivated is the prefix of a unit D-Bus starts a service in, dbus-:1.2-<name>@0.service.
var dbusActivated = regexp.MustCompile(`^dbus-:\d+\.\d+-`)

// cgroupService is the unit a cgroup is or nests in: the one right under its deepest slice,
// so a terminal's tab scopes are the terminal's, its escapes undone, less its unit type, any
// instance, a launcher's app- prefix and a counter.
func cgroupService(id string) string {
	parts := strings.Split(strings.Trim(id, "/"), "/")
	at := len(parts) - 1
	for i := len(parts) - 1; i >= 0; i-- {
		if strings.HasSuffix(parts[i], ".slice") {
			at = min(i+1, len(parts)-1)
			break
		}
	}
	name := escaped.ReplaceAllStringFunc(parts[at], func(e string) string {
		b, err := strconv.ParseUint(e[2:], 16, 8)
		if err != nil {
			return e
		}
		return string(rune(b))
	})
	name = dbusActivated.ReplaceAllString(name, "")
	if m := unit.FindStringSubmatch(name); m != nil {
		return m[1]
	}
	return name
}

// containerService names a container no pod runs: the host for the root cgroup, a cgroup's
// unit, a runtime's short id, or the name a runtime gave it.
func containerService(id, host string) string {
	if id == "/" {
		return cmp.Or(host, id)
	}
	if long := longID.FindString(id); long != "" {
		return long[:12]
	}
	if !strings.HasPrefix(id, "/") {
		return id
	}
	return cgroupService(id)
}

// set gives r's column name the value s, in place of any it has.
func set(r segment.Record, name, s string) segment.Record {
	if i := slices.IndexFunc(r.Columns, func(c segment.Column) bool { return c.Name == name }); i >= 0 {
		r.Columns[i] = column(name, s)
		return r
	}
	r.Columns = append(r.Columns, column(name, s))
	return r
}

// PodService is the service of a pod on host: the workload it belongs to, or a static
// pod's name without its node's suffix.
func PodService(name, host string) string {
	if host != "" && strings.HasSuffix(name, "-"+host) {
		return strings.TrimSuffix(name, "-"+host)
	}
	if workload := podSuffix.ReplaceAllString(name, ""); workload != name {
		return workload
	}
	if i := strings.LastIndexByte(name, '-'); i >= 0 && isID(name[i+1:]) {
		return name[:i]
	}
	return name
}

// isID recognizes instance names without treating words made of hex letters as ids.
func isID(name string) bool {
	for _, part := range strings.FieldsFunc(name, func(r rune) bool { return r == '-' || r == '_' || r == '.' }) {
		if strings.Trim(part, "0123456789") == "" {
			return true
		}
		if len(part) >= 8 && strings.Trim(part, "0123456789abcdefABCDEF") == "" && strings.ContainsAny(part, "0123456789") {
			return true
		}
	}
	return false
}

func has(r segment.Record, name string) bool {
	_, ok := lookup(r, name)
	return ok
}

func lookup(r segment.Record, name string) (string, bool) {
	i := slices.IndexFunc(r.Columns, func(c segment.Column) bool { return c.Name == name })
	if i < 0 {
		return "", false
	}
	return value.Format(r.Columns[i].Type, r.Columns[i].Value), true
}

// fileService uses a log directory or file name, walking past instance directories
// when the file name is an id.
func fileService(path string) string {
	if rel, err := filepath.Rel(LogRoot, path); err == nil && !strings.HasPrefix(rel, "..") {
		if dir, _, ok := strings.Cut(rel, string(filepath.Separator)); ok {
			return dir
		}
	}
	name := rotation.ReplaceAllString(filepath.Base(path), "")
	name = strings.TrimSuffix(name, filepath.Ext(name))
	if !isID(name) {
		return strings.Trim(name, "-_.")
	}
	for dir := filepath.Dir(path); dir != filepath.Dir(dir); dir = filepath.Dir(dir) {
		if !isID(filepath.Base(dir)) && !isID(filepath.Base(filepath.Dir(dir))) {
			return strings.Trim(filepath.Base(dir), "-_.")
		}
	}
	return name
}

func column(name, s string) segment.Column {
	return segment.Column{Name: name, Type: value.TypeString, Value: value.Value{String: s}}
}
