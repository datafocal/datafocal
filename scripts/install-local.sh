#!/usr/bin/env bash
# install-local.sh standalone|kind|uninstall: run DataFocal on this machine as systemd user
# services from deploy/local, with the binaries in ~/.local/bin. The collector follows the
# journal and the coding agents' transcripts (deploy/local/collector.json), and a cAdvisor
# container samples the host and each docker container into the journal.
#   standalone  the collector, querier and compactor over a MinIO container on localhost:9000.
#   kind        the collector alone, writing to the kind cluster's MinIO, relayed to
#               localhost:9000, so the cluster's one compactor and querier serve both.
#   uninstall   stops and removes them, keeping the collected data and saved offsets.
# CLAUDE_QUOTA=1 also sets Claude Code's status line to one that logs the account's quota.
set -euo pipefail

mode=${1:?usage: install-local.sh standalone|kind|uninstall}
repo=$(cd "$(dirname "$0")/.." && pwd)
bin=$HOME/.local/bin
conf=$HOME/.config/datafocal
state=$HOME/.local/state/datafocal
units=$HOME/.config/systemd/user
backend=(datafocal-querier datafocal-compactor)
all=(datafocal-collector "${backend[@]}")
claude_settings=$HOME/.claude/settings.json
statusline=$HOME/.claude/datafocal-statusline.sh

# The MinIO keys come from the env file the services read.
set -a
# shellcheck source=SCRIPTDIR/../deploy/local/datafocal.env
. "$repo/deploy/local/datafocal.env"
set +a

install_services() {
	mkdir -p "$bin" "$conf/definitions" "$state" "$units"
	for name in "$@"; do
		(cd "$repo" && CGO_ENABLED=0 "${GO:-go}" build -trimpath -o "$bin/$name" "./cmd/${name#datafocal-}")
		cp "$repo/deploy/local/$name.service" "$units/"
	done
	sed "s|\$HOME|$HOME|g" "$repo/deploy/local/datafocal.env" >"$conf/datafocal.env"
	sed -e "s|@STATE@|$state|" -e "s|@HOME@|$HOME|g" "$repo/deploy/local/collector.json" >"$conf/collector.json"
	systemctl --user daemon-reload
	systemctl --user enable "${@/%/.service}"
	systemctl --user restart "${@/%/.service}"
}

# cadvisor samples the host, each docker container and each systemd unit every 15s, so a
# desktop app or a system service shows beside the containers, writing to the journal through
# docker's journald driver, which the collector follows. resource_usage sums only the cgroups
# holding no other, so a slice and its units are not counted twice. Recreated on each install,
# so a change to its flags takes.
cadvisor() {
	docker rm -f datafocal-cadvisor >/dev/null 2>&1 || true
	docker run -d --name datafocal-cadvisor --restart unless-stopped \
		--log-driver=journald --log-opt tag=cadvisor --privileged --device /dev/kmsg \
		-v /:/rootfs:ro -v /var/run:/var/run:ro -v /sys:/sys:ro -v /var/lib/docker/:/var/lib/docker:ro -v /dev/disk/:/dev/disk:ro \
		ghcr.io/google/cadvisor:0.60.6 --storage_driver=stdout \
		--housekeeping_interval=15s --allow_dynamic_housekeeping=false \
		--disable_metrics=advtcp,cpu_topology,cpuset,diskIO,hugetlb,memory_numa,percpu,perf_event,process,referenced_memory,resctrl,sched,tcp,udp
}

# The status line logs a quota reading each minute while a session is open, even between
# messages (deploy/local/claude-statusline.sh). Only with CLAUDE_QUOTA=1; one the user set
# is kept, and named.
claude_quota() {
	[ "${CLAUDE_QUOTA:-}" = 1 ] || return 0
	mkdir -p "$HOME/.claude"
	install -m 0755 "$repo/deploy/local/claude-statusline.sh" "$statusline"
	[ -s "$claude_settings" ] || echo '{}' >"$claude_settings"
	local current
	current=$(jq -r '.statusLine.command // ""' "$claude_settings")
	if [ -n "$current" ] && [ "$current" != "$statusline" ]; then
		echo "kept the Claude Code status line $current; set it to $statusline to log the quota" >&2
		return
	fi
	jq --arg cmd "$statusline" '.statusLine = {type: "command", command: $cmd, refreshInterval: 60}' \
		"$claude_settings" >"$claude_settings.tmp" && mv "$claude_settings.tmp" "$claude_settings"
}

# Each mode serves localhost:9000 from its own container, so taking one removes the other.
standalone() {
	docker rm -f datafocal-kind-minio >/dev/null 2>&1 || true
	docker inspect datafocal-minio >/dev/null 2>&1 || docker run -d --name datafocal-minio --restart unless-stopped \
		-p 127.0.0.1:9000:9000 -v datafocal-minio:/data \
		-e MINIO_ROOT_USER="$AWS_ACCESS_KEY_ID" -e MINIO_ROOT_PASSWORD="$AWS_SECRET_ACCESS_KEY" \
		cgr.dev/chainguard/minio:latest server /data
	docker run --rm --network container:datafocal-minio --entrypoint sh \
		-e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e DATAFOCAL_S3_BUCKET \
		cgr.dev/chainguard/minio-client:latest-dev -c \
		'until mc alias set local http://127.0.0.1:9000 "$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY" >/dev/null 2>&1; do sleep 1; done
		mc mb --ignore-existing "local/$DATAFOCAL_S3_BUCKET"'
	cadvisor
	claude_quota
	install_services "${all[@]}"
	echo "datafocal is running: the UI at http://localhost:8080/ui/, MCP at http://localhost:8080/mcp"
}

# make install-local-kind deploys the cluster first, its MinIO on node port 30900
# (deploy/k8s/kind/minio.yaml); a container on kind's network relays the port, recreated so
# it follows KIND_CLUSTER.
kind_collector() {
	local cluster=${KIND_CLUSTER:-datafocal}
	local node=$cluster-control-plane
	docker inspect "$node" >/dev/null 2>&1 || { echo "no kind node $node: run make deploy-kind, or set KIND_CLUSTER" >&2; exit 1; }
	systemctl --user disable --now "${backend[@]/%/.service}" 2>/dev/null || true
	docker rm -f datafocal-minio datafocal-kind-minio >/dev/null 2>&1 || true
	docker run -d --name datafocal-kind-minio --restart unless-stopped \
		--network kind -p 127.0.0.1:9000:9000 alpine/socat:1.8.0.3 \
		tcp-listen:9000,fork,reuseaddr "tcp-connect:$node:30900"
	timeout 60 sh -c 'until curl -sf http://127.0.0.1:9000/minio/health/ready; do sleep 1; done' ||
		{ echo "kind's MinIO is not answering on localhost:9000" >&2; exit 1; }
	cadvisor
	claude_quota
	install_services datafocal-collector
	echo "the collector is writing to kind's MinIO; reach its querier with:"
	echo "  kubectl --context kind-${KIND_CLUSTER:-datafocal} -n datafocal port-forward svc/querier 8080:8080"
}

uninstall() {
	systemctl --user disable --now "${all[@]/%/.service}" 2>/dev/null || true
	for name in "${all[@]}"; do rm -f "$units/$name.service" "$bin/$name"; done
	systemctl --user daemon-reload
	docker rm -f datafocal-minio datafocal-kind-minio datafocal-cadvisor >/dev/null 2>&1 || true
	if [ -f "$claude_settings" ] && [ "$(jq -r '.statusLine.command // ""' "$claude_settings")" = "$statusline" ]; then
		jq 'del(.statusLine)' "$claude_settings" >"$claude_settings.tmp" && mv "$claude_settings.tmp" "$claude_settings"
	fi
	rm -f "$conf/datafocal.env" "$conf/collector.json" "$statusline"
	echo "removed; the data is kept in the datafocal-minio volume, the offsets in $state and the definitions in $conf/definitions"
}

case $mode in
standalone) standalone ;;
kind) kind_collector ;;
uninstall) uninstall ;;
*) echo "usage: install-local.sh standalone|kind|uninstall" >&2; exit 2 ;;
esac
