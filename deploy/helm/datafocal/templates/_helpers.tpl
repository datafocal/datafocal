{{- define "datafocal.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default "datafocal" .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "datafocal.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ default "datafocal" .Values.nameOverride }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/* The labels selecting one component's pods; called with (list $ "component"). */}}
{{- define "datafocal.selector" -}}
{{- $root := index . 0 -}}
app.kubernetes.io/name: {{ default "datafocal" $root.Values.nameOverride }}
app.kubernetes.io/instance: {{ $root.Release.Name }}
app.kubernetes.io/component: {{ index . 1 }}
{{- end -}}

{{/* A DataFocal image; called with (list $ "collector"). */}}
{{- define "datafocal.image" -}}
{{- $root := index . 0 -}}
{{- printf "%s/datafocal-%s:%s" $root.Values.image.registry (index . 1) (default $root.Chart.AppVersion $root.Values.image.tag) -}}
{{- end -}}

{{/* "true" when the openshift profile applies, else empty. */}}
{{- define "datafocal.openshift" -}}
{{- if eq .Values.platform "openshift" -}}true
{{- else if and (eq .Values.platform "auto") (.Capabilities.APIVersions.Has "security.openshift.io/v1") -}}true
{{- else if not (has .Values.platform (list "auto" "kubernetes" "openshift")) -}}
{{- fail (printf "platform is %q, not auto, kubernetes or openshift" .Values.platform) -}}
{{- end -}}
{{- end -}}

{{- define "datafocal.s3Endpoint" -}}
{{- if .Values.store.endpoint -}}{{ .Values.store.endpoint }}
{{- else if .Values.minio.enabled -}}{{ include "datafocal.fullname" . }}-minio:9000
{{- else -}}{{ fail "store.endpoint is required unless minio.enabled" }}
{{- end -}}
{{- end -}}

{{- define "datafocal.s3Secret" -}}
{{- default (printf "%s-s3" (include "datafocal.fullname" .)) .Values.store.credentials.existingSecret -}}
{{- end -}}

{{/* The shared settings and the bucket's keys. */}}
{{- define "datafocal.envFrom" -}}
envFrom:
  - configMapRef:
      name: {{ include "datafocal.fullname" . }}
  # Optional, for a bucket reached by the node's or workload's own identity.
  - secretRef:
      name: {{ include "datafocal.s3Secret" . }}
      optional: true
{{- end -}}

{{/* A component's own variables; called with a map of name to value. */}}
{{- define "datafocal.extraEnv" -}}
{{- range $name, $value := . }}
- name: {{ $name }}
  value: {{ $value | quote }}
{{- end -}}
{{- end -}}

{{/* What every container runs with, the restricted Pod Security Standard's. */}}
{{- define "datafocal.containerSecurity" -}}
allowPrivilegeEscalation: false
capabilities:
  drop: [ALL]
seccompProfile:
  type: RuntimeDefault
{{- end -}}

{{/* A pod running as the image's non-root user, or the namespace's range on OpenShift; called with (list $ uid). */}}
{{- define "datafocal.nonrootPod" -}}
{{- $root := index . 0 -}}
securityContext:
  runAsNonRoot: true
  {{- if not (include "datafocal.openshift" $root) }}
  runAsUser: {{ index . 1 }}
  fsGroup: {{ index . 1 }}
  {{- end }}
{{- end -}}

{{- define "datafocal.pullSecrets" -}}
{{- with .Values.imagePullSecrets }}
imagePullSecrets:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}


{{/* A component's service account name; called with (list $ "collector" .Values.collector.serviceAccount). */}}
{{- define "datafocal.serviceAccountName" -}}
{{- $sa := index . 2 -}}
{{- if $sa.create -}}
{{- default (printf "%s-%s" (include "datafocal.fullname" (index . 0)) (index . 1)) $sa.name -}}
{{- else -}}
{{- default "default" $sa.name -}}
{{- end -}}
{{- end -}}

{{/* A component's ServiceAccount and a document separator, when it creates one; called as datafocal.serviceAccountName is. */}}
{{- define "datafocal.serviceAccount" -}}
{{- $sa := index . 2 -}}
{{- if $sa.create }}
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{ include "datafocal.serviceAccountName" . }}
  labels: {{- include "datafocal.labels" (index . 0) | nindent 4 }}
  {{- with $sa.annotations }}
  annotations: {{- toYaml . | nindent 4 }}
  {{- end }}
---
{{- end }}
{{- end -}}

{{/* The days a segment compacted into hour/ is kept: retention rounded up to whole days, or empty to keep it. */}}
{{- define "datafocal.retentionDays" -}}
{{- with .Values.retention -}}
{{- if not (regexMatch "^[0-9]+h$" .) -}}
{{- fail (printf "retention is %q: minio.lifecycle needs it in whole hours, such as 720h" .) -}}
{{- end -}}
{{- div (add (trimSuffix "h" . | int) 23) 24 -}}
{{- end -}}
{{- end -}}

{{/* The bundled MinIO's lifecycle configuration, as mc ilm import reads it. */}}
{{- define "datafocal.lifecycle" -}}
{{- $tenant := default "local" .Values.tenant -}}
{{- $l := .Values.minio.lifecycle -}}
{{- $rules := list -}}
{{- range $tier := list "raw" "window" -}}
{{- $rules = append $rules (dict "ID" $tier "Status" "Enabled" "Filter" (dict "Prefix" (printf "%s/%s/" $tenant $tier)) "Expiration" (dict "Days" (int $l.unmergedDays))) -}}
{{- end -}}
{{- with include "datafocal.retentionDays" . -}}
{{- $rules = append $rules (dict "ID" "hour" "Status" "Enabled" "Filter" (dict "Prefix" (printf "%s/hour/" $tenant)) "Expiration" (dict "Days" (int .))) -}}
{{- end -}}
{{- $rules = append $rules (dict "ID" "catalog-snapshot" "Status" "Enabled" "Filter" (dict "Prefix" (printf "%s/catalog/snapshot/" $tenant)) "Expiration" (dict "Days" (int $l.snapshotDays))) -}}
{{- dict "Rules" $rules | toJson -}}
{{- end -}}
