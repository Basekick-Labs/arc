{{/* vim: set filetype=mustache: */}}

{{/*
Expand the name of the chart.
*/}}
{{- define "arc-enterprise.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "arc-enterprise.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "arc-enterprise.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "arc-enterprise.labels" -}}
helm.sh/chart: {{ include "arc-enterprise.chart" . }}
{{ include "arc-enterprise.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "arc-enterprise.selectorLabels" -}}
app.kubernetes.io/name: {{ include "arc-enterprise.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Role-specific selector labels (for writer / reader / compactor).
Call as: include "arc-enterprise.roleSelectorLabels" (dict "ctx" . "role" "writer")
*/}}
{{- define "arc-enterprise.roleSelectorLabels" -}}
{{- include "arc-enterprise.selectorLabels" .ctx }}
app.kubernetes.io/component: {{ .role }}
{{- end }}

{{- define "arc-enterprise.roleLabels" -}}
{{- include "arc-enterprise.labels" .ctx }}
app.kubernetes.io/component: {{ .role }}
{{- end }}

{{/*
Image reference.
*/}}
{{- define "arc-enterprise.image" -}}
{{- printf "%s:%s" .Values.image.repository (.Values.image.tag | default .Chart.AppVersion) -}}
{{- end }}

{{/*
Names for derived resources.
*/}}
{{- define "arc-enterprise.writerName" -}}
{{- printf "%s-writer" (include "arc-enterprise.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "arc-enterprise.readerName" -}}
{{- printf "%s-reader" (include "arc-enterprise.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "arc-enterprise.compactorName" -}}
{{- printf "%s-compactor" (include "arc-enterprise.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "arc-enterprise.writerHeadlessName" -}}
{{- printf "%s-headless" (include "arc-enterprise.writerName" .) | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "arc-enterprise.readerHeadlessName" -}}
{{- printf "%s-headless" (include "arc-enterprise.readerName" .) | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "arc-enterprise.compactorHeadlessName" -}}
{{- printf "%s-headless" (include "arc-enterprise.compactorName" .) | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "arc-enterprise.minioName" -}}
{{- printf "%s-minio" (include "arc-enterprise.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "arc-enterprise.licenseSecretName" -}}
{{- if .Values.license.existingSecret -}}
{{ .Values.license.existingSecret }}
{{- else -}}
{{ printf "%s-license" (include "arc-enterprise.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end -}}
{{- end }}

{{- define "arc-enterprise.sharedSecretName" -}}
{{- if .Values.cluster.sharedSecret.existingSecret -}}
{{ .Values.cluster.sharedSecret.existingSecret }}
{{- else -}}
{{ printf "%s-shared-secret" (include "arc-enterprise.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end -}}
{{- end }}

{{- define "arc-enterprise.bootstrapTokenSecretName" -}}
{{- if .Values.auth.bootstrapToken.existingSecret -}}
{{ .Values.auth.bootstrapToken.existingSecret }}
{{- else -}}
{{ printf "%s-bootstrap-token" (include "arc-enterprise.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end -}}
{{- end }}

{{- define "arc-enterprise.minioSecretName" -}}
{{- if .Values.minio.credentials.existingSecret -}}
{{ .Values.minio.credentials.existingSecret }}
{{- else -}}
{{ printf "%s-minio-creds" (include "arc-enterprise.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end -}}
{{- end }}

{{- define "arc-enterprise.objectStorageSecretName" -}}
{{- if .Values.storage.shared.credentials.existingSecret -}}
{{ .Values.storage.shared.credentials.existingSecret }}
{{- else if and (eq .Values.storage.mode "shared") (not .Values.storage.shared.external) -}}
{{- include "arc-enterprise.minioSecretName" . -}}
{{- else -}}
{{ printf "%s-object-storage" (include "arc-enterprise.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end -}}
{{- end }}

{{/*
Whether to render the bundled MinIO resources.
Only true when storage.mode=shared AND storage.shared.external=false AND minio.enabled=true.
Always compare with `eq (include ...) "true"` — Helm returns the string "true".
*/}}
{{- define "arc-enterprise.minioBundled" -}}
{{- and (eq .Values.storage.mode "shared") (not .Values.storage.shared.external) .Values.minio.enabled -}}
{{- end }}

{{/*
True when the object-storage credentials come from the chart-managed MinIO
Secret (root-user / root-password keys), false when they come from an
operator-supplied external-S3 secret (access-key / secret-key keys).
*/}}
{{- define "arc-enterprise.useMinioCredKeys" -}}
{{- if .Values.storage.shared.credentials.existingSecret -}}
false
{{- else if eq (include "arc-enterprise.minioBundled" .) "true" -}}
true
{{- else -}}
false
{{- end -}}
{{- end }}

{{/*
S3 endpoint — auto-populate when using bundled MinIO, otherwise use whatever the operator configured.
*/}}
{{- define "arc-enterprise.s3Endpoint" -}}
{{- if eq (include "arc-enterprise.minioBundled" .) "true" -}}
{{ printf "http://%s:9000" (include "arc-enterprise.minioName" .) }}
{{- else -}}
{{ .Values.storage.shared.endpoint }}
{{- end -}}
{{- end }}

{{/*
Chart-wide validation is implemented in two layers:

  1. values.schema.json — types, enums, ranges, and shape checks. Helm
     validates this automatically before any template renders (best UX
     for typos and wrong types).

  2. _validation.tpl — cross-field rules that depend on lookup() or the
     combination of multiple values (e.g. "tls.existingSecret required
     when tls.enabled=true"). Invoked from writer-statefulset.yaml via
     {{- include "arc-enterprise.validate" . -}}.

Keep "arc-enterprise.validate" (and friends) in _validation.tpl — do not
add new fail() blocks in this file.
*/}}

{{/*
Role-specific scheduling fields with fallback to the global top-level values.
Call as: include "arc-enterprise.roleScheduling" (dict "ctx" . "role" "writer")
*/}}
{{- define "arc-enterprise.roleScheduling" -}}
{{- $role := index .ctx.Values .role -}}
{{- $ns := or $role.nodeSelector .ctx.Values.nodeSelector -}}
{{- $tol := or $role.tolerations .ctx.Values.tolerations -}}
{{- $aff := or $role.affinity .ctx.Values.affinity -}}
{{- with $ns }}
nodeSelector:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with $tol }}
tolerations:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with $aff }}
affinity:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end }}

{{/*
Common Arc cluster env vars — shared across writer/reader/compactor.
Writer-only vars (Raft bind, WAL) are emitted by the writer template instead.
Call as: include "arc-enterprise.commonClusterEnv" .
*/}}
{{- define "arc-enterprise.commonClusterEnv" -}}
- name: POD_NAME
  valueFrom:
    fieldRef:
      fieldPath: metadata.name
- name: ARC_CLUSTER_ENABLED
  value: "true"
- name: ARC_CLUSTER_NODE_ID
  valueFrom:
    fieldRef:
      fieldPath: metadata.name
- name: ARC_CLUSTER_CLUSTER_NAME
  value: {{ .Values.cluster.name | quote }}
- name: ARC_CLUSTER_COORDINATOR_ADDR
  value: ":9100"
- name: ARC_CLUSTER_SHARED_SECRET
  valueFrom:
    secretKeyRef:
      name: {{ include "arc-enterprise.sharedSecretName" . }}
      key: shared-secret
- name: ARC_LICENSE_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "arc-enterprise.licenseSecretName" . }}
      key: license-key
{{- if or .Values.auth.bootstrapToken.existingSecret .Values.auth.bootstrapToken.value }}
- name: ARC_AUTH_BOOTSTRAP_TOKEN
  valueFrom:
    secretKeyRef:
      name: {{ include "arc-enterprise.bootstrapTokenSecretName" . }}
      key: bootstrap-token
{{- end }}
{{- if .Values.cluster.tls.enabled }}
- name: ARC_CLUSTER_TLS_ENABLED
  value: "true"
- name: ARC_CLUSTER_TLS_CERT_FILE
  value: "/etc/arc/tls/tls.crt"
- name: ARC_CLUSTER_TLS_KEY_FILE
  value: "/etc/arc/tls/tls.key"
- name: ARC_CLUSTER_TLS_CA_FILE
  value: "/etc/arc/tls/ca.crt"
{{- end }}
{{- if .Values.cluster.failover.enabled }}
- name: ARC_CLUSTER_FAILOVER_ENABLED
  value: "true"
{{- end }}
{{- if not .Values.telemetry.enabled }}
- name: ARC_TELEMETRY_ENABLED
  value: "false"
{{- end }}
{{- end }}

{{/*
Raft-specific env vars — only for writers (they run Raft consensus).
*/}}
{{- define "arc-enterprise.writerRaftEnv" -}}
- name: ARC_CLUSTER_RAFT_BIND_ADDR
  value: ":9200"
{{- end }}

{{/*
WAL env vars for writers. Enabled by default for Enterprise durability.
*/}}
{{- define "arc-enterprise.writerWalEnv" -}}
{{- if .Values.writer.wal.enabled }}
- name: ARC_WAL_ENABLED
  value: "true"
- name: ARC_WAL_DIRECTORY
  value: "/app/data/wal"
- name: ARC_WAL_SYNC_MODE
  value: {{ .Values.writer.wal.syncMode | quote }}
{{- end }}
{{- end }}

{{/*
Storage env vars — depends on storage.mode.
*/}}
{{- define "arc-enterprise.storageEnv" -}}
{{- if eq .Values.storage.mode "shared" }}
- name: ARC_STORAGE_BACKEND
  value: "s3"
- name: ARC_STORAGE_S3_BUCKET
  value: {{ .Values.storage.shared.bucket | quote }}
- name: ARC_STORAGE_S3_REGION
  value: {{ .Values.storage.shared.region | quote }}
- name: ARC_STORAGE_S3_ENDPOINT
  value: {{ include "arc-enterprise.s3Endpoint" . | quote }}
- name: ARC_STORAGE_S3_USE_SSL
  value: {{ .Values.storage.shared.useSSL | quote }}
- name: ARC_STORAGE_S3_PATH_STYLE
  value: {{ .Values.storage.shared.usePathStyle | quote }}
{{- if .Values.storage.shared.prefix }}
- name: ARC_STORAGE_S3_PREFIX
  value: {{ .Values.storage.shared.prefix | quote }}
{{- end }}
- name: ARC_STORAGE_S3_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "arc-enterprise.objectStorageSecretName" . }}
      key: {{ if eq (include "arc-enterprise.useMinioCredKeys" .) "true" }}root-user{{ else }}access-key{{ end }}
- name: ARC_STORAGE_S3_SECRET_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "arc-enterprise.objectStorageSecretName" . }}
      key: {{ if eq (include "arc-enterprise.useMinioCredKeys" .) "true" }}root-password{{ else }}secret-key{{ end }}
- name: ARC_CLUSTER_REPLICATION_ENABLED
  value: "false"
{{- else }}
- name: ARC_STORAGE_BACKEND
  value: "local"
- name: ARC_STORAGE_LOCAL_PATH
  value: "/app/data/storage"
- name: ARC_CLUSTER_REPLICATION_ENABLED
  value: "true"
- name: ARC_CLUSTER_REPLICATION_PULL_WORKERS
  value: {{ .Values.cluster.replication.pullWorkers | quote }}
- name: ARC_CLUSTER_REPLICATION_FETCH_TIMEOUT_MS
  value: {{ .Values.cluster.replication.fetchTimeoutMs | quote }}
- name: ARC_CLUSTER_REPLICATION_SERVE_TIMEOUT_MS
  value: {{ .Values.cluster.replication.serveTimeoutMs | quote }}
- name: ARC_CLUSTER_REPLICATION_CATCHUP_ENABLED
  value: {{ .Values.cluster.replication.catchup.enabled | quote }}
- name: ARC_CLUSTER_REPLICATION_CATCHUP_BARRIER_TIMEOUT_MS
  value: {{ .Values.cluster.replication.catchup.barrierTimeoutMs | quote }}
{{- end }}
{{- end }}

{{/*
Writer seed list — DNS names of all writer pods via headless service.
Used by reader and compactor for cluster discovery.
*/}}
{{- define "arc-enterprise.writerSeeds" -}}
{{- $fullname := include "arc-enterprise.writerName" . -}}
{{- $headless := include "arc-enterprise.writerHeadlessName" . -}}
{{- $replicas := int .Values.writer.replicas -}}
{{- $namespace := .Release.Namespace -}}
{{- $seeds := list -}}
{{- range $i, $e := until $replicas -}}
{{- $seeds = append $seeds (printf "%s-%d.%s.%s.svc.cluster.local:9100" $fullname $i $headless $namespace) -}}
{{- end -}}
{{ join "," $seeds }}
{{- end }}

{{/*
Writer entrypoint — wraps /arc in a sh script that sets
ARC_CLUSTER_RAFT_BOOTSTRAP=true only when POD_NAME ends in "-0".
Needed because StatefulSet env is rendered at chart-render time, not
per-pod, so we can't key bootstrap off the ordinal any other way.
Using `exec` preserves signal handling.
*/}}
{{- define "arc-enterprise.writerEntrypoint" -}}
command: ["/bin/sh","-c"]
args:
  - |
    case "$POD_NAME" in
      *-0) export ARC_CLUSTER_RAFT_BOOTSTRAP=true ;;
      *)   export ARC_CLUSTER_RAFT_BOOTSTRAP=false ;;
    esac
    cd /app && exec ./arc
{{- end }}
