{{/* vim: set filetype=mustache: */}}

{{/*
Cross-field validation — rules that can't be expressed in values.schema.json
because they depend on the combination of multiple values, or on the
presence of an existing Kubernetes Secret.

Type / enum / range validation lives in values.schema.json and runs before
any template is rendered.

Called once at the top of writer-statefulset.yaml (every install renders
that template, so validation fires exactly once).
*/}}

{{- define "arc-enterprise.validate" -}}
{{- include "arc-enterprise.validate.writerReplicas" . -}}
{{- include "arc-enterprise.validate.license" . -}}
{{- include "arc-enterprise.validate.sharedSecret" . -}}
{{- include "arc-enterprise.validate.tls" . -}}
{{- include "arc-enterprise.validate.minioCredentials" . -}}
{{- include "arc-enterprise.validate.externalStorageCredentials" . -}}
{{- end }}

{{/*
Forbid writer.replicas=2 with an explanatory message. JSON Schema can't
carry custom error strings, so this one stays in a fail() for UX —
anything else (type, minimum) is caught by values.schema.json first.
*/}}
{{- define "arc-enterprise.validate.writerReplicas" -}}
{{- if eq (int .Values.writer.replicas) 2 -}}
{{- fail "writer.replicas=2 creates a Raft split-brain hazard (a quorum of 2 requires both pods — a single failure kills the cluster). Use 1 (dev) or 3+ (HA)." -}}
{{- end -}}
{{- end }}

{{/*
License key is required unless an existing Secret (chart-managed from a
prior install, or operator-supplied) already contains it.
*/}}
{{- define "arc-enterprise.validate.license" -}}
{{- if and (not .Values.license.existingSecret) (not .Values.license.key) -}}
{{- $existing := lookup "v1" "Secret" .Release.Namespace (include "arc-enterprise.licenseSecretName" .) -}}
{{- if not $existing -}}
{{- fail "license.key is required (or set license.existingSecret)" -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/*
Cluster shared secret is required on every install. Skip on upgrade when
the Secret already exists and neither new value nor existingSecret were
provided (i.e. `helm upgrade --reuse-values` without touching it).
*/}}
{{- define "arc-enterprise.validate.sharedSecret" -}}
{{- if and (not .Values.cluster.sharedSecret.existingSecret) (not .Values.cluster.sharedSecret.value) -}}
{{- $existing := lookup "v1" "Secret" .Release.Namespace (include "arc-enterprise.sharedSecretName" .) -}}
{{- if not $existing -}}
{{- fail "cluster.sharedSecret.value is required (or set cluster.sharedSecret.existingSecret)" -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/*
TLS requires the operator to have created the cert Secret themselves —
the chart does not generate certificates. Enforced only when TLS is on.
*/}}
{{- define "arc-enterprise.validate.tls" -}}
{{- if and .Values.cluster.tls.enabled (not .Values.cluster.tls.existingSecret) -}}
{{- fail "cluster.tls.existingSecret is required when cluster.tls.enabled=true" -}}
{{- end -}}
{{- end }}

{{/*
Bundled MinIO must have credentials — no weak defaults. Only enforced
when MinIO is actually rendered (shared mode + not external).
*/}}
{{- define "arc-enterprise.validate.minioCredentials" -}}
{{- if eq (include "arc-enterprise.minioBundled" .) "true" -}}
{{- if and (not .Values.minio.credentials.existingSecret) (or (not .Values.minio.credentials.rootUser) (not .Values.minio.credentials.rootPassword)) -}}
{{- $existing := lookup "v1" "Secret" .Release.Namespace (include "arc-enterprise.minioSecretName" .) -}}
{{- if not $existing -}}
{{- fail "minio.credentials.rootUser and rootPassword are required (or set minio.credentials.existingSecret)" -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/*
External S3 / Azure requires the operator to provide credentials.
Only enforced when shared storage is explicitly external.
*/}}
{{- define "arc-enterprise.validate.externalStorageCredentials" -}}
{{- if and (eq .Values.storage.mode "shared") .Values.storage.shared.external -}}
{{- if and (not .Values.storage.shared.credentials.existingSecret) (or (not .Values.storage.shared.credentials.accessKey) (not .Values.storage.shared.credentials.secretKey)) -}}
{{- $existing := lookup "v1" "Secret" .Release.Namespace (include "arc-enterprise.objectStorageSecretName" .) -}}
{{- if not $existing -}}
{{- fail "storage.shared.credentials.accessKey/secretKey are required when storage.shared.external=true (or set storage.shared.credentials.existingSecret)" -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end }}
