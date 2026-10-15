{{/*
Expand the name of the chart.
*/}}
{{- define "arc.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "arc.fullname" -}}
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

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "arc.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "arc.labels" -}}
helm.sh/chart: {{ include "arc.chart" . }}
{{ include "arc.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "arc.selectorLabels" -}}
app.kubernetes.io/name: {{ include "arc.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "arc.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "arc.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "arc.bootstrapTokenSecretName" -}}
{{- if .Values.auth.bootstrapToken.existingSecret -}}
{{ .Values.auth.bootstrapToken.existingSecret }}
{{- else -}}
{{ printf "%s-bootstrap-token" (include "arc.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end -}}
{{- end }}

{{/*
WAL directory cap in MB (#676). An explicit arc.wal.maxDirectoryMB wins. With
persistence disabled the data is in an emptyDir, whose statfs reports the
node's disk, not its sizeLimit, so free space cannot gate the WAL: cap it at
80% of the emptyDir sizeLimit, or 1024 MB without one. With a PVC, 0 (no cap;
the PVC's free space is the signal).
*/}}
{{- define "arc.walMaxDirectoryMB" -}}
{{- $explicit := int (.Values.arc.wal.maxDirectoryMB | default 0) -}}
{{- if gt $explicit 0 -}}
{{- $explicit -}}
{{- else if not .Values.persistence.enabled -}}
{{- $limit := int (.Values.persistence.emptyDirSizeLimitMB | default 0) -}}
{{- if gt $limit 0 -}}
{{- div (mul $limit 4) 5 -}}
{{- else -}}
1024
{{- end -}}
{{- else -}}
0
{{- end -}}
{{- end }}
