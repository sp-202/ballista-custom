{{- define "op.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "op.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else if contains (include "op.name" .) .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "op.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "op.selector" -}}
app.kubernetes.io/name: {{ include "op.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "op.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{ include "op.selector" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "op.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "op.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/* The operator's permissions, shared by the ClusterRole and the Role. */}}
{{- define "op.rules" -}}
- apiGroups: [ballista.apache.org]
  resources: [ballistaclusters, ballistaapplications]
  verbs: [get, list, watch, create, patch, update, delete]
- apiGroups: [ballista.apache.org]
  resources: [ballistaclusters/status, ballistaapplications/status]
  verbs: [get, patch, update]
# Scheduler, connect and history deployments and services (server-side apply).
- apiGroups: [apps]
  resources: [deployments]
  verbs: [get, list, watch, create, patch, delete]
- apiGroups: [""]
  resources: [services]
  verbs: [get, create, patch, delete]
# Executor and driver pods.
- apiGroups: [""]
  resources: [pods]
  verbs: [get, list, watch, create, patch, delete]
# Autoscaling reads the scheduler's /api/scaling through the API server proxy.
- apiGroups: [""]
  resources: [services/proxy]
  verbs: [get]
# Each cluster's <cluster>-security Secret (CA, certificate, tokens): created once,
# then read for the UI token. Not list/watch, so other Secrets stay unseen.
- apiGroups: [""]
  resources: [secrets]
  verbs: [get, create]
{{- end -}}
