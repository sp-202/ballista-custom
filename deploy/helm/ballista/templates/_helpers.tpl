{{- define "ballista.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "ballista.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "ballista.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "ballista.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/* Selector labels for one component: include "ballista.selector" (list . "scheduler") */}}
{{- define "ballista.selector" -}}
{{- $ := index . 0 -}}
app.kubernetes.io/name: {{ include "ballista.name" $ }}
app.kubernetes.io/instance: {{ $.Release.Name }}
app.kubernetes.io/component: {{ index . 1 }}
{{- end -}}

{{- define "ballista.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "ballista.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "ballista.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end -}}

{{- define "ballista.schedulerHost" -}}
{{ include "ballista.fullname" . }}-scheduler
{{- end -}}

{{- define "ballista.warehouseClaim" -}}
{{- default (printf "%s-warehouse" (include "ballista.fullname" .)) .Values.warehouse.persistence.existingClaim -}}
{{- end -}}

{{- define "ballista.historyClaim" -}}
{{- default (printf "%s-event-log" (include "ballista.fullname" .)) .Values.history.persistence.existingClaim -}}
{{- end -}}

{{/* Common pod spec fields */}}
{{- define "ballista.podCommon" -}}
serviceAccountName: {{ include "ballista.serviceAccountName" . }}
automountServiceAccountToken: {{ .Values.serviceAccount.automount }}
{{- with .Values.imagePullSecrets }}
imagePullSecrets:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.podSecurityContext }}
securityContext:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}

{{/* env + envFrom for a component: include "ballista.env" (list . .Values.scheduler.env) */}}
{{- define "ballista.env" -}}
{{- $ := index . 0 -}}
{{- $env := concat (list (dict "name" "HOME" "value" "/tmp")) ($.Values.env | default list) ((index . 1) | default list) -}}
env:
  {{- toYaml $env | nindent 2 }}
{{- with $.Values.envFrom }}
envFrom:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}

{{/* Writable /tmp (the root filesystem is read-only) plus the data volumes */}}
{{- define "ballista.dataVolumeMounts" -}}
- {name: tmp, mountPath: /tmp}
{{- range .Values.dataVolumes }}
- name: {{ .name }}
  mountPath: {{ .mountPath }}
  readOnly: {{ .readOnly | default false }}
{{- end }}
{{- end -}}

{{- define "ballista.dataVolumes" -}}
- {name: tmp, emptyDir: {}}
{{- range .Values.dataVolumes }}
- name: {{ .name }}
  {{- toYaml .volume | nindent 2 }}
{{- end }}
{{- end -}}
