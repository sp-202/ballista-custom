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

{{/* env + envFrom for a component:
     include "ballista.env" (list . .Values.scheduler.env "ui")   role: ui | member | connect */}}
{{- define "ballista.env" -}}
{{- $ := index . 0 -}}
{{- $env := concat ($.Values.env | default list) ((index . 1) | default list) -}}
env:
  - {name: HOME, value: /tmp}
  {{- include "ballista.securityEnv" (list $ (index . 2)) | nindent 2 }}
  {{- with $env }}
  {{- toYaml . | nindent 2 }}
  {{- end }}
{{- with $.Values.envFrom }}
envFrom:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}

{{/* --- Security (values `security`) --- */}}
{{- define "ballista.securitySecret" -}}
{{- default (printf "%s-security" (include "ballista.fullname" .)) .Values.security.existingSecret -}}
{{- end -}}

{{/* Env pointing a component at the mounted certificate (and token):
     include "ballista.securityEnv" (list . "ui"|"member"|"connect") */}}
{{- define "ballista.securityEnv" -}}
{{- $ := index . 0 -}}
{{- $role := index . 1 -}}
{{- if $.Values.security.enabled -}}
- {name: BALLISTA_TLS_CERT, value: /etc/ballista/tls/tls.crt}
- {name: BALLISTA_TLS_KEY, value: /etc/ballista/tls/tls.key}
- {name: BALLISTA_TLS_CA, value: /etc/ballista/tls/ca.crt}
{{- if eq $role "ui" }}
- {name: BALLISTA_UI_TOKEN_FILE, value: /etc/ballista/tls/ui-token}
{{- if not $.Values.security.uiTls }}
- {name: BALLISTA_UI_TLS, value: "false"}
{{- end }}
{{- end }}
{{- if eq $role "connect" }}
- name: SPARK_CONNECT_AUTHENTICATE_TOKEN
  valueFrom:
    secretKeyRef: {name: {{ include "ballista.securitySecret" $ }}, key: connect-token}
{{- if $.Values.security.connectTls }}
- {name: BALLISTA_CONNECT_TLS, value: "true"}
{{- end }}
{{- end }}
{{- end -}}
{{- end -}}

{{- define "ballista.securityMount" -}}
{{- if .Values.security.enabled -}}
- {name: ballista-tls, mountPath: /etc/ballista/tls, readOnly: true}
{{- end -}}
{{- end -}}

{{/* The Secret's files a component needs: include "ballista.securityVolume" (list . "ui") */}}
{{- define "ballista.securityVolume" -}}
{{- $ := index . 0 -}}
{{- if $.Values.security.enabled -}}
- name: ballista-tls
  secret:
    secretName: {{ include "ballista.securitySecret" $ }}
    items:
      - {key: ca.crt, path: ca.crt}
      - {key: tls.crt, path: tls.crt}
      - {key: tls.key, path: tls.key}
      {{- if eq (index . 1) "ui" }}
      - {key: ui-token, path: ui-token}
      {{- end }}
{{- end -}}
{{- end -}}

{{/* URL scheme of the web UI ports */}}
{{- define "ballista.uiScheme" -}}
{{- if and .Values.security.enabled .Values.security.uiTls -}}https{{- else -}}http{{- end -}}
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
