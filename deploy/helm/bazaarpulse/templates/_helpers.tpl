{{- define "bp.name" -}}{{ .Release.Name }}{{- end -}}

{{- define "bp.tag" -}}{{ .Values.image.tag | default .Chart.AppVersion }}{{- end -}}

{{- define "bp.labels" -}}
app.kubernetes.io/name: bazaarpulse
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ include "bp.tag" . | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{/* Settings shared by every backend container. */}}
{{- define "bp.backendEnv" -}}
envFrom:
  - configMapRef:
      name: {{ include "bp.name" . }}-config
  - secretRef:
      name: {{ .Values.existingSecret }}
{{- end -}}

{{- define "bp.securityContext" -}}
securityContext:
  runAsNonRoot: true
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: true
  capabilities: { drop: ["ALL"] }
{{- end -}}
