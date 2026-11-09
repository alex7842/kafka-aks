{{- define "kafka-ui.fullname" -}}
{{ .Release.Name }}-{{ .Chart.Name }}
{{- end }}
