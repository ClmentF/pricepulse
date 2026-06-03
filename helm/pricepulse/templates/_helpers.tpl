{{- define "pricepulse.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "pricepulse.kafkaBootstrap" -}}
{{- range $i := until (int .Values.kafka.replicas) -}}
kafka-{{ $i }}.kafka:9092{{ if lt $i (sub (int $.Values.kafka.replicas) 1) }},{{ end }}
{{- end -}}
{{- end }}
