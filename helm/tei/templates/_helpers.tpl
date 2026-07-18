{{- define "tei.name" -}}
tei
{{- end }}

{{- define "tei.clusterScopedName" -}}
{{- printf "%s-%s" (include "tei.fullname" .) (sha256sum .Release.Namespace | trunc 8) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "tei.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "tei.name" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "tei.prometheusServerName" -}}
{{- if .Values.prometheus.server.fullnameOverride -}}
{{- .Values.prometheus.server.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default "prometheus" .Values.prometheus.nameOverride -}}
{{- $serverName := default "server" .Values.prometheus.server.name -}}
{{- if contains $name .Release.Name -}}
{{- printf "%s-%s" .Release.Name $serverName | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s-%s" .Release.Name $name $serverName | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end }}

{{- define "tei.prometheusURL" -}}
{{- if .Values.prometheus.enabled -}}
{{- printf "http://%s:%v" (include "tei.prometheusServerName" .) .Values.prometheus.server.service.servicePort -}}
{{- else -}}
{{- required "prometheus.url is required when prometheus.enabled=false" .Values.prometheus.url -}}
{{- end -}}
{{- end }}

{{- define "tei.postgresqlName" -}}
{{- if .Values.postgresql.fullnameOverride -}}
{{- .Values.postgresql.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default "postgresql" .Values.postgresql.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end }}

{{- define "tei.postgresqlDSN" -}}
{{- printf "postgresql://%s@%s:5432/%s" .Values.postgresql.auth.username (include "tei.postgresqlName" .) .Values.postgresql.auth.database -}}
{{- end }}

{{- define "tei.labels" -}}
app.kubernetes.io/name: {{ include "tei.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end }}

{{- define "tei.selectorLabels" -}}
app.kubernetes.io/name: {{ include "tei.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "tei.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "tei.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- required "serviceAccount.name is required when serviceAccount.create=false" .Values.serviceAccount.name }}
{{- end }}
{{- end }}
