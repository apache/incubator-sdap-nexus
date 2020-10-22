{{/* vim: set filetype=mustache: */}}

{{/*
Name of the generated configmap containing the contents of the collections config file.
*/}}
{{- define "nexus.collectionsConfig.configmapName" -}}
{{ .Values.ingestion.collections.configMap | default "collections-config" }}
{{- end -}}

{{/*
Path at which to mount the collections config file in the Collection Manager.
*/}}
{{- define "nexus.collectionsConfig.mountPath" -}}
/var/lib/sdap/collections
{{- end -}}

{{/*
Path at which to mount the history directory in the Collection Manager, if not using Solr for history.
*/}}
{{- define "nexus.history.mountPath" -}}
/var/lib/sdap/history
{{- end -}}


{{/*
The data volume which is used in both the Collection Manager and the Granule Ingester.
*/}}
{{- define "nexus.ingestion.dataVolume" -}}
- name: data-volume
  {{- if .Values.ingestion.granules.nfsServer }}
  nfs:
    server: {{ .Values.ingestion.granules.nfsServer }}
    path: {{ .Values.ingestion.granules.path }}
  {{- else }}
  hostPath:
    path: {{ .Values.ingestion.granules.path }}
{{- end }}
{{- end -}}

{{/*
The data volume mount which is used in both the Collection Manager and the Granule Ingester.
*/}}
{{- define "nexus.ingestion.dataVolumeMount" -}}
- name: data-volume
  mountPath: {{ .Values.ingestion.granules.mountPath }}
{{- end -}}

{/*
The scripts volume which is used in both the Collection Manager and the Granule Ingester.
*/}}
{{- define "nexus.ingestion.scriptsVolume" -}}
- name: scripts-volume
  hostPath:
    path: {{ .Values.ingestion.scripts.path }}
{{- end }}
{{- end -}}

{{/*
The scripts volume mount which is used in both the Collection Manager and the Granule Ingester.
*/}}
{{- define "nexus.ingestion.scriptsVolumeMount" -}}
- name: scripts-volume
  mountPath: {{ .Values.ingestion.scripts.mountPath }}
{{- end -}}


{{- define "nexus.urls.solr" -}}
{{ .Values.external.solrHostAndPort | default (print "http://" .Release.Name "-solr-svc:8983") }}
{{- end -}}

{{- define "nexus.urls.zookeeper" -}}
{{ .Values.external.zookeeperHostAndPort | default (print .Release.Name "-zookeeper:2181") }}
{{- end -}}