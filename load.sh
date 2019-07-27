REPORT_ID="2019-05-10"

bq load \
--source_format=AVRO \
 --clustering_fields=domain_name \
 --time_partitioning_expiration=-1 \
 --time_partitioning_type=DAY \
 --[no]use_avro_logical_types \
"webcrawl.url_resource\$$REPORT_ID" \
"gs://us-east1-dta-airflow-b3415db4-bucket/data/bqload/*.avro"


{
  "sourceUris": [
"gs://us-east1-dta-airflow-b3415db4-bucket/data/bqload/*.avro"
  ],
  "destinationTable": {
  "projectId": "dta-ga-bigquery",
  "datasetId": "webcrawl",
  "tableId": "url_resource$2019-05-10"
},
  "timePartitioning":{   "type": "DAY"},
  "clustering": {
  "fields": [
    "domain_name"
  ]
  },
  
  "useAvroLogicalTypes": true
 
}