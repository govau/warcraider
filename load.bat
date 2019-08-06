SET REPORT_NUM=04

bq load^
 --source_format=AVRO^
 --clustering_fields=domain_name^
 --time_partitioning_expiration=-1^
 --time_partitioning_type=DAY^
 --use_avro_logical_types^
 "web_crawl.url_resource"^
 "gs://us-east1-dta-airflow-b3415db4-bucket/data/bqload/dta-report%REPORT_NUM%-*.avro"