"""
This module is an end to end airflow data pipeline that ingests and processes daily stock
market data from multiple stock exchanges. Data is read from and written to Google Cloud Storage.
Once data is ingested, end of day corrections are processed and analytical data required 
for business analysis is obtained.
"""

import datetime

from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator, DataprocDeleteBatchOperator, DataprocGetBatchOperator, DataprocListBatchesOperator

)
from airflow.utils.dates import days_ago
PROJECT_ID = "{{ var.value.project_id }}"
REGION = "{{ var.value.region_name}}"
INGEST_BUCKET = "{{ var.value.ingest_bucket_name }}"
PROCESS_BUCKET = "{{ var.value.process_bucket_name }}"
PHS_CLUSTER = "{{ var.value.phs_cluster }}"
INGEST_FILE_LOCATION = "gs://{{var.value.ingest_bucket_name }}/spark-job.py"
PROCESS_FILE_LOCATION = "gs://{{var.value.process_bucket_name }}/spark-job.py"
# for e.g.  "gs//my-bucket/spark-job.py"
# Start a single node Dataproc Cluster for viewing Persistent History of Spark jobs
PHS_CLUSTER_PATH = \
    "projects/{{ var.value.project_id }}/regions/{{ var.value.region_name}}/clusters/{{ var.value.phs_cluster }}"
# for e.g. projects/my-project/regions/my-region/clusters/my-cluster"
SPARK_GCS_JAR_FILE = "gs://trade_quote_analysis/spark_airflow/gcs-connector-latest-hadoop2.jar"

default_args = {
    # Tell airflow to start one day ago, so that it runs as soon as you upload it
    "start_date": days_ago(1),
    "project_id": PROJECT_ID,
    "region": REGION,

}
with models.DAG(
    "Trade_Quote_Analysis",  # The id you will see in the DAG airflow page
    default_args=default_args,  # The interval with which to schedule the DAG
    schedule_interval=datetime.timedelta(days=1),  # Override to match your needs
) as dag:

    ingest_trade_data = DataprocCreateBatchOperator(
        task_id="ingest_trade_data",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": INGEST_FILE_LOCATION,
                "jar_file_uris": [SPARK_GCS_JAR_FILE],
            },
            "environment_config": {
                "peripherals_config": {
                    "spark_history_server_config": {
                        "dataproc_cluster": PHS_CLUSTER_PATH,
                    },
                },
            },
        },
        batch_id="ingest-batch-create",
    )

    analyze_trade_data = DataprocCreateBatchOperator(
        task_id="analyze_trade_data",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": PROCESS_FILE_LOCATION,
                "jar_file_uris": [SPARK_GCS_JAR_FILE],
            },
            "environment_config": {
                "peripherals_config": {
                    "spark_history_server_config": {
                        "dataproc_cluster": PHS_CLUSTER_PATH,
                    },
                },
            },
        },
        batch_id="process-batch-create",
    )

    delete_ingest_batch = DataprocDeleteBatchOperator(
        task_id="delete_ingest_batch",
        batch_id="ingest-batch-create",
    )
    delete_process_batch = DataprocDeleteBatchOperator(
        task_id="delete_process_batch",
        batch_id="process-batch-create",
    )
    ingest_trade_data >> analyze_trade_data >> [delete_ingest_batch,delete_process_batch]
  

