"""
This module is an end to end airflow data pipeline that ingests and processes US Agricultural land
use data and soil organic content data within US as well as the world. Data is read from Google Cloud Storage
and written to Big Query. There is further transformation of data within Big Query and analytical data required 
for business analysis is obtained.
"""

import datetime

from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator, DataprocDeleteBatchOperator
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator
)
from airflow.utils.dates import days_ago
PROJECT_ID = "{{ var.value.project_id }}"
DATASET_ID = "{{ var.value.dataset_id }}"
REGION = "{{ var.value.region_name}}"
AGRI_BUCKET = "{{ var.value.agri_bucket_name }}"
US_SOIL_BUCKET = "{{ var.value.us_soil_bucket_name }}"
WORLD_SOIL_BUCKET = "{{ var.value.world_soil_bucket_name }}"
PHS_CLUSTER = "{{ var.value.phs_cluster }}"
AGRI_FILE_LOCATION = "gs://{{var.value.agri_bucket_name }}/spark-job.py"
US_SOIL_FILE_LOCATION = "gs://{{var.value.us_soil_bucket_name }}/spark-job.py"
WORLD_SOIL_FILE_LOCATION = "gs://{{var.value.world_soil_bucket_name }}/spark-job.py"
# for e.g.  "gs//my-bucket/spark-job.py"
# Start a single node Dataproc Cluster for viewing Persistent History of Spark jobs
PHS_CLUSTER_PATH = \
    "projects/{{ var.value.project_id }}/regions/{{ var.value.region_name}}/clusters/{{ var.value.phs_cluster }}"
# for e.g. projects/my-project/regions/my-region/clusters/my-cluster"
SPARK_GCS_JAR_FILE = "gs://save_soil_analysis/gcs-connector-latest-hadoop2.jar"
SPARK_BIGQUERY_JAR_FILE = "gs://save_soil_analysis/spark-bigquery-with-dependencies_2.12-0.27.1.jar"

default_args = {
    # Tell airflow to start one day ago, so that it runs as soon as you upload it
    "start_date": days_ago(1),
    "project_id": PROJECT_ID,
    "region": REGION,

}
with models.DAG(
    "Soil_Analysis",  # The id you will see in the DAG airflow page
    default_args=default_args,  # The interval with which to schedule the DAG
    schedule_interval=datetime.timedelta(days=1),  # Override to match your needs
) as dag:

    ingest_agriculture_data = DataprocCreateBatchOperator(
        task_id="ingest_agriculture_data",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": AGRI_FILE_LOCATION,
                "jar_file_uris": [SPARK_GCS_JAR_FILE,SPARK_BIGQUERY_JAR_FILE],
            },
            "environment_config": {
                "peripherals_config": {
                    "spark_history_server_config": {
                        "dataproc_cluster": PHS_CLUSTER_PATH,
                    },
                },
            },
        },
        batch_id="agri-batch-create",
    )

    ingest_usa_soil_data = DataprocCreateBatchOperator(
        task_id="ingest_usa_soil_data",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": US_SOIL_FILE_LOCATION,
                "jar_file_uris": [SPARK_GCS_JAR_FILE,SPARK_BIGQUERY_JAR_FILE],
            },
            "environment_config": {
                "peripherals_config": {
                    "spark_history_server_config": {
                        "dataproc_cluster": PHS_CLUSTER_PATH,
                    },
                },
            },
        },
        batch_id="usa-soil-batch-create",
    )

    ingest_world_soil_data = DataprocCreateBatchOperator(
        task_id="ingest_world_soil_data",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": WORLD_SOIL_FILE_LOCATION,
                "jar_file_uris": [SPARK_GCS_JAR_FILE,SPARK_BIGQUERY_JAR_FILE],
            },
            "environment_config": {
                "peripherals_config": {
                    "spark_history_server_config": {
                        "dataproc_cluster": PHS_CLUSTER_PATH,
                    },
                },
            },
        },
        batch_id="world-soil-batch-create",
    )

    process_soil_data_bigquery = BigQueryInsertJobOperator(
            task_id="process_soil_data_bigquery",
            configuration={
                "query": {
                    "query": "CALL `" + PROJECT_ID + "." + DATASET_ID + "." + "process_soil_data`();",
                    "useLegacySql": False
                }
            }
        )

    delete_agri_batch = DataprocDeleteBatchOperator(
        task_id="delete_agri_batch",
        batch_id="agri-batch-create",
    )
    delete_usa_soil_batch = DataprocDeleteBatchOperator(
        task_id="delete_usa_soil_batch",
        batch_id="usa-soil-batch-create",
    )
    delete_world_soil_batch = DataprocDeleteBatchOperator(
        task_id="delete_world_soil_batch",
        batch_id="world-soil-batch-create",
    )
    [ingest_agriculture_data,ingest_usa_soil_data,ingest_world_soil_data] >> process_soil_data_bigquery >> [delete_agri_batch,delete_usa_soil_batch,delete_world_soil_batch]
  

