"""
This module is an end to end airflow data pipeline that ingests and processes daily stock
market data from multiple stock exchanges. Data is read from and written to Google Cloud Storage.
Once data is ingested, end of day corrections are processed and analytical data required 
for business analysis is obtained.
"""

from airflow import DAG
#from airflow.operators.python_operator import PythonOperator
#from airflow.operators.bash_operator import BashOperator
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule
import pendulum
from datetime import date, datetime, timedelta

local_tz = pendulum.timezone("America/Detroit")
gcp_project_id = 'splendid-planet-367217'
gcp_region = 'us-east1'
gcp_zone = 'us-east1-b'
gke_cluster = 'spark-airflow-cluster'
dataproc_cluster = 'spark-airflow-dataproc'
bucket_name = 'trade_quote_analysis'

VIRTUAL_CLUSTER_CONFIG = {
    "kubernetes_cluster_config": {
        "gke_cluster_config": {
            "gke_cluster_target": f"projects/{gcp_project_id}/locations/{gcp_zone}/clusters/{gke_cluster}",
            "node_pool_target": [
                {
                    "node_pool": f"projects/{gcp_project_id}/locations/{gcp_zone}/clusters/{gke_cluster}/nodePools/dp",  # noqa
                    "roles": ["DEFAULT"],
                }
            ],
        },
        "kubernetes_software_config": {"component_version": {"SPARK": b'3'}},
    },
    "staging_bucket": "trade-analysis-staging-bucket",
}

ingest_data_job = {
    "reference": {"project_id": gcp_project_id},
    "placement": {"cluster_name": dataproc_cluster},
    "pyspark_job": {"main_python_file_uri": f"gs://{bucket_name}/ingest_trade_data.py",
                    "python_file_uris":f"gs://{bucket_name}/spark_airflow/cloud_setup.py"}
}

trade_analysis_job = {
    "reference": {"project_id": gcp_project_id},
    "placement": {"cluster_name": dataproc_cluster},
    "pyspark_job": {"main_python_file_uri": f"gs://{bucket_name}/spark_airflow/trade_analysis.py",
                    "python_file_uris":f"gs://{bucket_name}/spark_airflow/cloud_setup.py"}
}

default_args = {
            "owner": "airflow",
            "start_date": datetime(2022, 11, 6,tzinfo=local_tz),
            "depends_on_past": False,
            "email_on_failure": False,
            "retries": 2, # retry twice
            "retry_delay": timedelta(minutes=5), # five minutes interval
            "project_id":"splendid-planet-367217",
            "region": "us-east1"
        }

with models.DAG(dag_id="TradeAnalysis",
         schedule_interval="0 18 * * 1-5", # running at 6pm for weekdays
         default_args=default_args,
         description='Ingest and Analyze Stock Market Data',
         dagrun_timeout=timedelta(minutes=120) 
         ) as dag:

    create_cluster_in_gke = DataprocCreateClusterOperator(
        task_id="create_cluster_in_gke",
        cluster_name=dataproc_cluster,
        project_id=gcp_project_id,
        region=gcp_region,
        virtual_cluster_config=VIRTUAL_CLUSTER_CONFIG
    )

    ingest_data = DataprocSubmitJobOperator(
        task_id="ingest_data", job=ingest_data_job, region=gcp_region, project_id=gcp_project_id
    )

    trade_analysis = DataprocSubmitJobOperator(
        task_id="trade_analysis", job=trade_analysis_job, region=gcp_region, project_id=gcp_project_id
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=gcp_project_id,
        cluster_name=dataproc_cluster,
        region=gcp_region,
        trigger_rule=TriggerRule.ALL_DONE
    )


    create_cluster_in_gke >> ingest_data >> trade_analysis >> delete_cluster    

