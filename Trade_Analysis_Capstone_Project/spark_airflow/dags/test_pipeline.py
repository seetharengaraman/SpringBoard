"""
Example Airflow DAG that show how to create a Dataproc cluster in Google Kubernetes Engine.
"""
from datetime import datetime,timedelta

from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
)
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKECreateClusterOperator,
    GKEDeleteClusterOperator,
)
from airflow.utils.trigger_rule import TriggerRule


DAG_ID = "dataproc-gke"

PROJECT_ID = "splendid-planet-367217"


REGION = "us-east1"

CLUSTER_NAME = "test-cluster-dp-1"

GKE_CLUSTER_NAME = "test-gke-cluster-dp-1"

GKE_CLUSTER_CONFIG = {
    "name": GKE_CLUSTER_NAME,
    "workload_identity_config": {
        "workload_pool": f"{PROJECT_ID}.svc.id.goog",
    },
    "initial_node_count": 1,

}

# [START how_to_cloud_dataproc_create_cluster_in_gke_config]

VIRTUAL_CLUSTER_CONFIG = {
    "kubernetes_cluster_config": {
        "gke_cluster_config": {
            "gke_cluster_target": f"projects/{PROJECT_ID}/locations/{REGION}/clusters/{GKE_CLUSTER_NAME}",
            "node_pool_target": [
                {
                    "node_pool": f"projects/{PROJECT_ID}/locations/{REGION}/clusters/{GKE_CLUSTER_NAME}/nodePools/dp",  # noqa
                    "roles": ["DEFAULT"],
                }
            ],
        },
        "kubernetes_software_config": {"component_version": {"SPARK": b'3'}},
    },
    "staging_bucket": "test-staging-bucket",

}

# [END how_to_cloud_dataproc_create_cluster_in_gke_config]


with models.DAG(
    DAG_ID,
    schedule='@once',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
    dagrun_timeout=timedelta(minutes=120) 
) as dag:
    create_gke_cluster = GKECreateClusterOperator(
        task_id="create_gke_cluster",
        project_id=PROJECT_ID,
        location=REGION,
        body=GKE_CLUSTER_CONFIG,

    )

    # [START how_to_cloud_dataproc_create_cluster_operator_in_gke]
    create_cluster_in_gke = DataprocCreateClusterOperator(
        task_id="create_cluster_in_gke",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        virtual_cluster_config=VIRTUAL_CLUSTER_CONFIG,
    )
    # [END how_to_cloud_dataproc_create_cluster_operator_in_gke]

    delete_dataproc_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_gke_cluster = GKEDeleteClusterOperator(
        task_id="delete_gke_cluster",
        name=GKE_CLUSTER_NAME,
        project_id=PROJECT_ID,
        location=REGION,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_gke_cluster >> create_cluster_in_gke >> [delete_dataproc_cluster, delete_gke_cluster]

    