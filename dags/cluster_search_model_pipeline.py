"""
This module is dedicated to searching for ideal hyperparameters using a Spark Cluster.
"""
from datetime import datetime

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

with DAG(
        dag_id='cluster_search_model_pipeline',
        schedule_interval=None,
        start_date=datetime(1981, 1, 1)
) as dag:
    cluster_id = "0728-180646-stoop524"
    timestamp = datetime.now()
    uid = timestamp.strftime("%b%d%Y")
    print("UID: " + uid)
    preprocess_data = DatabricksSubmitRunOperator(
        task_id="preprocess_data",
        spark_python_task={"python_file": "dbfs:/datalake/code/preprocessing/__main__.py",
                           "parameters": f"{uid}"},
        existing_cluster_id=cluster_id
    )

    search_model = DatabricksSubmitRunOperator(
        task_id="search_model",
        spark_python_task={"python_file": "dbfs:/datalake/code/cluster_search/__main__.py",
                           "parameters": f"{uid}"},
        existing_cluster_id=cluster_id
    )

    preprocess_data >> search_model
