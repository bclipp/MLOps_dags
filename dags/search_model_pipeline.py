from datetime import datetime

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

with DAG(
        dag_id='search_model_pipeline',
        schedule_interval=None,
        start_date=datetime(1981, 1, 1)
) as dag:
    preprocess_data = DatabricksSubmitRunOperator(
        task_id="preprocess_data",
        spark_python_task={"python_file": "dbfs:/datalake/code/preprocessing/__main__.py",
                           "parameters": ""},
        existing_cluster_id="0505-024656-wont404"
    )

    search_model = DatabricksSubmitRunOperator(
        task_id="search_model",
        spark_python_task={"python_file": "dbfs:/datalake/code/search/__main__.py",
                           "parameters": ""},
        existing_cluster_id="0505-024656-wont404"
    )

    preprocess_data >> search_model
