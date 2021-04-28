from datetime import datetime

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

with DAG(
        dag_id='search_model_pipeline',
        schedule_interval=None,
        start_date=datetime(1981, 1, 1)
) as dag:
    preprocess_data = DatabricksSubmitRunOperator(
        task_id="preprocess_data",
        spark_python_task={"python_file": "dbfs:/datalake/code/preprocessing/app/__main__.py",
                           "parameters": ""},
        existing_cluster_id="0421-172042-scats73"
    )

    search_model = DatabricksRunNowOperator(task_id='search_model',
                                            job_id=2)
    search_model = DatabricksSubmitRunOperator(
        task_id="search_model",
        spark_python_task={"python_file": "dbfs:/datalake/code/search/app/__main__.py",
                           "parameters": ""},
        existing_cluster_id="0421-172042-scats73"
    )

    preprocess_data >> search_model
