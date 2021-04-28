from datetime import datetime

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator, DatabricksSubmitRunOperator

with DAG(
        dag_id="build_model_pipeline",
        schedule_interval=None,
        start_date=datetime(1981, 1, 1)
) as dag:
    # preprocess_data = DatabricksRunNowOperator(task_id="preprocess_data",
    #                                           job_id=3)
    # build_model = DatabricksRunNowOperator(task_id="build_model_old",
    #                                      job_id=4)
    
    preprocess_data = DatabricksSubmitRunOperator(
        task_id="preprocess_data",
        spark_python_task={"python_file": "dbfs:/datalake/code/preprocessing/app/__main__.py",
                           "parameters": ""},
        existing_cluster_id="0421-172042-scats73"
    )

    build_model = DatabricksSubmitRunOperator(
        task_id="build_model",
        spark_python_task={"python_file": "dbfs:/datalake/code/model/app/__main__.py",
                           "parameters": ""},
        existing_cluster_id="0421-172042-scats73"
    )

    preprocess_data >> build_model
