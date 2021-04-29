from datetime import datetime
import uuid
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

with DAG(
        dag_id="build_model_pipeline",
        schedule_interval=None,
        start_date=datetime(1981, 1, 1)
) as dag:
    # preprocess_data = DatabricksRunNowOperator(task_id="preprocess_data",
    #                                           job_id=3)
    # build_model = DatabricksRunNowOperator(task_id="build_model_old",
    #                                      job_id=4)
    uid = str(uuid.uuid1()).replace('-', '')
    preprocess_data = DatabricksSubmitRunOperator(
        task_id="preprocess_data",
        spark_python_task={"python_file": "dbfs:/datalake/code/preprocessing/__main__.py",
                           "parameters": f"{uid}"},
        existing_cluster_id="0421-172042-scats73"
    )

    build_model = DatabricksSubmitRunOperator(
        task_id="build_model",
        spark_python_task={"python_file": "dbfs:/datalake/code/model/__main__.py",
                           "parameters": f"{uid}"},
        existing_cluster_id="0421-172042-scats73"
    )

    preprocess_data >> build_model
