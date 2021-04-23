from datetime import datetime

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

with DAG(
        dag_id='build_model_pipeline',
        schedule_interval=None,
        start_date=datetime(1981, 1, 1)
) as dag:
    preprocess_data = DatabricksRunNowOperator(task_id='preprocess_data',
                                               job_id=3)
    build_model = DatabricksRunNowOperator(task_id='build_model',
                                           job_id=4)

    preprocess_data >> build_model
