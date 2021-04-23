from datetime import datetime
import os

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

with DAG(
        dag_id='search_model_pipeline',
        schedule_interval=None,
        start_date=datetime(1981, 1, 1)
) as dag:

    preprocess_data = DatabricksRunNowOperator(task_id='preprocess_data',
                                               job_id=3)
    search_model = DatabricksRunNowOperator(task_id='search_model',
                                            job_id=2)

    preprocess_data >> search_model
