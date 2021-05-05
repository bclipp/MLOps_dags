from datetime import datetime
import uuid
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

with DAG(
        dag_id="build_model_pipeline",
        schedule_interval=None,
        start_date=datetime(1981, 1, 1)
) as dag:
    timestamp = datetime.now()
    uid = timestamp.strftime("%b%d%Y")
    print("UID: " + uid)
    cluster_id = "0428-143146-wig801"
    preprocess_data = DatabricksSubmitRunOperator(
        task_id="preprocess_data",
        spark_python_task={"python_file": "dbfs:/datalake/code/preprocessing/__main__.py",
                           "parameters": f"{uid}"},
        existing_cluster_id=cluster_id
    )

    build_model = DatabricksSubmitRunOperator(
        task_id="build_model",
        spark_python_task={"python_file": "dbfs:/datalake/code/model/__main__.py",
                           "parameters": [f"{uid}",
                                          '{{ (dag_run.conf["max_depth"] if dag_run else "") | tojson }}',
                                          '{{ (dag_run.conf["n_estimators"] if dag_run else "") | tojson }}']},
        existing_cluster_id=cluster_id
    )

    preprocess_data >> build_model
