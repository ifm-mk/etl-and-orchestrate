from datetime import timedelta

from airflow import DAG
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.utils.dates import days_ago

# define DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["martin.kaehne@infomotion.de"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# instantiate DAG
with DAG(
    dag_id="start_dataflow_pipelines_python",
    default_args=default_args,
    description="start dataflow pipelines using python operator",
    schedule_interval="@once",
    start_date=days_ago(0),
    tags=["python", "pipelines"],
) as dag:
    
    # define tasks
    task1 = DataFlowPythonOperator(
        task_id="start_raw_to_transform",
        py_file="/home/airflow/gcs/data/raw_to_transform.py"
    )

    task2 = DataFlowPythonOperator(
        task_id="start_transform_to_serving",
        py_file="/home/airflow/gcs/data/transform_to_serving.py"
    )

    # specify dependencies
    task1 >> task2