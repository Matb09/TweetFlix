from datetime import datetime, timedelta
import os

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator

DAGS_FOLDER = os.environ["DAGS_FOLDER"]

default_args = {
    'owner': 'default_user',
    'start_date': datetime(2022, 5, 12),
    'retry_delay': timedelta(minutes=1),
}

with DAG(
        "retrieve_tweets",
        schedule_interval="30 2 * * *",
        default_args=default_args,
) as dag:
    do_stuff1 = BashOperator(
        task_id="task_1",
        bash_command='python /home/airflow/gcs/dags/tasks/case_upload/code_tweet.py --start_time={{ dag_run.conf.get("start_time", "")}} --end_time={{ dag_run.conf.get("end_time", "")}} --disable_override={{ dag_run.conf.get("disable_override", "")}}',
    )
