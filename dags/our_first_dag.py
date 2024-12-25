"""Example DAG demonstrating the usage of the BashOperator."""

from __future__ import annotations

import datetime

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="our_first_dag",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    is_paused_upon_creation=False
) as dag:
    task1 = BashOperator(
        task_id="task1",
        bash_command="echo this is the first task",
    )

    # [START howto_operator_bash]
    task2 = BashOperator(
        task_id="task2",
        bash_command="echo this is task 2",
    )
    # [END howto_operator_bash]

    task1 >> task2

if __name__ == "__main__":
    dag.test()