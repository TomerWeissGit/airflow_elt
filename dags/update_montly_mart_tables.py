from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import pendulum

# Define the DAG
with DAG(
    'update_mart_table',
    schedule="0 0 1 * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False

) as dag:
    update_stg_usage = BashOperator(
        task_id='run_dbt_stg_usage',
        bash_command='cd ~/projects/airflow_elt/dbt_folder && dbt run --select stg_usage')
    update_stg_created_date = BashOperator(
        task_id='run_dbt_stg_created_date',
        bash_command='cd ~/projects/airflow_elt/dbt_folder && dbt run --select stg_created_date')

    update_mart_monthly_usage = BashOperator(
        task_id='run_dbt_monthly_usage',
        bash_command='cd ~/projects/airflow_elt/dbt_folder && dbt run --select mart_monthly_usage'
    )

    update_mart_close_to_limit = BashOperator(
        task_id='run_dbt_close_to_limit',
        bash_command='cd ~/projects/airflow_elt/dbt_folder && dbt run --select mart_close_to_limit'
    )

    update_mart_monthly_subscriptions = BashOperator(
        task_id='run_dbt_monthly_subscriptions',
        bash_command='cd ~/projects/airflow_elt/dbt_folder && dbt run --select mart_monthly_subscriptions'
    )

    update_mart_manual_limit = BashOperator(
        task_id='run_dbt_manual_limit',
        bash_command='cd ~/projects/airflow_elt/dbt_folder && dbt run --select mart_manual_limit'
    )
    update_stg_created_date
    update_mart_monthly_subscriptions
    update_mart_manual_limit
    update_stg_usage >> update_mart_monthly_usage
    update_stg_usage >> update_mart_close_to_limit
if __name__ == "__main__":
    dag.test()