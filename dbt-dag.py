from airflow import DAG
from airflow.providers.apache.airflow.operators.bash import BashOperator
from datetime import datetime
 
dag = DAG(
    'dbt_airflow_example',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
)
 
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /path/to/dbt_project && dbt run',
    dag=dag,
)
 
dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='cd /path/to/dbt_project && dbt test',
    dag=dag,
)
 
dbt_run >> dbt_test
