from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
from include.scrapper import scrapper
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

DBT_PROJECT_DIR = "/usr/local/airflow/dbt"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='olympic_pipeline',
    default_args=default_args,
    description='Scrape Olympic data then run dbt seed, run and test',
    start_date=datetime(2025, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['olympics', 'scraping', 'dbt'],
) as dag:

    scraping_task = PythonOperator(
        task_id='scraping_task',
        python_callable=scrapper,
    )

    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt seed --profiles-dir {DBT_PROJECT_DIR}',
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROJECT_DIR}',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROJECT_DIR}',
    )

    scraping_task >> dbt_seed >> dbt_run >> dbt_test
