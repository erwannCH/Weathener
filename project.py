from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
        'project',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='Project DAG',
        schedule_interval=None,
        start_date=datetime(2023, 1, 1),
        catchup=False,
        tags=['example'],
) as dag:
    dag.doc_md = """This is my project DAG."""

    # 1st data source
    def raw_1():
        print("Hello Airflow - This is Task 1")

    def prepared_1():
        print("Hello Airflow - This is Prepared 1")

    # 2nd data source
    def raw_2():
        print("Hello Airflow - This is Task 2")

    def prepared_2():
        print("Hello Airflow - This is Prepared 2")

    raw_1 = PythonOperator(
        task_id='task1',
        python_callable=raw_1,
    )

    prepared_1 = PythonOperator(
       task_id='format1',
       python_callable=prepared_1,
    )

    raw_2 = PythonOperator(
       task_id='task2',
       python_callable=raw_2
   )

    prepared_2 = PythonOperator(
       task_id='format2',
       python_callable=prepared_2,
   )

    raw_1 >> prepared_1
    raw_2 >> prepared_2
