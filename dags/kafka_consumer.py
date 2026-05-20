# dags/kafka_consumer_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from services.kafka.consumers import read_currency
from datetime import timedelta

default_args = {
    'owner': 'Cheburek',
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': False,
}

with DAG('kafka_currency_consumer',
         default_args=default_args,
         schedule_interval=None,  # Запускается вручную или постоянно работает
         catchup=False) as dag:
    consume_messages = PythonOperator(
        task_id='consume_currency_messages',
        python_callable=read_currency,
        op_kwargs={'max_messages': 10, 'timeout_ms': 60000}
    )