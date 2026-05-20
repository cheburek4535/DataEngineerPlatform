# dags/test_redpanda_connection.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


def test_producer():
    """Тестирует producer с confluent_kafka"""
    from services.kafka.producer_confluent import create_producer, send_message

    # Создаем producer
    producer = create_producer()

    if producer:
        logger.info("✅ Producer created successfully")

        # Отправляем тестовое сообщение
        test_data = {
            "test": True,
            "timestamp": datetime.now().isoformat(),
            "message": "Test from Airflow"
        }

        result = send_message('currencies.raw', key='test', value=test_data)

        if result:
            logger.info("✅ Test message sent successfully")
        else:
            logger.error("❌ Failed to send test message")
    else:
        logger.error("❌ Failed to create producer")


def test_consumer():
    """Тестирует consumer"""
    from services.kafka.consumer_confluent import read_currency

    read_currency(max_messages=5, timeout_ms=15000)


with DAG(
        'test_redpanda_connection',
        start_date=datetime(2024, 1, 1),
        schedule_interval=None,
        catchup=False,
        tags=['test'],
) as dag:
    test_prod = PythonOperator(
        task_id='test_producer',
        python_callable=test_producer,
    )

    test_cons = PythonOperator(
        task_id='test_consumer',
        python_callable=test_consumer,
    )

    test_prod >> test_cons