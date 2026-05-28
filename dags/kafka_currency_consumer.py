from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from logger import logger


def run_currency_consumer():
    """
    Запускает consumer для обработки одного батча сообщений.
    Используется внутри DAG для периодической обработки.
    """
    from confluent_kafka import Consumer, KafkaError
    import json
    from services.currency.consumer_service import process_raw_currency_message

    consumer_config = {
        'bootstrap.servers': 'redpanda:9092',
        'group.id': 'currency_processor_group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'session.timeout.ms': 30000,
        'max.poll.interval.ms': 300000,
    }

    consumer = Consumer(consumer_config)
    consumer.subscribe(['currencies.raw'])

    messages_processed = 0
    max_messages = 150
    timeout_seconds = 60

    import time
    start_time = time.time()

    try:
        while messages_processed < max_messages:
            if time.time() - start_time > timeout_seconds:
                logger.info("Timeout reached")
                break

            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                continue

            value = json.loads(msg.value().decode('utf-8'))
            logger.info(f"Processing currency data: {value.get('Date')}")

            if process_raw_currency_message(value):
                consumer.commit(msg)
                messages_processed += 1
            else:
                logger.error("Failed to process message")
    except Exception as e:
        logger.error(e)
        raise

    finally:
        consumer.close()
        logger.info(f"Processed {messages_processed} messages")

default_args = {
    'owner': 'Cheburek',
    'start_date': datetime(2026, 5, 21),
    'retries': 0
}

with DAG(
        'kafka_currency_consumer',
    default_args=default_args,
        start_date=datetime(2026, 5, 21),
        description='Process currency data from Kafka',
        schedule_interval=None,
        catchup=False,
    max_active_runs=1,
        tags=['currency', 'kafka', 'consumer'],
) as dag:
    consume_and_process = PythonOperator(
        task_id='consume_and_process_currency',
        python_callable=run_currency_consumer,
    )