from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from logger import logger

def run_weather_consumer():
    from confluent_kafka import Consumer, KafkaError
    import json
    from services.weather.consumer_service import process_raw_weather_batch

    consumer_config = {
        'bootstrap.servers': 'redpanda:9092',
        'group.id': 'weather_processor_group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'session.timeout.ms': 30000,
        'heartbeat.interval.ms': 10000,
        'fetch.min.bytes': 10000,  # Ждем хотя бы 10KB данных
        'max.poll.interval.ms': 600000,
        'fetch.wait.max.ms': 5000,
    }

    consumer = Consumer(consumer_config)
    logger.info('Подписываемся на weather.raw')
    consumer.subscribe(['weather.raw'])
    logger.info("Подписка успешна")

    messages_processed = 0
    max_messages = 5000
    timeout_seconds = 1500
    BATCH_SIZE = 500

    import time
    start = time.time()

    # Буфер для батча
    batch_messages = []  # сами данные
    batch_kafka_msgs = []  # оригинальные kafka сообщения для коммита

    try:
        while messages_processed < max_messages:
            if time.time() - start > timeout_seconds:
                logger.warning("Timeout reached")
                break
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.warning("Reached end of partition")
                    break
                else:
                    logger.info(f"Ошибка msg: {msg.error()}")
                    continue

            value = json.loads(msg.value().decode('utf-8'))
            logger.info(f"Buffering weather for location {value.get('location_id')}, timestamp {value.get('timestamp')}")

            batch_messages.append(value)
            batch_kafka_msgs.append(msg)

            # Если накопили 500 - обрабатываем батч
            if len(batch_messages) >= BATCH_SIZE:
                logger.info(f"Processing batch of {len(batch_messages)} messages")

                # Обрабатываем весь батч
                if process_raw_weather_batch(batch_messages):
                    # Коммитим только после успешной обработки
                    for kafka_msg in batch_kafka_msgs:
                        consumer.commit(kafka_msg)
                    messages_processed += len(batch_messages)
                    logger.info(f"Batch processed successfully. Total: {messages_processed}")
                else:
                    logger.error("Failed to process batch, messages will be retried")

                # Очищаем буфер
                batch_messages = []
                batch_kafka_msgs = []
    except Exception as e:
        logger.error(e)
        raise

    finally:
        # Обрабатываем оставшиеся сообщения
        if batch_messages:
            logger.info(f"Processing remaining {len(batch_messages)} messages")
            if process_raw_weather_batch(batch_messages):
                for kafka_msg in batch_kafka_msgs:
                    consumer.commit(kafka_msg)
                messages_processed += len(batch_messages)

        consumer.close()
        logger.info(f"Processed {messages_processed} messages")

default_args = {
    'owner': 'Cheburek',
    'start_date': datetime(2026, 5, 21),
    'retries': 0
}

with DAG(
   'kafka_weather_consumer',
    default_args=default_args,
        start_date=datetime(2026, 5, 21),
        description='Process weather data from Kafka',
        schedule_interval=None,
        catchup=False,
        tags=['weather', 'kafka', 'consumer'],
max_active_runs=1,
) as dag:
    consume_and_process = PythonOperator(
        task_id='consume_and_process_weather',
        python_callable=run_weather_consumer,
    )