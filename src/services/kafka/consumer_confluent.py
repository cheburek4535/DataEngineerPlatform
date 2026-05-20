from confluent_kafka import Consumer, KafkaError, KafkaException
import json
import logging
import time

logger = logging.getLogger(__name__)

consumer_config = {
    'bootstrap.servers': 'redpanda:9092',
    'group.id': 'currency_processor',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'auto.commit.interval.ms': 5000,
    'session.timeout.ms': 30000,
    'max.poll.interval.ms': 300000,
}


def read_currency(max_messages=10, timeout_ms=10000):
    """Читает сообщения из Kafka"""

    consumer = None
    try:
        logger.info("Creating consumer...")
        consumer = Consumer(consumer_config)

        # Подписываемся на топик
        consumer.subscribe(['currencies.raw'])
        logger.info("Subscribed to currencies.raw")

        messages_received = 0
        start_time = time.time()

        while messages_received < max_messages:
            # Проверяем таймаут
            elapsed_ms = (time.time() - start_time) * 1000
            if elapsed_ms > timeout_ms:
                logger.info(f"Timeout reached: {timeout_ms}ms")
                break

            # Получаем сообщение с таймаутом 1 секунда
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"Reached end of partition: {msg.topic()} [{msg.partition()}]")
                    break
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

            # Обрабатываем сообщение
            try:
                value = json.loads(msg.value().decode('utf-8'))
                logger.info(f"Received message from {msg.topic()} [{msg.partition()}]:")
                logger.info(f"Key: {msg.key().decode('utf-8') if msg.key() else 'None'}")
                logger.info(f"Date: {value.get('Date', 'unknown')}")
                logger.info(f"Valutes count: {len(value.get('Valute', {}))}")

                messages_received += 1

            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode message: {e}")
            except Exception as e:
                logger.error(f"Error processing message: {e}")

        logger.info(f"Finished reading. Total messages: {messages_received}")

    except KafkaException as e:
        logger.error(f"Kafka error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        if consumer:
            consumer.close()
            logger.info("Consumer closed")