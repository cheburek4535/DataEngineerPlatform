from confluent_kafka import Consumer, KafkaError
import json
import logging
import signal
import sys
from services.currency.consumer_service import process_raw_currency_message
from logger import logger

class CurrencyKafkaConsumer:
    def __init__(self):
        self.consumer_config = {
            'bootstrap.servers': 'redpanda:9092',
            'group.id': 'currency_processor_group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,  # Ручной коммит после обработки
            'session.timeout.ms': 30000,
            'max.poll.interval.ms': 300000,
        }
        self.running = True
        self.consumer = None

        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def shutdown(self, signum, frame):
        logger.info("Shutting down consumer...")
        self.running = False

    def start(self):
        try:
            self.consumer = Consumer(self.consumer_config)
            self.consumer.subsribe(['currencies.raw'])
            logger.info("Consumer started")

            while self.running:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.error("Reached end of partition")
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        continue
                try:
                    value = json.loads(msg.value().decode('utf-8'))
                    logger.info(f"Received currency data for date: {value.get('Date', 'unknown')}")
                    success = process_raw_currency_message(value)
                    if success:
                        self.consumer.commit(msg)
                        logger.info("Commited offset for message")
                    else:
                        logger.error(f"Failed to process message, offset not committed")
                except json.JSONDecodeError as e:
                        logger.error(f"Failed to decode message: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")

        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            if self.consumer:
                self.consumer.close()
                logger.info("Consumer closed")


if name == '__main__':
    consumer = CurrencyKafkaConsumer()
    consumer.start()
