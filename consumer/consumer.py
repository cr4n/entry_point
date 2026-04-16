#!/usr/bin/env python
import os
import json
import logging
import random
import time
import psycopg2 as pg
import pika

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "10"))
RETRY_BASE_DELAY_SECONDS = float(os.getenv("RETRY_BASE_DELAY_SECONDS", "5"))
RETRY_MAX_DELAY_SECONDS = float(os.getenv("RETRY_MAX_DELAY_SECONDS", "60"))

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://rabbitmq:5672")
QUEUE_NAME = os.getenv("RABBITMQ_QUEUE", "user_operations")

def _retry_sleep_seconds(attempt: int) -> float:
    delay = min(RETRY_MAX_DELAY_SECONDS, RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1)))
    jitter = random.uniform(0, delay * 0.2)
    return delay + jitter

def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"{name} environment variable not set or is empty")
    return value

def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError as e:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from e

def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger(__name__)

logger = setup_logging()

# PostgreSQL connection parameters
postgres_params = {
    "host": _require_env("POSTGRES_HOST"),
    "port": _env_int("POSTGRES_PORT", 5432),
    "dbname": _require_env("POSTGRES_DB"),
    "user": _require_env("POSTGRES_USER"),
    "password": _require_env("POSTGRES_PASSWORD"),
}

def connect_to_postgres(params):
    """
    Connects to the PostgreSQL db with retry logic.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            conn = pg.connect(**params)
            logger.info("Connected to PostgreSQL database")
            return conn
        except Exception as e:
            if attempt < MAX_RETRIES:
                logger.warning(f"Failed to connect to PostgreSQL (attempt {attempt}/{MAX_RETRIES}): {e}")
                time.sleep(_retry_sleep_seconds(attempt))
            else:
                logger.error(f"Failed to connect to PostgreSQL after {MAX_RETRIES} attempts: {e}")
                raise

def load_csv_to_postgres(csv_file_path, conn, table_name):
    """
    Load CSV data into a PostgreSQL table.
    """
    try:
        cur = conn.cursor()
        
        with open(csv_file_path, 'r') as f:
            next(f)  # Skip the header row
            cur.copy_from(f, table_name, sep=',')
        conn.commit()
        logger.info(f"Data loaded into table {table_name} successfully.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to load data into table {table_name}: {e}")

def setup_database(conn):
    """
    Creates DB pipeline schema and raw table if they do not exist.
    """
    try:
        cur = conn.cursor()
        ddl_schema = 'CREATE SCHEMA IF NOT EXISTS pipeline'
        cur.execute(ddl_schema)
        conn.commit()
        logger.info("Schema `pipeline` created")

        create_table_query = '''
        CREATE TABLE IF NOT EXISTS pipeline.raw_user_operations (
            snapshot_timestamp TIMESTAMP,
            user_op_hash VARCHAR(255),
            sender VARCHAR(255),
            paymaster VARCHAR(255),
            nonce NUMERIC,
            success BOOLEAN,
            actual_gas_cost NUMERIC(35, 0),
            actual_gas_used NUMERIC(35, 0),
            event VARCHAR(255),
            log_index INTEGER,
            transaction_index INTEGER,
            transaction_hash VARCHAR(255),
            address VARCHAR(255),
            block_hash VARCHAR(255),
            block_number BIGINT
        )
        '''
        cur.execute(create_table_query)
        conn.commit()
        logger.info("Table `raw_user_operations` ensured to exist")

    except Exception as e:
        logger.error(f"Failed to setup PostgreSQL database: {e}")
        conn.rollback()
        raise

def get_rabbitmq_connection():
    """
    Connects to the RabbitMQ server with retry logic.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
            logger.info("Connected to RabbitMQ")
            return connection
        except pika.exceptions.AMQPConnectionError:
            if attempt < MAX_RETRIES:
                sleep_for = _retry_sleep_seconds(attempt)
                logger.warning(
                    f"RabbitMQ not ready (attempt {attempt}/{MAX_RETRIES}), retrying in {sleep_for:.1f}s..."
                )
                time.sleep(sleep_for)
            else:
                logger.error(f"Failed to connect to RabbitMQ after {MAX_RETRIES} attempts")
                raise

def insert_event_to_db(conn, event):
    """
    Inserts events from the queue into a table.
    Reconnects if the connection is closed.
    """
    insert_query = """
    INSERT INTO pipeline.raw_user_operations (
        snapshot_timestamp, user_op_hash, sender, paymaster, nonce, success, actual_gas_cost, actual_gas_used, 
        event, log_index, transaction_index, transaction_hash, address, block_hash, block_number
    )
    VALUES (current_timestamp, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    data_to_insert = (
        event['userOpHash'], event['sender'], event['paymaster'], event['nonce'], 
        event['success'], event['actualGasCost'], event['actualGasUsed'], event['event'], 
        event['logIndex'], event['transactionIndex'], event['transactionHash'], 
        event['address'], event['blockHash'], event['blockNumber']
    )

    try:
        cur = conn.cursor()
        cur.execute(insert_query, data_to_insert)
        conn.commit()
        logger.info("Event inserted into PostgreSQL")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to insert event into PostgreSQL: {e}")
        raise

class Consumer:
    """
    Consumer that maintains a persistent PostgreSQL connection
    and processes messages from RabbitMQ.
    """
    def __init__(self, postgres_params):
        self.postgres_params = postgres_params
        self.db_conn = None

    def get_db_connection(self):
        """Returns the existing connection or creates a new one."""
        if self.db_conn is None or self.db_conn.closed:
            self.db_conn = connect_to_postgres(self.postgres_params)
        return self.db_conn

    def callback(self, ch, method, properties, body):
        """
        Callback function to be called when a message is received from the queue.
        Reuses a persistent database connection for better performance.
        """
        try:
            logger.info(f"Received event - {body}")
            event = json.loads(body)
            conn = self.get_db_connection()
            insert_event_to_db(conn, event)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.error(f"Dropping invalid message (will not requeue): {e}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        except Exception as e:
            logger.error(f"Error processing event: {e}")
            # Reset connection on failure so it reconnects next time
            if self.db_conn and not self.db_conn.closed:
                self.db_conn.close()
            self.db_conn = None
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def close(self):
        """Close the persistent database connection."""
        if self.db_conn and not self.db_conn.closed:
            self.db_conn.close()
            logger.info("PostgreSQL connection closed")

def main():
    """
    Main function to set up the PostgreSQL database, connect to RabbitMQ,
    and start consuming messages.
    """
    logger.info("Connecting to PostgreSQL...")
    conn = connect_to_postgres(postgres_params)
    setup_database(conn)
    conn.close()

    logger.info("Connecting to RabbitMQ...")
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME)
    channel.basic_qos(prefetch_count=10)

    consumer = Consumer(postgres_params)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=consumer.callback, auto_ack=False)
    logger.info('Waiting for messages. CTRL+C to exit.')
    try:
        channel.start_consuming()
    except KeyboardInterrupt: # CTRL + C halts the consumer
        channel.stop_consuming()
    finally:
        consumer.close()
        connection.close()
        logger.info('Connection to RabbitMQ closed')

if __name__ == "__main__":
    main()
