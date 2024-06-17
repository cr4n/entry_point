#!/usr/bin/env python
import os
import json
import logging
import time
import psycopg2 as pg
import pika
import pandas as pd

def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger(__name__)

logger = setup_logging()

# PostgreSQL connection parameters
postgres_params = {
    'host': os.getenv('POSTGRES_HOST'),
    'port': 5432,
    'dbname': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD')
}

def connect_to_postgres(params):
    """
    Connects to the PostgreSQL db
    """
    try:
        conn = pg.connect(**params)
        logger.info("Connected to PostgreSQL database")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise

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

        create_bundlers_table_query = '''
        CREATE TABLE IF NOT EXISTS pipeline.bundlers (
            entity_name VARCHAR(255),
            address VARCHAR(255),
            ethereum BOOLEAN,
            polygon BOOLEAN,
            arbitrum BOOLEAN,
            optimism BOOLEAN,
            bnb BOOLEAN,
            avalanche BOOLEAN,
            base BOOLEAN
        )
        '''
        cur.execute(create_bundlers_table_query)
        conn.commit()
        logger.info("Table `bundlers` ensured to exist")

    except Exception as e:
        logger.error(f"Failed to setup PostgreSQL database: {e}")
        conn.rollback()
        raise

def get_rabbitmq_connection():
    """
    Connects to the RabbitMQ server.
    """
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        logger.info("Connected to RabbitMQ")
        return connection
    except Exception as e:
        logger.error(f"Failed to connect to RabbitMQ: {e}")
        raise

def insert_event_to_db(conn, event):
    """
    Inserts events from the queue into a table.    
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

def callback(ch, method, properties, body):
    """
    Callback function to be called when a message is received from the queue.
    """
    logger.info(f"Received event - {body}")
    event = json.loads(body)
    conn = None
    try:
        conn = connect_to_postgres(postgres_params)
        insert_event_to_db(conn, event)
    finally:
        if conn:
            conn.close()


def load_csv_to_postgres(csv_file_path, conn, table_name):
    """
    Load CSV data into a PostgreSQL table.
    """
    # Read the CSV file
    df = pd.read_csv(csv_file_path)
    
    # Create a temporary CSV file
    temp_csv = "/tmp/temp_bundlers.csv"
    df.to_csv(temp_csv, index=False, header=False)

    try:
        cur = conn.cursor()
        with open(temp_csv, 'r') as f:
            cur.copy_from(f, table_name, sep=',')
        conn.commit()
        logger.info(f"Data loaded into table {table_name} successfully.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to load data into table {table_name}: {e}")

    
def main():
    """
    Main function to set up the PostgreSQL database, connect to RabbitMQ,
    and start consuming messages.
    """
    time.sleep(10) # Wait for RabbitMQ and Postgres to be ready

    logger.info("Connecting to PostgreSQL...")
    conn = connect_to_postgres(postgres_params)
    setup_database(conn)
    load_csv_to_postgres('bundlers.csv', conn, 'bundlers')
    conn.close()

    logger.info("Connecting to RabbitMQ...")
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue='user_operations')

    channel.basic_consume(queue='user_operations', on_message_callback=callback, auto_ack=True)
    logger.info('Waiting for messages. CTRL+C to exit.')
    try:
        channel.start_consuming()
    except KeyboardInterrupt: # CTRL + C halts the consumer
        channel.stop_consuming()
    finally:
        connection.close()
        logger.info('Connection to RabbitMQ closed')

if __name__ == "__main__":
    main()