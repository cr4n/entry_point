#!/usr/bin/env python
from web3 import Web3
import json, logging
import os
import random
import time
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

def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger(__name__)

logger = setup_logging()

# Verify if environment variables are loaded
alchemy_url = os.getenv('ALCHEMY_URL')
if not alchemy_url:
    raise ValueError("ALCHEMY_URL environment variable not set or is None")
else:
    logger.info("ALCHEMY_URL loaded successfully")


entry_point_address = os.getenv('ENTRY_POINT_ADDRESS')
if not entry_point_address:
    raise ValueError("ENTRY_POINT_ADDRESS environment variable not set or is None")
else:
    logger.info(f"ENTRY_POINT_ADDRESS: {entry_point_address}")

# Setup Web3 connection using Alchemy
logger.info('Connecting to Alchemy...')
web3 = Web3(Web3.HTTPProvider(alchemy_url))

class RabbitMQPublisher:
    def __init__(self, url: str, queue_name: str):
        self.url = url
        self.queue_name = queue_name
        self.connection = None
        self.channel = None

    def _connect(self):
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                logger.info(f"Connecting to RabbitMQ (attempt {attempt}/{MAX_RETRIES})...")
                self.connection = pika.BlockingConnection(pika.URLParameters(self.url))
                self.channel = self.connection.channel()
                self.channel.queue_declare(queue=self.queue_name)
                logger.info("Connected to RabbitMQ!")
                return
            except pika.exceptions.AMQPConnectionError:
                if attempt < MAX_RETRIES:
                    sleep_for = _retry_sleep_seconds(attempt)
                    logger.warning(f"RabbitMQ not ready, retrying in {sleep_for:.1f}s...")
                    time.sleep(sleep_for)
                else:
                    logger.error("Failed to connect to RabbitMQ after max retries")
                    raise

    def publish(self, body: str | bytes):
        if self.connection is None or self.channel is None or self.connection.is_closed or self.channel.is_closed:
            self._connect()
        if isinstance(body, str):
            body = body.encode("utf-8")
        try:
            self.channel.basic_publish(exchange="", routing_key=self.queue_name, body=body)
        except (pika.exceptions.AMQPError, OSError):
            logger.warning("Publish failed, reconnecting and retrying once...")
            self.close()
            self._connect()
            self.channel.basic_publish(exchange="", routing_key=self.queue_name, body=body)

    def close(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()
        self.connection = None
        self.channel = None

publisher = RabbitMQPublisher(RABBITMQ_URL, QUEUE_NAME)

# Define the contract ABI
contract_abi = [
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "bytes32", "name": "userOpHash", "type": "bytes32"},
            {"indexed": True, "internalType": "address", "name": "sender", "type": "address"},
            {"indexed": True, "internalType": "address", "name": "paymaster", "type": "address"},
            {"indexed": False, "internalType": "uint256", "name": "nonce", "type": "uint256"},
            {"indexed": False, "internalType": "bool", "name": "success", "type": "bool"},
            {"indexed": False, "internalType": "uint256", "name": "actualGasCost", "type": "uint256"},
            {"indexed": False, "internalType": "uint256", "name": "actualGasUsed", "type": "uint256"},
        ],
        "name": "UserOperationEvent",
        "type": "event"
    }
]

# Initialize the contract
contract = web3.eth.contract(address=entry_point_address, abi=contract_abi)

# Convert event to dictionary
def to_dict(event):
    return {
        "userOpHash": event['args']['userOpHash'].hex(),
        "sender": event['args']['sender'],
        "paymaster": event['args']['paymaster'],
        "nonce": event['args']['nonce'],
        "success": event['args']['success'],
        "actualGasCost": event['args']['actualGasCost'],
        "actualGasUsed": event['args']['actualGasUsed'],
        "event": event['event'],
        "logIndex": event['logIndex'],
        "transactionIndex": event['transactionIndex'],
        "transactionHash": event['transactionHash'].hex(),
        "address": event['address'],
        "blockHash": event['blockHash'].hex(),
        "blockNumber": event['blockNumber']
    }

# Dump json to RabbitMQ
def handle_event(event):
    event_dict = to_dict(event)
    event_json = json.dumps(event_dict)
    publisher.publish(event_json)
    logger.info(f"Event written to RabbitMQ: {event_json}")

# Setup event filter
logger.info('Setting up connection to Alchemy')
event_filter = contract.events.UserOperationEvent.create_filter(fromBlock='latest')
logger.info('Start monitoring events...')

# Listen for events and handle them until the program is interrupted
try:
    while True:
        for event in event_filter.get_new_entries():
            logger.info('New event detected!')
            handle_event(event)
        time.sleep(2)
finally:
    logger.info('Stopped monitoring events')
    publisher.close()
    logger.info('Connection to RabbitMQ closed')
