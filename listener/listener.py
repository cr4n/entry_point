#!/usr/bin/env python
from web3 import Web3
import json, logging
import os
import time
import pika

# entry_point_address = Web3.to_checksum_address("0x5ff137d4b0fdcd49dca30c7cf57e578a026d2789")

def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger(__name__)

logger = setup_logging()

# Verify if environment variables are loaded
alchemy_url = os.getenv('ALCHEMY_URL')
if not alchemy_url:
    raise ValueError("ALCHEMY_URL environment variable not set or is None")
else:
    logger.info(f"ALCHEMY_URL: {alchemy_url}")


entry_point_address = os.getenv('ENTRY_POINT_ADDRESS')
if not entry_point_address:
    raise ValueError("entry_point_address environment variable not set or is None")
else:
    logger.info(f"entry_point_address: {entry_point_address}")

# Setup Web3 connection using Alchemy
logger.info('Connecting to Alchemy...')
web3 = Web3(Web3.HTTPProvider(alchemy_url))

# Setup RabbitMQ connection 
logger.info('Waiting for RabbitMQ...')
time.sleep(10)
logger.info('Connecting to RabbitMQ...')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()
channel.queue_declare(queue='user_operations')
logger.info('Connected to RabbitMQ!')

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
    channel.basic_publish(exchange='', routing_key='user_operations', body=event_json)
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
finally:
    logger.info('Stopped monitoring events')
    connection.close()
    logger.info('Connection to RabbitMQ closed')
