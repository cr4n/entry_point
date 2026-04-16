#!/usr/bin/env python
from collections import OrderedDict
from pathlib import Path
import json, logging
import os
import random
import time
from eth_abi import encode
import pika
from web3 import Web3
from web3.middleware import geth_poa_middleware

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "10"))
RETRY_BASE_DELAY_SECONDS = float(os.getenv("RETRY_BASE_DELAY_SECONDS", "5"))
RETRY_MAX_DELAY_SECONDS = float(os.getenv("RETRY_MAX_DELAY_SECONDS", "60"))

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://rabbitmq:5672")
QUEUE_NAME = os.getenv("RABBITMQ_QUEUE", "user_operations")
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
MAX_TRANSACTION_CACHE_SIZE = int(os.getenv("MAX_TRANSACTION_CACHE_SIZE", "512"))

def _retry_sleep_seconds(attempt: int) -> float:
    delay = min(RETRY_MAX_DELAY_SECONDS, RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1)))
    jitter = random.uniform(0, delay * 0.2)
    return delay + jitter

def _load_entry_point_abi():
    abi_path = Path(__file__).with_name("abi_entry_point.json")
    with abi_path.open(encoding="utf-8") as abi_file:
        return json.load(abi_file)

def _normalize_address(value):
    if not value:
        return None
    return Web3.to_checksum_address(value)

def _bytes_to_hex(value: bytes) -> str:
    return Web3.to_hex(value)

def _normalize_hash_hex(value) -> str:
    if isinstance(value, bytes):
        value = value.hex()
    elif hasattr(value, "hex") and not isinstance(value, str):
        value = value.hex()
    return value[2:] if isinstance(value, str) and value.startswith("0x") else value

def _keccak_hex(value: bytes) -> str:
    return Web3.keccak(value).hex()

def _address_from_prefixed_bytes(value: bytes):
    if len(value) < 20:
        return None
    return Web3.to_checksum_address("0x" + value[:20].hex())

def _nonce_parts(nonce: int) -> tuple[int, int]:
    return nonce >> 64, nonce & ((1 << 64) - 1)

def _user_op_gas_price(max_fee_per_gas: int, max_priority_fee_per_gas: int, base_fee_per_gas: int) -> int:
    return min(max_fee_per_gas, max_priority_fee_per_gas + base_fee_per_gas)

def _required_prefund(user_op: dict, paymaster: str) -> int:
    multiplier = 3 if paymaster and paymaster != ZERO_ADDRESS else 1
    required_gas = (
        user_op["callGasLimit"]
        + (user_op["verificationGasLimit"] * multiplier)
        + user_op["preVerificationGas"]
    )
    return required_gas * user_op["maxFeePerGas"]

def _compute_user_op_hash(user_op: dict, chain_id: int) -> str:
    packed_user_op = encode(
        [
            "address",
            "uint256",
            "bytes32",
            "bytes32",
            "uint256",
            "uint256",
            "uint256",
            "uint256",
            "uint256",
            "bytes32",
        ],
        [
            user_op["sender"],
            user_op["nonce"],
            Web3.keccak(user_op["initCode"]),
            Web3.keccak(user_op["callData"]),
            user_op["callGasLimit"],
            user_op["verificationGasLimit"],
            user_op["preVerificationGas"],
            user_op["maxFeePerGas"],
            user_op["maxPriorityFeePerGas"],
            Web3.keccak(user_op["paymasterAndData"]),
        ],
    )
    return _normalize_hash_hex(Web3.keccak(
        encode(
            ["bytes32", "address", "uint256"],
            [Web3.keccak(packed_user_op), ENTRY_POINT_ADDRESS, chain_id],
        )
    ).hex())

def _normalize_user_op(user_op: dict) -> dict:
    return {
        "sender": _normalize_address(user_op["sender"]),
        "nonce": int(user_op["nonce"]),
        "initCode": bytes(user_op["initCode"]),
        "callData": bytes(user_op["callData"]),
        "callGasLimit": int(user_op["callGasLimit"]),
        "verificationGasLimit": int(user_op["verificationGasLimit"]),
        "preVerificationGas": int(user_op["preVerificationGas"]),
        "maxFeePerGas": int(user_op["maxFeePerGas"]),
        "maxPriorityFeePerGas": int(user_op["maxPriorityFeePerGas"]),
        "paymasterAndData": bytes(user_op["paymasterAndData"]),
        "signature": bytes(user_op["signature"]),
    }

def _enrich_user_op(user_op: dict, bundle_index: int, bundle_size: int, beneficiary: str, base_fee_per_gas: int, chain_id: int) -> tuple[str, dict]:
    factory = _address_from_prefixed_bytes(user_op["initCode"])
    paymaster = _address_from_prefixed_bytes(user_op["paymasterAndData"]) or ZERO_ADDRESS
    nonce_key, nonce_sequence = _nonce_parts(user_op["nonce"])
    user_op_hash = _compute_user_op_hash(user_op, chain_id)

    enriched_user_op = {
        "beneficiary": beneficiary,
        "bundleIndex": bundle_index,
        "bundleSize": bundle_size,
        "callGasLimit": user_op["callGasLimit"],
        "verificationGasLimit": user_op["verificationGasLimit"],
        "preVerificationGas": user_op["preVerificationGas"],
        "maxFeePerGas": user_op["maxFeePerGas"],
        "maxPriorityFeePerGas": user_op["maxPriorityFeePerGas"],
        "userOpGasPrice": _user_op_gas_price(
            user_op["maxFeePerGas"],
            user_op["maxPriorityFeePerGas"],
            base_fee_per_gas,
        ),
        "requiredPrefund": _required_prefund(user_op, paymaster),
        "factory": factory,
        "nonceKey": nonce_key,
        "nonceSequence": nonce_sequence,
        "initCodeHash": _keccak_hex(user_op["initCode"]),
        "initCodeLength": len(user_op["initCode"]),
        "callDataHash": _keccak_hex(user_op["callData"]),
        "callDataLength": len(user_op["callData"]),
        "paymasterAndDataHash": _keccak_hex(user_op["paymasterAndData"]),
        "paymasterAndDataLength": len(user_op["paymasterAndData"]),
        "paymasterDataLength": max(len(user_op["paymasterAndData"]) - 20, 0),
        "signatureHash": _keccak_hex(user_op["signature"]),
        "signatureLength": len(user_op["signature"]),
    }

    return user_op_hash, enriched_user_op

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

ENTRY_POINT_ADDRESS = _normalize_address(entry_point_address)
ENTRY_POINT_ABI = _load_entry_point_abi()

# Setup Web3 connection using Alchemy
logger.info('Connecting to Alchemy...')
web3 = Web3(Web3.HTTPProvider(alchemy_url))
web3.middleware_onion.inject(geth_poa_middleware, layer=0)
CHAIN_ID = web3.eth.chain_id
transaction_cache = OrderedDict()

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
contract = web3.eth.contract(address=ENTRY_POINT_ADDRESS, abi=ENTRY_POINT_ABI)

def _cache_transaction_context(transaction_hash_hex: str, context: dict):
    transaction_cache[transaction_hash_hex] = context
    transaction_cache.move_to_end(transaction_hash_hex)
    while len(transaction_cache) > MAX_TRANSACTION_CACHE_SIZE:
        transaction_cache.popitem(last=False)

def _decode_transaction_context(transaction_hash):
    transaction_hash_hex = _bytes_to_hex(transaction_hash)
    if transaction_hash_hex in transaction_cache:
        transaction_cache.move_to_end(transaction_hash_hex)
        return transaction_cache[transaction_hash_hex]

    transaction = web3.eth.get_transaction(transaction_hash)
    receipt = web3.eth.get_transaction_receipt(transaction_hash)
    block = web3.eth.get_block(transaction["blockNumber"])

    context = {
        "bundler": _normalize_address(transaction["from"]),
        "beneficiary": None,
        "bundleSize": None,
        "blockTimestamp": int(block["timestamp"]),
        "blockBaseFeePerGas": int(block.get("baseFeePerGas") or 0),
        "effectiveGasPrice": int(receipt.get("effectiveGasPrice") or transaction.get("gasPrice") or 0),
        "entryPointMethod": None,
        "opsByHash": {},
    }

    try:
        function_call, function_args = contract.decode_function_input(transaction["input"])
    except ValueError as error:
        logger.warning("Failed to decode transaction input for %s: %s", transaction_hash_hex, error)
        _cache_transaction_context(transaction_hash_hex, context)
        return context

    context["entryPointMethod"] = function_call.fn_name
    if function_call.fn_name != "handleOps":
        _cache_transaction_context(transaction_hash_hex, context)
        return context

    beneficiary = _normalize_address(function_args["beneficiary"])
    ops_by_hash = {}

    for bundle_index, raw_user_op in enumerate(function_args["ops"]):
        user_op = _normalize_user_op(raw_user_op)
        user_op_hash, enriched_user_op = _enrich_user_op(
            user_op,
            bundle_index,
            len(function_args["ops"]),
            beneficiary,
            context["blockBaseFeePerGas"],
            CHAIN_ID,
        )
        ops_by_hash[user_op_hash] = enriched_user_op

    context["beneficiary"] = beneficiary
    context["bundleSize"] = len(function_args["ops"])
    context["opsByHash"] = ops_by_hash
    _cache_transaction_context(transaction_hash_hex, context)
    return context

# Convert event to dictionary
def to_dict(event):
    event_dict = {
        "userOpHash": _normalize_hash_hex(event['args']['userOpHash']),
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
        "blockNumber": event['blockNumber'],
        "chainId": CHAIN_ID,
    }

    transaction_context = _decode_transaction_context(event["transactionHash"])
    event_dict.update(
        {
            "bundler": transaction_context["bundler"],
            "beneficiary": transaction_context["beneficiary"],
            "bundleSize": transaction_context["bundleSize"],
            "blockTimestamp": transaction_context["blockTimestamp"],
            "blockBaseFeePerGas": transaction_context["blockBaseFeePerGas"],
            "effectiveGasPrice": transaction_context["effectiveGasPrice"],
            "entryPointMethod": transaction_context["entryPointMethod"],
        }
    )

    enriched_user_op = transaction_context["opsByHash"].get(event_dict["userOpHash"])
    if enriched_user_op is None and transaction_context["entryPointMethod"] == "handleOps":
        logger.warning("Could not match decoded UserOperation for %s", event_dict["userOpHash"])
    elif enriched_user_op is not None:
        event_dict.update(enriched_user_op)

    return event_dict

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
