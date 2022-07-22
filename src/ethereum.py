import json
import time
import websockets
import asyncio
import dotenv
from web3s import Web3s
from web3s.datastructures import AttributeDict
from hexbytes import HexBytes
from sink_connector.kafka_producer import KafkaProducer
from helpers.normalise_transaction import normalise_transaction
from helpers.normalise_block import normalise_block
import logging
import sys

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='[%(asctime)s] - %(levelname)s - %(message)s',
)

logging.info("Starting collector")

def serialize_transaction(transaction):
    """
    Helper for serializing a transaction to a dictionary
    """
    return json.loads(json.dumps(transaction, default=vars))

class Web3JsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, HexBytes):
            return obj.hex()
        if isinstance(obj, AttributeDict):
            return obj.__dict__
        return super().default(obj)

async def get_all_transactions(web3, conf, producer):
    async with websockets.connect(conf["INFURA_WS_ENDPOINT"]) as ws:
        await ws.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": ["newHeads"]}))
        await ws.recv()
        logging.info("Connected to websocket")
        old_num = -1
        while True:
            new_block = await ws.recv()
            new_num = int(json.loads(new_block)["params"]["result"]["number"], 16)
            if new_num == old_num:
                logging.error("Getting same block twice")
                continue
            if old_num != -1 and new_num > old_num + 1:
                logging.error("One or more blocks have been skipped")
            block = await web3.eth.getBlock(new_num, full_transactions=True)
            block = json.loads(json.dumps(block, cls=Web3JsonEncoder))
            block_msg = normalise_block(block)
            for transaction in block['transactions']:
                new_tx = normalise_transaction(transaction, block_msg)
                producer.produce(key=new_tx["tx_hash"], msg=new_tx)
            logging.info("Produced block number: %s", block["number"])
            old_num = new_num


if __name__ == "__main__":
    producer = KafkaProducer("ethereum-raw")
    conf = dotenv.dotenv_values('./keys/.env')
    web3 = Web3s(Web3s.HTTPProvider(conf["INFURA_REST_ENDPOINT"]))
    asyncio.run(get_all_transactions(web3, conf, producer))
