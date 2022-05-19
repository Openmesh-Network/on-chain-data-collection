import json
import websockets
import asyncio
import dotenv
from web3 import Web3
from sink_connector.kafka_producer import KafkaProducer
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
    return Web3.toJSON(transaction)


async def get_all_transactions(web3, conf, producer):
    async with websockets.connect(conf["INFURA_WS_ENDPOINT"]) as ws:
        await ws.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": ["newHeads"]}))
        await ws.recv()
        logging.info("Connected to websocket")
        while True:
            await ws.recv()
            block = web3.eth.getBlock("latest", full_transactions=True)
            for transaction in block.transactions:
                producer.produce(key=str(transaction.hash),
                                 msg=serialize_transaction(transaction))
                logging.info("Produced transaction hash: %s",
                             str(transaction.hash))
            logging.info("Produced block number: %s", block.number)


if __name__ == "__main__":
    producer = KafkaProducer("ethereum-raw")
    conf = dotenv.dotenv_values('./keys/.env')
    web3 = Web3(Web3.WebsocketProvider(conf["INFURA_WS_ENDPOINT"]))
    asyncio.run(get_all_transactions(web3, conf, producer))
