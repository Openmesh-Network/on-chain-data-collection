import dotenv
import websockets
import asyncio
import json
import requests
from web3 import Web3
from hexbytes import HexBytes
import time
from sink_connector.redis_producer import RedisProducer
import logging
import sys

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
)

graph_endpoint="https://api.thegraph.com/subgraphs/name/dodoex/dodoex-v2"

class Web3JsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, HexBytes):
            return obj.hex()
        if obj.__class__.__name__ == "AttributeDict":
            return obj.__dict__
        return super().default(obj)

def get_top_100_pairs():
    query = """
    {
        pairs(first: 100, orderBy: volumeUSD, orderDirection: desc, where: {type_not: "VIRTUAL"}) {
            id
            baseToken {
                name
                symbol
            }
            quoteToken {
                name
                symbol
            }
            type
        }
    }

    """
    pairs = {}
    res = requests.post(url=graph_endpoint, json={"query": query})
    result = res.json()
    for pair in result['data']['pairs']:
        pairs[pair['id']] = {
            'baseTokenName': pair['baseToken']['name'],
            'quoteTokenName': pair['quoteToken']['name'],
            'baseTokenSymbol': pair['baseToken']['symbol'],
            'quoteTokenSymbol': pair['quoteToken']['symbol']
        }
    return pairs

def convert_list_to_hexbytes(list_):
    for i in range(len(list_)):
        if isinstance(list_[i], str):
            list_[i] = HexBytes(list_[i])
        elif isinstance(list_[i], list):
            convert_list_to_hexbytes(list_[i])
        elif isinstance(list_[i], dict):
            convert_dict_to_hexbytes(list_[i])
    return list_

def convert_dict_to_hexbytes(dict_):
    for key in dict_:
        if isinstance(dict_[key], str):
            dict_[key] = HexBytes(dict_[key])
        elif isinstance(dict_[key], dict):
            convert_dict_to_hexbytes(dict_[key])
        elif isinstance(dict_[key], list):
            convert_list_to_hexbytes(dict_[key])
    return dict_

def process_base_buy(contract, hex_log):
    processed_log = contract.events.BuyBaseToken().processLog(hex_log)
    return processed_log

def process_base_sell(contract, hex_log):
    processed_log = contract.events.SellBaseToken().processLog(hex_log)
    return processed_log

def process_pair_log(contract, log, topics, pairs):
    log_topic_0 = log['topics'][0]
    hex_log = convert_dict_to_hexbytes(log.copy())
    process_function = topics[log_topic_0]
    print(json.dumps(hex_log, cls=Web3JsonEncoder, indent=4))
    processed_log = process_function(contract, hex_log).__dict__
    event_pair_info = pairs[processed_log['address'].hex()]
    processed_log['blockNumber'] = int(processed_log['blockNumber'].hex(), 16)
    processed_log['logIndex'] = int(processed_log['logIndex'].hex(), 16)
    processed_log['transactionIndex'] = int(processed_log['transactionIndex'].hex(), 16)
    processed_log['type'] = processed_log.pop('event')
    processed_log['exchange'] = 'DoDoEx'
    processed_log['data'] = processed_log.pop('args').__dict__
    processed_log['data']['baseTokenName'] = event_pair_info['baseTokenName']
    processed_log['data']['quoteTokenName'] = event_pair_info['quoteTokenName']
    processed_log['data']['baseTokenSymbol'] = event_pair_info['baseTokenSymbol']
    processed_log['data']['quoteTokenSymbol'] = event_pair_info['quoteTokenSymbol']
    processed_log['processedTimestamp'] = int(time.time() * (10 ** 3))
    return processed_log

async def subscribe_to_logs(ws, pair, msg, topics):
    msg["params"][1]["address"] = pair
    for topic in topics.keys():
        msg["params"][1]["topics"] = [topic]
        await ws.send(json.dumps(msg))

async def handle_events():
    conf = dotenv.dotenv_values("./keys/.env")
    ws_endpoint = conf["INFURA_WS_ENDPOINT"]
    # Topics for buys and sells, mapping to event handlers
    topics = {
        "0xd8648b6ac54162763c86fd54bf2005af8ecd2f9cb273a5775921fd7f91e17b2d": process_base_sell, "0xe93ad76094f247c0dafc1c61adc2187de1ac2738f7a3b49cb20b2263420251a3": process_base_buy
    }
    contract_addr = "0x8876819535b48b551C9e97EBc07332C7482b4b2d"

    web3 = Web3(Web3.WebsocketProvider(ws_endpoint))

    contract_as_parser = web3.eth.contract(address=contract_addr, abi=json.load(open("src/ABIs/dodo_pair_abi.json")))

    event_filter_sub_message = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_subscribe",
        "params": [
            "logs",
            {
                "address": "",
                "topics": [],
            }
        ]
    }

    pairs = get_top_100_pairs()

    producer = RedisProducer('dodo-raw')

    async with websockets.connect(ws_endpoint) as infura_connection:
        tasks = []
        for pair in pairs.keys():
            tasks.append(subscribe_to_logs(infura_connection, pair, event_filter_sub_message.copy(), topics))
        await asyncio.gather(*tasks)
        for _ in range(len(pairs) * len(topics)):
            msg = await infura_connection.recv()
            while "result" not in json.loads(msg):
                msg = await infura_connection.recv()
        logging.info("Finished Subscribing")
        while True:
            message = await infura_connection.recv()
            processed_log = process_pair_log(contract_as_parser, json.loads(message)['params']['result'], topics, pairs)
            await producer.produce(processed_log['processedTimestamp'], json.dumps(processed_log, cls=Web3JsonEncoder))

if __name__ == "__main__":
    asyncio.run(handle_events())