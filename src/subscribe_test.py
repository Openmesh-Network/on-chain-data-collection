import dotenv
import websockets
import asyncio
import json
import requests
from web3 import Web3
import eth_utils
import binascii
from hexbytes import HexBytes

graph_endpoint="https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2"

def get_top_100_pairs():
    query = """
    {
      pairs(first: 100, orderBy: reserveUSD, orderDirection: desc) {
        id
      }
    }
    """
    pairs = []
    res = requests.post(url=graph_endpoint, json={"query": query})
    result = res.json()
    for pair in result['data']['pairs']:
        pairs.append(pair['id'])
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

def process_swap_log(contract, log):
    hex_log = convert_dict_to_hexbytes(log.copy())
    processed_log = contract.events.Swap().processLog(hex_log)
    return processed_log

async def subscribe_to_swap_logs(ws, pair, msg):
    msg["params"][1]["address"] = pair
    await ws.send(json.dumps(msg))

async def main():
    conf = dotenv.dotenv_values("./keys/.env")
    ws_endpoint = conf["INFURA_WS_ENDPOINT"]
    topic = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
    contract_addr = "0xAE461cA67B15dc8dc81CE7615e0320dA1A9aB8D5"

    web3 = Web3(Web3.WebsocketProvider(ws_endpoint))

    contract_as_parser = web3.eth.contract(address=contract_addr, abi=json.load(open("./src/ABIs/uniswap_pair_abi.json")))

    event_filter_sub_message = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_subscribe",
        "params": [
            "logs",
            {
                "address": "",
                "topics": [topic],
            }
        ]
    }

    pairs = get_top_100_pairs()

    async with websockets.connect(ws_endpoint) as infura_connection:
        tasks = []
        for pair in pairs:
            tasks.append(subscribe_to_swap_logs(infura_connection, pair, event_filter_sub_message.copy()))
        await asyncio.gather(*tasks)
        for _ in range(len(pairs)):
            print(await infura_connection.recv())
        async for message in infura_connection:
            processed_log = process_swap_log(contract_as_parser, json.loads(message)['params']['result'])
            print(json.dumps(processed_log, default=vars, indent=4))

if __name__ == "__main__":
    asyncio.run(main())