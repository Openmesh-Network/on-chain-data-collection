import json
from tabnanny import check
from typing import Callable
from web3s import Web3s
import aiohttp
import requests
import asyncio
import dotenv
import re
import time
import websockets
import logging
import sys
import csv

from sink_connector.kafka_producer import KafkaProducer

logging.basicConfig(
    format='[%(asctime)s] %(levelname)s - %(message)s',
    level=logging.INFO,
    stream=sys.stdout
)

# Accesses the Infura ETH websocket node
conf = dotenv.dotenv_values("./keys/.env")
provider = Web3s.HTTPProvider(conf["INFURA_REST_ENDPOINT"])
web3 = Web3s(provider)

graph_endpoint="https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2"

# Uniswap addresses
addresses = json.load(open("./helpers/constants.json"))

# Uniswap ABIs
uniswap_router_abi = json.load(open("./ABIs/uniswap_router_abi.json"))
uniswap_factory_abi = json.load(open("./ABIs/uniswap_factory_abi.json"))
uniswap_pool_abi = json.load(open("./ABIs/uniswap_pool_abi.json"))
uniswap_pair_abi = json.load(open("./ABIs/uniswap_pair_abi.json"))

# Creating Contract objects for all the different smart contracts data will be pulled from
factory_contract = web3.eth.contract(address=addresses["uniswap_factory"], abi=uniswap_factory_abi)
router_contract = web3.eth.contract(address=addresses["uniswap_router"], abi=uniswap_router_abi)

logging.info("Starting collector")

last_prices = {}

def get_top_100_pairs(pairs, web3):
    query = """
    {
      pairs(first: 100, orderBy: reserveUSD, orderDirection: desc) {
        id
      }
    }
    """
    res = requests.post(url=graph_endpoint, json={"query": query})
    result = res.json()
    for pair in result['data']['pairs']:
        pair_contract = web3.eth.contract(address=Web3s.toChecksumAddress(pair['id']), abi=uniswap_pair_abi)
        pairs.append(pair_contract)

def get_global_stats(result):
    request = \
    """{
        uniswapFactory(id: "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"){
            pairCount
            totalVolumeUSD
            totalLiquidityUSD
            txCount
        }
    }"""
    res = requests.post(url=graph_endpoint, json={"query": request})
    query_res = res.json()
    result["total_pairs"] = query_res["data"]["uniswapFactory"]["pairCount"]
    result["total_transactions"] = int(query_res["data"]["uniswapFactory"]["txCount"])
    result["total_USD_volume"] = float(query_res["data"]["uniswapFactory"]["totalVolumeUSD"])
    result["total_USD_liquidity"] = float(query_res["data"]["uniswapFactory"]["totalLiquidityUSD"])

async def get_amount_in_USD(token_address, amount):
    logging.info("Getting USD amount for address %s", token_address)
    if token_address == addresses["USDC"]:
        logging.info("USDC: %d", amount / 10**6)
        return amount / 10**6
    USDC_pair_address = await factory_contract.functions.getPair(token_address, addresses["USDC"]).call()
    USDC_pair_contract = web3.eth.contract(address=USDC_pair_address, abi=uniswap_pair_abi)
    try:
        # reserves = await USDC_pair_contract.functions.getReserves().call()
        # amountInWithFee = amount * 997
        # if await USDC_pair_contract.functions.token0().call() == token_address:
        #     numerator = amountInWithFee * reserves[1]
        #     denominator = reserves[0] * 1000 + amountInWithFee
        #     logging.info("Amount in USD: %s", numerator / denominator)
        #     return numerator / denominator
        # else:
        #     numerator = amountInWithFee * reserves[0]
        #     denominator = reserves[1] * 1000 + amountInWithFee
        #     logging.info("Amount in USD: %s", numerator / denominator)
        #     return numerator / denominator
        if await USDC_pair_contract.functions.token0().call() == token_address:
            new_new_price = await USDC_pair_contract.functions.price0CumulativeLast().call()
        else:
            new_new_price = await USDC_pair_contract.functions.price1CumulativeLast().call()
        curr_time = time.time()
        result = ((new_new_price - last_prices[token_address][0]) / (curr_time - last_prices[token_address][1])) * amount
        logging.info("Amount in USD: %s", result)
        last_prices[token_address] = (new_new_price, curr_time)
        return result
    except Exception as e:
        logging.error(e)
        return 0

    # logging.info("Getting USD amount for address %s", token_address)
    # if token_address == addresses["USDC"]:
    #     logging.info("USDC FIRST -- Amount in USD: %d" % (amount / 10**6))
    #     return amount / 10**6
    # amount = uniswap.get_price_input(token_address, addresses["USDC"], amount) / (10**11)
    # logging.info(f"Amount in USD: {amount}")
    # return amount

async def handle_burn(event, response, token0_address, token1_address, producer):
    response["num_burns_last_block"] += 1
    logging.info("Burn event")
    # total_amount = await get_amount_in_USD(token0_address, event['args']['amount0']) + await get_amount_in_USD(token1_address, event['args']['amount1'])
    # response["total_USD_liquidity"] -= total_amount
    # response["amount_burned_USD"] += total_amount
    new_response = {
        "type": "burn",
        "token0": token0_address,
        "token1": token1_address,
        "amount0": event['args']['amount0'],
        "amount1": event['args']['amount1'],
        "timestamp": time.time() * 10**6
    }
    producer.produce(key=str(new_response["timestamp"]), msg=new_response)
    logging.info("Burn event sent to kafka")

async def handle_mint(event, response, token0_address, token1_address, producer):
    response["num_burns_last_block"] += 1
    logging.info("Mint event")
    # total_amount = await get_amount_in_USD(token0_address, event['args']['amount0']) +  await get_amount_in_USD(token1_address, event['args']['amount1'])
    # response["total_USD_liquidity"] += total_amount
    # response["amount_minted_USD"] += total_amount
    new_response = {
        "type": "mint",
        "token0": token0_address,
        "token1": token1_address,
        "amount0": event['args']['amount0'],
        "amount1": event['args']['amount1'],
        "timestamp": time.time() * 10**6
    }
    producer.produce(key=str(new_response["timestamp"]), msg=new_response)
    logging.info("Mint event sent to kafka")

async def handle_swap(event, response, token0_address, token1_address, producer):
    response["num_swaps_last_block"] += 1
    # amount = event['args']['amount0In'] if event['args']['amount0In'] != 0 else event['args']['amount1In']
    # amount_in_USD = await get_amount_in_USD(token0_address, amount) 
    # response["total_USD_volume"] += amount_in_USD
    # response["amount_swapped_USD"] += amount_in_USD
    logging.info("Swap event")
    new_response = {
        "type": "swap",
        "token0": token0_address,
        "token1": token1_address,
        "amount0In": event['args']['amount0In'],
        "amount1In": event['args']['amount1In'],
        "amount0Out": event['args']['amount0Out'],
        "amount1Out": event['args']['amount1Out'],
        "timestamp": time.time() * 10**6
    }
    producer.produce(key=str(new_response["timestamp"]), msg=new_response)
    logging.info("Swap event sent to kafka")

# Async loop process new events
async def filter_loop(event_filter, handler: Callable, response: dict, flag, token0_address, token1_address, producer, poll_interval: float = 5):
    while True:
        await flag.wait()
        for event in await event_filter.get_new_entries():
            await handler(event, response, token0_address, token1_address, producer)
        await asyncio.sleep(poll_interval)

async def update_pairs(pairs, factory_filter, response):
    new_pairs = []
    for new_pair in await factory_filter.get_new_entries():
        pair_contract = web3.eth.contract(address=Web3s.toChecksumAddress(new_pair['args']['pair']), abi=uniswap_pair_abi)
        pairs.append(pair_contract)
        new_pairs.append(pair_contract)

async def create_task(pair, flag, response, producer):
    token0_address = Web3s.toChecksumAddress(await pair.functions.token0().call())
    token1_address = Web3s.toChecksumAddress(await pair.functions.token1().call())
    last_prices[token0_address] = (await pair.functions.price0CumulativeLast().call(), time.time())
    last_prices[token1_address] = (await pair.functions.price1CumulativeLast().call(), time.time())
    burn_filter = await pair.events.Burn.createFilter(fromBlock='latest')
    mint_filter = await pair.events.Mint.createFilter(fromBlock='latest')
    swap_filter = await pair.events.Swap.createFilter(fromBlock='latest')
    asyncio.create_task(filter_loop(burn_filter, handle_burn, response, flag, token0_address, token1_address, producer))
    asyncio.create_task(filter_loop(mint_filter, handle_mint, response, flag, token0_address, token1_address, producer))
    asyncio.create_task(filter_loop(swap_filter, handle_swap, response, flag, token0_address, token1_address, producer))
    logging.info("Created task for pair: " + pair.address)

async def create_tasks(new_pairs, response, flag, producer):
    if new_pairs is None:
        return
    tasks = []
    for pair in new_pairs:
        tasks.append(create_task(pair, flag, response, producer))
    await asyncio.gather(*tasks)

def refresh_response(response):
    response["num_burns_last_block"] = 0
    response["num_swaps_last_block"] = 0
    response["num_mints_last_block"] = 0


def create_filters(pairs):
    if pairs is None:
        return None, None, None
    burn_filters = []
    mint_filters = []
    swap_filters = []
    for pair in pairs:
        burn_filter = pair.events.Burn.createFilter(fromBlock='latest')
        mint_filter = pair.events.Mint.createFilter(fromBlock='latest')
        swap_filter = pair.events.Swap.createFilter(fromBlock='latest')
        burn_filters.append(burn_filter)
        mint_filters.append(mint_filter)
        swap_filters.append(swap_filter)
        logging.info("Created filter for pair: " + pair.address)
    return burn_filters, mint_filters, swap_filters

def check_filters(burns, mints, swaps, pairs, response, producer):
    for burn, mint, swap, pair in zip(burns, mints, swaps, pairs):
        burn_events, mint_events, swap_events = burn.get_new_entries(), mint.get_new_entries(), swap.get_new_entries()
        if not burn_events and not mint_events and not swap_events:
            continue
        token0_address = Web3s.toChecksumAddress(pair.functions.token0().call())
        token1_address = Web3s.toChecksumAddress(pair.functions.token1().call())
        for event in burn_events:
            handle_burn(event, response, token0_address, token1_address, producer)
        for event in mint_events:
            handle_mint(event, response, token0_address, token1_address, producer)
        for event in swap_events:
            handle_swap(event, response, token0_address, token1_address, producer)
        
async def update_stats(response):
    while True:
        get_global_stats(response)
        await asyncio.sleep(60)

# Main function creates event listeners for all the different smart contracts and desired event, and sets up the async loop
async def main():
    raw_producer = KafkaProducer("uniswap-raw")
    indicators_producer = KafkaProducer("uniswap-indicators")
    async with websockets.connect(conf["INFURA_WS_ENDPOINT"]) as ws:
        flag = asyncio.Event()
        logging.info("Connected to websocket")
        await ws.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": ["newHeads"]}))
        await ws.recv()
        logging.info("Subscribed to websocket")
        pairs = []
        response = json.load(open(".//schemas/indicator_response_template.json"))
        get_top_100_pairs(pairs, web3)
        await create_tasks(pairs, response, flag, raw_producer)
        logging.info("Tasks created")
        asyncio.create_task(update_stats(response))
        logging.info("Global stats collected")
        # From the factory, we want to know when a new pool is created
        factory_event_filter = await factory_contract.events.PairCreated.createFilter(fromBlock='latest')
        flag.set()
        while True:           
            new_block = await ws.recv()
            new_block = json.loads(new_block)
            flag.clear()
            response["block_number"] = int(new_block["params"]["result"]["number"], 16)
            response["timestamp"] = int(new_block["params"]["result"]["timestamp"], 16)
            logging.info("Received new block: %d" % response["block_number"])
            new_pairs = await update_pairs(pairs, factory_event_filter, response)
            #indicators_producer.produce(key=str(response["block_number"]), msg=json.dumps(response).encode())
            print(json.dumps(response))
            await create_tasks(new_pairs, response, flag, raw_producer)
            refresh_response(response)
            flag.set()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting by user request.\n")