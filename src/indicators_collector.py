import asyncio
import json
import requests

response = json.load(open("./schemas/indicator_response_template.json"))
v2_graph_endpoint="https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2"
v3_graph_endpoint="https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3"

QUERY_INTERVAL = 30

def get_global_stats():
    request = \
    """{
            factory(id:"0x1F98431c8aD98523631AE4a59f267346ea31F984") {
                poolCount
                txCount
                totalVolumeUSD
                totalVolumeETH
                totalFeesUSD
                totalValueLockedUSD
            }
            
            pools(orderBy:totalValueLockedUSD, orderDirection: desc, first: 10) {
                token0 {
                    symbol
                    name
                }
                token1 {
                    symbol
                    name
                }
                id
                liquidity
                volumeToken0
                volumeToken1
                feesUSD
            }
        }"""
    res = requests.post(url=v3_graph_endpoint, json={"query": request})
    query_res = res.json()
    print(json.dumps(query_res, indent=4))
    result = {}
    return result

async def collect_periodic_indicators():
    while True:
        get_global_stats()
        await asyncio.sleep(QUERY_INTERVAL)

if __name__ == '__main__':
    asyncio.run(collect_periodic_indicators())