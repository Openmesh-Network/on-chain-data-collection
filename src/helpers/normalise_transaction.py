template = {
    "block_data": {},
    "tx_hash": "",
    "from": "",
    "to": "",
    "gas": 0,
    "gas_price": 0,
    "value": 0,
}

def normalise_transaction(transaction, block_msg):
    """
    Helper for normalising a transaction
    """
    res = template.copy()
    res["block_data"] = block_msg
    res["tx_hash"] = transaction["hash"]
    res["from"] = transaction["from"]
    res["to"] = transaction["to"]
    res["gas"] = transaction["gas"]
    res["gas_price"] = transaction["gasPrice"]
    res["value"] = transaction["value"]
    return res