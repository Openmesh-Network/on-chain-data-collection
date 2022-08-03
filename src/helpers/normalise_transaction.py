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
    res["gas"] = int(transaction["gas"], 16)
    res["gas_price"] = int(transaction["gasPrice"], 16)
    res["value"] = int(transaction["value"], 16)
    return res