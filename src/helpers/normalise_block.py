template = {
    "block_num": 0,
    "block_hash": "",
    "block_timestamp": 0,
    "miner": "",
    "parent_hash": "",
    "num_transactions": 0,
}

def normalise_block(block):
    """
    Helper for normalising a block
    """
    res = template.copy()
    res["block_num"] = block["number"]
    res["block_hash"] = block["hash"]
    res["block_timestamp"] = block["timestamp"]
    res["miner"] = block["miner"]
    res["parent_hash"] = block["parentHash"]
    res["num_transactions"] = len(block["transactions"])
    return res