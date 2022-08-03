import dotenv

async def main():
    conf = dotenv.dotenv_values("./keys/.env")
    rest_endpont = conf["INFURA_POLYGON_REST_ENDPOINT"]
    