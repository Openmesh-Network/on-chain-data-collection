import dotenv
import requests
import json

def main():
    conf = dotenv.dotenv_values("./keys/.env")
    project_id = conf["INFURA_IPFS_PROJECT_ID"]
    project_secret = conf["INFURA_IPFS_PROJECT_SECRET"]
    ipfs_endpoint = "https://ipfs.infura.io:5001"
    res = requests.post(
        ipfs_endpoint + "/api/v0/block/get",
        auth=(project_id, project_secret),
        params={
            "arg": "QmaYL7E4gDTPNfLxrCEEEcNJgcHBJ55NxxTnxpDKWqMtJ3"
        }
    )
    data = res.content
    png_file = open("test.png", "wb")
    png_file.write(data)
    png_file.close()
    
if __name__ == "__main__":
    main()