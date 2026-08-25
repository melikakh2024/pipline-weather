#---- libraries---------
import requests
from pathlib import Path
from google.cloud import storage
from dotenv import load_dotenv
import os

load_dotenv()
s_client=storage.Client()
bucket_name=os.environ["GCS_BUCKET_NAME"]
bucketc= s_client.get_bucket(bucket_name)

path=Path("src/raw")
path.mkdir(parents=True,exist_ok=True)

base="https://www.ncei.noaa.gov/pub/data/ghcn/daily"
output=["ghcnd-countries","ghcnd-stations"]

for f in output:
    file_path=path/f"{f}.txt"
    if not file_path.exists():
        with open(file_path,"wb") as file:
            url=f"{base}/{f}.txt"
            response=requests.get(url)
            file.write(response.content)
            print(f"{f}downloaded")

    blob=bucketc.blob(f"raw/{f}.txt")
    if not blob.exists():
        blob.upload_from_filename(file_path)
        print(f"{f} uploaded ")