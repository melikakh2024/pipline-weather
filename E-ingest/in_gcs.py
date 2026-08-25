#-------------Libraries----------------
import requests
from pathlib import Path
from  google.cloud  import storage
from dotenv import load_dotenv
import os

#------------Environments---------------
load_dotenv()
bucket_name=os.environ["GCS_BUCKET_NAME"]


#----------- googleAccess--------------
client=storage.Client()
bucket=client.bucket(bucket_name)


#----- config----------------------
local_path=Path("src/raw")
local_path.mkdir(parents= True,exist_ok=True)
year=[2024,2025]
base="https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_year"

#-----------upload and download to bucket------
for yr in year:
    file=local_path/f"{yr}.csv.gz"
    url=f"{base}/{yr}.csv.gz"
    if not file.exists():
        response=requests.get(url, stream=True)
        with open(file,"wb") as f:
            for chunk in response.iter_content(chunk_size=5*1024*1024):
                f.write(chunk)
        print(f"{yr} downloaded")
        #----upload TO GCS from HARD -----
    blob=bucket.blob(f"raw/{yr}.csv.gz")
    if  not blob.exists():
            blob.chunk_size = 5 * 1024 * 1024
            blob.upload_from_filename(file , timeout=300)
            print(f"{yr} uploaded to gs://{bucket_name}/raw/{yr}.csv.gz")
