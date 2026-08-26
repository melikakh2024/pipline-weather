from pyspark.sql import functions as sf
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

load_dotenv()
name_bucket=os.getenv("GCS_RAW_BUCKET")
credential_location=os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
KEY_PATH = os.path.abspath(credential_location)
output_file=os.getenv("GCS_PROCESSED_BUCKET")
MASTER_URL=os.getenv("SPARK_MASTER_URL")

spark=SparkSession.builder\
    .master(MASTER_URL)\
    .appName("APP")\
    .config("spark.jars", "/opt/bitnami/spark/gcs-connector.jar") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", KEY_PATH) \
    .getOrCreate()

df=spark.read.text(f"{name_bucket}/ghcnd-countries.txt")

df=df.withColumn("ID", sf.substring(sf.col("value"),1 ,2))\
    .withColumn("country", sf.substring(sf.col("value"),4,61))\
    .drop("value")

target=["GM", "AS", "AU"]
df=df.filter(sf.col("ID").isin(target))
df.write.parquet(f"{output_file}/country")
spark.stop()
