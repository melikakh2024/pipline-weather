import os
from pyspark.sql import functions as sf
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

credential_location=os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
KEY_PATH=os.path.abspath(credential_location)
name_bucket=os.getenv("GCS_RAW_BUCKET")
output=os.getenv("GCS_PROCESSED_BUCKET")
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

df=spark.read.text(f"{name_bucket}/ghcnd-stations.txt")

df=df.withColumn("ID",sf.substring(sf.col("value"),1,11))\
     .withColumn("LATITUDE",sf.substring(sf.col("value"),13,8))\
     .withColumn("LONGITUDE",sf.substring(sf.col("value"),22,8))\
     .withColumn("ELEVATION",sf.substring(sf.col("value"),32,6))\
     .drop(df.value)

df = df.filter(
    sf.col("ID").startswith("GM") | sf.col("ID").startswith("AS") | sf.col("ID").startswith("AU"))
df.write.mode("overwrite").parquet(f"{output}/stations")

spark.stop()
