import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from pyspark.sql import types as st
from pyspark.sql import functions as sf
import argparse

# Create parser
parser = argparse.ArgumentParser(description="A simple CLI example.")
parser.add_argument("--year",type=int,required=True,help="Year to process. Available data years: 2024, 2025")
args = parser.parse_args()

load_dotenv()

credential_location=os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
bucket_raw=os.getenv("GCS_RAW_BUCKET")
processed_bucket=os.getenv("GCS_PROCESSED_BUCKET")


spark = SparkSession.builder \
    .appName("weather_pipeline") \
    .master(os.environ.get("SPARK_MASTER_URL", "local[*]"))\
    .config("spark.jars", "/opt/bitnami/spark/gcs-connector.jar") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credential_location) \
    .getOrCreate()



schema_m = st.StructType([st.StructField('ID', st.StringType(), True),
                          st.StructField('DATE', st.DateType(), True),
                          st.StructField('ELEMENT', st.StringType(), True),
                          st.StructField('VALUE',st.IntegerType(),True),
                          st.StructField('M_FLAG', st.StringType(), True),
                          st.StructField('Q_FLAG', st.StringType(), True),
                          st.StructField('S_FLAG', st.StringType(), True),
                          st.StructField('OBS_TIME', st.StringType(), True)])

input_path=f"{bucket_raw}/{args.year}.csv.gz"
df = spark.read.csv(input_path, header=False , schema=schema_m , dateFormat="yyyyMMdd")

df_repartitioned = df.repartition(10)

df1 = df_repartitioned.filter(
    sf.col("ID").startswith("GM") | sf.col("ID").startswith("AS") | sf.col("ID").startswith("AU"))

df1=df1.select("ID", "DATE", "ELEMENT", "VALUE", "OBS_TIME")
df1=df1.withColumn("OBS_TIME",sf.coalesce(sf.col("OBS_TIME"),sf.lit("0000")))\
    .withColumn("OBS_TIME",sf.concat(sf.substring(sf.col("OBS_TIME"),1,2),sf.lit(":"),sf.substring(sf.col("OBS_TIME"),3,2)))

output_path=f"{processed_bucket}/{args.year}"

df1.coalesce(1).write.mode("overwrite").parquet(output_path)
spark.stop()