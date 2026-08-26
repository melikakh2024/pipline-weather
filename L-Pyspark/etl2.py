# %%
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("spark://spark-master:7077").appName("app").getOrCreate()



# %%
!gzip -dc "../phase 1 - ingest/ingestion/src/raw/2025.csv.gz" | head -n 10 > head.csv


# %%
from pyspark.sql import types as st
schema_m = st.StructType([st.StructField('ID', st.StringType(), True),
                          st.StructField('DATE', st.DateType(), True),
                          st.StructField('ELEMENT', st.StringType(), True),
                          st.StructField('VALUE',st.IntegerType(),True),
                          st.StructField('M_FLAG', st.StringType(), True),
                          st.StructField('Q_FLAG', st.StringType(), True),
                          st.StructField('S_FLAG', st.StringType(), True),
                          st.StructField('OBS_TIME', st.StringType(), True)])

df=spark.read.csv("../phase 1 - ingest/ingestion/src/raw/2025.csv.gz",
                  header=False , schema=schema_m , dateFormat="yyyyMMdd")

df.printSchema()
df.show(5)


# %%
from pyspark.sql import functions as sf
df2=df.select(sf.col("value"))
df2.show(40)


# %%
from pyspark.sql import functions as sf
df2=df.select([sf.sum(sf.isnull(sf.col(c)).cast("int")).alias(c)   for c in df.columns])
df2.show()


# %%
df2=df.limit(1000)


# %%
print(len(df.columns),df.count())


# %%
df3=df2.select("ID", "DATE", "ELEMENT", "VALUE", "OBS_TIME")


# %%
from pyspark.sql import functions as sf
df.select("element").distinct().show()


# %%
df.select("OBS_TIME").show(1000,truncate=False)


# %%
df.filter(sf.isnotnull(sf.col("OBS_TIME"))).orderBy("ID","ELEMENT").show(5)


# %%
pwd


# %%

df.withColumn("OBS_TIME",sf.coalesce(sf.col("OBS_TIME"),sf.lit("0000")))\
   .withColumn("OBS_TIME",sf.concat(sf.substring(sf.col("OBS_TIME"),1,2),sf.lit(":"),sf.substring(sf.col("OBS_TIME"),3,2)))\
   .orderBy("ID","ELEMENT").show(5)


# %%
!head -n 10 "../phase 1 - ingest/ingestion/src/raw/ghcnd-countries.txt"


# %%
from pyspark.sql import types as st
schema_c=st.StructType([st.StructField("CODE",st.StringType(), True),
                        st.StructField("NAME",st.StringType(), True)])


# %%
df_c = spark.read.text("../phase 1 - ingest/ingestion/src/raw/ghcnd-countries.txt")
df_c.show(5)


# %%
df_c=df_c.withColumn("CODE",sf.substring(sf.col("value"),1,2))\
        . withColumn("NAME",sf.substring(sf.col("value"),4,64))\
        .drop(df_c.value)
df_c.show(5)


# %%
!jupyter nbconvert --to script test.ipynb


