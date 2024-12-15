from pyspark.sql import SparkSession, DataFrame
from dotenv import load_dotenv
import os

SERVER_POSTGRES_HOST = os.getenv("SERVER_POSTGRES_HOST")
SERVER_POSTGRES_PORT = os.getenv("SERVER_POSTGRES_PORT")
SERVER_POSTGRES_DB = os.getenv("SERVER_POSTGRES_DB")
SERVER_POSTGRES_USER = os.getenv("SERVER_POSTGRES_USER")
SERVER_POSTGRES_PASSWORD = os.getenv("SERVER_POSTGRES_PASSWORD")

load_dotenv()

def fetch_pg_table_with_spark(spark:SparkSession,table_name:str):
    df = spark.read.format("jdbc") \
    .option("url", f"jdbc:postgresql://{SERVER_POSTGRES_HOST}:{SERVER_POSTGRES_PORT}/{SERVER_POSTGRES_DB}") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", table_name) \
    .option("user", SERVER_POSTGRES_USER) \
    .option("password", SERVER_POSTGRES_PASSWORD).load()
    df.createOrReplaceTempView(table_name)
    return df

def write_spark_df_to_parquet(df:DataFrame, parquet_result_name:str):
    df.write.mode('overwrite') \
    .partitionBy('date') \
    .option('compression', 'snappy') \
    .option('partitionOverwriteMode', 'dynamic') \
    .save(parquet_result_name)