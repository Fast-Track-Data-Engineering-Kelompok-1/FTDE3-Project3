import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import psycopg2
import os
from sqlalchemy import create_engine
import pandas as pd
from modules.spark import fetch_pg_table_with_spark, write_spark_df_to_parquet
from modules.tidb import load_to_mysql
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

DATA_OWNER = os.getenv("DATA_OWNER", "default_owner")
spark:SparkSession = SparkSession.builder \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0") \
        .master("local") \
        .appName("PySpark_Postgres").getOrCreate()
        
def extract_transform_top_countries():
    df_city = fetch_pg_table_with_spark(spark=spark, table_name="city")
    
    df_country = fetch_pg_table_with_spark(spark=spark,table_name="country")
    
    df_customer = fetch_pg_table_with_spark(spark=spark,table_name="customer")
    
    df_address = fetch_pg_table_with_spark(spark=spark,table_name="address")
    
    df_result = spark.sql(f'''
        SELECT
            country,
            COUNT(country) as total,
            current_date() as date,
            '{DATA_OWNER}' as data_owner
        FROM customer
        JOIN address ON customer.address_id = address.address_id
        JOIN city ON address.city_id = city.city_id
        JOIN country ON city.country_id = country.country_id
        GROUP BY country
        ORDER BY total DESC
        ''')
    parquet_result_name = 'data_result_1'

    write_spark_df_to_parquet(df_result, parquet_result_name)
    
def load_top_countries():
    load_to_mysql(parquet_result_name='data_result_1',mysql_table_name='top_country')