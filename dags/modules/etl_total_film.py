import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import psycopg2
import os
from sqlalchemy import create_engine
import pandas as pd
from modules.spark import fetch_pg_table_with_spark, write_spark_df_to_parquet
from dotenv import load_dotenv
from modules.tidb import load_to_mysql

load_dotenv()  # Load environment variables from .env file

spark :SparkSession = SparkSession.builder \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0") \
        .master("local") \
        .appName("PySpark_Postgres").getOrCreate()
DATA_OWNER = os.getenv("DATA_OWNER", "default_owner")

def extract_total_film():
    df_category = fetch_pg_table_with_spark(spark=spark, table_name="category")
    
    df_film_category = fetch_pg_table_with_spark(spark=spark, table_name="film_category" )
    
    df_film = fetch_pg_table_with_spark(spark=spark, table_name="film")
    
    df_result = spark.sql(f'''
        SELECT
            category.name as category_name,
            COUNT(film_category.film_id) as total_film,
            current_date() as date,
            '{DATA_OWNER}' as data_owner
        FROM category
        JOIN film_category ON category.category_id = film_category.category_id
        JOIN film ON film_category.film_id = film.film_id
        GROUP BY category.name
        ORDER BY total_film DESC
        ''')
    
    parquet_result_name = 'data_result_2'
    write_spark_df_to_parquet(df_result, parquet_result_name)
    
def load_total_film():
    load_to_mysql(parquet_result_name='data_result_2',mysql_table_name='total_film_by_category')