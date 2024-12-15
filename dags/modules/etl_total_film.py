import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import psycopg2
import os
from sqlalchemy import create_engine
import pandas as pd

spark = SparkSession.builder \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0") \
        .master("local") \
        .appName("PySpark_Postgres").getOrCreate()
        
def extract_total_film():
    df_city = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://34.56.65.122:5231/project3") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "category") \
    .option("user", "postgres") \
    .option("password", "ftdebatch3").load()
    df_city.createOrReplaceTempView("category")
    
    df_country = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://34.56.65.122:5231/project3") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "film_category") \
    .option("user", "postgres") \
    .option("password", "ftdebatch3").load()
    df_country.createOrReplaceTempView("film_category")
    
    df_customer = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://34.56.65.122:5231/project3") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "film") \
    .option("user", "postgres") \
    .option("password", "ftdebatch3").load()
    df_customer.createOrReplaceTempView("film")
    
    df_result = spark.sql('''
        SELECT
            category.name as category_name,
            COUNT(film_category.film_id) as total_film,
            current_date() as date,
            'raihan' as data_owner
        FROM category
        JOIN film_category ON category.category_id = film_category.category_id
        JOIN film ON film_category.film_id = film.film_id
        GROUP BY category.name
        ORDER BY total_film DESC
        ''')
    
    df_result.write.mode('overwrite') \
    .partitionBy('date') \
    .option('compression', 'snappy') \
    .option('partitionOverwriteMode', 'dynamic') \
    .save('data_result_2')
    
def load_total_film():
    from sqlalchemy import create_engine
    import pandas as pd

    df = pd.read_parquet('data_result_2')

    engine = create_engine(
        'mysql+mysqlconnector://4FFFhK9fXu6JayE.root:9v07S0pKe4ZYCkjE@gateway01.ap-southeast-1.prod.aws.tidbcloud.com:4000/project3',
        echo=False)
    df.to_sql(name='total_film_by_category', con=engine, if_exists='append')