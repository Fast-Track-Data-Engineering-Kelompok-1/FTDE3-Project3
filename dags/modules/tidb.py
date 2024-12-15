import dotenv
import os
dotenv.load_dotenv()

MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_DB = os.getenv('MYSQL_DB')
MYSQL_PORT = os.getenv('MYSQL_PORT')

def load_to_mysql(parquet_result_name:str,mysql_table_name):
    from sqlalchemy import create_engine
    import pandas as pd

    df = pd.read_parquet(parquet_result_name)

    engine = create_engine(
        f'mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}',
        echo=False)
    df.to_sql(name=mysql_table_name, con=engine, if_exists='append')