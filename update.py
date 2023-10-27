import os
from urllib.parse import quote_plus
import pandas as pd
import sqlalchemy as sa
from dotenv import load_dotenv
import psycopg2
# from scraper import Scraper

load_dotenv()
# scraper = Scraper();
# scraper.execute();

conn_string = f"postgresql://{os.getenv('DB_USERNAME')}:%s@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"

engine = sa.create_engine(conn_string % quote_plus(os.getenv('DB_PASSWORD')))
conn = engine.connect()

query = """SELECT * FROM stores"""

df = pd.read_sql(query, conn)
print(df.head(20))

conn.close()
# print(conn)