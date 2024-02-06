import os
from urllib.parse import quote_plus
import pandas as pd
import sqlalchemy as sa
import googlemaps
from dotenv import load_dotenv
import psycopg2
# from scraper import Scraper

load_dotenv()
# scraper = Scraper();
# scraper.execute();

gmaps = googlemaps.Client(key=os.getenv('GM_KEY'))

# run scraper to create new csv
# check all stores are present
# if any stores aren't in db, add them to current db
# update inventory of each liquor bottle in each store
# add new liquor/liquor_store entries if needed

# connect to db, format db stores into dataframe
conn_string = os.getenv('CONN_STRING')
engine = sa.create_engine(conn_string)
conn = engine.connect()
query = """SELECT * FROM stores"""
df_db = pd.read_sql(query, conn)

# format csv into dataframe
df_csv = pd.read_csv('./ols-results-latest.csv')
# grab and format stores
stores = df_csv.loc[:, ["Store ID", "Address", "City", "State", "Postcode", "Phone #", "New Item Code", "Qty"]]
# grab and format liquor
liquor = df_csv.loc[:, ["Description", "Item Code", "New Item Code", "Size", "Proof", "Age", "Case Price", "Bottle Price", "Store ID", "Qty"]]

# connect to db and set actions to autocommit
conn = psycopg2.connect(conn_string)
conn.autocommit = True
cursor = conn.cursor()

# check if store exists and insert into db if not
for entry in stores.iterrows():
  store_id = entry[1].iloc[0]
  if store_id not in df_db['id'].values:
    # format address
    address = f'{entry[1].iloc[1]} {entry[1].iloc[2]}, {entry[1].iloc[3]} {entry[1].iloc[4]}'
    phone_number = entry[1].iloc[5]
    # destructure lat/lng from geocode
    lat, lng = gmaps.geocode(address)[0]['geometry']['location'].values()
    # update query to add new store
    query = """
    INSERT INTO stores (id, address, phone_number, coordinates)
    VALUES (%s, %s, %s, %s);
    """
    cursor.execute(query, (store_id, address, phone_number, [lat, lng]))
    print(f'store {store_id} added')

conn.close()