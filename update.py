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
conn_string = os.getenv('CONN_STRING')

engine = sa.create_engine(conn_string)
conn = engine.connect()



query = """SELECT * FROM stores"""

df_db = pd.read_sql(query, conn)

df_csv = pd.read_csv('./ols-results-latest.csv')

stores = df_csv.loc[:, ["Store ID", "Address", "City", "State", "Postcode", "Phone #", "New Item Code", "Qty"]]

liquor = df_csv.loc[:, ["Description", "Item Code", "New Item Code", "Size", "Proof", "Age", "Case Price", "Bottle Price", "Store ID", "Qty"]]

conn = psycopg2.connect(conn_string)
conn.autocommit = True
cursor = conn.cursor()









# store_map = dict({})
count = 0
for entry in stores.iterrows():
  store_id = entry[1].iloc[0]
  if store_id not in df_db['id']:
    # store_map[store_id] = entry
    address = f'{entry[1].iloc[1]} {entry[1].iloc[2]}, {entry[1].iloc[3]} {entry[1].iloc[4]}'
    print(entry[1].iloc[5])
    print(entry[1].iloc[0])
    break
    coordinates = gmaps.geocode(address)
    query = f"""
    INSERT INTO stores (id, address, phone_number, coordinates)
    VALUES ({entry[0]}, {address}, {entry[5]}, {[coordinates[0]['geometry']['location']['lat'], coordinates[0]['geometry']['location']['lng']]})
    """
    cursor.execute(query)
    count = count + 1
    print(f'store {store_id} added')
  else:
    print('nothing to add')


# print(count)  
# print(df_db.head)

# for entry in df_csv['']

conn.close()

