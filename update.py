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

# connect to db
conn_string = os.getenv('CONN_STRING')
engine = sa.create_engine(conn_string)
conn = engine.connect()


# format csv into dataframe
df_csv = pd.read_csv('./ols-results-latest.csv')

# grab and format stores
stores = df_csv.loc[:, ["Store ID", "Address", "City", "State", "Postcode", "Phone #", "New Item Code", "Qty"]]

# grab and format liquor
liquor = df_csv.loc[:, ["Description", "Item Code", "New Item Code", "Size", "Proof", "Age", "Case Price", "Bottle Price", "Category", "Store ID", "Qty"]]

# ormat db stores into dataframe
query = """SELECT * FROM stores"""
df_stores = pd.read_sql(query, conn)
query = """SELECT * FROM liquor"""
df_liquor = pd.read_sql(query, conn)


# connect to db and set actions to autocommit
conn = psycopg2.connect(conn_string)
conn.autocommit = True
cursor = conn.cursor()

# check if store exists and insert into db if not
for entry in stores.iterrows():
  store_id = entry[1].iloc[0]
  if store_id not in df_stores['id'].values:
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
    try:
      cursor.execute(query, (store_id, address, phone_number, [lat, lng]))
      print(f'store {store_id} added')
    except:
      print(f'store {store_id} already exists')




for entry in liquor.iterrows():
  liquor_id = str(entry[1].iloc[2])
  if liquor_id not in df_liquor['id'].values:
    print(liquor_id)
    description, item_code, _, size, proof, age, case_price, bottle_price, type, _, _ = entry[1].iloc
    case_price = float(case_price.replace('$', ''))
    bottle_price = float(bottle_price.replace('$', ''))
    img = ''
    query = """
    INSERT INTO liquor (id, item_code, description, size, proof, age, case_price, bottle_price, type, img)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    try:
      cursor.execute(query, (liquor_id, item_code, description, size, proof, age, case_price, bottle_price, type, img))
      print(f'liquor {liquor_id} added')
    except:
      print(f'liquor {liquor_id} already exists')

conn.close()