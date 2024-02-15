import os
import pandas as pd
import sqlalchemy as sa
import googlemaps
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_batch
import time
from scraper.scraper import Scraper

# start timer to track length of db update
start = time.time()

# load env variables
load_dotenv()

# connect to gmaps api
gmaps = googlemaps.Client(key=os.getenv('GM_KEY'))

# run scraper to obtain latest Oregon alcohol data
scraper = Scraper();
scraper.execute();

# connect to db
conn_string = os.getenv('CONN_STRING')
engine = sa.create_engine(conn_string)
conn = engine.connect()

# format db into dataframes
query = """SELECT * FROM stores"""
df_stores = pd.read_sql(query, conn)

# format csv into dataframe
df_csv = pd.read_csv('./ols-results-latest.csv')

# grab and format stores from csv
stores = df_csv.loc[:, ["Store ID", "Address", "City", "State", "Postcode", "Phone #", "New Item Code", "Qty"]]

# grab and format liquor from csv
liquor = df_csv.loc[:, ["Description", "Item Code", "New Item Code", "Size", "Proof", "Age", "Case Price", "Bottle Price", "Category", "Store ID", "Qty"]]

# connect to db and set actions to autocommit
conn = psycopg2.connect(conn_string)
conn.autocommit = True
cursor = conn.cursor()

# create list of ids from current db
store_id_list = []
for store in df_stores.iterrows():
  store_id_list.append(store[1].iloc[0])

# check if store exists and insert into db if not
# this logic is separate from liquor/liquor_store
# to avoid calling gmaps api unnecessarily
for entry in stores.iterrows():
  store_id = entry[1].iloc[0]
  if store_id in store_id_list:
    print(f'store {store_id} already exists')
    continue
  else:
    store_id_list.append(store_id)
    # format address
    address = f'{entry[1].iloc[1]} {entry[1].iloc[2]}, {entry[1].iloc[3]} {entry[1].iloc[4]}'
    phone_number = entry[1].iloc[5]
    # destructure lat/lng from geocode
    lat, lng = gmaps.geocode(address)[0]['geometry']['location'].values()
    # update query to add new store
    store_query = """
    INSERT INTO stores (id, address, phone_number, coordinates)
    VALUES (%s, %s, %s, %s);
    """
    cursor.execute(store_query, (store_id, address, phone_number, [lat, lng]))
    print(f'store {store_id} added')

# create input lists for liquor and liquor_store models
# these lists allow me to batch sql queries later
liquor_input_list = []
liquor_store_input_list = []

# row_count tracks the number of rows currently in each list
# for every 100 members, a batch is sent to the db for inserting
row_count = 0

# query the inserts new liquor entry if none exists already
liquor_query = """
INSERT INTO liquor (id, item_code, description, size, proof, age, case_price, bottle_price, type, img)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (id)
DO NOTHING;
"""
# query that inserts new liquor_store entry if none exists already
# updates the quantity for each entry
liquor_store_query = """
INSERT INTO liquor_store (liquor_id, store_id, quantity)
VALUES (%s, %s, %s)
ON CONFLICT (id)
DO UPDATE SET quantity = %s;
"""

# run above queries for each liquor and liquor_store row
for entry in liquor.iterrows():
  # destructure data for each column from each row
  description, item_code, liquor_id, size, proof, age, case_price, bottle_price, type, store_id, quantity = entry[1].iloc
  print(liquor_id, store_id)
  # format prices into floats by removing '$' and converting data type
  case_price = float(case_price.replace('$', ''))
  bottle_price = float(bottle_price.replace('$', ''))
  # set initial img reference to empty
  # separate script updates img currently
  img = ''
  # append data to respective list
  liquor_input_list.append((liquor_id, item_code, description, size, proof, age, case_price, bottle_price, type, img))
  liquor_store_input_list.append((liquor_id, store_id, quantity, quantity))
  # record added rows
  row_count += 1
  # if number of rows are 100 or greater
  # execute a batch, then reset lists and count
  if row_count >= 100:
    execute_batch(cursor, liquor_query, liquor_input_list)
    liquor_input_list = []
    execute_batch(cursor, liquor_store_query, liquor_store_input_list)
    liquor_store_input_list = []
    row_count = 0
# execute any remaining batches not covered in loop
execute_batch(cursor, liquor_query, liquor_input_list)
execute_batch(cursor, liquor_store_query, liquor_store_input_list)

# end and print timer for db update
end = time.time()    
print(end - start)

# close connection to db
conn.close()