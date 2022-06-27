# import packages
import pandas as pd
import json
import requests

# location table - read source from local machine and create dataframe
location_table = pd.read_json("location.json", orient="records")


# couchDB table

# get data from remote database
response = requests.get("https://53c1a51d-616b-4aea-9e2a-89b2c8024b72-bluemix.cloudant.com/events/_all_docs?include_docs=true")
couch_values = json.loads(response.text)

# loop through nested dictionary values to obtain the necessary columns only
couch_subset = []
for row in couch_values["rows"]:
    data = row["doc"]
    couch_subset.append(data)

# convert values to dataframe and convert timestamp column to datetime column type
couchdb_table = pd.DataFrame(couch_subset, columns=["_id", "_rev", "event_type", "user_id", "timestamp"])
# modify couchdb_table to remove timezone in timestamp column
# couchdb_table["timestamp"] = couchdb_table["timestamp"].apply(pd.to_datetime)
couchdb_table["timestamp"] = pd.to_datetime(couchdb_table["timestamp"]).dt.tz_localize(None)

# df1: desired schema 1
df1 = couchdb_table.merge(location_table, on=['user_id','timestamp'], how='left')
df1 = df1.drop(columns='_rev')
df1 = df1[['_id', 'user_id', 'event_type', 'location', 'timestamp']]
df1 = df1.rename(columns={"_id":"id"})

# modify here to set index to 'id' column to remove default index
df1 = df1.set_index('id')

# df2: desired schema 2

# sort location_table according to each individual user_id and timestamp
df2 = location_table.sort_values(by=['user_id','timestamp'])

# create date column by extracting date from timestamp
df2['date'] = pd.to_datetime(df2['timestamp']).dt.date

# create time_spent column by converting timestamp to minutes format and getting the difference from previous record for same user
df2['timestamp'] = pd.to_datetime(df2['timestamp'])
df2['timestamp'] = (df2['timestamp'] - df2['timestamp'].iloc[0])/pd.to_timedelta('1Min')
df2['time_spent'] = list(df2.groupby(df2.user_id).apply(lambda x: x['timestamp']-x['timestamp'].shift()))
df2['time_spent'] = df2.groupby(['user_id', 'location'])['time_spent'].transform('sum')

# create visit_count column
df2['visit_count'] = df2.groupby(['user_id', 'location'])['location'].transform('count')

# tidy df2 by dropping unrequired columns, arranging columns, dropping duplicates and sort according to user_id and location
df2 = df2.drop(columns='timestamp')
df2 = df2[['date', 'user_id', 'location', 'time_spent', 'visit_count']]
df2 = df2.drop_duplicates()
df2 = df2.sort_values(by=['user_id','location']).reset_index().set_index('date').drop('index', axis=1) # modify index to remove default index


# establish connection to postgres
import os
import psycopg2
from dotenv import load_dotenv

# required to load the previously defined environment variables
load_dotenv()  

# create connection to postgres
pgcon = psycopg2.connect(
    host = os.getenv('PG_HOST'),user = os.getenv('PG_USER'),password = os.getenv('PG_PASSWORD'))

# creating cursor object using the cursor() method
pgcursor = pgcon.cursor()

# import ISOLATION_LEVEL_AUTOCOMMIT from psycopg2 extensions to lock the server
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
pgcon.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

# to create database and set isolation level to autocommit
pgcursor.execute("DROP DATABASE IF EXISTS {0}".format(os.getenv('PG_DATABASE')))
pgcursor.execute("CREATE DATABASE {0}".format(os.getenv('PG_DATABASE')))

# connect to postgre again to connect to created database
pgcon = psycopg2.connect(
    host = os.getenv('PG_HOST'),database = os.getenv('PG_database'),user = os.getenv('PG_USER'),password = os.getenv('PG_PASSWORD'))

# import create_engine from sqlalchemy
import sqlalchemy
from sqlalchemy import create_engine

load_dotenv()

# create engine to faciliate communication between python and database in postgres
con_string = "postgresql+psycopg2://{0}:{1}@{2}/{3}".format(os.getenv('PG_USER'),os.getenv('PG_PASSWORD'), os.getenv('PG_HOST'), os.getenv('PG_DATABASE'))
engine = create_engine(con_string)

# load df1 into PostgreSQL table in database
df1.to_sql('df1', engine, if_exists='replace', index = True)

# Change your column datatype for df1 and set id as primary key
engine.execute('ALTER TABLE df1 ALTER COLUMN id TYPE varchar(50)')
engine.execute('ALTER TABLE df1 ALTER COLUMN user_id TYPE varchar(50)')
engine.execute('ALTER TABLE df1 ALTER COLUMN event_type TYPE varchar(100)')
engine.execute('ALTER TABLE df1 ALTER COLUMN location TYPE varchar(100)')
engine.execute('ALTER TABLE df1 ALTER COLUMN timestamp TYPE timestamp')
engine.execute('ALTER TABLE df1 ADD PRIMARY KEY ("id");')

# load df2 into PostgreSQL table in database
df2.to_sql('df2', engine, if_exists='replace', index = True)

# Change your column datatype for df2
engine.execute('ALTER TABLE df2 ALTER COLUMN date TYPE date')
engine.execute('ALTER TABLE df2 ALTER COLUMN user_id TYPE varchar(50)')
engine.execute('ALTER TABLE df2 ALTER COLUMN location TYPE varchar(100)')
engine.execute('ALTER TABLE df2 ALTER COLUMN time_spent TYPE decimal(10,2)')
engine.execute('ALTER TABLE df2 ALTER COLUMN visit_count TYPE int USING visit_count :: integer')

pgcon.close()