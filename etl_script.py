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
