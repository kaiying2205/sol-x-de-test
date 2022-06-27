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
df2 = df2.sort_values(by=['user_id','location']).reset_index(drop=True)

print(df2)