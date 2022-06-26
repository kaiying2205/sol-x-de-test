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
couchdb_table["timestamp"] = couchdb_table["timestamp"].apply(pd.to_datetime)

