# script to get daily Census 2020 response rates for Austin MSA census Tracts
import pandas as pd
import requests
import json
import csv
import os
from sodapy import Socrata

app_token = os.getenv("APP_TOKEN", None)
socrata_key_id = os.getenv("SOCRATA_KEY_ID", None)
socrata_key_secret = os.getenv("SOCRATA_KEY_SECRET", None)
socrata_client = Socrata(
    "data.austintexas.gov",
    app_token,
    username=socrata_key_id,
    password=socrata_key_secret
)

fields_with_labels = {
    'DRRALL': "Daily Self-Response Rate - Overall",
    'CRRINT': "Cumulative Self-Response Rate - Internet",
    'RESP_DATE': "Response Date",
    'CRRALL': "Cumulative Self-Response Rate - Overall",
    'GEO_ID': "Geography Code",
    'DRRINT': "Daily Self-Response Rate - Internet"
}
fields_as_str = ','.join(fields_with_labels.keys())
tract = "*"         # ALL TRACTS
state = "48"        # TEXAS
counties_with_labels = {
    '453': 'Travis',
    '021': 'Bastrop',
    '055': 'Caldwell',
    '209': 'Hays',
    '491': 'Williamson'
}
counties_as_str = ','.join(counties_with_labels.keys())

calledAPI = f'https://api.census.gov/data/2020/dec/responserate?get={fields_as_str}&for=tract:{tract}&in=state:{state}&in=county:{counties_as_str}'
response = requests.get(calledAPI)

formattedResponse = json.loads(response.text)[1:]
RespDF = pd.DataFrame(
    columns=['DRRALL', 'CRRINT', 'RESP_DATE', 'CRRALL',
             'GEO_ID', 'DRRINT', 'state', 'county', 'tract'],
    data=formattedResponse
)


# Add column for 10 digit Geography ID
goidstr = RespDF.loc[:, 'GEO_ID'].str[9:]
RespDF['GEOID10'] = goidstr

# Replace State & County ID with lookup string
RespDF['state'] = 'Texas'
RespDF['county'] = RespDF["county"].replace(counties_with_labels)


# Format Tract values

def format_tract(string, index):
    return string[:index] + '.' + string[index:]


RespDF['tract'] = format_tract(RespDF['tract'].str, 4).str.lstrip('0')

# Reorder columns
reindexed_data = RespDF.reindex(columns=['RESP_DATE', 'GEOID10', 'DRRALL', 'DRRINT',
                                         'CRRALL', 'CRRINT', 'county', 'tract', 'state', 'GEO_ID'])

json_data = reindexed_data.to_dict(orient="records")

try:
    socrata_upsert = socrata_client.upsert('ajri-btbu', json_data)
    print(socrata_upsert)
except Exception as e:
    print("Exception, could not insert: " + str(e))
    print("Query: '%s'" % json_data)
