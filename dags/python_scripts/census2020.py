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

calledAPI = "https://api.census.gov/data/2020/dec/responserate?get=DRRALL,CRRINT,RESP_DATE,CRRALL,GEO_ID,DRRINT&for=tract:*&in=state:48&in=county:453,021,055,209,491"
response = requests.get(calledAPI)

# print(response.json())
formattedResponse = json.loads(response.text)[1:]
RespDF = pd.DataFrame(
    columns=['DRRALL', 'CRRINT', 'RESP_DATE', 'CRRALL', 'GEO_ID', 'DRRINT', 'state', 'county', 'tract'],
    data=formattedResponse
)
goidstr = RespDF.loc[:, 'GEO_ID'].str[9:]
RespDF['GEOID10'] = goidstr
# TODO, won't need to save CSV
vintage = RespDF.loc[0, 'RESP_DATE']
fname = 'MSATracts_' + vintage + '.csv'
json_data = RespDF.to_dict(orient="records")
try:
    socrata_client.upsert('ajri-btbu', json_data)
except Exception as e:
    print("Exception, could not insert: " + str(e))
    print("Query: '%s'" % json_data)

# rdf2.to_csv(fname, index=False, quoting=csv.QUOTE_NONE)