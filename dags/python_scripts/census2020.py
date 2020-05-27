# script to get daily Census 2020 response rates for Austin MSA census Tracts
import pandas as pd
import requests
import json
import csv
import os
from sodapy import Socrata
import dateutil.parser as parser
import json

print(os.getcwd())

# Required to locate files in temporary execution folders
absolute_path = os.path.abspath(os.path.dirname(__file__))

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
tract = "*"             # ALL TRACTS
TX_STATE_CODE = "48"     # TEXAS
counties_with_labels = {
    '453': 'Travis',
    '021': 'Bastrop',
    '055': 'Caldwell',
    '209': 'Hays',
    '491': 'Williamson'
}
counties_as_str = ','.join(counties_with_labels.keys())

calledAPI = f'https://api.census.gov/data/2020/dec/responserate?get={fields_as_str}&for=tract:{tract}&in=state:{TX_STATE_CODE}&in=county:{counties_as_str}'
response = requests.get(calledAPI)

formattedResponse = json.loads(response.text)[1:]
tracts_df = pd.DataFrame(
    columns=['DRRALL', 'CRRINT', 'RESP_DATE', 'CRRALL',
             'GEO_ID', 'DRRINT', 'state', 'county', 'tract'],
    data=formattedResponse
)


# Add column for 10 digit Geography ID
goidstr = tracts_df.loc[:, 'GEO_ID'].str[9:]
tracts_df['GEOID20'] = goidstr

# Replace State & County ID with lookup string
tracts_df['state'] = 'Texas'
tracts_df['county'] = tracts_df["county"].replace(counties_with_labels)

# Create UID field


def make_uid(x):
    date = parser.parse(x['RESP_DATE'])
    return date.strftime('%Y-%m-%d-') + str(x['GEOID20'])


tracts_df['UID'] = tracts_df.apply(make_uid, axis=1)

# Format Tract values


def format_tract(string, index):
    return string[:index] + '.' + string[index:]


tracts_df['tract'] = format_tract(tracts_df['tract'].str, 4).str.lstrip('0')

# Reorder columns
tracts_df = tracts_df.reindex(columns=['RESP_DATE', 'GEOID20', 'DRRALL', 'DRRINT',
                                       'CRRALL', 'CRRINT', 'county', 'tract', 'state', 'GEO_ID', 'UID'])


##########
# Get Response Rates from Top 30 cities in the US
##########

top_30_places = {
    "Austin": "1600000US4805000",
    "Seattle": "1600000US5363000",
    "San Jose": "1600000US0668000",
    "San Diego": "1600000US0666000",
    "Las Vegas": "1600000US3240000",
    "Portland": "1600000US4159000",
    "Louisville": "1600000US4743820",
    "Denver": "1600000US0820000",
    "San Francisco": "1600000US0667000",
    "Indianapolis": "1600000US1836003",
    "Phoenix": "1600000US0455000",
    "Oklahoma City": "1600000US4055000",
    "Washington DC": "1600000US1150000",
    "Boston": "1600000US2507000",
    "Fort Worth": "1600000US4827000",
    "Columbus": "1600000US3918000",
    "Memphis": "1600000US4748000",
    "El Paso": "1600000US4824000",
    "San Antonio": "1600000US4865000",
    "Charlotte": "1600000US3712000",
    "Baltimore": "1600000US2404000",
    "Chicago": "1600000US1714000",
    "Dallas": "1600000US4819000",
    "Nashville": "1600000US0548560",
    "Philadelphia": "1600000US4260000",
    "Houston": "1600000US4835000",
    'Detroit': "1600000US2622000",
    "Los Angeles": "1600000US0644000",
    "New York": "1600000US3651000",
    "Jacksonville": "1600000US1235000",
}

top_30_GEOIDS_list = list(top_30_places.values())

# Get list of all places
places_api_call = f'https://api.census.gov/data/2020/dec/responserate?get={fields_as_str}&for=place:*'
places_response = requests.get(places_api_call)

places_formatted_response = json.loads(places_response.text)

places_df = pd.DataFrame(
    columns=['DRRALL', 'CRRINT', 'RESP_DATE', 'CRRALL',
             'GEO_ID', 'DRRINT', 'state', 'place'],
    data=places_formatted_response
)

# Filter out only top 30 cities from dataframe
top_30_df = places_df[places_df['GEO_ID'].isin(top_30_GEOIDS_list)]

# Replace place ID with human-readable City Name


def replace_place_id_with_value(x):
    full_id = '1600000US' + x['state'] + x['place']
    for city_name, city_full_id in top_30_places.items():
        if city_full_id == full_id:
            return city_name


top_30_df['place'] = top_30_df.apply(replace_place_id_with_value, axis=1)

# Replace state ID with human-readable State Name
with open(f"{absolute_path}/census_states.json") as json_file:
    states_data = json.load(json_file)

states_df = pd.DataFrame(states_data)


def make_state_id(x):
    id = str(x['GEO_ID'])
    return id[-2:]


def get_state_value(x):
    value = states_df.get(x['state'] == states_df['state'])
    return value.iloc[0]['State']


states_df['state'] = states_df.apply(make_state_id, axis=1)

top_30_df['state'] = top_30_df.apply(get_state_value, axis=1)


# Add column for 10 digit Geography ID
shortened_geoid = top_30_df.loc[:, 'GEO_ID'].str[9:]
top_30_df['GEOID20'] = shortened_geoid

# Add column for UID in same format as Tracts DF
top_30_df['UID'] = top_30_df.apply(make_uid, axis=1)


##########
# Get Top Texas Counties
##########

counties = {
    "Collin": "0500000US48085",
    "Tarrant": "0500000US48439",
    "Dallas": "0500000US48113",
    "Bexar": "0500000US48029",
    "El Paso": "0500000US48141",
    "Harris": "0500000US48201",
    "Travis": "0500000US48453",
    "Williamson": "0500000US48491",
    "Hays": "0500000US48209",
    "Bastrop": "0500000US48021",
    "Caldwell": "0500000US48055",
    "Hidalgo": "0500000US48215"
}

county_code_str_list = ','.join(
    [cnty_code[-3:] for cnty_code in counties.values()])

counties_api_call = f'https://api.census.gov/data/2020/dec/responserate?get=DRRALL,CRRINT,RESP_DATE,CRRALL,GEO_ID,DRRINT&for=county:{county_code_str_list}&in=state:{TX_STATE_CODE}'
counties_response = requests.get(counties_api_call)
counties_formatted_response = json.loads(counties_response.text)[1:]

counties_df = pd.DataFrame(columns=['DRRALL', 'CRRINT', 'RESP_DATE', 'CRRALL',
                                    'GEO_ID', 'DRRINT', 'state', 'county'], data=counties_formatted_response)


# Add column for 10 digit Geography ID
shortened_geoid = counties_df.loc[:, 'GEO_ID'].str[9:]
counties_df['GEOID20'] = shortened_geoid

# Add column for UID in same format as Tracts DF
counties_df['UID'] = counties_df.apply(make_uid, axis=1)

counties_df['state'] = "Texas"


def get_county_value(x):
    full_id = '0500000US48' + x['county']
    for county_name, county_full_id in counties.items():
        if county_full_id == full_id:
            return county_name


counties_df['county'] = counties_df.apply(get_county_value, axis=1)


# TODO: Columns to add to Open Date Portal
#
# Zip columns
# council district
# centroid
# 2020 vs 2010 census tracts
# congressional district


##########
# Get State of Texas Data
##########


state_api_call = f'https://api.census.gov/data/2020/dec/responserate?get=DRRALL,CRRINT,RESP_DATE,CRRALL,GEO_ID,DRRINT&for=state:{TX_STATE_CODE}'
state_response = requests.get(state_api_call)
state_formatted_response = json.loads(state_response.text)[1:]

texas_df = pd.DataFrame(columns=['DRRALL', 'CRRINT', 'RESP_DATE', 'CRRALL',
                                 'GEO_ID', 'DRRINT', 'state'], data=state_formatted_response)


# Add column for 10 digit Geography ID
shortened_geoid = texas_df.loc[:, 'GEO_ID'].str[9:]
texas_df['GEOID20'] = shortened_geoid

# Add column for UID in same format as Tracts DF
texas_df['UID'] = texas_df.apply(make_uid, axis=1)

texas_df['state'] = "Texas"


##########
# Combine the Data Frames
##########

# Concatenate the dataframes into one unified DF
df_combined = pd.concat(
    [tracts_df, top_30_df, counties_df, texas_df], sort=True)

# Replace NaN values that Socrata doesn't like
df_combined['place'] = df_combined['place'].fillna("N/A")
df_combined['state'] = df_combined['state'].fillna("N/A")
df_combined['county'] = df_combined['county'].fillna("N/A")
df_combined['tract'] = df_combined['tract'].fillna(0)

json_data = df_combined.to_dict(orient="records")

##########
# Send Data to Socrata
##########

try:
    socrata_upsert = socrata_client.upsert('evvr-qwa7', json_data)
    print(socrata_upsert)
except Exception as e:
    print("Exception, could not insert: " + str(e))
    print("Query: '%s'" % json_data)
