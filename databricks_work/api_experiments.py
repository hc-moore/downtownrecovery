# Databricks notebook source
import safegraphql.client as sgql
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport

# COMMAND ----------

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# COMMAND ----------

def create_and_save_as_table(df, table_name):
    spark_df = spark.createDataFrame(df)
    spark_df.write.format('csv').saveAsTable(table_name)
    
def get_table_as_pandas_df(table_name):
    return sqlContext.sql("select * from " + table_name + " tables").toPandas()

# COMMAND ----------

# headers
headers = {
    'Content-Type': 'application/json',
    'apikey': 'ACEow06Stq4WX1JXp5BREdnm98WMu2iS', # Dan's api key
#     'apikey': 'MBqZIqnE7UPzsRVQpxu5xZYyqNnK4mkv' # Michael's api key
#      'apikey': 'Cf7EHS1B4njwwihTMHyUU4glL1m7mIVl'
#     'apikey': '0bo56aB5aqJGW7tLhfsZGgWkTeclF4EJ'
}

# Select your transport with a defined url endpoint
# transport = AIOHTTPTransport(url="https://api.safegraph.com/v2/graphql/", headers=headers)

# Create a GraphQL client using the defined transport
# client = Client(transport=transport, fetch_schema_from_transport=True)

# COMMAND ----------

import requests
import json

def sf_gql_request(params):
    query = """
        query($postal_code: String, $start_date: DateTime!, $end_date: DateTime!) {
          search(filter: { 
            address: {
              postal_code: $postal_code
            }
          }){
            weekly_patterns(start_date: $start_date end_date: $end_date) {
              results(first: 500) {
                pageInfo { hasNextPage endCursor }
                edges {
                  node {
                    placekey
                    location_name
                    postal_code
                    raw_visit_counts
                    normalized_visits_by_total_visits
                    normalized_visits_by_state_scaling
                    date_range_start
                    date_range_end
                  }
                }
              }
            }
          }
        }
    """

    req = requests.post(
        'https://api.safegraph.com/v2/graphql',
        json={
            'query': query,
            'variables': params
        },
        headers=headers
    )

    return req.json()
params = {
    'start_date': '2020-01-06',
    'end_date': '2020-01-11',
    'postal_code': '94588'
}
# ptown = sf_gql_request(params)
# ptown

# COMMAND ----------

# Create pandas dataframe from graphql response
def sf_search_resp_to_pandas_df(result):
    try:
        places = list(map(lambda x: x['node'], result['data']['search']['weekly_patterns']['results']['edges']))
        places_df = pd.DataFrame.from_dict(places)
        return places_df
    except:
        print("no results or error")
        
def sf_batchlookup_resp_to_pandas_df(result, zipcode, week):
    try:
        places = list(map(lambda x: x['safegraph_weekly_patterns'], result['data']['batch_lookup']))
        clean_places = [x for x in places if x != None]
        clean_places_df = pd.DataFrame.from_dict(clean_places)
        return clean_places_df
    except:
        print("no results or error for ", week, zipcode)
        
return_self = lambda col: col

# Create list of column title strings 
def generate_lq_week_date_cols(col_naming_func=return_self):
    last_week = datetime.datetime(2022,2,25) 

    list_of_cols = [col_naming_func('2019-01-07')]
    current_monday_date = datetime.datetime(2019,1,7)

    while(current_monday_date < last_week):
        last_datetime = current_monday_date
        current_monday_date = last_datetime + timedelta(days=7)
        list_of_cols.append(col_naming_func(current_monday_date.strftime('%Y-%m-%d')))

    return list_of_cols

# Create the string of last date of the week given a start date
def weekend_date_string(start_date):
    start_date = datetime.strptime(start_date,'%Y-%m-%d') if isinstance(start_date, str) else start_date
    end_date = start_date + timedelta(days=6)
    end_date = end_date.strftime('%Y-%m-%d')
    return end_date

# Convert string list to python list
def string_list_python_list(string_list):
    return list(map(lambda x: x.strip("''"), string_list.strip('][').split(', ')))

# COMMAND ----------

import requests
import json

def sf_batchlookup_gql_request(params):
    query = """
        query($placekeys: [Placekey!], $date: DateTime!) {
          batch_lookup(placekeys: $placekeys) {
            safegraph_weekly_patterns(date:$date) {
                postal_code
                raw_visit_counts
                normalized_visits_by_total_visits
                normalized_visits_by_state_scaling
                date_range_start
            }
          }
        }
    """

    req = requests.post(
        'https://api.safegraph.com/v2/graphql',
        json={
            'query': query,
            'variables': params
        },
        headers=headers
    )
    
    if (
        req.status_code != 204 and
        req.headers["content-type"].strip().startswith("application/json")
    ):
        try:
            return req.json()
        except ValueError:
            return {}

# params = {
#     'date': '2019-01-07',
#     'placekeys': first_placekeys
# }
# ptown = sf_batchlookup_gql_request(params)
# ptown
# sf_batchlookup_resp_to_pandas_df(ptown)
# print(json.dumps(ptown['data']['batch_lookup'], indent=4, sort_keys=True))

# COMMAND ----------

metro_definition_df = get_table_as_pandas_df('city_index_0119_csv')
us_city_index = get_table_as_pandas_df('city_csa_fips_codes___us_cities_downtowns__1__csv')
can_city_index = get_table_as_pandas_df('can_city_index_da_0312')

# COMMAND ----------

can_city_index = can_city_index.rename(columns={"0": "city", "downtown_da": "dt_zipcodes", "CMA_da": "zipcodes"})
metro_definition_df = metro_definition_df.rename(columns={"0": "city"})
us_cities = us_city_index['city'].tolist()
can_cities = can_city_index['city'].tolist()
all_cities = us_cities + can_cities

# COMMAND ----------

def get_downtown_city_zipcodes(city_name):
    dt_zipcodes = []
    dt_vc = pd.DataFrame()
    
    dt_vc = us_city_index if city_name in us_cities else can_city_index
    
    dt_zipcodes = dt_vc[dt_vc['city'] == city_name]['dt_zipcodes'] 
    dt_zipcodes = dt_zipcodes.tolist()[0].strip('][').split(', ')
    return [zipcode.replace("'","") for zipcode in dt_zipcodes]

def get_metro_zipcodes(city_name):
    metro_df = metro_definition_df if city_name in us_cities else can_city_index
    city_zipcodes = metro_df[metro_df['city'] == city_name]['zipcodes'].values[0]
    city_zipcodes = string_list_python_list(city_zipcodes)
    return city_zipcodes

# get_metro_zipcodes('Toronto')

# COMMAND ----------

def get_zipcode_poi_from_table(zipcode):
    try:
        print(zipcode)
        poi_in_zipcode = us_dc_oct_2020.groupby('postal_code')['placekey'].apply(list)
        display(poi_in_zipcode[zipcode])
        return poi_in_zipcode[zipcode]
    except:
        return False
# get_zipcode_poi_from_table('94588')

# COMMAND ----------

import concurrent.futures
from itertools import repeat

def set_initial_zipcode_pois(zipcode):
    try:
        params = {
            'start_date': '2020-01-06',
            'end_date': '2020-01-11',
            'postal_code': zipcode
        }
        sf_gql_response = sf_gql_request(params)
        zipcode_poi = sf_search_resp_to_pandas_df(sf_gql_response)
        return zipcode_poi['placekey'].to_list()
    except:
        return []

def populate_dc_for_zipcode(zipcode, initial_zipcode_pois=set_initial_zipcode_pois):
#     covid_weeks = generate_lq_week_date_cols()
    covid_weeks = [
        '2022-03-14',
        '2022-03-21',
        '2022-03-28',
        '2022-04-04',
        '2022-04-11',
        '2022-04-18',
        '2022-04-25',
        '2022-05-02',
        '2022-05-09',
        '2022-05-16',
        '2022-05-23',
        '2022-05-30',
    ]
    in_scope_placekeys = initial_zipcode_pois(zipcode)
    if in_scope_placekeys == False: 
        in_scope_placekeys = set_initial_zipcode_pois(zipcode)
        print(in_scope_placekeys)
    zipcode_poi_df = pd.DataFrame()
    
    def set_week_counts(week, placekeys):
        zipcode_week_poi_df = pd.DataFrame()
        max_poi = 500
        placekeys = [placekeys[i:i + max_poi] for i in range(0, len(placekeys), max_poi)]
        
        for placekey_chunk in placekeys:
            params = {
                'date': week,
                'placekeys': placekey_chunk
            }
            zipcode_poi_response = sf_batchlookup_gql_request(params)
            zipcode_week_poi_df = zipcode_week_poi_df.append(sf_batchlookup_resp_to_pandas_df(zipcode_poi_response, zipcode, week))
        return zipcode_week_poi_df
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        the_futures = [executor.submit(set_week_counts, week, in_scope_placekeys) for week in covid_weeks]
        for future in concurrent.futures.as_completed(the_futures):
            current_places_df = future.result()
            zipcode_poi_df = zipcode_poi_df.append(current_places_df)
#             print("added more places! ", zipcode_poi_df.shape)
            pass
        
    return zipcode_poi_df

# populate_dc_for_zipcode('84144', get_zipcode_poi_from_table)

# COMMAND ----------

cities_to_download = metro_definition_df['0'].to_list()
for city in ['Dallas', 'Salt Lake City', 'Sacramento', 'New York', 'Los Angeles', 'Chicago', 'Houston','Phoenix',
 'Philadelphia',
 'San Antonio',
 'San Diego',
 'San Jose',
 'Austin',
 'Jacksonville',
 'Fort Worth',
 'Columbus',
 'Indianapolis',
 'Charlotte',
 'San Francisco',
 'Seattle',
 'Denver',
 'Washington DC',
 'Nashville',
 'Oklahoma City',
 'El Paso',
 'Boston',
 'Portland',
 'Las Vegas',
 'Detroit',
 'Memphis',
 'Louisville',
 'Baltimore',
 'Milwaukee',
 'Albuquerque',
 'Tucson',
 'Fresno',
 'Kansas City',
 'Mesa',
 'Atlanta',
 'Omaha',
 'Colorado Springs',
  'Raleigh',
 'Long Beach',
 'Virginia Beach',
 'Miami',
 'Oakland',
 'Minneapolis',
 'Tulsa',
 'Bakersfield',
 'Wichita',
 'Arlington',
 'Aurora',
 'Tampa',
 'New Orleans',
 'Cleveland',
 'Honolulu',
 'Cincinnati',
 'Pittsburgh',
 'St Louis',
 'Orlando']:
    cities_to_download.remove(city)
cities_to_download

# COMMAND ----------

## RUN IF DOWNLOADING DATA ##
## NOTE: need to download nyc individually
import time
cities_to_download = ['Washington DC']
for city_name in cities_to_download:
    dc_df = pd.DataFrame()
    metro_zipcodes = get_metro_zipcodes(city_name)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        the_futures = [executor.submit(populate_dc_for_zipcode, zipcode, get_zipcode_poi_from_table) for zipcode in metro_zipcodes]
        for future in concurrent.futures.as_completed(the_futures):
            current_places_df = future.result()
            
            current_places_df['city'] = city_name

            dc_df = dc_df.append(current_places_df)
            
            if dc_df.shape[0] % 4 == 0:
                print("added more places! ", dc_df.shape)
            pass
    
#     cleaned_all_dc_df = aggregate_and_clean_raw_dc(dc_df)
    create_and_save_as_table(cleaned_all_dc_df, city_name.replace(" ", "_") + "_device_count")
    print("Done with " + city_name)
    time.sleep(300)

cleaned_all_dc_df

# COMMAND ----------

can_agg_dc_df = get_table_as_pandas_df("all_canada_device_count_agg_dt")
can_agg_dc_df = can_agg_dc_df.rename(columns={"DAUID": "postal_code", "CMA": "city", "week": "date_range_start"})
# can_agg_dc_df = can_agg_dc_df.drop('downtown',axis=1)
can_agg_dc_df

# COMMAND ----------

## MERGE INDIVIDUAL CITY DEVICE COUNT TABLES INTO 1 ANALYSIS DF ##
all_cities_dc_df = pd.DataFrame()

## 1. Merge all US Cities from databricks tables ##
standard_cities_for_tables = ['Los Angeles', 'Chicago', 'Houston','Phoenix', 'Philadelphia','San Antonio','San Diego','San Jose', 'Austin', 'Jacksonville', 'Fort Worth',
 'Columbus','Indianapolis', 'Charlotte', 'San Francisco', 'Seattle', 'Denver', 'Washington DC', 'Nashville', 'Oklahoma City', 'El Paso', 'Boston', 'Portland', 'Las Vegas', 'Detroit', 'Memphis', 'Louisville', 'Baltimore', 'Milwaukee', 'Albuquerque', 'Tucson', 'Fresno', 'Kansas City', 'Mesa', 'Atlanta', 'Omaha', 'Colorado Springs',  'Raleigh', 'Long Beach', 'Virginia Beach', 'Miami', 'Oakland', 'Minneapolis', 'Tulsa','Bakersfield', 'Wichita', 'Arlington', 'Aurora', 'Tampa', 'New Orleans', 'Cleveland', 'Honolulu', 'Cincinnati', 'Pittsburgh', 'St Louis', 'Orlando', 'New York']

cities_tables = [ city_name.replace(" ", "_").lower() + "_device_count" for city_name in standard_cities_for_tables]

list_of_dc_tables = [
    *cities_tables
]

for table_name in list_of_dc_tables:
    city_table = get_table_as_pandas_df(table_name)
    all_cities_dc_df = all_cities_dc_df.append(city_table)
    
tables_that_need_names = ['dallas_device_count_3_5',
    'saltlakecity_device_count_3_6',
    'sacramento_device_count_3_6',]
cities = ['Dallas', 'Salt Lake City', 'Sacramento']

zipped_city_tables = zip(tables_that_need_names, cities)

for table_name in zipped_city_tables:
    city_table = get_table_as_pandas_df(table_name[0])
    city_table['city'] = table_name[1]
    all_cities_dc_df = all_cities_dc_df.append(city_table)

all_cities_dc_df

# COMMAND ----------

all_cities_dc_df = get_table_as_pandas_df('all_us_cities_dc')
all_cities_dc_df

# COMMAND ----------

all_cities_dc_df = all_cities_dc_df.append(can_agg_dc_df)
all_cities_dc_df

# COMMAND ----------

def aggregate_and_clean_raw_dc(input_dc_df):
    input_dc_df = input_dc_df.groupby(by=['postal_code','date_range_start', 'city']).sum()
    input_dc_df = input_dc_df.reset_index(['postal_code', 'date_range_start', 'city'])
    input_dc_df['date_range_start'] = input_dc_df['date_range_start'].str[:10]
    display(input_dc_df)
    return input_dc_df
    
# slc_dc_df = dc_df.copy(deep=True)
# slc_dc_df_cleaned = aggregate_and_clean_raw_dc(slc_dc_df)
# slc_dc_df_cleaned.shape[0]
# create_and_save_as_table(slc_dc_df_cleaned, "saltlakecity_device_count_3_6")

# COMMAND ----------

# MAGIC %md **Create LQ's** 
# MAGIC 
# MAGIC LQ = (*Device count in ZCTA COVID Period A / Total device count for NY COVID Period A*) /   (*Device count in ZCTA Period B / Total device count for NY Period B*)
# MAGIC 
# MAGIC A = Active or Post COVID Period (e.g. June - August 2020)  
# MAGIC 
# MAGIC a = Pre-COVID Period Equivalent (e.g. June - August 2019)

# COMMAND ----------

import datetime

# COMMAND ----------

def create_sub_lq(dc_df, downtown_zipcodes, column_name):
    device_counts_from_all_zipcodes_in_df = dc_df.groupby('date_range_start')[column_name].sum()
    dc_df = dc_df.set_index('postal_code')
    dt_dc_df = dc_df.loc[downtown_zipcodes,:].groupby('date_range_start')[column_name].sum()
    return dt_dc_df / device_counts_from_all_zipcodes_in_df

def generate_lq_for_city(city_name, agg_df):
    dt_zipcodes = get_downtown_city_zipcodes(city_name) 
    # Replace buggy New Orleans zipcode
    if city_name == 'New Orleans':
        index_of_buggy_zipcode = dt_zipcodes.index('70162')
        dt_zipcodes[index_of_buggy_zipcode] = '70163'
    if city_name == 'New York':
        dt_zipcodes.remove('10170')
    
    # Cast to Date
    agg_df['date_range_start'] = pd.to_datetime(agg_df['date_range_start']).dt.date

    # Define COVID Research time frame filters
    pre_covid = agg_df['date_range_start'] < datetime.date(2020, 3, 2)
    covid = datetime.date(2020, 3, 2) <= agg_df['date_range_start']
    
    # Separated time frames of device counts
    a_dc_df = agg_df[pre_covid]
    A_dc_df = agg_df[covid]
    
     # Check if Can or US
    if city_name in can_cities:
        pre_covid_dt_zipcodes = agg_df[(agg_df['city'] == city_name) & pre_covid]['postal_code'].unique()
        dt_zipcodes = agg_df[(agg_df['city'] == city_name) & covid]['postal_code'].unique()
    
    # Initialize df to populate
    covid_lq_df = pd.DataFrame()
    pre_covid_lq_df = pd.DataFrame()
    lq_df = pd.DataFrame()
    
    # Create Sub LQ's for each timeframe
    covid_lq_df['sub_covid_rvc_lq'] = create_sub_lq(A_dc_df, dt_zipcodes, 'raw_visit_counts')
    covid_lq_df['sub_covid_nvt_lq'] = create_sub_lq(A_dc_df, dt_zipcodes, 'normalized_visits_by_total_visits')
    covid_lq_df['sub_covid_nvs_lq'] = create_sub_lq(A_dc_df, dt_zipcodes, 'normalized_visits_by_state_scaling')
        
    pre_covid_lq_df['sub_precovid_rvc_lq'] = create_sub_lq(a_dc_df, pre_covid_dt_zipcodes, 'raw_visit_counts')
    pre_covid_lq_df['sub_precovid_nvt_lq'] = create_sub_lq(a_dc_df, pre_covid_dt_zipcodes, 'normalized_visits_by_total_visits')
    pre_covid_lq_df['sub_precovid_nvs_lq'] = create_sub_lq(a_dc_df, pre_covid_dt_zipcodes, 'normalized_visits_by_state_scaling')
    
    # Set date as column
    covid_lq_df = covid_lq_df.reset_index()
    pre_covid_lq_df = pre_covid_lq_df.reset_index()
    
    # Set COVID's 2019 comparison date
    covid_lq_df['comparison_date'] = covid_lq_df['date_range_start'] - pd.Timedelta(364, unit='d')
    covid_lq_df.loc[(covid_lq_df['date_range_start'] > datetime.date(2021, 1,1)), 'comparison_date'] -= pd.Timedelta(364, unit='d')
    covid_lq_df.loc[(covid_lq_df['date_range_start'] > datetime.date(2022, 1,1)), 'comparison_date'] -= pd.Timedelta(364, unit='d')
    
    # Merge both periods into 1 lq df
    lq_df = covid_lq_df.merge(pre_covid_lq_df, 
                          left_on=['comparison_date'],
                          right_on=['date_range_start'],
                          suffixes=['_covid','_precovid']
                         )
    lq_df['rvc_lq'] = lq_df['sub_covid_rvc_lq'] / lq_df['sub_precovid_rvc_lq']
    lq_df['nvt_lq'] = lq_df['sub_covid_nvt_lq'] / lq_df['sub_precovid_nvt_lq']
    lq_df['nvs_lq'] = lq_df['sub_covid_nvs_lq'] / lq_df['sub_precovid_nvs_lq']
    return lq_df

test_lq_df = generate_lq_for_city("Toronto", all_cities_dc_df)

# COMMAND ----------

test_lq_df

# COMMAND ----------

## RUN THIS CELL TO GENERATE LQ'S GIVEN THE DF OF ALL CITIES COUNTES
cities_to_download = all_cities
all_cities_lq_df = pd.DataFrame()

for city in cities_to_download:
    if city not in ['Mesa', 'Long Beach', 'Virginia Beach', 'Arlington', 'Aurora']:
        print("Generating LQ for: ", city)
        city_lq = generate_lq_for_city(city, all_cities_dc_df)
        city_lq['city'] = city
        all_cities_lq_df = all_cities_lq_df.append(city_lq)
        
all_cities_lq_df

# COMMAND ----------

all_cities_lq_df = get_table_as_pandas_df('all_cities_lq')
all_cities_lq_df

# COMMAND ----------

plt.figure(figsize = (15,8))
sns.set_style("darkgrid")
# all_cities_lq_df['rvc_rolling'] = all_cities_lq_df[['city','rvc_lq']].groupby('city').rolling(1, min_periods=1).mean().reset_index(level=0,drop=True)
# rolling_lq_df['date'] = all_cities_lq_df['date_range_start_covid']

# rolling_lq_df
# cities = ['Seattle', 'Portland', 'Sacramento', 'Fresno', 'Bakersfield']
cities = ['New York', 'Boston', 'Washington DC', 'Philadelphia']
# cities = ['Salt Lake City', 'Miami', 'Charlotte', 'Dallas', 'Denver']
# cities = ['Raleigh']
# cities = ['San Francisco', 'Oakland', 'San Jose', 'San Diego']
in_scope_cities = all_cities_lq_df['city'].isin(cities)

filtered_cities = all_cities_lq_df[in_scope_cities]
# .rolling(10, min_periods=1).mean()
filtered_cities['date_range_start_covid'] = all_cities_lq_df['date_range_start_covid']
filtered_cities
sns.lineplot(x = "date_range_start_covid", 
             y = "rvc_lq",
             hue="city",
             data = filtered_cities)
plt.title("LQ Time Series - No Rolling Average")
plt.xticks(rotation = 25)

# COMMAND ----------

sns.lineplot(x = "date_range_start_covid", 
             y = "nvs_lq",
             color='r',
             data = test_lq_df)
plt.xticks(rotation = 25)
plt.title("All Toronto POI LQ Time Series")

# COMMAND ----------

