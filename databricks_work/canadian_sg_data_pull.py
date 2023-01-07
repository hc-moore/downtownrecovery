# Databricks notebook source
# Uncomment line 2 if there's no safegraphQL package
!pip install safegraphQL
!pip install ratemate

# COMMAND ----------

import safegraphql.client as sgql
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport

# COMMAND ----------

import pandas as pd
import numpy as np
import seaborn as sns
import time
import matplotlib.pyplot as plt
import datetime as dt
from ratemate import RateLimit
# halve it for the two workers
rate_limit = RateLimit(max_count = 500, per = 30)

# COMMAND ----------

def create_and_save_as_table(df, table_name):
    spark_df = spark.createDataFrame(df)
    spark_df.write.format('csv').saveAsTable(table_name)
    
def get_table_as_pandas_df(table_name):
    return sqlContext.sql("select * from " + table_name + " tables").toPandas()
  # Create list of column title strings 
return_self = lambda col: col
# Create list of column title strings 
def generate_lq_week_date_cols(col_naming_func=return_self):
    last_week = dt.datetime(2022,2,25) 

    list_of_cols = [col_naming_func('2019-01-07')]
    current_monday_date = dt.datetime(2019,1,7)

    while(current_monday_date < last_week):
        last_datetime = current_monday_date
        current_monday_date = last_datetime + dt.timedelta(days=7)
        list_of_cols.append(col_naming_func(current_monday_date.strftime('%Y-%m-%d')))

    return list_of_cols

# Create the string of last date of the week given a start date
def weekend_date_string(start_date):
    start_date = dt.datetime.strptime(start_date,'%Y-%m-%d') if isinstance(start_date, str) else start_date
    end_date = start_date + dt.timedelta(days=6)
    end_date = end_date.strftime('%Y-%m-%d')
    return end_date

# COMMAND ----------

new_counts = get_table_as_pandas_df('safegraph_patterns_count')
zipcodes_placekeys = get_table_as_pandas_df('us_zipcode_placekey_202010')
zipcodes_placekeys = zipcodes_placekeys[["postal_code", "placekey"]]
all_city_index = get_table_as_pandas_df('all_city_index')

# COMMAND ----------

zipcodes_placekeys["postal_code"] = zipcodes_placekeys["postal_code"].astype(int)
all_city_index["postal_code"] = all_city_index["postal_code"].astype(int)

# COMMAND ----------



# COMMAND ----------

new_counts = new_counts.merge(zipcodes_placekeys, on = "placekey", how = "left").drop(columns = "placekey").merge(all_city_index, on = "postal_code", how = "left")
new_counts

# COMMAND ----------

all_canada_device_count = get_table_as_pandas_df('all_canada_device_count_0317')
#all_canada_device_count = all_canada_device_count.dropna(subset = ["city"])

# COMMAND ----------

all_canada_device_count.columns

# COMMAND ----------

postal_code_placekey_can = all_canada_device_count[["placekey", "postal_code"]].drop_duplicates()

# COMMAND ----------

create_and_save_as_table(postal_code_placekey_can, 'postal_code_placekey_can')

# COMMAND ----------

postal_code_placekey_can

# COMMAND ----------

create_and_save_as_table(postal_code_placekey_can.merge(study_das, on = "postal_code", how = "inner"), 'postal_code_placekey_can')

# COMMAND ----------

study_das = get_table_as_pandas_df('all_city_index')

# COMMAND ----------

oct5th_patterns_data = get_table_as_pandas_df('your_data_mar_15_2022_0846am_csv') # data was purchased in march, but was of oct pattern info for placekeys & their POIs. 
# this is all eligible plackeys that existed in ALL of canada, in all records possessed by safegraph
# this, and the places dataset are as comprehensive collections of placekeys as possible
# subset 
oct5th_patterns_data["FSA"] = oct5th_patterns_data["postal_code"].str[:3]
oct5th_patterns_data["DA"] = oct5th_patterns_data["poi_cbg"].str[3:]

# COMMAND ----------

display(oct5th_patterns_data[oct5th_patterns_data["DA"].isin(all_canada_device_count["postal_code"].unique())])

# COMMAND ----------

FSAs_to_DAUIDs = get_table_as_pandas_df('canada_fsa_to_da_csv')

# COMMAND ----------

all_canada_device_count = get_table_as_pandas_df('all_canada_device_count')

# COMMAND ----------

# some weeks do not have as much coverage as others
# request all of canada week by week
sink_weeks = ['2019-12-23', '2019-12-30', '2020-12-21', '2020-12-28', '2021-12-20', '2021-12-27']
weeks_to_seen_placekeys = {}
for weeks in sink_weeks:
    weeks_to_seen_placekeys[weeks] = all_canada_device_count.loc[all_canada_device_count["date_range_start"] == weeks, "placekey"].unique().tolist()

# COMMAND ----------

#covid_analysis_weeks = generate_lq_week_date_cols()

# COMMAND ----------

can_city_index = get_table_as_pandas_df('can_city_index_da_0312')
can_city_index = can_city_index.rename(columns = {"0" : "city", "CMA_da" : "zipcodes", "downtown_da" : "dt_zipcodes"})
# toronto and mississauga have identical CMA_da fields
can_city_index = can_city_index.drop_duplicates(subset = ["zipcodes"])
can_city_index

# COMMAND ----------

# source: forward sortation boundary area shapefile spatially joined with dissemination area boundary shapefile, both available via statcan
# https://www150.statcan.gc.ca/n1/pub/92-179-g/92-179-g2016001-eng.htm : FSA shapefile reference guide
# spatial join was done in R then uploaded to databricks
FSAs_to_DAUIDs = get_table_as_pandas_df('canada_fsa_to_da_csv')

# COMMAND ----------

def get_downtown_city_DAUIDs(city_name):
    dt_DAUIDs = [str.strip(dauid)[1:-1] for dauid in can_city_index[can_city_index["city"] == city_name]["dt_zipcodes"].values[0].split(",")]
    dt_DAUIDs[0] = dt_DAUIDs[0][1:]
    dt_DAUIDs[-1] = dt_DAUIDs[-1][:-1]
    return dt_DAUIDs
    
def get_metro_DAUIDs(city_name):
    city_DAUIDs = [str.strip(dauid)[1:-1] for dauid in can_city_index[can_city_index["city"] == city_name]["zipcodes"].values[0].split(",")]
    city_DAUIDs[0] = city_DAUIDs[0][1:]
    city_DAUIDs[-1] = city_DAUIDs[-1][:-1]
    return city_DAUIDs

# COMMAND ----------

all_study_DAUIDs = pd.DataFrame()
for city in can_city_index["0"].unique():
    CMA_DAUIDs = pd.DataFrame(get_metro_DAUIDs(city))
    CMA_DAUIDs["CMA"] = city
    all_study_DAUIDs = all_study_DAUIDs.append(CMA_DAUIDs)
all_study_DAUIDs = all_study_DAUIDs[~all_study_DAUIDs.duplicated(subset = 0)]
FSAs_to_DAUIDs = FSAs_to_DAUIDs[FSAs_to_DAUIDs["DAUID"].isin(all_study_DAUIDs[0])]
all_study_DAUIDs  = all_study_DAUIDs.rename(columns = {0 : "DAUID"})

# COMMAND ----------

## oct 5th search
all_CMA_DAUIDs = pd.DataFrame()
all_downtown_DAUIDs = pd.DataFrame()
for city in can_city_index["city"].unique():
    city_CMA_DAUIDs = pd.DataFrame(get_metro_DAUIDs(city))
    city_CMA_DAUIDs["city"] = city
    all_CMA_DAUIDs = all_CMA_DAUIDs.append(city_CMA_DAUIDs)
    
    city_dt_DAUIDs = pd.DataFrame(get_downtown_city_DAUIDs(city))
    city_dt_DAUIDs["city"] = city
    all_downtown_DAUIDs = all_downtown_DAUIDs.append(city_dt_DAUIDs)
all_CMA_DAUIDs = all_CMA_DAUIDs.rename(columns = {0 : "DAUID"})
all_downtown_DAUIDs = all_downtown_DAUIDs.rename(columns = {0 : "DAUID"})
all_study_DAUIDs = all_CMA_DAUIDs[~all_CMA_DAUIDs.duplicated(subset = "DAUID")]
FSAs_to_DAUIDs = FSAs_to_DAUIDs[FSAs_to_DAUIDs["DAUID"].isin(all_study_DAUIDs["DAUID"])]
# subset only places that are in study FSAs, it is the best filtering that can be done at the moment 
oct5th_placekeys_study = oct5th_placekeys[oct5th_placekeys["FSA"].isin(FSAs_to_DAUIDs["CFSAUID"])]

# COMMAND ----------

len(oct5th_placekeys_study)

# COMMAND ----------

def sf_ca_batchlookup_resp_to_pandas_df(result):
    try:
        places = list(map(lambda x: x['safegraph_weekly_patterns'], result['data']['batch_lookup']))
        clean_places = [x for x in places if x != None]
        clean_places_df = pd.concat([pd.DataFrame.from_dict([clean_place]) for clean_place in clean_places])
        return clean_places_df
    except:
        print(result)

# COMMAND ----------

import requests
import json

def sf_ca_batchlookup_gql_request(params):
    """
        batch_lookup does 20 placekeys at a time
        see: https://docs.safegraph.com/reference/batch_lookup
    """
    
    query = """
        query($placekeys: [Placekey!], $date: DateTime!) {
          batch_lookup(placekeys: $placekeys) {
          placekey
          safegraph_weekly_patterns(start_date: $start_date end_date: $end_date) {
                placekey
                city
                postal_code
                raw_visit_counts
                poi_cbg
                date_range_start
                normalized_visits_by_total_visits
                normalized_visits_by_state_scaling
            }
          }
        }
    """
    rate_limit.wait()
    req = requests.post(
        'https://api.safegraph.com/v2/graphql',
        json={
            'query': query,
            'variables': params
        },
        headers=headers
    )
    
    if ("content-type" in req.headers.keys() and
        req.status_code != 204 and
        req.headers["content-type"].strip().startswith("application/json")
    ):
        try:
            return req.json()
        except ValueError:
            return {}


# COMMAND ----------

import requests
import json

def sf_ca_batchlookup_gql_request_new(params):
    """
        batch_lookup does 20 placekeys at a time
        see: https://docs.safegraph.com/reference/batch_lookup
    """
    
    query = """
        query($placekeys: [Placekey!], $date: DateTime!) {
          batch_lookup(placekeys: $placekeys) {
          placekey
          safegraph_weekly_patterns(date: $date) {
                placekey
                city
                postal_code
                raw_visit_counts
                poi_cbg
                date_range_start
                normalized_visits_by_total_visits
                normalized_visits_by_state_scaling
            }
          }
        }
    """
    rate_limit.wait()
    req = requests.post(
        'https://api.safegraph.com/v2/graphql',
        json={
            'query': query,
            'variables': params
        },
        headers=headers
    )
    
    if ("content-type" in req.headers.keys() and
        req.status_code != 204 and
        req.headers["content-type"].strip().startswith("application/json")
    ):
        try:
            return req.json()
        except ValueError:
            return {}

# COMMAND ----------

#covid_analysis_weeks = generate_lq_week_date_cols()

# COMMAND ----------

import concurrent.futures
from itertools import repeat

def pull_all_weeks(week, placekeys, request):
    
    """
        3 placekeys * 165 weeks == exactly 495 records per page, provided that each placekey is present for all 165 weeks. this is rarely the case, however
        
    """
    params = {
        #'date': covid_analysis_weeks[0],
        'date': week,
        'placekeys': placekeys
    }
    sg_response = request(params)
    places_df = sf_ca_batchlookup_resp_to_pandas_df(sg_response)
    return places_df

# COMMAND ----------

# Aggregate all places in a zipcode to 1 zipcode row
def agg_zipcode_places(zipcode_places_df):
    return zipcode_places_df.groupby('postal_code')['raw_visit_counts'].sum()
  
# Aggregate all places in a cbg to 1 cbg row
def agg_cacbg_places(cbg_places_df):
    return cbg_places_df.groupby('poi_cbg')['raw_visit_counts'].sum()

# COMMAND ----------

import concurrent.futures
from itertools import repeat

def populate_all_weekly_visitor_counts_for_ca_city_threaded(week, placekeys):

    dc_df = pd.DataFrame()
    the_futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers = 2) as executor:
        # there will be ~10k tasks
        for i in range(0, len(placekeys), 20):
            rate_limit.wait()
            # adds a city's entire unseen placekeys for the given week to a thread
            future = executor.submit(pull_all_weeks, week, placekeys[i:(i + 20)], sf_ca_batchlookup_gql_request_new)
            the_futures.append(future)
        for future in concurrent.futures.as_completed(the_futures):
            current_places_df = future.result()
            # the result of each will be appended to a df and returned 
            dc_df = dc_df.append(current_places_df)
            if dc_df.shape[0] % 10 == 0:
                print("added more places! ", dc_df.shape)
            pass
        
    print("Done with week of " + week)

    return dc_df

# COMMAND ----------

# headers
headers = {
    'Content-Type': 'application/json',
#    'apiKey': 'MBqZIqnE7UPzsRVQpxu5xZYyqNnK4mkv'
    'apiKey': 'LUIw2EM8ALARPr70oTlW8QhM88FJiHuA'
#    'apiKey': 'rTmZV2oOrWgx9R4Zu4AXbROMYs64r1rv'
#    'apikey': 'nEGp0BnECJfpZtyjarbMnuV7t6pYTJGv',
#     'apikey': 'Cf7EHS1B4njwwihTMHyUU4glL1m7mIVl'
#     'apikey': '0bo56aB5aqJGW7tLhfsZGgWkTeclF4EJ'
}

# Select your transport with a defined url endpoint
transport = AIOHTTPTransport(url="https://api.safegraph.com/v2/graphql/", headers=headers)

# Create a GraphQL client using the defined transport
client = Client(transport=transport, fetch_schema_from_transport=True)

# COMMAND ----------

# test on a random sample of placekeys before doing them all
city = can_city_index["0"].unique()[0]
city_DAUIDs = get_metro_DAUIDs(city)
city_FSAs_to_DAUIDs = FSAs_to_DAUIDs[FSAs_to_DAUIDs["DAUID"].isin(city_DAUIDs)]
city_places = all_canadian_places[(all_canadian_places["FSA"].isin(city_FSAs_to_DAUIDs["CFSAUID"])) | (all_canadian_places["city"] == city)]
city_placekeys = city_places["placekey"]
print("Total placekeys to add: " + str(len(city_placekeys)))
random_idx = np.random.randint(0, len(city_placekeys), 500)
# test 
all_dc_df_test = pd.DataFrame()
for week in weeks_to_placekeys.keys():
    seen_placekeys = weeks_to_placekeys[week]
    new_places_by_week = all_canadian_places[~all_canadian_places["placekey"].isin(seen_placekeys)]
    if city == "Toronto":
        city_places = new_places_by_week[(new_places_by_week["FSA"].isin(city_FSAs_to_DAUIDs["CFSAUID"])) | 
                                                            (new_places_by_week["city"] == city) | 
                                                            (new_places_by_week["city"] == "Mississauga")]
    else:
        city_places = new_places_by_week[(new_places_by_week["FSA"].isin(city_FSAs_to_DAUIDs["CFSAUID"])) | 
                                         (new_places_by_week["city"] == city)]
    city_placekeys = city_places["placekey"]
    city_placekeys = city_placekeys.to_numpy()[random_idx].tolist()
    print("Week of " + str(week) + "Placekeys to search: " + str(len(city_placekeys)))
    all_dc_df_test = all_dc_df_test.append(populate_all_weekly_visitor_counts_for_ca_city_threaded(week, city_placekeys, city))
#city_placekeys = city_placekeys.to_numpy()[random_idx].tolist()
all_dc_df_test = all_dc_df_test[~all_dc_df_test.duplicated()]
#all_dc_df_test["week"] = all_dc_df_test["date_range_start"].str[:10]
all_dc_df_test["DAUID"] = all_dc_df_test["poi_cbg"].str[3:]
all_dc_df_test = all_dc_df_test.rename(columns = {"city" : "safegraph_city", "city_study" : "CMA"})
total_devices_by_week = all_dc_df_test.groupby("date_range_start").sum().reset_index()
total_devices_by_week["date_range_start"] = pd.to_datetime(total_devices_by_week["date_range_start"])

# COMMAND ----------

all_dc_df_test = all_dc_df_test[~all_dc_df_test.duplicated()]
#all_dc_df_test["week"] = all_dc_df_test["date_range_start"].str[:10]
all_dc_df_test["DAUID"] = all_dc_df_test["poi_cbg"].str[3:]
all_dc_df_test = all_dc_df_test.rename(columns = {"city" : "safegraph_city", "city_study" : "CMA"})
total_devices_by_week = all_dc_df_test.groupby("date_range_start").size().reset_index()
total_devices_by_week["date_range_start"] = pd.to_datetime(total_devices_by_week["date_range_start"])
total_devices_by_week

# COMMAND ----------

all_study_DAUIDs = pd.DataFrame()
for city in can_city_index["0"].unique():
    CMA_DAUIDs = pd.DataFrame(get_metro_DAUIDs(city))
    CMA_DAUIDs["CMA"] = city
    all_study_DAUIDs = all_study_DAUIDs.append(CMA_DAUIDs)
all_study_DAUIDs = all_study_DAUIDs[~all_study_DAUIDs.duplicated(subset = 0)]
FSAs_to_DAUIDs = FSAs_to_DAUIDs[FSAs_to_DAUIDs["DAUID"].isin(all_study_DAUIDs[0])]
# subset only places that are in study FSAs, it is the best filtering that can be done at the moment 
all_canadian_places = all_canadian_places[all_canadian_places["FSA"].isin(FSAs_to_DAUIDs["CFSAUID"])]

# COMMAND ----------

city_DAUIDs = get_metro_DAUIDs("Toronto")
FSAs_to_DAUIDs[FSAs_to_DAUIDs["DAUID"].isin(city_DAUIDs)]

# COMMAND ----------

city_df.groupby("week").size().max()

# COMMAND ----------

all_dc_df = pd.DataFrame()
city_df = all_canada_device_count[all_canada_device_count["city"] == city]
city_dc_df = pd.DataFrame()
city_DAUIDs = get_metro_DAUIDs(city)
city_FSAs = FSAs_to_DAUIDs[FSAs_to_DAUIDs["DAUID"].isin(city_DAUIDs)]
# subset only places that are in study FSAs, it is the best filtering that can be done at the moment 
city_places = all_canadian_places[all_canadian_places["FSA"].isin(city_FSAs["CFSAUID"])]
for week in city_problem_weeks[city]:
    # already seen this week
    seen_placekeys = weeks_to_placekeys[city][week]
    # not yet 'seen' placekeys for the ENTIRE city. there is no guarantee they will be in this week. this cannot exceed the max number of placekeys seen in a week
    new_places_by_week = city_places[~city_places["placekey"].isin(seen_placekeys)]
    if (len(new_places_by_week) > 20000):
        random_idx = np.random.randint(0, len(new_places_by_week), 20000)
    else:
        random_idx = np.random.randint(0, len(new_places_by_week), len(new_places_by_week) // 2)
    city_placekeys = new_places_by_week["placekey"].to_numpy()[random_idx].tolist()
    print("Week of " + str(week))
    print("Placekeys to search: " + str(len(city_placekeys)))
    city_weekly_sg_raw_data = populate_all_weekly_visitor_counts_for_ca_city_threaded(week, city_placekeys)
    city_dc_df = city_dc_df.append(city_weekly_sg_raw_data)
    create_and_save_as_table(city_dc_df, city + '_device_count_sink_weeks')
    all_dc_df = all_dc_df.append(city_dc_df)
create_and_save_as_table(all_dc_df, 'all_canada_device_count_sink_weeks')

# COMMAND ----------

all_dc_df = pd.DataFrame()
for week in weeks_to_seen_placekeys.keys():
    seen_placekeys = weeks_to_seen_placekeys[week]
    # only search placekeys that have not been seen for a given week
    new_places_by_week = oct5th_placekeys_study.loc[~oct5th_placekeys_study["placekey"].isin(seen_placekeys), "placekey"].tolist()
    print("Week of " + str(week))
    print("Placekeys to search: " + str(len(new_places_by_week)))
    city_sg_raw_data = populate_all_weekly_visitor_counts_for_ca_city_threaded(week, new_places_by_week)
    create_and_save_as_table(city_sg_raw_data, week.replace("-", "_") + 'device_count_0315_sink_week')
    all_dc_df = all_dc_df.append(city_sg_raw_data)
create_and_save_as_table(all_dc_df, 'all_canada_device_count_0315_sink_weeks')

# COMMAND ----------

all_dc_df = all_dc_df[~all_dc_df.duplicated()]

# COMMAND ----------

can_device_counts = all_dc_df
can_device_counts["DAUID"] = can_device_counts["poi_cbg"].str[3:]
can_device_counts = can_device_counts.merge(all_study_DAUIDs, left_on = "DAUID", right_on = 0, how = "left")
can_device_counts.dropna(subset = ["CMA"])

# COMMAND ----------

for CMA in can_device_counts.dropna(subset = ["CMA"])["CMA"].unique():
    sns.set_style('dark')
    city_df = can_device_counts[can_device_counts["CMA"] == CMA]
    sns.lineplot(x = "date_range_start", 
                 y = "normalized_visits_by_state_scaling",
                 color='r',
                 data = city_df)
    plt.xticks(rotation = 25)
    plt.title("All " + CMA + " normalized_visits_by_state_scaling")
    plt.show()

# COMMAND ----------

all_study_DAUIDs[all_study_DAUIDs["CMA"] == CMA]

# COMMAND ----------

week

# COMMAND ----------

# old
all_canada_raw_data = pd.DataFrame()
all_canadian_placekeys = all_canadian_places["placekey"]
all_study_DAUIDs = pd.DataFrame()
for city in can_city_index["0"].unique():
    CMA_DAUIDs = pd.DataFrame(get_metro_DAUIDs(city))
    CMA_DAUIDs["city"] = city
    all_study_DAUIDs = all_study_DAUIDs.append(CMA_DAUIDs)
    if city == "Hamilton":
        city_sg_raw_data = get_table_as_pandas_df(city + "_device_count")
    else:
        city_sg_raw_data_1 = get_table_as_pandas_df(city + "_device_count")
        city_sg_raw_data_2 = get_table_as_pandas_df(city + "_device_count_pt2")
        city_sg_raw_data_3 = get_table_as_pandas_df(city + "_device_count_missing_weeks")
        city_sg_raw_data = pd.concat([city_sg_raw_data_1, city_sg_raw_data_2, city_sg_raw_data_3])
    city_sg_raw_data["DA"] = city_sg_raw_data["poi_cbg"].str[3:]
    city_sg_raw_data["city_study"] = city
    all_canada_raw_data = all_canada_raw_data.append(city_sg_raw_data)
study_FSAs_to_DAUIDs = FSAs_to_DAUIDs[FSAs_to_DAUIDs["DAUID"].isin(all_study_DAUIDs)]
study_placekeys = all_canadian_placekeys[all_canadian_places["FSA"].isin(study_FSAs_to_DAUIDs["CFSAUID"])]
# duplicates for ALL  columns- do not want these, get rid of them
all_canada_raw_data = all_canada_raw_data[~all_canada_raw_data.duplicated()]
create_and_save_as_table(all_canada_raw_data, 'all_canada_device_count_0313_2')
sg_raw_data_placekeys = all_canada_raw_data["placekey"].unique()
all_study_DAUIDs[all_study_DAUIDs.duplicated(subset = 0, keep = False)].groupby("city").size()

# COMMAND ----------

all_study_DAUIDs = all_study_DAUIDs.rename(columns = {0 : "DAUID"})

# COMMAND ----------

can_device_counts = all_canada_raw_data
can_device_counts = can_device_counts[~can_device_counts.duplicated()]
can_device_counts["week"] = can_device_counts["date_range_start"].str[:10]
can_device_counts["week_num"] = pd.to_datetime(can_device_counts["week"]).dt.week
can_device_counts["year"] = can_device_counts["date_range_start"].str[:4]
can_device_counts["week_uid"] = can_device_counts["year"].astype(str) + "." + can_device_counts["week_num"].astype(str).str.zfill(2)
can_device_counts["DAUID"] = can_device_counts["poi_cbg"].str[3:]
can_device_counts = can_device_counts.rename(columns = {"city" : "safegraph_city", "city_study" : "CMA"})
total_devices_by_week = can_device_counts.groupby("week").sum()
can_device_counts

# COMMAND ----------

can_device_counts_agg = can_device_counts[["raw_visit_counts", "normalized_visits_by_total_visits", "normalized_visits_by_state_scaling", "DAUID", "week"]].groupby(["DAUID", "week"]).sum().reset_index()
can_device_counts_city = can_device_counts_agg.merge(all_study_DAUIDs, on = "DAUID", how = "left")
can_device_counts_city

# COMMAND ----------

for city in can_city_index["0"].unique():
    sns.set_style('dark')
    city_df = can_device_counts_city[can_device_counts_city["city"] == city].groupby("week").sum().reset_index()
    sns.lineplot(x = "week", 
                     y = "normalized_visits_by_state_scaling",
                     color='r',
                     data = city_df)
    plt.xticks(rotation = 25)
    plt.title("All " + city + " normalized_visits_by_state_scaling")
    plt.show()

# COMMAND ----------

can_device_counts = all_canada_raw_data
#can_device_counts = get_table_as_pandas_df('all_canada_device_count')
can_device_counts["week"] = can_device_counts["date_range_start"].str[:10]
can_device_counts["week_num"] = pd.to_datetime(can_device_counts["week"]).dt.week
can_device_counts["year"] = can_device_counts["date_range_start"].str[:4]
can_device_counts["week_uid"] = can_device_counts["year"].astype(int)+can_device_counts["week_num"].astype(int)/100
can_device_counts["week_uid"] = round(can_device_counts["week_uid"],2).astype(str)
can_device_counts["poi_cbg"] = can_device_counts["poi_cbg"].str[3:]
can_device_counts = can_device_counts.rename(columns = {"city" : "safegraph_city", "city_study" : "CMA"})
can_device_counts = can_device_counts[~can_device_counts.duplicated()]
can_device_counts

# COMMAND ----------

can_device_counts_city = can_device_counts.merge(all_study_DAUIDs, left_on = "poi_cbg", right_on = 0, how = "left")


# COMMAND ----------

can_device_counts_city = can_device_counts_city.rename(columns = {0 : "downtown_DA", "city" : "downtown_area"})

# COMMAND ----------

can_device_counts_city.groupby(["city", "poi_cbg", "week"]).sum()

# COMMAND ----------

create_and_save_as_table(can_device_counts_city, 'all_canada_device_counts_dt')

# COMMAND ----------

import datetime as dt
devices_weeks_df = pd.DataFrame(total_devices_by_week)
devices_weeks_df["week"] = pd.to_datetime(devices_weeks_df.index.values)
devices_weeks_df["week_num"] = devices_weeks_df["week"].dt.week
devices_weeks_df["year"] = devices_weeks_df["week"].dt.year
devices_weeks_df["week_uid"] = devices_weeks_df["year"].astype(int)+devices_weeks_df["week_num"].astype(int)/100
devices_weeks_df["week_uid"] = round(devices_weeks_df["week_uid"],2).astype(str)
devices_weeks_df

# COMMAND ----------

city_raw_data

# COMMAND ----------

def get_lq_by_week(da_list, idx, week):
    dt_DAUIDs = [str.strip(dauid)[1:-1] for dauid in da_list.split(",")]
    dt_DAUIDs[0] = dt_DAUIDs[0][1:]
    dt_DAUIDs[-1] = dt_DAUIDs[-1][:-1]
    downtown_devices = can_device_counts[can_device_counts["poi_cbg"].isin(dt_DAUIDs)]
    downtown_devices_week = downtown_devices[downtown_devices["week_uid"] == week]
    downtown_devices_sum = np.sum(downtown_devices_week["normalized_visits_by_state_scaling"])
    total_devices = devices_weeks_df[devices_weeks_df["week_uid"]==week]["normalized_visits_by_state_scaling"]
    
   #print(downtown_devices_sum)
        
    comparison_week = "2019" + week[4:]
    downtown_devices_comparison_week = downtown_devices[downtown_devices["week_uid"] == comparison_week]
    downtown_devices_comparison_sum = np.sum(downtown_devices_comparison_week["normalized_visits_by_state_scaling"])
    total_comparison_devices = devices_weeks_df[devices_weeks_df["week_uid"]==comparison_week]["normalized_visits_by_state_scaling"]
    
    #print(downtown_devices_comparison_sum)
        
    n = float(float(downtown_devices_sum)/float(total_devices))
    d = float(float(downtown_devices_comparison_sum)/float(total_comparison_devices))
       
    #print(n,d)
    #print(np.divide(n,d))

    return np.divide(n,d)

# COMMAND ----------

week_list = np.arange(2020.03,2020.53,0.01)
week_list = np.append(week_list, np.arange(2021.01,2021.53,0.01))
week_list = np.append(week_list, np.arange(2022.01,2022.09,0.01))
week_list = [str(round(i,2)) for i in week_list]

for idx, rows in can_city_index.iterrows():
    if (rows["0"] == "Hamilton"):
        print(rows["downtown_da"])
    for i, week in enumerate(week_list):
        can_city_index.loc[idx, "LQ_" + str(week)] = round(get_lq_by_week(rows["downtown_da"], i, week), 2)

# COMMAND ----------

rows["downtown_da"]

# COMMAND ----------

hamilton = can_city_index[can_city_index["0"] == "Hamilton"]
dt_DAUIDs = hamilton["downtown_da"]
#dt_DAUIDs[0] = dt_DAUIDs[0][1:]
#dt_DAUIDs[-1] = dt_DAUIDs[-1][:-1]
dt_DAUIDs.unique()

# COMMAND ----------

can_device_counts[can_device_counts["city_study"] == "Hamilton"]["poi_cbg"].isin(dt_DAUIDs.unique()).sum()

# COMMAND ----------

downtown_devices = can_device_counts[can_device_counts["poi_cbg"].isin(dt_DAUIDs)]
downtown_devices_week = downtown_devices[downtown_devices["week_uid"] == week]
downtown_devices_sum = np.sum(downtown_devices_week["normalized_visits_by_state_scaling"])
total_devices = devices_weeks_df[devices_weeks_df["week_uid"]==week]["normalized_visits_by_state_scaling"]
    
print(downtown_devices_sum)
        
comparison_week = "2019" + week[4:]
downtown_devices_comparison_week = downtown_devices[downtown_devices["week_uid"] == comparison_week]
downtown_devices_comparison_sum = np.sum(downtown_devices_comparison_week["normalized_visits_by_state_scaling"])
total_comparison_devices = devices_weeks_df[devices_weeks_df["week_uid"]==comparison_week]["normalized_visits_by_state_scaling"]
    
print(downtown_devices_comparison_sum)
        
n = float(float(downtown_devices_sum)/float(total_devices))
d = float(float(downtown_devices_comparison_sum)/float(total_comparison_devices))
       
print(n,d)
print(np.divide(n,d))

# COMMAND ----------

downtown_devices

# COMMAND ----------

can_city_index_long = can_city_index.drop(columns = ["_c0", "1", "2", "geometry_x", "geometry_y", "CMA_da", "downtown_da"]).melt("0")
can_city_index_long["variable"] = can_city_index_long["variable"].str[3:]
can_city_index_long = can_city_index_long.merge(devices_weeks_df, left_on = "variable", right_on = "week_uid", how = "left")
can_city_index_long = can_city_index_long[["0", "value", "week"]]

# COMMAND ----------

hamilton_df = can_city_index_long[can_city_index_long["0"] == "Hamilton"]
hamilton_df["value"].unique()

# COMMAND ----------

can_city_index_long[can_city_index_long["0"] != "Hamilton"].sort_values(by = "value", ascending = False)

# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt
sns.set_style('dark')
fig = plt.figure(figsize = (15, 8))
sns.lineplot(data = can_city_index_long[can_city_index_long["0"] != "Hamilton"], x = "week", y = "value", hue = "0");

# COMMAND ----------

dt_DAUIDs = [str.strip(dauid)[1:-1] for dauid in rows["downtown_da"].split(",")]
dt_DAUIDs[0] = dt_DAUIDs[0][1:]
dt_DAUIDs[-1] = dt_DAUIDs[-1][:-1]
dt_DAUIDs

# COMMAND ----------

downtown_devices = can_device_counts[can_device_counts["poi_cbg"].isin(dt_DAUIDs)]
downtown_devices_week = downtown_devices[downtown_devices["week_uid"] == week]
downtown_devices_sum = np.sum(downtown_devices_week["normalized_visits_by_state_scaling"])
total_devices = devices_weeks_df[devices_weeks_df["week_uid"]==week]["normalized_visits_by_state_scaling"]
print(downtown_devices_sum)

# COMMAND ----------

downtown_devices

# COMMAND ----------

        
comparison_week = "2019" + week[4:]
downtown_devices_comparison_week = downtown_devices[downtown_devices["week_uid"] == comparison_week]
downtown_devices_comparison_sum = np.sum(downtown_devices_comparison_week["normalized_visits_by_state_scaling"])
total_comparison_devices = devices_weeks_df[devices_weeks_df["week_uid"]==comparison_week]["normalized_visits_by_state_scaling"]
    
print(downtown_devices_comparison_sum)
        
n = float(float(downtown_devices_sum)/float(total_devices))
d = float(float(downtown_devices_comparison_sum)/float(total_comparison_devices))
       
print(n,d)
print(np.divide(n,d))

# COMMAND ----------



# COMMAND ----------

all_canada_raw_data = get_table_as_pandas_df('all_canada_device_count')

# COMMAND ----------

study_placekeys = all_canadian_places_study["placekey"]

# COMMAND ----------

print("Percent of requested placekeys present in raw data df: ", str(study_placekeys.isin(all_canada_raw_data["placekey"]).sum() / len(study_placekeys)))
print("Percent of study DAs present in raw data df: ", str(all_study_DAUIDs.isin(all_canada_raw_data["DA"].unique()).sum() / len(all_study_DAUIDs)))
print("Date range in raw data df: ", str(all_canada_raw_data["date_range_start"].sort_values().str[:10].unique()[0]), str(all_canada_raw_data["date_range_end"].sort_values().str[:10].unique()[-1]))

# COMMAND ----------



# COMMAND ----------

pd.Series(covid_analysis_weeks)[~pd.Series(covid_analysis_weeks).isin(all_canada_raw_data["date_range_start"].str[:10].unique())]

# COMMAND ----------

import requests
import json

def sf_ca_gql_request(params):
    query = """
        query($city: String, $postal_code: String, $start_date: DateTime!, $end_date: DateTime!, $after: String="") {
          search(filter: { 
            address: {
              city: $city
              postal_code: $postal_code
              iso_country_code: "CA"
            }
          }){
            weekly_patterns(start_date: $start_date end_date: $end_date) {
              results(first: 500, after: $after) {
                pageInfo { hasNextPage endCursor }
                edges {
                  node {
                      placekey
                      city
                      postal_code
                      raw_visit_counts
                      poi_cbg
                      normalized_visits_by_total_visits
                      normalized_visits_by_state_scaling
                  }
                }
              }
            }
          }
        }
    """
    print(params)
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
            'start_date': '2020-01-04',
            'end_date': '2020-01-11',
            'city': 'Toronto',
            'postal_code': 'M1B',
            'after': ''
        }
ca_toronto_test = sf_ca_gql_request(params)
ca_toronto_test

# COMMAND ----------

# Create 1 zipcode device count pandas df row for all weeks
def populate_all_weekly_visitor_counts_for_zipcode(zipcode):
    # initialize lq_df - a df storing all zipcodes' weekly LQ's (cols=lq_week_date, rows=zipcode)
    vc_col_func = lambda col: 'vc_week_'+col
    cols = generate_lq_week_date_cols(vc_col_func)
    cols.insert(0,'zipcode')
    dc_df = pd.DataFrame(columns=cols)

    covid_analysis_weeks = generate_lq_week_date_cols()
    new_weekly_visitor_count_row = {}


    # For each week starting from 2018-03-05
    for week in covid_analysis_weeks:
        # Get all the places for in-scope zipcodes
        params = {
            'start_date': week,
            'end_date': weekend_date_string(week),
            'postal_code': zipcode,
        }
        zipcode_places_resp = sf_gql_request(params)

        # Convert API response to pandas df
        zipcode_places_df = sf_resp_to_pandas_df(zipcode_places_resp)

        if zipcode_places_df.size > 0:
            # Aggregate places by zipcodes
            agg_zipcode_df = agg_zipcode_places(zipcode_places_df)

            # Append new weekly raw_visit_count to lq_df
            new_weekly_visitor_count_row[vc_col_func(week)] = agg_zipcode_df.iloc[0]
    
    new_weekly_visitor_count_row['zipcode'] = zipcode
    dc_df = dc_df.append(new_weekly_visitor_count_row, ignore_index=True)
    dc_df.fillna(0)
    return dc_df
  
# test_dc_df = populate_all_weekly_visitor_counts_for_zipcode('10026')
# test_dc_df

# COMMAND ----------



# COMMAND ----------

display(all_canada_raw_data)

# COMMAND ----------

## Raw data pull aggregation by DA and week ## 
all_canada_raw_data["week_start"] = all_canada_raw_data["date_range_start"].str[:10]
week_cbg_canada_agg = all_canada_raw_data.groupby(by=['city_study', 'poi_cbg','week_start']).sum()
week_cbg_canada_agg = week_cbg_canada_agg.reset_index(level=['city_study','poi_cbg', 'week_start'])
display(week_cbg_canada_agg)

# COMMAND ----------

covid_analysis_weeks

# COMMAND ----------

create_and_save_as_table(week_cbg_canada_agg, 'all_canada_device_count_agg')

# COMMAND ----------

week_list = np.arange(2020.03,2020.53,0.01)
week_list = np.append(week_list, np.arange(2021.01,2021.53,0.01))
week_list = np.append(week_list, np.arange(2022.01,2022.09,0.01))
week_list = [str(round(i,2)) for i in week_list]

for i in week_list:
    can_city_index["LQ_"+i] = round(can_city_index[["CMA_da","downtown_da"]].apply(lambda x: get_lq_by_week(x,i), axis=1),2)

# COMMAND ----------

toronto_dt_da = canadian_city_index['downtown_da'][0]
toronto_dt_da = list(map(lambda x: x.strip("''"), toronto_dt_da.strip('][').split(', ')))
week_cbg_toronto_agg['poi_cbg'] = week_cbg_toronto_agg['poi_cbg'].str.replace('CA:','')
#downtown_toronto = week_cbg_toronto_agg['poi_cbg'].isin(toronto_dt_da)
downtown_toronto_dc = week_cbg_toronto_agg[week_cbg_toronto_agg['poi_cbg'].isin(toronto_dt_da)]
downtown_toronto_dc

# COMMAND ----------

toronto_dt_da

# COMMAND ----------

downtown_toronto_dc.groupby(by=['poi_cbg'])['date_range_start'].nunique().hist()

# COMMAND ----------

unique_weeks_per_cbg = downtown_toronto_dc.groupby(by='poi_cbg')['date_range_start'].nunique()
unique_weeks_per_cbg = unique_weeks_per_cbg.reset_index(level=['poi_cbg'])
unique_weeks_per_cbg

# COMMAND ----------

# unique_weeks_per_cbg.loc[unique_weeks_per_cbg[0] == 1, 'only 1 week'] = True

unique_weeks_per_cbg['single week data'] = False
unique_weeks_per_cbg.loc[unique_weeks_per_cbg['week'] == 1, 'single week data'] = True
display(unique_weeks_per_cbg)

# COMMAND ----------


def populate_dc_for_zipcodes(in_range_zipcodes):
    vc_col_func = lambda col: 'vc_week_'+col
    cols = generate_lq_week_date_cols(vc_col_func)
    cols.insert(0,'zipcode')
    all_zipcodes_df = pd.DataFrame(columns=cols)
    
    for zipcode in in_range_zipcodes:
        dc_df = populate_all_weekly_visitor_counts_for_zipcode_threaded(zipcode)
        all_zipcodes_df = all_zipcodes_df.append(dc_df, ignore_index=True)

    return all_zipcodes_df

# COMMAND ----------

# dc_df = populate_all_weekly_visitor_counts_for_zipcode('10026')

# test_dc_df = populate_dc_for_zipcodes(['10026','10027'])
# test_dc_df

# COMMAND ----------

# MAGIC %md **Create LQ's** 
# MAGIC 
# MAGIC LQ = (*Device count in ZCTA COVID Period A / Total device count for NY COVID Period A*) /   (*Device count in ZCTA Period B / Total device count for NY Period B*)
# MAGIC 
# MAGIC A = Active or Post COVID Period (e.g. June - August 2020)  
# MAGIC 
# MAGIC a = Pre-COVID Period Equivalent (e.g. June - August 2019)

# COMMAND ----------

# Generate Device Count Normalization

## HELPER FUNCTIONS ========================
# Closest monday from date
def closest_monday_from_date(date):
    date = datetime.strptime(date,'%Y-%m-%d') if isinstance(date, str) else date
    return date + timedelta(days=-date.weekday(), weeks=0)
    
# Create list of in-range date columns
def in_range_date_cols(start_date, end_date):
    vc_col_func = lambda col: 'vc_week_'+col
    
    # determines the closest monday from start and end date
    first_monday_from_start = closest_monday_from_date(start_date)
    last_monday_from_end = closest_monday_from_date(end_date)
    list_of_mondays = [vc_col_func(first_monday_from_start.strftime('%Y-%m-%d'))]
    
    current_monday = first_monday_from_start
    
    while current_monday < last_monday_from_end:
        last_datetime = current_monday
        current_monday = last_datetime + timedelta(days=7)
        list_of_mondays.append(vc_col_func(current_monday.strftime('%Y-%m-%d')))
        
    return list_of_mondays

# Expect custom dc_df where each row is a zipcode and each column is a week's device/visitor count
def sum_dc_in_date_range(start_date, end_date, dc_df, dc_sum_col_title):
    in_range_dc_df = pd.DataFrame()
#     in_range_dc_df['zipcode'] = dc_df['zipcode']
    in_range_date_col_names = in_range_date_cols(start_date, end_date)
    in_range_dc_df[dc_sum_col_title] = dc_df[in_range_date_col_names].sum(axis=1)
    return in_range_dc_df

# Sum columns for a list of in range zipcodes
def sum_in_range_zipcodes_dc(dc_df):
    summed_dc_df = dc_df.sum(axis=0)
    summed_dc_df = summed_dc_df.to_frame().transpose()
    return summed_dc_df
  
# Caculate the total number of visits in a week (given a start date) in a dataframe of zipcodes + visitor counts
def set_total_weekly_vc(start_date, zipcode_vc_df):
    end_date = weekend_date_string(start_date)
    total_vc_by_zipcode_in_covid_range = sum_dc_in_date_range(start_date, end_date, zipcode_vc_df, 'total_zipcode_vc')
    total_vc = sum_in_range_zipcodes_dc(total_vc_by_zipcode_in_covid_range).values[0]
    return total_vc[0]
    
## HELPER FUNCTIONS ========================^^

def generate_dc_normalization(zcta_dc, total_dc):
    return zcta_dc / total_dc

# COMMAND ----------

## ZCTA LQ generation helper functions

# return list of tuples of period date ranges (start datetime, end datetime)
def set_in_scope_periods(start_date, end_date, period_duration_in_weeks):
    start_date = datetime.strptime(start_date,'%Y-%m-%d')
    end_date = datetime.strptime(end_date,'%Y-%m-%d')
    
    in_scope_periods = []
    
    current_date = start_date
    while current_date < end_date:
        current_end_date = current_date + timedelta(weeks=period_duration_in_weeks) + timedelta(days=-1)
        current_period_range = {'start_date': current_date, 'end_date': current_end_date}
        in_scope_periods.append(current_period_range)
        
        current_date = current_end_date + timedelta(days=1)
    
    return in_scope_periods

# return a list of tuples of equivalent covid periods (covid_period_date_range, precovid_period_date_range)
# assumed that there will be 2 years diff between covid date and control date
def set_equivalent_covid_periods(start_covid_date, end_covid_date, period_duration_in_weeks):
    start_covid_datetime = datetime.strptime(start_covid_date,'%Y-%m-%d')
    end_covid_datetime = datetime.strptime(end_covid_date,'%Y-%m-%d')
    
    earliest_covid_start_date = datetime.strptime('2020-03-02','%Y-%m-%d')
    if start_covid_datetime < earliest_covid_start_date:
        return 'Start date is too early. Start COVID date from 2018-03-05 or beyond'
    
    latest_covid_end_date = datetime.strptime('2021-12-27','%Y-%m-%d')
    if end_covid_datetime > latest_covid_end_date:
        return 'End date is too late. End COVID date from 2019-12-27 or before'
    
    covid_periods = set_in_scope_periods(start_covid_date, end_covid_date, period_duration_in_weeks)
    first_covid_period = covid_periods[0]
    last_covid_period = covid_periods[-1]
    
    # Find an equivalent period in 2019 from the current date
    weeks_back = lambda year : -104 if year == 2021 else -52
    
    equivalent_control_period_start = first_covid_period['start_date'] + timedelta(weeks=weeks_back(first_covid_period['start_date'].year))
    equivalent_control_period_end = last_covid_period['end_date'] + timedelta(weeks=weeks_back(last_covid_period['end_date'].year))
    control_periods = set_in_scope_periods(equivalent_control_period_start.strftime('%Y-%m-%d'), 
                                           '2019-12-30', 
                                           period_duration_in_weeks
                                          )
    control_periods_second_half = set_in_scope_periods('2019-01-07', 
                                           equivalent_control_period_end.strftime('%Y-%m-%d'), 
                                           period_duration_in_weeks
                                          )
    control_periods.extend(control_periods_second_half)
    print(len(covid_periods), len(control_periods))
    equivalent_covid_period_list = list(zip(control_periods, covid_periods))
    
    return equivalent_covid_period_list
    
# def create_zcta_lq_dictionary(period_date_range, dc_df, zcta_dictionary):
    # Set total visitor counts for zcta = sum of total visitor counts for zipcode
    # Set period A device count normalization (generate_dc_normalization)
    # Set period B device count normalization (generate_dc_normalization)
    # Set LQ = period A dc normalization / period B dc normalization
    # Set scope period LQ (col title) to LQ in zcta dictionary
    
test = set_equivalent_covid_periods('2020-03-02', '2021-12-27', 1)

# COMMAND ----------

## WARNING: RUNNING THIS WILL TAKE HOURSSSSSSS. ONLY RUN THIS IF ABSOLUTELY NECESSARY!!! ##

# Generate data table of visitor count in range
# Get list of downtown definitions
downtown_definition_df = get_table_as_pandas_df('city_index_0119_csv')
downtown_definition_df.drop([46, 47])

downtowns_zipcodes_list = downtown_definition_df['zipcodes'].tolist() # ['['12345','23456', etc.]']
city_name = downtown_definition_df['0'].tolist()
city_zipcodes = list(zip(downtowns_zipcodes_list, city_name))

def create_visitor_count_table_for_city(city):
    if city[1] != 'Los Angeles' and city[1] != 'New York':
        downtown = list(map(lambda x: x.strip("''"), city[0].strip('][').split(', ')))
        city_name = city[1]
        table_name = city_name.replace(' ', '_') + '_weekly_vc_2018_2021'
        
        print("Downloading " + city_name)
        
        # get visitor counts for in range dates (2018 - 2021)
        downtown_vc_df = populate_dc_for_zipcodes(downtown)

        # save to datatable
        create_and_save_as_table(downtown_vc_df, table_name)
        
        print("Done with " + city_name)

## UNCOMMENT WHEN CELL IN USE ##
# with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
#         the_futures = [executor.submit(create_visitor_count_table_for_city, city) for city in city_zipcodes]
#         for future in concurrent.futures.as_completed(the_futures):
#             pass

# COMMAND ----------

canadian_city_index = get_table_as_pandas_df('can_city_index_da')

# COMMAND ----------

# Pull canadian data
params = {
            'start_date': '2019-01-07',
            'end_date': '2019-02-04',
            'city': 'Toronto',
        }
zipcode_places_resp = sf_gql_request(params)
zipcode_places_resp
canadian_city_index['City'].tolist()
zipcode_places_resp

# COMMAND ----------

"""Calculates LQ's for a set analysis period

Parameters
----------
covid_period : tuple datetime, required
    A pair of start and end dates. First date range is for pre-covid, second date range is for covid analysis.
analysis_zipcodes : list string or list, required
    A list of zipcodes or lists of zipcodes. This gives us the unit of analysis
comparison_zipcodes: list string, required
    A list of zipcodes considered the comparison scope. For instance, if we want to compare the visitor count of the analysis unit to the entire LA metro area, we would provide all the zipcodes in the LA metro area.
visitorcount_df: dataframe, required
    A dataframe with rows of zipcodes and columns of weekly visit counts

Returns
-------
dict
    A dictionary of LQ's in unit of analysis_zipcodes
"""
def generate_lq_in_daterange_and_zipcode_scope(covid_period, analysis_zipcodes, comparison_zipcodes, visitorcount_df):
    analysis_lq = {}

    # Generate LQ's given a data table name, start_date, end_date, period length (in num weeks)
    vc_col_func = lambda col: 'vc_week_'+col
    lq_col_func = lambda covid_period: "lq_"+covid_period[1]['start_date'].strftime('%Y-%m-%d')
    
    precovid_start_date = covid_period[0]['start_date']
    covid_start_date = covid_period[1]['start_date']
    analysis_zipcodes = [analysis_zipcodes] if isinstance(analysis_zipcodes, str) else analysis_zipcodes

    # Set Total VC for all zipcodes in downtown
    precovid_vc_week_total = set_total_weekly_vc(precovid_start_date, vistorcount_df)
    covid_vc_week_total = set_total_weekly_vc(covid_start_date, vistorcount_df)

    # Find ZCTA VC
    set_difference = set(analysis_zipcodes) - set(visitorcount_df.index)
    list_difference = list(set_difference)
    if len(list_difference) > 0:
        print('Zipcodes not in visitor counts: ', list_difference)
        analysis_zipcodes = [zipcode for zipcode in analysis_zipcodes if zipcode not in list_difference]
    zipcode_lq = visitorcount_df.loc[analysis_zipcodes,:]
    precovid_lq_col_name = vc_col_func(covid_period[0]['start_date'].strftime('%Y-%m-%d'))
    covid_lq_col_name = vc_col_func(covid_period[1]['start_date'].strftime('%Y-%m-%d'))
    
    precovid_vc = zipcode_lq[precovid_lq_col_name].sum()
    covid_vc = zipcode_lq[covid_lq_col_name].sum()
#     print("covid vc: ", covid_vc, " covid total vc: ", covid_vc_week_total, " precovid_vc: ", precovid_vc, "precovid_vc_week_total: ", precovid_vc_week_total)
    current_week_lq = (covid_vc/covid_vc_week_total) / (precovid_vc / precovid_vc_week_total)
    current_lq_col_name = lq_col_func(covid_period)
    
    raw_vcs = False
    
    if current_week_lq > 10:
        raw_vcs = {
            'covid_vc': covid_vc,
            'covid_vc_week_total': covid_vc_week_total,
            'precovid_vc': precovid_vc,
            'precovid_vc_week_total': precovid_vc_week_total,
        }
        
    if math.isinf(current_week_lq) is not True and math.isnan(current_week_lq) is not True:
        analysis_lq[current_lq_col_name] = current_week_lq 
    else:
        analysis_lq[current_lq_col_name] = 0
        current_week_lq = 0
        list_of_weird_zipcodes.append((current_lq_col_name, covid_period))
    return (current_lq_col_name, current_week_lq, raw_vcs)
    

# COMMAND ----------

def get_city_zipcodes(city_name):
    dt_vc = get_table_as_pandas_df('city_csa_fips_codes___us_cities_downtowns__1__csv')
    dt_zipcodes = dt_vc[dt_vc['city'] == city_name]['dt_zipcodes']
    
    return dt_zipcodes.tolist()[0].strip('][').split(', ')
    
# get_city_zipcodes('New York')

# COMMAND ----------

## LOAD DATA FOR CMD 22
# downtown_in_range_vc = get_table_as_pandas_df('los_angeles_weekly_vc_2018_2021')
metro_in_range_vc = get_table_as_pandas_df('new_york_weekly_vc_2018_2021')

# COMMAND ----------

## Create DF of device count by zipcode
import numpy as np
import math

list_of_problem_cities = ['Phoenix','Dallas','Charlotte','Oklahoma City','Sacramento','Atlanta','Miami','Bakersfield','Wichita','Tampa','Cleveland','Cincinnati','Salt Lake City','Orlando','Colorado Springs']
# list_of_problem_cities = ['Dallas']

# Generate LQ's given a data table name, start_date, end_date, period length (in num weeks)
vc_col_func = lambda covid_period: 'vc_week_'+covid_period[1]['start_date'].strftime('%Y-%m-%d')
lq_col_func = lambda covid_period: "lq_"+covid_period[1]['start_date'].strftime('%Y-%m-%d')

# Set lq column names
equivalent_covid_periods = set_equivalent_covid_periods('2020-03-02', '2021-12-27', 1)
vc_col_names = [vc_col_func(covid_period) for covid_period in equivalent_covid_periods]
vc_col_names = vc_col_names.append('zipcode')

# Create empty lq df with lq columns 
zipcode_vc_df = pd.DataFrame(columns = vc_col_names)

for city_name in list_of_problem_cities:
    metro_in_range_vc = get_table_as_pandas_df(city_name.lower().replace(' ','_') + '_weekly_vc_2018_2021')
    metro_zipcodes = metro_in_range_vc['zipcode']
    metro_in_range_vc = metro_in_range_vc.set_index('zipcode')
    # equivalent_covid_periods

    list_of_weird_zipcodes = []
    # For each zcta in downtown:
    for zipcode in metro_zipcodes:
        # Create empty zcta dictionary 
        zcta_vc = {}

        # For each in scope period:
        for covid_period in equivalent_covid_periods:
            analysis_zipcodes = zipcode
            comparison_zipcodes = metro_zipcodes
            visitorcount_df = metro_in_range_vc
            current_vc_week = covid_period[1]['start_date'].strftime('%Y-%m-%d')
            zcta_vc['zipcode'] = zipcode
            zcta_vc['city'] = city_name
            zcta_vc['week'] = current_vc_week
            zcta_vc['precovid_vc_zipcode_count'] = visitorcount_df.loc[zipcode,'vc_week_'+covid_period[0]['start_date'].strftime('%Y-%m-%d')]
            zcta_vc['covid_vc_zipcode_count'] = visitorcount_df.loc[zipcode,'vc_week_'+covid_period[1]['start_date'].strftime('%Y-%m-%d')]
            zipcode_vc_df = zipcode_vc_df.append(zcta_vc, ignore_index=True)

# COMMAND ----------

# create_and_save_as_table(zipcode_vc_df, 'sink_city_zipcode_counts')
zipcode_vc_df['zipcode'].unique().size

# COMMAND ----------

## Create DF of LQ's by zipcode
import numpy as np
import math

# list_of_problem_cities = ['Phoenix','Dallas','Charlotte','Oklahoma City','Sacramento','Atlanta','Miami','Bakersfield','Wichita','Tampa','Cleveland','Cincinnati','Salt Lake City','Orlando','Colorado Springs']
list_of_problem_cities = ['Dallas']

for city_name in list_of_problem_cities:
    metro_in_range_vc = get_table_as_pandas_df(city_name.lower().replace(' ','_') + '_weekly_vc_2018_2021')

    # Generate LQ's given a data table name, start_date, end_date, period length (in num weeks)
    vc_col_func = lambda col: 'vc_week_'+col
    lq_col_func = lambda covid_period: "lq_"+covid_period[1]['start_date'].strftime('%Y-%m-%d')

    # Set lq column names
    equivalent_covid_periods = set_equivalent_covid_periods('2020-03-02', '2021-12-27', 1)
    lq_col_names = [lq_col_func(covid_period) for covid_period in equivalent_covid_periods]
    lq_col_names = lq_col_names.append('zipcode')

    # Create empty lq df with lq columns 
    lq_df = pd.DataFrame(columns = lq_col_names)

    # metro_zipcodes = downtown_in_range_vc['zipcode'].tolist()
    metro_zipcodes = get_city_zipcodes(city_name)
    metro_in_range_vc = metro_in_range_vc.set_index('zipcode')

    # equivalent_covid_periods

    list_of_weird_zipcodes = []
    # For each zcta in downtown:
    for zipcode in metro_zipcodes:
        # Create empty zcta dictionary 
        zcta_lqs = {}

        # For each in scope period:
        for covid_period in equivalent_covid_periods:
            analysis_zipcodes = zipcode
            comparison_zipcodes = metro_zipcodes
            vistorcount_df = metro_in_range_vc
            current_lq_col_name, current_week_lq, raw_vcs = generate_lq_in_daterange_and_zipcode_scope(covid_period, analysis_zipcodes, comparison_zipcodes, vistorcount_df)
            zcta_lqs[current_lq_col_name] = current_week_lq
        # Append zcta dictionary as row in lq_df
        zcta_lqs['zipcode'] = zipcode
        lq_df = lq_df.append(zcta_lqs, ignore_index=True)
    print(len(list_of_weird_zipcodes))
    lq_df['city'] = city_name

# lq_df = lq_df.transpose()
# lq_df['Weeks Since COVID Start'] = list(range(1,95))
# lq_df.plot(x='Weeks Since COVID Start')
# lq_df

# COMMAND ----------

lq_df

# COMMAND ----------

## Downtown to City LQ Generation Code ##
test_lq_df = pd.DataFrame(columns = lq_col_names)
## Change the cities in the city_name_list
# city_name_list = ['New York', 'Los Angeles', 'San Francisco', 'Phoenix', 'Houston', 'Bakersfield']
# city_name_list = ['Boston','New York','Philadelphia','Baltimore','Washington DC']
# city_name_list = ['San Francisco','Oakland','San Jose','Los Angeles','San Diego']
# city_name_list = ['Seattle', 'Portland', 'Sacramento', 'Fresno', 'Bakersfield']
# city_name_list = ['Salt Lake City','Las Vegas','Phoenix','Tucson','Albuquerque']
# city_name_list = ['Dallas', 'Houston', 'San Antonio', 'Austin', 'El Paso']
# city_name_list = ['Jacksonville', 'Miami', 'Orlando', 'Tampa', 'New Orleans']
# city_name_list = ['Chicago', 'Milwaukee', 'Minneapolis', 'Detroit', 'Cleveland']
# city_name_list = ['Denver', 'Colorado Springs', 'Wichita', 'Tulsa', 'Oklahoma City']
# city_name_list = ['Omaha', 'St Louis', 'Nashville', 'Memphis', 'Kansas City']
# city_name_list = ['Louisville', 'Indianapolis', 'Cincinnati', 'Columbus', 'Pittsburgh']
# city_name_list = ['Charlotte', 'Raleigh', 'Atlanta']
city_name_list = downtown_definition_df['0'].tolist()
skip_city_list = ['Virginia Beach', 'Mesa', 'Arlington', 'Aurora', 'Long Beach']

for city_name in city_name_list:
    if city_name not in skip_city_list:
        zcta_lqs = {}

        dtw_zipcodes = get_city_zipcodes(city_name)
        metro_in_range_vc = get_table_as_pandas_df(city_name.lower().replace(' ','_') + '_weekly_vc_2018_2021')
        metro_zipcodes = metro_in_range_vc['zipcode'].tolist()
        metro_in_range_vc = metro_in_range_vc.set_index('zipcode')

        for covid_period in equivalent_covid_periods:
            analysis_zipcodes = dtw_zipcodes
            comparison_zipcodes = metro_zipcodes
            vistorcount_df = metro_in_range_vc
            current_lq_col_name, current_week_lq, raw_vc = generate_lq_in_daterange_and_zipcode_scope(covid_period, analysis_zipcodes, comparison_zipcodes, vistorcount_df)
            zcta_lqs[current_lq_col_name] = current_week_lq

        # Append zcta dictionary as row in lq_df
        zcta_lqs['city'] = city_name
        test_lq_df = test_lq_df.append(zcta_lqs, ignore_index=True)
# print(len(list_of_weird_zipcodes))
test_lq_df = test_lq_df.set_index('city')
# test_lq_df
# Pandas plot them
test_lq_df = test_lq_df.transpose()

# COMMAND ----------

create_and_save_as_table(test_lq_df, 'all_cities_weekly_lq')
# test_lq_df

# COMMAND ----------

# Create an all metro df
downtown_definition_df = get_table_as_pandas_df('city_index_0119_csv')
city_name_list = downtown_definition_df['0'].tolist()
all_metro_vc_df = pd.DataFrame()
skip_city_list = ['Virginia Beach', 'Mesa', 'Arlington', 'Aurora', 'Long Beach']

for city_name in city_name_list:
    print(city_name)
    if city_name not in skip_city_list:
        metro_in_range_vc = get_table_as_pandas_df(city_name.lower().replace(' ','_') + '_weekly_vc_2018_2021')
        print(metro_in_range_vc.size)
        all_metro_vc_df = all_metro_vc_df.append(metro_in_range_vc, ignore_index = True)
    
all_metro_vc_df.size

# COMMAND ----------

all_metro_vc_df

# COMMAND ----------

## Downtown to all metro zipcodes LQ Generation Code ## 
dt_all_metro_lq_df = pd.DataFrame(columns = lq_col_names)
city_name_list = downtown_definition_df['0'].tolist()
skip_city_list = ['Virginia Beach', 'Mesa', 'Arlington', 'Aurora', 'Long Beach']

for city_name in city_name_list:
    if city_name not in skip_city_list:
        zcta_lqs = {}

        dtw_zipcodes = get_city_zipcodes(city_name)
        metro_in_range_vc = all_metro_vc_df
        metro_zipcodes = metro_in_range_vc['zipcode'].tolist()
        metro_in_range_vc = metro_in_range_vc.set_index('zipcode')

        for covid_period in equivalent_covid_periods:
            analysis_zipcodes = dtw_zipcodes
            comparison_zipcodes = metro_zipcodes
            vistorcount_df = metro_in_range_vc
            current_lq_col_name, current_week_lq, raw_vc = generate_lq_in_daterange_and_zipcode_scope(covid_period, analysis_zipcodes, comparison_zipcodes, vistorcount_df)
            zcta_lqs[current_lq_col_name] = current_week_lq

        # Append zcta dictionary as row in lq_df
        zcta_lqs['city'] = city_name
        dt_all_metro_lq_df = dt_all_metro_lq_df.append(zcta_lqs, ignore_index=True)

dt_all_metro_lq_df = dt_all_metro_lq_df.set_index('city')
dt_all_metro_lq_df = dt_all_metro_lq_df.transpose()

# COMMAND ----------

dt_all_metro_lq_df.rolling(5, center=True).mean()

# COMMAND ----------

create_and_save_as_table(dt_all_metro_lq_df, 'all_cities_to_all_metro_weekly_lq')
dt_all_metro_lq_df

# COMMAND ----------

test_lq_df

# COMMAND ----------

## Find sinks by flagging cities with LQ's over 10
test_lq_df = pd.DataFrame(columns = lq_col_names)
city_name_list = downtown_definition_df['0'].tolist()
skip_city_list = ['Virginia Beach', 'Mesa', 'Arlington', 'Aurora', 'Long Beach']
high_lq_city_list = []

for city_name in city_name_list:
    print(city_name)
    if city_name not in skip_city_list:
        print("processing...")
        zcta_lqs = {}

        dtw_zipcodes = get_city_zipcodes(city_name)
        metro_in_range_vc = get_table_as_pandas_df(city_name.lower().replace(' ','_') + '_weekly_vc_2018_2021')
        metro_zipcodes = metro_in_range_vc['zipcode'].tolist()
        metro_in_range_vc = metro_in_range_vc.set_index('zipcode')

        for covid_period in equivalent_covid_periods:
            analysis_zipcodes = dtw_zipcodes
            comparison_zipcodes = metro_zipcodes
            vistorcount_df = metro_in_range_vc
            current_lq_col_name, current_week_lq, raw_vc = generate_lq_in_daterange_and_zipcode_scope(covid_period, analysis_zipcodes, comparison_zipcodes, vistorcount_df)
            if current_week_lq > 10:
                print("**FLAG HIGH LQ: ", current_week_lq," **")
                high_lq_city_list.append(city_name)
            zcta_lqs[current_lq_col_name] = current_week_lq

        # Append zcta dictionary as row in lq_df
        zcta_lqs['city'] = city_name
        test_lq_df = test_lq_df.append(zcta_lqs, ignore_index=True)
        print("done.")
    else:
        print("skipped")
test_lq_df = test_lq_df.set_index('city')
# test_lq_df
# Pandas plot them
test_lq_df = test_lq_df.transpose()

# COMMAND ----------

pd.set_option('display.max_rows', None)

# COMMAND ----------

# Sink Investigation pt 2
high_lq_city_list = list(set(high_lq_city_list))
print(high_lq_city_list)
test_lq_df.loc[:,high_lq_city_list]

# test_lq_df = pd.DataFrame(columns = lq_col_names)
# raw_vcs = {
#             'covid_vc': covid_vc,
#             'covid_vc_week_total': covid_vc_week_total,
#             'precovid_vc': precovid_vc,
#             'precovid_vc_week_total': precovid_vc_week_total,
#         }
high_lq_df = pd.DataFrame(columns = ['city', 'week', 'lq', 'covid_vc', 'covid_vc_week_total', 'precovid_vc', 'precovid_vc_week_total'])
city_name_list = high_lq_city_list

for city_name in city_name_list:
    high_lq_breakdown = {}
    
    dtw_zipcodes = get_city_zipcodes(city_name)
    metro_in_range_vc = get_table_as_pandas_df(city_name.lower().replace(' ','_') + '_weekly_vc_2018_2021')
    metro_zipcodes = metro_in_range_vc['zipcode'].tolist()
    metro_in_range_vc = metro_in_range_vc.set_index('zipcode')

    for covid_period in equivalent_covid_periods:
        analysis_zipcodes = dtw_zipcodes
        comparison_zipcodes = metro_zipcodes
        vistorcount_df = metro_in_range_vc
        current_lq_col_name, current_week_lq, raw_vcs = generate_lq_in_daterange_and_zipcode_scope(covid_period, analysis_zipcodes, comparison_zipcodes, vistorcount_df)
        if current_week_lq > 10:
            high_lq_breakdown = {
                'city': city_name,
                'week': current_lq_col_name,
                'lq': current_week_lq,
                **raw_vcs,
            }
            high_lq_df = high_lq_df.append(high_lq_breakdown, ignore_index=True)

    # Append zcta dictionary as row in lq_df
    
# print(len(list_of_weird_zipcodes))
# test_lq_df = test_lq_df.set_index('city')
# test_lq_df
# Pandas plot them

# COMMAND ----------

high_lq_df.display()

# COMMAND ----------

import matplotlib as mpl
import matplotlib.pyplot as plt
plt.figure(figsize=(20,12))

mpl.style.use('seaborn')

## Change cities_to_highlight what cities you want to specifically see
# cities_to_highlight = ['San Francisco', 'Los Angeles']
#cities_to_highlight = ['Houston']
cities_to_highlight = []
num_highlights = len(cities_to_highlight)
for city in test_lq_df.iteritems():
    if num_highlights > 0:
        if city[0] not in cities_to_highlight:
            plt.plot(list(range(1,95)), city[1].tolist(), color='silver', label=city[0], zorder=0)
        else:
            plt.plot(list(range(1,95)), city[1].tolist(), label=city[0], zorder=10)
    else:
        plt.plot(list(range(1,95)), city[1].tolist(), label=city[0])

plt.title('City COVID Recovery Trends')
plt.xlabel('Weeks Since COVID Start (2020-03-05)')
plt.ylabel('LQ of Business Activity')
plt.legend()
plt.show()

# COMMAND ----------

mpl.style.use('seaborn')
plt.figure(figsize=(20,12))

for city in city_name_list:
    plt.plot(list(range(1,95)), test_lq_df[city].rolling(25, center=True).mean(), label=city)

plt.title('City COVID Recovery Trends')
plt.xlabel('Weeks Since COVID Start (2020-03-05)')
plt.ylabel('LQ of Business Activity')
plt.legend()
plt.axhline(y=1, color='r', linestyle='-')
#plt.ylim([0,2])
plt.show()

# COMMAND ----------

test_lq_df["Phoenix_R"] = test_lq_df["Phoenix"].rolling(5, center=True).mean()
test_lq_df["Houston_R"] = test_lq_df["Houston"].rolling(5, center=True).mean()
test_lq_df

# COMMAND ----------

pd.set_option('display.max_rows', 20)
lq_df

# COMMAND ----------

#testing cell by michael
downtown_in_range_vc

# COMMAND ----------

total_20211101 = np.sum(downtown_in_range_vc.iloc[:,120])
total_20211101

# COMMAND ----------

import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

lq_df.fillna(0)

scalar = StandardScaler()
X_transform = scalar.fit_transform(lq_df)
X_transform_arr = np.array(X_transform)
X_transform = pd.DataFrame(X_transform_arr, columns=lq_df.columns)
kmeans = KMeans(n_clusters=3)
kmeans.fit(X_transform)
lq_df["Cluster ID"] = kmeans.labels_  
lq_df

# COMMAND ----------

Sum_of_squared_distances = []
K = range(1,10)
for k in K:
    km = KMeans(n_clusters=k)
    km = km.fit(X_transform)
    Sum_of_squared_distances.append(km.inertia_)

# COMMAND ----------

import matplotlib
import matplotlib.pyplot as plt

plt.figure(figsize = (10,6))
ax = plt.gca()
plt.plot(K, Sum_of_squared_distances, linewidth = 2)
plt.plot(K, Sum_of_squared_distances, '.', c='r',markersize = 6)
plt.plot(K,[Sum_of_squared_distances[-1] for i in range(len(Sum_of_squared_distances))],'--',
         linewidth = 1.5, c = 'black')

plt.xlabel('Number of clusters', fontsize = 12)
plt.ylabel('Sum of squared distances', fontsize = 12)
plt.title('Variation of the sum of squared distances in function of the number of clusters', fontsize = 15, y = 1.05)
ax.yaxis.set_ticks_position('none')
ax.xaxis.set_ticks_position('none')
#plt.xticks(np.arange(1, 21, step=1))
ax.grid(True)
plt.show()

# COMMAND ----------

# Scratch work
# downtown_definition_df.index[downtown_definition_df['0'] == 'Los Angeles'].tolist()
# df = downtown_definition_df.drop([47])

# downtowns_zipcodes_list = downtown_definition_df['zipcodes'].tolist() # ['['12345','23456', etc.]']
# city_name = downtown_definition_df['0'].tolist()
# city_zipcodes = list(zip(downtowns_zipcodes_list, city_name))


# downtown_definition_df
# city_zipcodes
# smallest_dt = 10000
# smallest_dt_idx = -1
# for (index, downtown) in enumerate(downtowns_zipcodes_list):
#     downtown_clean_list = list(map(lambda x: x.strip("''"),downtown.strip('][').split(', ')))
# #     print(len(downtown_clean_list))
#     if len(downtown_clean_list) < smallest_dt:
#         smallest_dt = len(downtown_clean_list)
#         smallest_dt_idx = index
        
# print(smallest_dt, " index: ", smallest_dt_idx)


    

# COMMAND ----------

downtown_definition_df = get_table_as_pandas_df('city_index_0119_csv')
cities = downtown_definition_df['0']
dt_zipcodes = []
for city in cities:
    dt_zipcodes.append(get_city_zipcodes(city))
    


# COMMAND ----------

np_dt_zipcodes = np.array(dt_zipcodes, dtype=object).flatten()
np_dt_zipcodes.size

# COMMAND ----------

#         precovid_start_date = covid_period[1]['start_date']
#         covid_start_date = covid_period[0]['start_date']
        
#         # Set Total VC for all zipcodes in downtown
#         precovid_vc_week_total = set_total_weekly_vc(precovid_start_date, downtown_in_range_vc, downtown_zipcodes)
#         covid_vc_week_total = set_total_weekly_vc(covid_start_date, downtown_in_range_vc, downtown_zipcodes)

#         # Find ZCTA VC
#         zipcode_lq = downtown_in_range_vc.loc[zipcode]
        
#         precovid_lq_col_name = vc_col_func(covid_period[1]['start_date'].strftime('%Y-%m-%d'))
#         covid_lq_col_name = vc_col_func(covid_period[0]['start_date'].strftime('%Y-%m-%d'))
        
#         precovid_vc = zipcode_lq.loc[precovid_lq_col_name]
#         covid_vc = zipcode_lq.loc[covid_lq_col_name]
        
#         current_week_lq = (covid_vc/covid_vc_week_total) / (precovid_vc / precovid_vc_week_total)
#         current_lq_col_name = lq_col_func(covid_period)
        
#         if math.isinf(current_week_lq) is not True and math.isnan(current_week_lq) is not True:
#             zcta_lqs[current_lq_col_name] = current_week_lq
#         else:
#             zcta_lqs[current_lq_col_name] = 0
#             list_of_weird_zipcodes.append((zipcode, covid_period))

# COMMAND ----------

