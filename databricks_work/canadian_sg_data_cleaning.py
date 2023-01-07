# Databricks notebook source
import pandas as pd
import numpy as np
import time
import datetime as dt
import seaborn as sns
import matplotlib.pyplot as plt

# COMMAND ----------

def create_and_save_as_table(df, table_name):
    spark_df = spark.createDataFrame(df)
    spark_df.write.format('csv').saveAsTable(table_name)
    
def get_table_as_pandas_df(table_name):
    return sqlContext.sql("select * from " + table_name + " tables").toPandas()

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

us_cities = get_table_as_pandas_df('all_us_cities_dc')
#city_index = can_city_index.rename(columns = {"0" : "city", "CMA_da" : "zipcodes", "downtown_da" : "dt_zipcodes"})
us_cities = us_cities[["postal_code", "city"]]

# COMMAND ----------

display(us_cities.drop_duplicates())

# COMMAND ----------

# appended is all canada dfs appended together as of 3/15
all_canada_dc_df = get_table_as_pandas_df('all_canada_device_count_0317')
all_canada_dc_df

# COMMAND ----------

all_canada_dc_df_sink["postal_code"] = all_canada_dc_df_sink["poi_cbg"].str[3:]
all_canada_dc_df_sink["date_range_start"] = all_canada_dc_df_sink["date_range_start"].str[:10]
all_canada_dc_df_sink = all_canada_dc_df_sink.drop(columns = ["city", "poi_cbg"])
all_canada_dc_df = all_canada_dc_df.append(all_canada_dc_df_sink)
all_canada_dc_df = all_canada_dc_df.drop_duplicates()

# COMMAND ----------

create_and_save_as_table(all_canada_dc_df, 'all_canada_device_count_0317')

# COMMAND ----------

# the placekeys
oct5th_patterns_data = get_table_as_pandas_df('your_data_mar_15_2022_0846am_csv')
oct5th_placekeys = oct5th_patterns_data[["placekey", "city", "poi_cbg", "postal_code"]]
oct5th_placekeys["FSA"] = oct5th_placekeys["postal_code"].str[:3]
oct5th_placekeys["DAUID"] = oct5th_placekeys["poi_cbg"].str[3:]

# COMMAND ----------

FSAs_to_DAUIDs = get_table_as_pandas_df('canada_fsa_to_da_csv')

# COMMAND ----------

can_city_index = get_table_as_pandas_df('can_city_index_da_0312')
can_city_index = can_city_index.rename(columns = {"0" : "city", "CMA_da" : "zipcodes", "downtown_da" : "dt_zipcodes"})

# COMMAND ----------

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
oct5th_placekeys_study = oct5th_placekeys[oct5th_placekeys["DAUID"].isin(all_study_DAUIDs["DAUID"])]

# COMMAND ----------

len(oct5th_placekeys_study)

# COMMAND ----------

# this takes about 2 minutes
can_device_counts = all_canada_dc_df[all_canada_dc_df["placekey"].isin(oct5th_placekeys_study["placekey"])]
can_device_counts = can_device_counts.rename(columns = {"DAUID" : "postal_code", "week" : "date_range_start"})
can_device_counts

# COMMAND ----------

can_device_counts.sort_values(by = "normalized_visits_by_state_scaling", ascending = False)

# COMMAND ----------

create_and_save_as_table(can_device_counts, 'all_canada_device_count_0317')

# COMMAND ----------

weeks_to_placekey = []
for weeks in can_device_counts_all["date_range_start"].unique():
     weeks_to_placekey.append(set(can_device_counts.loc[can_device_counts["date_range_start"] == weeks, "placekey"].unique()))
common_placekeys = set.intersection(*weeks_to_placekey)

# COMMAND ----------

len(common_placekeys)

# COMMAND ----------

can_device_counts_subset = can_device_counts[can_device_counts["placekey"].isin(common_placekeys)]

# COMMAND ----------

all_cities_dc_df_us = get_table_as_pandas_df('all_us_cities_dc')
all_cities_dc_df_us

# COMMAND ----------

 can_device_counts_agg = can_device_counts[["postal_code", "date_range_start", "raw_visit_counts", "normalized_visits_by_total_visits", "normalized_visits_by_state_scaling"]].groupby(["postal_code", "date_range_start"]).sum().reset_index()
can_device_counts_agg  = can_device_counts_agg.merge(all_downtown_DAUIDs, left_on = "postal_code", right_on = "DAUID", how = "left")
can_device_counts_agg

# COMMAND ----------

can_device_counts_agg.groupby('date_range_start').size().sort_values()

# COMMAND ----------

can_device_counts_subset[can_device_counts_subset["date_range_start"] == "2020-112-20"]

# COMMAND ----------

create_and_save_as_table(can_device_counts_agg.drop(columns = ["DAUID"]), 'all_canada_device_count_agg_dt_0317')

# COMMAND ----------

all_cities_dc_df = all_cities_dc_df_us.append(can_device_counts_agg.drop(columns = ["DAUID"]))
all_cities_dc_df

# COMMAND ----------

for city in can_device_counts_agg["city"].dropna().unique():
    test_lq_df = generate_lq_for_city(city, all_cities_dc_df)
    sns.set_style('dark')
    sns.lineplot(x = "date_range_start_covid", 
                 y = "nvs_lq",
                 color='r',
                 data = test_lq_df)
    plt.xticks(rotation = 25)
    plt.title("All " + city + " nvs_lq")
    plt.show()

# COMMAND ----------

test_lq_df = generate_lq_for_city("Mississauga", all_cities_dc_df)
test_lq_df.sort_values(by = "nvs_lq")

# COMMAND ----------

all_cities_dc_df.groupby("date_range_start").size().sort_values()

# COMMAND ----------

for city in can_cities:
    CMA_df = can_device_counts_all.merge(all_CMA_DAUIDs, left_on = "postal_code", right_on = "DAUID", how = "left")
    city_df = CMA_df[CMA_df["city"] == city]
    #print(city)
    #print(city_df.sort_values(by = "normalized_visits_by_state_scaling", ascending = False)[:5][["date_range_start", "placekey"]])
    city_df["date_range_start"] = pd.to_datetime(city_df["date_range_start"])
    sns.set_style('dark')
    sns.lineplot(x = "date_range_start", 
                 y = "normalized_visits_by_state_scaling",
                 color='r',
                 data = city_df)
    plt.xticks(rotation = 25)
    plt.title(city + " CMA nvs")
    plt.show()

# COMMAND ----------

test_lq_df = generate_lq_for_city("Hamilton", all_cities_dc_df)
test_lq_df

# COMMAND ----------

test_lq_df.sort_values(by = "nvs_lq", ascending = False)

# COMMAND ----------

hamilton_agg = can_device_counts_agg[can_device_counts_agg["city"] == "Hamilton"]
hamilton_agg["date_range_start"] = pd.to_datetime(hamilton_agg["date_range_start"])
sns.set_style('dark')
sns.lineplot(x = "date_range_start", 
             y = "normalized_visits_by_state_scaling",
             color='r',
             data = city_df)
plt.xticks(rotation = 25)
plt.show()

# COMMAND ----------

can_device_counts_test = can_device_counts_agg.drop(columns = ["DAUID", "city"])
can_device_counts_test = can_device_counts_test.merge(all_CMA_DAUIDs, left_on = "postal_code", right_on = "DAUID", how = "left")
city_test = can_device_counts_test[can_device_counts_test["city"] == "Hamilton"]
city_test

# COMMAND ----------

city_test[city_test["date_range_start"] == "2019-06-24"].sort_values(by = "normalized_visits_by_state_scaling")

# COMMAND ----------

create_and_save_as_table(can_device_counts_city, 'all_canada_device_count_agg_dt')

# COMMAND ----------

can_device_counts_agg.groupby("city").size() / (164 * all_downtown_DAUIDs.groupby("city").size())

# COMMAND ----------

#### start investigating why LQs are so weird:
metro_definition_df = get_table_as_pandas_df('city_index_0119_csv')
us_city_index = get_table_as_pandas_df('city_csa_fips_codes___us_cities_downtowns__1__csv')
can_city_index = get_table_as_pandas_df('can_city_index_da_0312')
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
    pre_covid = agg_df['date_range_start'] < dt.date(2020, 3, 2)
    covid = dt.date(2020, 3, 2) <= agg_df['date_range_start']
    
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
    covid_lq_df.loc[(covid_lq_df['date_range_start'] > dt.date(2021, 1,1)), 'comparison_date'] -= pd.Timedelta(364, unit='d')
    covid_lq_df.loc[(covid_lq_df['date_range_start'] > dt.date(2022, 1,1)), 'comparison_date'] -= pd.Timedelta(364, unit='d')
    
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

# COMMAND ----------

for city in can_cities:
    test_lq_df = generate_lq_for_city(city, all_cities_dc_df)
    sns.set_style('dark')
    sns.lineplot(x = "date_range_start_covid", 
                 y = "nvs_lq",
                 color='r',
                 data = test_lq_df)
    plt.xticks(rotation = 25)
    plt.title("All " + city + " nvs_lq")
    plt.show()

# COMMAND ----------

city_df.sort_values(by = "normalized_visits_by_state_scaling", ascending = False)

# COMMAND ----------

test_lq_df = generate_lq_for_city("Toronto", toronto_dc_df_agg)

# COMMAND ----------

# the month of june and last week of november + most of december are problem dates in 2019
problem_weeks_comparison = test_lq_df[test_lq_df["nvs_lq"] >= 10]["comparison_date"]
problem_weeks_covid = test_lq_df[test_lq_df["nvs_lq"] >= 10]["date_range_start_covid"]
np.unique(problem_weeks_comparison.values).astype(str)

# COMMAND ----------

np.unique(problem_weeks_covid.values).astype(str)

# COMMAND ----------

city_df_week = city_df.groupby("date_range_start").sum().reset_index()
city_df_week[city_df_week["date_range_start"].isin(problem_weeks_comparison.values.astype(str)) | city_df_week["date_range_start"].isin(problem_weeks_covid.values.astype(str))]

# COMMAND ----------

can_agg_dc_df_week[can_agg_dc_df_week["date_range_start"].isin(problem_weeks_covid.values.astype(str))]

# COMMAND ----------

canada_problem_weeks_precovid = can_agg_dc_df[can_agg_dc_df["date_range_start"].isin(problem_weeks.values.astype(str))].groupby(["city", "date_range_start"]).sum()
canada_problem_weeks_precovid

# COMMAND ----------

can_agg_dc_df["date_range_start"]

# COMMAND ----------

