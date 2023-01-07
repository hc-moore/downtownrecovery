# Databricks notebook source
import pandas as pd
import numpy as np
import datetime
import matplotlib.pyplot as plt
import seaborn as sns

def create_and_save_as_table(df, table_name):
    spark_df = spark.createDataFrame(df)
    spark_df.write.format('csv').saveAsTable(table_name)

def get_table_as_pandas_df(table_name):
    return sqlContext.sql("select * from " + table_name + " tables").toPandas()

# COMMAND ----------

# MAGIC %md ##Joining All Data##

# COMMAND ----------

us_dc = get_table_as_pandas_df("all_us_cities_dc")
can_dc = get_table_as_pandas_df("all_canada_device_count_agg_dt")

# COMMAND ----------

us_dc

# COMMAND ----------

can_dc

# COMMAND ----------

can_dc[can_dc["city"].isna()==False]

# COMMAND ----------

us_can_dc = us_dc.append(can_dc[can_dc["city"].isna()==False])

# COMMAND ----------

us_can_dc

# COMMAND ----------

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
    return [zipcode.replace("'","") for zipcode in city_zipcodes]

# COMMAND ----------

def create_sub_lq(dc_df, downtown_zipcodes, column_name):
    device_counts_from_all_zipcodes_in_df = dc_df.groupby('date_range_start')[column_name].sum()
    dc_df = dc_df.set_index('postal_code')
    dt_dc_df = dc_df.loc[downtown_zipcodes,:].groupby('date_range_start')[column_name].sum()
    return dt_dc_df / device_counts_from_all_zipcodes_in_df

# COMMAND ----------

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
    pre_covid_dt_zipcodes = dt_zipcodes
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

# COMMAND ----------

#THIS DOES NOT WORK

def generate_lq_for_metro(city_name, agg_df):
    metro_zipcodes = get_metro_zipcodes(city_name) 
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
    pre_covid_dt_zipcodes = dt_zipcodes
    if city_name in can_cities:
        pre_covid_metro_zipcodes = agg_df[(agg_df['city'] == city_name) & pre_covid]['postal_code'].unique()
        metro_zipcodes = agg_df[(agg_df['city'] == city_name) & covid]['postal_code'].unique()
    
    # Initialize df to populate
    covid_lq_df = pd.DataFrame()
    pre_covid_lq_df = pd.DataFrame()
    lq_df = pd.DataFrame()
    
    # Create Sub LQ's for each timeframe
    covid_lq_df['sub_covid_rvc_lq'] = create_sub_lq(A_dc_df, metro_zipcodes, 'raw_visit_counts')
    covid_lq_df['sub_covid_nvt_lq'] = create_sub_lq(A_dc_df, metro_zipcodes, 'normalized_visits_by_total_visits')
    covid_lq_df['sub_covid_nvs_lq'] = create_sub_lq(A_dc_df, metro_zipcodes, 'normalized_visits_by_state_scaling')
        
    pre_covid_lq_df['sub_precovid_rvc_lq'] = create_sub_lq(a_dc_df, pre_covid_metro_zipcodes, 'raw_visit_counts')
    pre_covid_lq_df['sub_precovid_nvt_lq'] = create_sub_lq(a_dc_df, pre_covid_metro_zipcodes, 'normalized_visits_by_total_visits')
    pre_covid_lq_df['sub_precovid_nvs_lq'] = create_sub_lq(a_dc_df, pre_covid_metro_zipcodes, 'normalized_visits_by_state_scaling')
    
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

# COMMAND ----------

## RUN THIS CELL TO GENERATE LQ'S GIVEN THE DF OF ALL CITIES COUNTES
cities_to_download = all_cities
all_cities_lq_df = pd.DataFrame()

for city in cities_to_download:
    if city not in ['Mesa', 'Long Beach', 'Virginia Beach', 'Arlington', 'Aurora']:
        print("Generating LQ for: ", city)
        city_lq = generate_lq_for_city(city, us_can_dc)
        city_lq['city'] = city
        all_cities_lq_df = all_cities_lq_df.append(city_lq)
        
all_cities_lq_df

# COMMAND ----------

create_and_save_as_table(all_cities_lq_df, "us_can_lqs_0317")

# COMMAND ----------

all_cities_lq_df["date_range_start_covid"] = pd.to_datetime(all_cities_lq_df['date_range_start_covid'], format='%Y-%m-%d')

# COMMAND ----------

all_cities_lq_df

# COMMAND ----------

# MAGIC %md
# MAGIC ##Generating Period LQ Averages and Metrics##

# COMMAND ----------

[i for i in all_cities if i not in ['Mesa', 'Long Beach', 'Virginia Beach', 'Arlington', 'Aurora', 'Hamilton']]

# COMMAND ----------

all_cities = [i for i in all_cities if i not in ['Mesa', 'Long Beach', 'Virginia Beach', 'Arlington', 'Aurora']]
display_titles = ["New York, NY", "Los Angeles, CA", "Chicago, IL", "Houston, TX", "Phoenix, AZ", "Philadelphia, PA", "San Antonio, TX", "San Diego, CA",
                 "Dallas, TX", "San Jose, CA", "Austin, TX", "Jacksonville, FL", "Fort Worth, TX", "Columbus, OH", "Indianapolis, IN", "Charlotte, NC",
                 "San Francisco, CA", "Seattle, WA", "Denver, CO", "Washington DC", "Nashville, TN", "Oklahoma City, OK", "El Paso, TX", "Boston, MA",
                 "Portland, OR", "Las Vegas, NV", "Detroit, MI", "Memphis, TN", "Louisville, KY", "Baltimore, MD", "Milwaukee, WI", "Albuquerque, NM",
                 "Tucson, AZ", "Fresno, CA", "Sacramento, CA", "Kansas City, MO", "Atlanta, GA", "Omaha, NE", "Colorado Springs, CO", "Raleigh, NC",
                 "Miami, FL", "Oakland, CA", "Minneapolis, MN", "Tulsa, OK", "Bakersfield, CA", "Wichita, KS",
                 "Tampa, FL", "New Orleans, LA", "Cleveland, OH", "Honolulu, HI", "Cincinnati, OH", "Pittsburgh, PA",
                 "Salt Lake City, UT", "St. Louis, MO", "Orlando, FL", "Toronto, ON", "Montréal, QC", "Calgary, AB", "Ottawa, ON", "Edmonton, AB",
                 "Missisauga, ON", "Winnipeg, MB", "Vancouver, BC", "Québec, QC", "Halifax, NS", "London, ON"]

# COMMAND ----------

metrics_df = pd.DataFrame({"city":all_cities, "display_title":display_titles})

# COMMAND ----------

metrics_df

# COMMAND ----------

#CELL IS FOR REFERENCE ONLY DO NOT RUN
# Load lq df
dt_all_metro_lq_df = get_table_as_pandas_df('us_can_lqs_0317')

# Create ranking of latest 8 week average of 5 week LQ rolling average
dt_all_metro_lq_df['rvc_lq_rolling'] = dt_all_metro_lq_df['nvs_lq'].rolling(5, center=True).mean()

dt_all_metro_lq_df['date_range_start_covid'] = pd.to_datetime(dt_all_metro_lq_df['date_range_start_covid'], format='%Y-%m-%d')

## UPDATE THE START AND END DATE RANGE FOR RANKING
start_date_range = datetime(2021, 8, 1)
end_date_range = datetime(2021, 12, 31)
metro_rolling_lqs = dt_all_metro_lq_df.loc[(dt_all_metro_lq_df['date_range_start_covid'] >= start_date_range) &
                                                          (dt_all_metro_lq_df['date_range_start_covid'] <= end_date_range)
                                                          ]

# metro_rolling_lqs
# # Average of last 8 weeks of rolling averages
mean_lq_of_dec_nov = metro_rolling_lqs.groupby('city').mean().sort_values(by=['rvc_lq_rolling'], ascending=True)
ranked_rvc_lq = mean_lq_of_dec_nov['rvc_lq_rolling']

# COMMAND ----------

def get_lq_based_metrics(city):
    city_lqs = all_cities_lq_df[all_cities_lq_df["city"]==city[0]]
    lq_avg_period1 = city_lqs.loc[(city_lqs['date_range_start_covid'] >= datetime.datetime(2020,3,1)) & (city_lqs['date_range_start_covid'] < datetime.datetime(2020,8,1))]["nvs_lq"].mean()
    lq_avg_period2 = city_lqs.loc[(city_lqs['date_range_start_covid'] >= datetime.datetime(2020,8,1)) & (city_lqs['date_range_start_covid'] < datetime.datetime(2021,3,1))]["nvs_lq"].mean()
    lq_avg_period3 = city_lqs.loc[(city_lqs['date_range_start_covid'] >= datetime.datetime(2021,3,1)) & (city_lqs['date_range_start_covid'] < datetime.datetime(2021,8,1))]["nvs_lq"].mean()
    lq_avg_period4 = city_lqs.loc[(city_lqs['date_range_start_covid'] >= datetime.datetime(2021,8,1)) & (city_lqs['date_range_start_covid'] < datetime.datetime(2022,3,1))]["nvs_lq"].mean()
    lq_avg_period4a = city_lqs.loc[(city_lqs['date_range_start_covid'] >= datetime.datetime(2021,8,1)) & (city_lqs['date_range_start_covid'] < datetime.datetime(2022,1,1))]["nvs_lq"].mean()
    lq_avg_period4b = city_lqs.loc[(city_lqs['date_range_start_covid'] >= datetime.datetime(2022,1,1)) & (city_lqs['date_range_start_covid'] < datetime.datetime(2022,3,1))]["nvs_lq"].mean()
    rate_of_recovery = lq_avg_period4a/lq_avg_period1
    downtown_relative_recovery = 0
    omicron_resilience = lq_avg_period4b/lq_avg_period4a
    return pd.Series([lq_avg_period1, lq_avg_period2, lq_avg_period3, lq_avg_period4, lq_avg_period4a, lq_avg_period4b,
                     rate_of_recovery, downtown_relative_recovery, omicron_resilience])

# COMMAND ----------

metrics_df[["lq_avg_period1", "lq_avg_period2", "lq_avg_period3", "lq_avg_period4",
            "lq_avg_period4a", "lq_avg_period4b", "rate_of_recovery",
            "downtown_relative_recovery", "omicron_resilience"]] = metrics_df.apply(lambda x: get_lq_based_metrics(x), axis=1)

# COMMAND ----------

metrics_df[metrics_df["city"]=="Québec"]
metrics_df.iloc[64,0]="Quebec"
metrics_df.iloc[56,0]="Montreal"
metrics_df.iloc[56]

# COMMAND ----------

create_and_save_as_table(metrics_df, "metrics_df_0320")

# COMMAND ----------



# COMMAND ----------

rolling_average_df_2 = pd.DataFrame()
for i in [i for i in all_cities if i not in ['Mesa', 'Long Beach', 'Virginia Beach', 'Arlington', 'Aurora', 'Hamilton']]:
    city_cut = all_cities_lq_df[all_cities_lq_df["city"]==i]
    city_cut["rolling_nvs_lq"] = city_cut["nvs_lq"].rolling(9, min_periods=3).mean()
    rolling_average_df_2 = rolling_average_df_2.append(city_cut)

# COMMAND ----------

create_and_save_as_table(rolling_average_df_2, "rolling_average_df_0320_2")

# COMMAND ----------

rolling_average_df_2

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ##Plot Generation##

# COMMAND ----------

for i in ["lq_avg_period1", "lq_avg_period2", "lq_avg_period3", "lq_avg_period4",
            "lq_avg_period4a", "lq_avg_period4b", "rate_of_recovery",
            "downtown_relative_recovery", "omicron_resilience"]:
    sorted_metrics_df = metrics_df[["display_title", i]].sort_values(by=[i], ascending=True)
    plt.figure(figsize=(6, 14))
    plt.barh(sorted_metrics_df["display_title"], sorted_metrics_df[i], align='center')
    plt.title("Downtowns Ranked by: "+i)
    plt.xlabel("Period Business Activity Compared to 2019 Business Activity Across Selected Metros")

# COMMAND ----------

test = all_cities_lq_df[all_cities_lq_df["city"]=="Dallas"]
test["rolling_nvs_lq"] = test["nvs_lq"].rolling(4).mean()
test

# COMMAND ----------

for i in [["Dallas", "Oklahoma City", "Salt Lake City", "Bakersfield", "Orlando", "Missassauga"],
          ['Boston', 'New York', 'Philadelphia', 'Baltimore', 'Washington DC'],
          ['Toronto', 'Montréal', 'Québec', 'Mississauga', 'London', 'Ottawa'],
          ['San Francisco', 'Sacramento', 'Oakland', 'San Jose'],
          ['Seattle', 'Portland','Honolulu'],
          ['Denver', 'Colorado Springs', 'Salt Lake City'],
          ['Los Angeles', 'San Diego', 'Fresno', 'Bakersfield'],
          ['Las Vegas', 'Phoenix', 'Tucson', 'Albuquerque'],
          ['Dallas', 'Fort Worth', 'Houston', 'San Antonio', 'Austin', 'El Paso'],
          ['New Orleans', 'Jacksonville', 'Orlando', 'Tampa', 'Miami'],
          ['Oklahoma City', 'Tulsa', 'Wichita', 'Kansas City', 'Omaha'],
          ['Chicago', 'Milwaukee', 'Detroit', 'Minneapolis'],
          ['St Louis', 'Indianapolis', 'Louisville'],
          ['Cincinnnati', 'Columbus', 'Cleveland', 'Pittsburgh'],
          ['Nashville', 'Memphis', 'Atlanta', 'Charlotte'],
          ['Jacksonville', 'Orlando', 'Tampa', 'Miami'],
          ['Vancouver', 'Calgary', 'Edmonton', 'Winnipeg', 'Halifax']]:
    plt.figure(figsize = (12,8))
    sns.set_style("darkgrid")
    in_scope_cities = all_cities_lq_df[all_cities_lq_df['city'].isin(i)]
    plotting_df = pd.DataFrame()
    for city in in_scope_cities['city'].unique():
        city_cut = in_scope_cities[in_scope_cities['city']==city]
        city_cut["rolling_nvs_lq"] = city_cut["nvs_lq"].rolling(4).mean()
        plotting_df = plotting_df.append(city_cut)
    sns.lineplot(x = "date_range_start_covid", 
             y = "rolling_nvs_lq",
             hue="city",
             data = plotting_df)
    plt.title("LQ Time Series - 16 week Rolling Average")
    plt.xticks(rotation = 25)

# COMMAND ----------

for i in [["Dallas", "Oklahoma City", "Salt Lake City", "Bakersfield", "Orlando", "Missassauga"],
          ['Boston', 'New York', 'Philadelphia', 'Baltimore', 'Washington DC'],
          ['Toronto', 'Montréal', 'Québec', 'Mississauga', 'London', 'Ottawa'],
          ['San Francisco', 'Sacramento', 'Oakland', 'San Jose'],
          ['Seattle', 'Portland','Honolulu'],
          ['Denver', 'Colorado Springs', 'Salt Lake City'],
          ['Los Angeles', 'San Diego', 'Fresno', 'Bakersfield'],
          ['Las Vegas', 'Phoenix', 'Tucson', 'Albuquerque'],
          ['Dallas', 'Fort Worth', 'Houston', 'San Antonio', 'Austin', 'El Paso'],
          ['New Orleans', 'Jacksonville', 'Orlando', 'Tampa', 'Miami'],
          ['Oklahoma City', 'Tulsa', 'Wichita', 'Kansas City', 'Omaha'],
          ['Chicago', 'Milwaukee', 'Detroit', 'Minneapolis'],
          ['St Louis', 'Indianapolis', 'Louisville'],
          ['Cincinnnati', 'Columbus', 'Cleveland', 'Pittsburgh'],
          ['Nashville', 'Memphis', 'Atlanta', 'Charlotte'],
          ['Jacksonville', 'Orlando', 'Tampa', 'Miami'],
          ['Vancouver', 'Calgary', 'Edmonton', 'Winnipeg', 'Halifax']]:
    plt.figure(figsize = (12,8))
    sns.set_style("darkgrid")
    in_scope_cities = all_cities_lq_df[all_cities_lq_df['city'].isin(i)]
    #plotting_df = pd.DataFrame()
    #for city in in_scope_cities['city'].unique():
    #    city_cut = in_scope_cities[in_scope_cities['city']==city]
    #    city_cut["rolling_nvs_lq"] = city_cut["nvs_lq"].rolling(16).mean()
    #    plotting_df = plotting_df.append(city_cut)
    sns.lineplot(x = "date_range_start_covid", 
             y = "nvs_lq",
             hue="city",
             data = in_scope_cities)
    plt.title("LQ Time Series - No Rolling Average")
    plt.xticks(rotation = 25)

# COMMAND ----------

for i in [['Toronto', 'Montréal', 'Québec', 'Mississauga', 'London', 'Ottawa'],
          ['New York', 'Washington DC', 'San Francisco', 'Los Angeles', 'Seattle'],
          ['Denver', 'Salt Lake City', 'Dallas', 'Chicago', 'Miami'],
          ['Vancouver', 'Calgary', 'Edmonton', 'Winnipeg', 'Halifax']]:
    plt.figure(figsize = (12,6))
    sns.set_style("darkgrid")
    in_scope_cities = all_cities_lq_df[all_cities_lq_df['city'].isin(i)]
    plotting_df = pd.DataFrame()
    for city in in_scope_cities['city'].unique():
        city_cut = in_scope_cities[in_scope_cities['city']==city]
        city_cut["rolling_nvs_lq"] = city_cut["nvs_lq"].rolling(9, min_periods=3).mean()
        plotting_df = plotting_df.append(city_cut)
    sns.lineplot(x = "date_range_start_covid", 
             y = "rolling_nvs_lq",
             hue="city",
             data = plotting_df)
    plt.title("LQ Time city - 9 week Rolling Average")
    plt.xticks(rotation = 25)

# COMMAND ----------

rolling_average_df = pd.DataFrame()
for i in [["Dallas", "Oklahoma City", "Salt Lake City", "Bakersfield", "Orlando", "Missassauga"],
          ['Boston', 'New York', 'Philadelphia', 'Baltimore', 'Washington DC'],
          ['Toronto', 'Montréal', 'Québec', 'Mississauga', 'London', 'Ottawa'],
          ['San Francisco', 'Sacramento', 'Oakland', 'San Jose'],
          ['Seattle', 'Portland','Honolulu'],
          ['Denver', 'Colorado Springs', 'Salt Lake City'],
          ['Los Angeles', 'San Diego', 'Fresno', 'Bakersfield'],
          ['Las Vegas', 'Phoenix', 'Tucson', 'Albuquerque'],
          ['Dallas', 'Fort Worth', 'Houston', 'San Antonio', 'Austin', 'El Paso'],
          ['New Orleans', 'Jacksonville', 'Orlando', 'Tampa', 'Miami'],
          ['Oklahoma City', 'Tulsa', 'Wichita', 'Kansas City', 'Omaha'],
          ['Chicago', 'Milwaukee', 'Detroit', 'Minneapolis'],
          ['St Louis', 'Indianapolis', 'Louisville'],
          ['Cincinnnati', 'Columbus', 'Cleveland', 'Pittsburgh'],
          ['Nashville', 'Memphis', 'Atlanta', 'Charlotte'],
          ['Jacksonville', 'Orlando', 'Tampa', 'Miami'],
          ['Vancouver', 'Calgary', 'Edmonton', 'Winnipeg', 'Halifax']]:
    plt.figure(figsize = (12,8))
    sns.set_style("darkgrid")
    in_scope_cities = all_cities_lq_df[all_cities_lq_df['city'].isin(i)]
    plotting_df = pd.DataFrame()
    for city in in_scope_cities['city'].unique():
        city_cut = in_scope_cities[in_scope_cities['city']==city]
        city_cut["rolling_nvs_lq"] = city_cut["nvs_lq"].rolling(9, min_periods=3).mean()
        plotting_df = plotting_df.append(city_cut)
    sns.lineplot(x = "date_range_start_covid", 
             y = "rolling_nvs_lq",
             hue="city",
             data = plotting_df)
    plt.title("LQ Time city - 9 week Rolling Average")
    plt.xticks(rotation = 25)
    rolling_average_df = rolling_average_df.append(plotting_df)

# COMMAND ----------

rolling_average_df

# COMMAND ----------

create_and_save_as_table(rolling_average_df, "rolling_average_df_0320")

# COMMAND ----------



# COMMAND ----------

all_cities_lq_df["city"].isin(['Vancouver', 'Calgary', 'Edmonton', 'Winnipeg', 'Halifax'])

# COMMAND ----------

plt.figure(figsize = (15,8))
sns.set_style("darkgrid")
# all_cities_lq_df['rvc_rolling'] = all_cities_lq_df[['city','rvc_lq']].groupby('city').rolling(1, min_periods=1).mean().reset_index(level=0,drop=True)
# rolling_lq_df['date'] = all_cities_lq_df['date_range_start_covid']

# rolling_lq_df
# cities = ['Seattle', 'Portland', 'Sacramento', 'Fresno', 'Bakersfield']
#cities = ['New York', 'Boston', 'Washington DC', 'Philadelphia']
# cities = ['Salt Lake City', 'Miami', 'Charlotte', 'Dallas', 'Denver']
# cities = ['Raleigh']
# cities = ['San Francisco', 'Oakland', 'San Jose', 'San Diego']
cities = ['Toronto', 'Montréal', 'Vancouver', 'Québec', 'Mississauga']
# cities = ['Winnipeg', 'Edmonton', 'Calgary', 'Halifax', 'London']
in_scope_cities = all_cities_lq_df['city'].isin(cities)

in_scope_cities = all_cities_lq_df[in_scope_cities]
# .rolling(10, min_periods=1).mean()
#filtered_cities['date_range_start_covid'] = all_cities_lq_df['date_range_start_covid']
#filtered_cities
sns.lineplot(x = "date_range_start_covid", 
             y = "nvs_lq",
             hue="city",
             data = in_scope_cities)
plt.title("LQ Time Series - No Rolling Average")
plt.xticks(rotation = 25)

# COMMAND ----------



# COMMAND ----------

display(get_table_as_pandas_df("all_canada_device_count_0317"))

# COMMAND ----------

display(get_table_as_pandas_df("new_york_device_count"))

# COMMAND ----------

display(get_table_as_pandas_df("san_francisco_device_count"))

# COMMAND ----------

display(get_table_as_pandas_df("houston_device_count"))

# COMMAND ----------

display(get_table_as_pandas_df("san_diego_device_count"))

# COMMAND ----------



# COMMAND ----------

display(get_table_as_pandas_df("your_data_mar_15_2022_0846am_csv"))

# COMMAND ----------

display(get_table_as_pandas_df("oakland_device_count"))

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 4/6 Raw Ranking ###

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
    return [zipcode.replace("'","") for zipcode in city_zipcodes]

# COMMAND ----------

us_can_dc

# COMMAND ----------

def create_recovery(dc_df, downtown_zipcodes, column_name):
    device_counts_from_all_zipcodes_in_df = dc_df.groupby('date_range_start')[column_name].sum()
    dc_df = dc_df.set_index('postal_code')
    dt_dc_df = dc_df.loc[downtown_zipcodes,:].groupby('date_range_start')[column_name].sum()
    return dt_dc_df

# COMMAND ----------

def generate_rec_for_city(city_name, agg_df):
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
    pre_covid_dt_zipcodes = dt_zipcodes
    if city_name in can_cities:
        pre_covid_dt_zipcodes = agg_df[(agg_df['city'] == city_name) & pre_covid]['postal_code'].unique()
        dt_zipcodes = agg_df[(agg_df['city'] == city_name) & covid]['postal_code'].unique()
    
    # Initialize df to populate
    covid_recovery_df = pd.DataFrame()
    pre_covid_recovery_df = pd.DataFrame()
    recovery_df = pd.DataFrame()
    
    # Create Sub LQ's for each timeframe
    covid_recovery_df['sub_covid_nvs_recovery'] = create_recovery(A_dc_df, dt_zipcodes, 'normalized_visits_by_state_scaling')
    pre_covid_recovery_df['sub_precovid_nvs_recovery'] = create_recovery(a_dc_df, pre_covid_dt_zipcodes, 'normalized_visits_by_state_scaling')
    
    # Set date as column
    covid_recovery_df = covid_recovery_df.reset_index()
    pre_covid_recovery_df = pre_covid_recovery_df.reset_index()
    
    # Set COVID's 2019 comparison date
    covid_recovery_df['comparison_date'] = covid_recovery_df['date_range_start'] - pd.Timedelta(364, unit='d')
    covid_recovery_df.loc[(covid_recovery_df['date_range_start'] > datetime.date(2021, 1,1)), 'comparison_date'] -= pd.Timedelta(364, unit='d')
    covid_recovery_df.loc[(covid_recovery_df['date_range_start'] > datetime.date(2022, 1,1)), 'comparison_date'] -= pd.Timedelta(364, unit='d')
 
    # Merge both periods into 1 lq df
    recovery_df = covid_recovery_df.merge(pre_covid_recovery_df, 
                          left_on=['comparison_date'],
                          right_on=['date_range_start'],
                          suffixes=['_covid','_precovid']
                         )
    recovery_df['nvs_rec'] = recovery_df['sub_covid_nvs_recovery'] / recovery_df['sub_precovid_nvs_recovery']
    return recovery_df

# COMMAND ----------

generate_rec_for_city("New York", us_can_dc)

# COMMAND ----------

cities_to_download = all_cities
all_cities_rec_df = pd.DataFrame()

for city in cities_to_download:
    if city not in ['Mesa', 'Long Beach', 'Virginia Beach', 'Arlington', 'Aurora']:
        print("Generating REC for: ", city)
        city_rec = generate_rec_for_city(city, us_can_dc)
        city_rec['city'] = city
        all_cities_rec_df = all_cities_rec_df.append(city_rec)
        
all_cities_rec_df

# COMMAND ----------

all_cities_rec_df["date_range_start_covid"] = pd.to_datetime(all_cities_rec_df['date_range_start_covid'], format='%Y-%m-%d')

# COMMAND ----------

def get_rec_based_metrics(city):
    city_rec = all_cities_rec_df[all_cities_rec_df["city"]==city[0]]
    rec_avg_period1 = city_rec.loc[(city_rec['date_range_start_covid'] >= datetime.datetime(2020,3,1)) & (city_rec['date_range_start_covid'] < datetime.datetime(2020,8,1))]["nvs_rec"].mean()
    rec_avg_period2 = city_rec.loc[(city_rec['date_range_start_covid'] >= datetime.datetime(2020,8,1)) & (city_rec['date_range_start_covid'] < datetime.datetime(2021,3,1))]["nvs_rec"].mean()
    rec_avg_period3 = city_rec.loc[(city_rec['date_range_start_covid'] >= datetime.datetime(2021,3,1)) & (city_rec['date_range_start_covid'] < datetime.datetime(2021,8,1))]["nvs_rec"].mean()
    rec_avg_period4 = city_rec.loc[(city_rec['date_range_start_covid'] >= datetime.datetime(2021,8,1)) & (city_rec['date_range_start_covid'] < datetime.datetime(2022,3,1))]["nvs_rec"].mean()
    rec_avg_period4a = city_rec.loc[(city_rec['date_range_start_covid'] >= datetime.datetime(2021,8,1)) & (city_rec['date_range_start_covid'] < datetime.datetime(2022,1,1))]["nvs_rec"].mean()
    rec_avg_period4b = city_rec.loc[(city_rec['date_range_start_covid'] >= datetime.datetime(2022,1,1)) & (city_rec['date_range_start_covid'] < datetime.datetime(2022,3,1))]["nvs_rec"].mean()
    rate_of_recovery = rec_avg_period4a/rec_avg_period1
    downtown_relative_recovery = 0
    omicron_resilience = rec_avg_period4b/rec_avg_period4a
    return pd.Series([rec_avg_period1, rec_avg_period2, rec_avg_period3, rec_avg_period4, rec_avg_period4a, rec_avg_period4b,
                     rate_of_recovery, downtown_relative_recovery, omicron_resilience])

# COMMAND ----------

rec_metrics_df = pd.DataFrame({"city":all_cities, "display_title":display_titles})

# COMMAND ----------

rec_metrics_df[["rec_avg_period1", "rec_avg_period2", "rec_avg_period3", "rec_avg_period4",
            "rec_avg_period4a", "rec_avg_period4b", "rate_of_recovery",
            "downtown_relative_recovery", "omicron_resilience"]] = rec_metrics_df.apply(lambda x: get_rec_based_metrics(x), axis=1)

# COMMAND ----------

rec_metrics_df

# COMMAND ----------

for i in ["rec_avg_period1", "rec_avg_period2", "rec_avg_period3", "rec_avg_period4",
            "rec_avg_period4a", "rec_avg_period4b", "rate_of_recovery",
            "downtown_relative_recovery", "omicron_resilience"]:
    sorted_rec_metrics_df = rec_metrics_df[["display_title", i]].sort_values(by=[i], ascending=True)
    plt.figure(figsize=(6, 14))
    plt.barh(sorted_rec_metrics_df["display_title"], sorted_rec_metrics_df[i], align='center')
    plt.title("Downtowns Ranked by: "+i)
    plt.xlabel("Period Business Activity Compared to 2019 Business Activity Across Selected Metros")

# COMMAND ----------

for i in [["Dallas", "Oklahoma City", "Salt Lake City", "Bakersfield", "Orlando", "Missassauga"],
          ['Boston', 'New York', 'Philadelphia', 'Baltimore', 'Washington DC'],
          ['Toronto', 'Montréal', 'Québec', 'Mississauga', 'London', 'Ottawa'],
          ['San Francisco', 'Sacramento', 'Oakland', 'San Jose'],
          ['Seattle', 'Portland','Honolulu'],
          ['Denver', 'Colorado Springs', 'Salt Lake City'],
          ['Los Angeles', 'San Diego', 'Fresno', 'Bakersfield'],
          ['Las Vegas', 'Phoenix', 'Tucson', 'Albuquerque'],
          ['Dallas', 'Fort Worth', 'Houston', 'San Antonio', 'Austin', 'El Paso'],
          ['New Orleans', 'Jacksonville', 'Orlando', 'Tampa', 'Miami'],
          ['Oklahoma City', 'Tulsa', 'Wichita', 'Kansas City', 'Omaha'],
          ['Chicago', 'Milwaukee', 'Detroit', 'Minneapolis'],
          ['St Louis', 'Indianapolis', 'Louisville'],
          ['Cincinnnati', 'Columbus', 'Cleveland', 'Pittsburgh'],
          ['Nashville', 'Memphis', 'Atlanta', 'Charlotte'],
          ['Jacksonville', 'Orlando', 'Tampa', 'Miami'],
          ['Vancouver', 'Calgary', 'Edmonton', 'Winnipeg', 'Halifax']]:
    plt.figure(figsize = (12,8))
    sns.set_style("darkgrid")
    in_scope_cities = all_cities_rec_df[all_cities_rec_df['city'].isin(i)]
    plotting_df = pd.DataFrame()
    for city in in_scope_cities['city'].unique():
        city_cut = in_scope_cities[in_scope_cities['city']==city]
        city_cut["nvs_rec_rolling"] = city_cut["nvs_rec"].rolling(4).mean()
        plotting_df = plotting_df.append(city_cut)
    sns.lineplot(x = "date_range_start_covid", 
             y = "nvs_rec_rolling",
             hue="city",
             data = plotting_df)
    plt.title("LQ Time Series - 16 week Rolling Average")
    plt.xticks(rotation = 25)

# COMMAND ----------

all_cities_rec_df

# COMMAND ----------



# COMMAND ----------

