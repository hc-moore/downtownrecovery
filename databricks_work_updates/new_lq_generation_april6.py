# Databricks notebook source
import pandas as pd
import numpy as np
import datetime
from dateutil.relativedelta import relativedelta
import matplotlib.pyplot as plt
import seaborn as sns

def create_and_save_as_table(df, table_name):
    spark_df = spark.createDataFrame(df)
    spark_df.write.format('csv').saveAsTable(table_name)

def get_table_as_pandas_df(table_name):
    return sqlContext.sql("select * from " + table_name + " tables").toPandas()

# COMMAND ----------

model_data_full = get_table_as_pandas_df("model_data_full")
display(model_data_full[(model_data_full["Season"]=="Season_9") & (model_data_full["metric"]=="city")].sort_values(by="total_pop_city", ascending=False))

# COMMAND ----------

us_dc = get_table_as_pandas_df("all_us_cities_dc")
can_dc = get_table_as_pandas_df("all_canada_device_count_agg_dt")

# COMMAND ----------

us_can_dc = us_dc.append(can_dc)
us_can_dc["postal_code"] = us_can_dc["postal_code"].astype(int)

# COMMAND ----------

can_city_index = get_table_as_pandas_df('can_city_index_da_0406')

# COMMAND ----------

can_city_index

# COMMAND ----------

metro_definition_df = get_table_as_pandas_df('city_index_0119_csv')
metro_definition_df = metro_definition_df.rename(columns={"zipcodes": "ma_zipcodes"})
us_city_index = get_table_as_pandas_df('city_csa_fips_codes___us_cities_downtowns__1__csv')
can_city_index = get_table_as_pandas_df('can_city_index_da_0406')
can_city_index = can_city_index.rename(columns={"0": "city", "downtown_da": "dt_zipcodes", "ccs_da": "ma_zipcodes"})
metro_definition_df = metro_definition_df.rename(columns={"0": "city"})
us_cities = us_city_index['city'].tolist()
can_cities = can_city_index['city'].tolist()
all_cities = us_cities + can_cities

# COMMAND ----------

can_city_index

# COMMAND ----------

metro_definition_df

# COMMAND ----------

#Add Week UID
us_can_dc['date_range_start'] = pd.to_datetime(us_can_dc['date_range_start'])
def get_week_uid(row):
    year = row["date_range_start"].year
    month = row["date_range_start"].week
    if year!=2020:
        return int(year) + int(month)/100
    else:
        return int(year) + int(month)/100 - 0.01
    
us_can_dc["week_uid"] = us_can_dc.apply(lambda x: get_week_uid(x), axis=1)

# COMMAND ----------

#Initialize Tables
all_cities = [i for i in all_cities if i not in ['Mesa', 'Long Beach', 'Virginia Beach', 'Arlington', 'Aurora','Hamilton']]
display_titles = ["New York, NY", "Los Angeles, CA", "Chicago, IL", "Houston, TX", "Phoenix, AZ", "Philadelphia, PA", "San Antonio, TX", "San Diego, CA",
                 "Dallas, TX", "San Jose, CA", "Austin, TX", "Jacksonville, FL", "Fort Worth, TX", "Columbus, OH", "Indianapolis, IN", "Charlotte, NC",
                 "San Francisco, CA", "Seattle, WA", "Denver, CO", "Washington DC", "Nashville, TN", "Oklahoma City, OK", "El Paso, TX", "Boston, MA",
                 "Portland, OR", "Las Vegas, NV", "Detroit, MI", "Memphis, TN", "Louisville, KY", "Baltimore, MD", "Milwaukee, WI", "Albuquerque, NM",
                 "Tucson, AZ", "Fresno, CA", "Sacramento, CA", "Kansas City, MO", "Atlanta, GA", "Omaha, NE", "Colorado Springs, CO", "Raleigh, NC",
                 "Miami, FL", "Oakland, CA", "Minneapolis, MN", "Tulsa, OK", "Bakersfield, CA", "Wichita, KS",
                 "Tampa, FL", "New Orleans, LA", "Cleveland, OH", "Honolulu, HI", "Cincinnati, OH", "Pittsburgh, PA",
                 "Salt Lake City, UT", "St. Louis, MO", "Orlando, FL", "Toronto, ON", "Montréal, QC", "Calgary, AB", "Ottawa, ON", "Edmonton, AB",
                 "Missisauga, ON", "Winnipeg, MB", "Vancouver, BC", "Québec, QC", "Halifax, NS", "London, ON"]
downtown_rec_df = pd.DataFrame({"city":all_cities, "display_title":display_titles})

# COMMAND ----------

# Get Downtowns

def get_downtown_city_zipcodes(city_name):
    dt_zipcodes = []
    dt_vc = pd.DataFrame()

    dt_vc = us_city_index if city_name in us_cities else can_city_index[["city","dt_zipcodes"]]
    dt_zipcodes = dt_vc[dt_vc['city'] == city_name]['dt_zipcodes'] 
    dt_zipcodes = dt_zipcodes.tolist()[0].strip('][').split(', ')
    return [zipcode.replace("'","") for zipcode in dt_zipcodes]

def get_metro_zipcodes(city_name):
    ma_zipcodes = []
    ma_vc = pd.DataFrame()
    
    ma_vc = metro_definition_df if city_name in us_cities else can_city_index[["city","ma_zipcodes"]]
    ma_zipcodes = ma_vc[ma_vc['city'] == city_name]['ma_zipcodes']
    ma_zipcodes = ma_zipcodes.tolist()[0].strip('][').split(', ')
    return [zipcode.replace("'","") for zipcode in ma_zipcodes]

downtown_rec_df["dt_zipcodes"] = downtown_rec_df["city"].apply(lambda x: get_downtown_city_zipcodes(x))
downtown_rec_df["ma_zipcodes"] = downtown_rec_df["city"].apply(lambda x: get_metro_zipcodes(x))
downtown_rec_df["dt_zipcodes"] = downtown_rec_df["dt_zipcodes"].apply(lambda x: [int(i) for i in x])
downtown_rec_df["ma_zipcodes"] = downtown_rec_df["ma_zipcodes"].apply(lambda x: [int(i) for i in x])

# COMMAND ----------

#ensure data types
us_can_dc["postal_code"] = us_can_dc["postal_code"].astype(int)

# COMMAND ----------

#generate week list for index
week_list = np.arange(2020.08,2020.53,0.01)
week_list = np.append(week_list, np.arange(2021.01,2021.53,0.01))
week_list = np.append(week_list, np.arange(2022.01,2022.09,0.01))

# COMMAND ----------

#Populate Downtown Recovery df

def get_downtown_recovery_metrics(city, week):
    #analysis week data
    downtown_devices = us_can_dc[us_can_dc["postal_code"].isin(city["dt_zipcodes"])]
    downtown_devices_week = downtown_devices[downtown_devices["week_uid"] == round(week,2)]
    downtown_devices_sum = np.sum(downtown_devices_week["normalized_visits_by_total_visits"])
    
    #comparison week data
    comparison_week = float("2019" + str(week)[4:])
    downtown_devices_comparison_week = downtown_devices[downtown_devices["week_uid"] == comparison_week]
    downtown_devices_comparison_sum = np.sum(downtown_devices_comparison_week["normalized_visits_by_total_visits"])
    
    return np.divide(downtown_devices_sum, downtown_devices_comparison_sum)

# COMMAND ----------

#iterate over week list
for i in week_list:
    downtown_rec_df["rec_"+str(round(i,2))] = round(downtown_rec_df.apply(lambda x: get_downtown_recovery_metrics(x, round(i,2)), axis=1),2)

downtown_rec_df["rec_2020.12"] = (downtown_rec_df["rec_2020.11"]+downtown_rec_df["rec_2020.13"])/2
downtown_rec_df["rec_2020.37"] = (downtown_rec_df["rec_2020.36"]+downtown_rec_df["rec_2020.38"])/2

# COMMAND ----------

create_and_save_as_table(downtown_rec_df.drop(columns=["dt_zipcodes","ma_zipcodes"]),"0713_raw_downtown_rec_df")

# COMMAND ----------

downtown_rec_df

# COMMAND ----------

downtown_rec_df["season_1"] = downtown_rec_df.iloc[:,4:17].mean(axis=1)
downtown_rec_df["season_2"] = downtown_rec_df.iloc[:,17:30].mean(axis=1)
downtown_rec_df["season_3"] = downtown_rec_df.iloc[:,30:43].mean(axis=1)
downtown_rec_df["season_4"] = downtown_rec_df.iloc[:,43:57].mean(axis=1)
downtown_rec_df["season_5"] = downtown_rec_df.iloc[:,57:70].mean(axis=1)
downtown_rec_df["season_6"] = downtown_rec_df.iloc[:,70:83].mean(axis=1)
downtown_rec_df["season_7"] = downtown_rec_df.iloc[:,83:96].mean(axis=1)
downtown_rec_df["season_8"] = downtown_rec_df.iloc[:,96:110].mean(axis=1)
display(downtown_rec_df)

# COMMAND ----------

downtown_rec_df.iloc[0:20]

# COMMAND ----------

create_and_save_as_table(downtown_rec_df.drop(columns=["dt_zipcodes","ma_zipcodes"]),"0713_downtown_rec_df_totaldevs")

# COMMAND ----------

#Populate Metro Recovery df

def get_metro_recovery_metrics(city, week):
    #analysis week data
    metro_devices = us_can_dc[us_can_dc["postal_code"].isin(city["ma_zipcodes"])]
    metro_devices_week = metro_devices[metro_devices["week_uid"] == round(week,2)]
    metro_devices_sum = np.sum(metro_devices_week["normalized_visits_by_total_visits"])
    
    #comparison week data
    comparison_week = float("2019" + str(week)[4:])
    metro_devices_comparison_week = metro_devices[metro_devices["week_uid"] == comparison_week]
    metro_devices_comparison_sum = np.sum(metro_devices_comparison_week["normalized_visits_by_total_visits"])
    
    return np.divide(metro_devices_sum, metro_devices_comparison_sum)

# COMMAND ----------

metro_rec_df = pd.DataFrame({"city":all_cities, "display_title":display_titles})
metro_rec_df["dt_zipcodes"] = metro_rec_df["city"].apply(lambda x: get_downtown_city_zipcodes(x))
metro_rec_df["ma_zipcodes"] = metro_rec_df["city"].apply(lambda x: get_metro_zipcodes(x))
metro_rec_df["dt_zipcodes"] = metro_rec_df["dt_zipcodes"].apply(lambda x: [int(i) for i in x])
metro_rec_df["ma_zipcodes"] = metro_rec_df["ma_zipcodes"].apply(lambda x: [int(i) for i in x])

#iterate over week list
for i in week_list:
    metro_rec_df["rec_"+str(round(i,2))] = round(metro_rec_df.apply(lambda x: get_metro_recovery_metrics(x, round(i,2)), axis=1),2)

metro_rec_df["rec_2020.12"] = (metro_rec_df["rec_2020.11"]+metro_rec_df["rec_2020.13"])/2
metro_rec_df["rec_2020.37"] = (metro_rec_df["rec_2020.36"]+metro_rec_df["rec_2020.38"])/2

# COMMAND ----------

metro_rec_df["season_1"] = metro_rec_df.iloc[:,4:17].mean(axis=1)
metro_rec_df["season_2"] = metro_rec_df.iloc[:,17:30].mean(axis=1)
metro_rec_df["season_3"] = metro_rec_df.iloc[:,30:43].mean(axis=1)
metro_rec_df["season_4"] = metro_rec_df.iloc[:,43:57].mean(axis=1)
metro_rec_df["season_5"] = metro_rec_df.iloc[:,57:70].mean(axis=1)
metro_rec_df["season_6"] = metro_rec_df.iloc[:,70:83].mean(axis=1)
metro_rec_df["season_7"] = metro_rec_df.iloc[:,83:96].mean(axis=1)
metro_rec_df["season_8"] = metro_rec_df.iloc[:,96:110].mean(axis=1)
display(metro_rec_df)

# COMMAND ----------

create_and_save_as_table(metro_rec_df.drop(columns=["dt_zipcodes","ma_zipcodes"]),"0713_metro_rec_df_totaldevs")

# COMMAND ----------

#Populate Relative Recovery df

def get_relative_recovery_metrics(city, week):
    #analysis week data
    downtown_devices = us_can_dc[us_can_dc["postal_code"].isin(city["dt_zipcodes"])]
    downtown_devices_week = downtown_devices[downtown_devices["week_uid"] == round(week,2)]
    downtown_devices_sum = np.sum(downtown_devices_week["normalized_visits_by_state_scaling"])
    metro_devices = us_can_dc[us_can_dc["postal_code"].isin(city["ma_zipcodes"])]
    metro_devices_week = metro_devices[metro_devices["week_uid"] == round(week,2)]
    metro_devices_sum = np.sum(metro_devices_week["normalized_visits_by_state_scaling"])
    
    #comparison week data
    comparison_week = float("2019" + str(week)[4:])
    downtown_devices_comparison_week = downtown_devices[downtown_devices["week_uid"] == comparison_week]
    downtown_devices_comparison_sum = np.sum(downtown_devices_comparison_week["normalized_visits_by_state_scaling"])
    metro_devices_comparison_week = metro_devices[metro_devices["week_uid"] == comparison_week]
    metro_devices_comparison_sum = np.sum(metro_devices_comparison_week["normalized_visits_by_state_scaling"])
    
    return np.divide(np.divide(downtown_devices_sum, downtown_devices_comparison_sum), np.divide(metro_devices_sum, metro_devices_comparison_sum))

# COMMAND ----------

relative_rec_df = pd.DataFrame({"city":all_cities, "display_title":display_titles})
relative_rec_df["dt_zipcodes"] = relative_rec_df["city"].apply(lambda x: get_downtown_city_zipcodes(x))
relative_rec_df["ma_zipcodes"] = relative_rec_df["city"].apply(lambda x: get_metro_zipcodes(x))
relative_rec_df["dt_zipcodes"] = relative_rec_df["dt_zipcodes"].apply(lambda x: [int(i) for i in x])
relative_rec_df["ma_zipcodes"] = relative_rec_df["ma_zipcodes"].apply(lambda x: [int(i) for i in x])

#iterate over week list
for i in week_list:
    relative_rec_df["rec_"+str(round(i,2))] = round(relative_rec_df.apply(lambda x: get_relative_recovery_metrics(x, round(i,2)), axis=1),2)

relative_rec_df["rec_2020.12"] = (relative_rec_df["rec_2020.11"]+relative_rec_df["rec_2020.13"])/2
relative_rec_df["rec_2020.37"] = (relative_rec_df["rec_2020.36"]+relative_rec_df["rec_2020.38"])/2

# COMMAND ----------



relative_rec_df["period_1"] = relative_rec_df.iloc[:,4:30].mean(axis=1)
relative_rec_df["period_2"] = relative_rec_df.iloc[:,30:57].mean(axis=1)
relative_rec_df["period_3"] = relative_rec_df.iloc[:,57:83].mean(axis=1)
relative_rec_df["period_4"] = relative_rec_df.iloc[:,83:110].mean(axis=1)
relative_rec_df["period_4a"] = relative_rec_df.iloc[:,83:102].mean(axis=1)
relative_rec_df["period_4b"] = relative_rec_df.iloc[:,102:110].mean(axis=1)
relative_rec_df["rate_of_recovery"] = relative_rec_df["period_4"]/relative_rec_df["period_1"]
relative_rec_df["omicron_resilience"] = relative_rec_df["period_4b"]/relative_rec_df["period_4a"]
display(relative_rec_df)

# COMMAND ----------

#Populate LQ df

def get_lq_metrics(city, week):
    #analysis week data
    downtown_devices = us_can_dc[us_can_dc["postal_code"].isin(city["dt_zipcodes"])]
    downtown_devices_week = downtown_devices[downtown_devices["week_uid"] == round(week,2)]
    downtown_devices_sum = np.sum(downtown_devices_week["normalized_visits_by_total_visits"])
    metro_devices = us_can_dc[us_can_dc["postal_code"].isin(city["ma_zipcodes"])]
    metro_devices_week = metro_devices[metro_devices["week_uid"] == round(week,2)]
    metro_devices_sum = np.sum(metro_devices_week["normalized_visits_by_total_visits"])
    
    #comparison week data
    comparison_week = float("2019" + str(week)[4:])
    downtown_devices_comparison_week = downtown_devices[downtown_devices["week_uid"] == comparison_week]
    downtown_devices_comparison_sum = np.sum(downtown_devices_comparison_week["normalized_visits_by_total_visits"])
    metro_devices_comparison_week = metro_devices[metro_devices["week_uid"] == comparison_week]
    metro_devices_comparison_sum = np.sum(metro_devices_comparison_week["normalized_visits_by_total_visits"])
    
    return np.divide(np.divide(downtown_devices_sum, metro_devices_sum), np.divide(downtown_devices_comparison_sum, metro_devices_comparison_sum))

# COMMAND ----------

localized_lq_df = pd.DataFrame({"city":all_cities, "display_title":display_titles})
localized_lq_df["dt_zipcodes"] = localized_lq_df["city"].apply(lambda x: get_downtown_city_zipcodes(x))
localized_lq_df["ma_zipcodes"] = localized_lq_df["city"].apply(lambda x: get_metro_zipcodes(x))
localized_lq_df["dt_zipcodes"] = localized_lq_df["dt_zipcodes"].apply(lambda x: [int(i) for i in x])
localized_lq_df["ma_zipcodes"] = localized_lq_df["ma_zipcodes"].apply(lambda x: [int(i) for i in x])

#iterate over week list
for i in week_list:
    localized_lq_df["llq_"+str(round(i,2))] = round(localized_lq_df.apply(lambda x: get_lq_metrics(x, round(i,2)), axis=1),2)

localized_lq_df["llq_2020.12"] = (localized_lq_df["llq_2020.11"]+localized_lq_df["llq_2020.13"])/2
localized_lq_df["llq_2020.37"] = (localized_lq_df["llq_2020.36"]+localized_lq_df["llq_2020.38"])/2

# COMMAND ----------

localized_lq_df["season_1"] = localized_lq_df.iloc[:,4:17].mean(axis=1)
localized_lq_df["season_2"] = localized_lq_df.iloc[:,17:30].mean(axis=1)
localized_lq_df["season_3"] = localized_lq_df.iloc[:,30:43].mean(axis=1)
localized_lq_df["season_4"] = localized_lq_df.iloc[:,43:57].mean(axis=1)
localized_lq_df["season_5"] = localized_lq_df.iloc[:,57:70].mean(axis=1)
localized_lq_df["season_6"] = localized_lq_df.iloc[:,70:83].mean(axis=1)
localized_lq_df["season_7"] = localized_lq_df.iloc[:,83:96].mean(axis=1)
localized_lq_df["season_8"] = localized_lq_df.iloc[:,96:110].mean(axis=1)
display(localized_lq_df)

# COMMAND ----------

create_and_save_as_table(localized_lq_df.drop(columns=["dt_zipcodes","ma_zipcodes"]),"0713_localized_lq_df_totaldevs")

# COMMAND ----------

downtown_rec_df_test = downtown_rec_df

# COMMAND ----------

##DO NOT USE DOES NOT WORK
def get_period_based_metrics(table):
    table["period_1"] = table.iloc[:,4:57].mean(axis=1)
    table["period_2"] = table.iloc[:,57:110].mean(axis=1)
    table["period_3"] = table.iloc[:,110:163].mean(axis=1)
    table["period_4"] = table.iloc[:,163:216].mean(axis=1)
    table["period_4a"] = table.iloc[:,163:208].mean(axis=1)
    table["period_4b"] = table.iloc[:,208:216].mean(axis=1)
    table["rate_of_recovery"] = table["period_4"]/table["period_1"]
    table["omicron_resilience"] = table["period_4b"]/table["period_4a"]
    return table

# COMMAND ----------

create_and_save_as_table(downtown_rec_df.drop(columns=["dt_zipcodes","ma_zipcodes"]), "0407_downtown_rec_df_2")
create_and_save_as_table(metro_rec_df.drop(columns=["dt_zipcodes","ma_zipcodes"]), "0407_metro_rec_df_2")
create_and_save_as_table(relative_rec_df.drop(columns=["dt_zipcodes","ma_zipcodes"]), "0407_relative_rec_df_2")
create_and_save_as_table(localized_lq_df.drop(columns=["dt_zipcodes","ma_zipcodes"]), "0407_localized_lq_df_2")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualization ###

# COMMAND ----------

import seaborn as sns
sns.set(style="darkgrid")

# COMMAND ----------

def get_sorted_ranking(table, title_text, x_text):
    for i in ["period_1", "period_2", "period_3", "period_4",
            "period_4a", "period_4b", "rate_of_recovery",
            "omicron_resilience"]:
        table_cut = table[["display_title",i]].sort_values(by=[i], ascending=True)
        plt.figure(figsize=(6, 14))
        plt.barh(table_cut["display_title"], table_cut[i], align='center')
        plt.title(title_text+i)
        plt.xlabel(x_text)

# COMMAND ----------

downtown_rec_df = get_table_as_pandas_df("0407_downtown_rec_df_2")
metro_rec_df = get_table_as_pandas_df("0407_metro_rec_df_2")
relative_rec_df = get_table_as_pandas_df("0407_relative_rec_df_2")

# COMMAND ----------

def get_datetime(x):
    year,week = divmod(x, 1)
    year = int(year)
    week = int(week*100)
    return datetime.date(year,1,1)+relativedelta(weeks=+week)

# COMMAND ----------

import matplotlib.animation as ani
import matplotlib.pyplot as plt
import plotly.express as px

# COMMAND ----------

downtown_rec_df_plot = downtown_rec_df.transpose()
downtown_rec_df_plot.columns = downtown_rec_df_plot.loc["display_title"]
downtown_rec_df_plot = downtown_rec_df_plot.iloc[2:108]
downtown_rec_df_plot = downtown_rec_df_plot.reset_index()
downtown_rec_df_plot["datetime"] = downtown_rec_df_plot["index"].str[4:].astype(float).apply(lambda x: get_datetime(x))
downtown_rec_df_plot = downtown_rec_df_plot.drop(columns=["index"])
display(downtown_rec_df_plot)

# COMMAND ----------

import random
import matplotlib
from matplotlib import animation
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from itertools import count
%matplotlib qt

# COMMAND ----------

y1 = [random.randint(0,4)+(i**1.4)/(random.randint(10,12)) for i in range(0,180,2)]
t = range(len(y1))
x,y=[], []

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

color = ['red', 'green', 'blue', 'orange', 'brown', 'purple']
cities = ['Boston, MA', 'New York, NY', 'Chicago, IL', 'San Francisco, CA', 'Denver, CO', 'Houston, TX']
fig = plt.figure()
plt.xticks(rotation=45, ha="right", rotation_mode="anchor") #rotate the x-axis values
plt.subplots_adjust(bottom = 0.2, top = 0.9) #ensuring the dates (on the x-axis) fit in the screen
plt.ylabel('Percentage of Downtown Business Activity')
plt.xlabel('Dates')

# COMMAND ----------

def buildmebarchart(i=int):
    plt.legend(downtown_rec_df_plot[cities].columns)
    p = plt.plot(downtown_rec_df_plot[cities][:i].index, downtown_rec_df_plot[cities][:i].values) #note it only returns the dataset, up to the point i
    for i in range(0,4):
        p[i].set_color(color[i]) #set the colour of each curve
import matplotlib.animation as ani
animator = ani.FuncAnimation(fig, buildmebarchart, interval = 100)
plt.show()

# COMMAND ----------

downtown_rec_df_plot[cities]

# COMMAND ----------

pd.Series(downtown_rec_df_plot.index)

# COMMAND ----------

get_sorted_ranking(downtown_rec_df, "Downtown Recovery from 2019 Normalized Visit Counts: ", "Period Business Activity Compared to 2019 Business Activity Across Selected Metros")

# COMMAND ----------

get_sorted_ranking(metro_rec_df, "Metro Area Recovery from 2019 Normalized Visit Counts: ", "Period Business Activity Compared to 2019 Business Activity Across Selected Metros")

# COMMAND ----------

get_sorted_ranking(relative_rec_df, "Downtown vs. Metro Area Recovery from 2019 Normalized Visit Counts: ", "Period Business Activity Compared to 2019 Business Activity Across Selected Metros")

# COMMAND ----------

get_sorted_ranking(localized_lq_df, "Metro Area Polycentricity Ranking: ", "Porportion of Period Business Activity in Downtown Compared to 2019 Business Activity Across Selected Metros")

# COMMAND ----------

