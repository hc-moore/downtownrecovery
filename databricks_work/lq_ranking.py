# Databricks notebook source
import safegraphql.client as sgql
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport

# COMMAND ----------

import pandas as pd
from datetime import datetime, timedelta, date
import matplotlib as mpl
import matplotlib.pyplot as plt

# COMMAND ----------

def create_and_save_as_table(df, table_name):
    spark_df = spark.createDataFrame(df)
    spark_df.write.format('csv').saveAsTable(table_name)
    
def get_table_as_pandas_df(table_name):
    return sqlContext.sql("select * from " + table_name + " tables").toPandas()

# COMMAND ----------

dt_all_metro_lq_df = get_table_as_pandas_df('us_can_lqs_0317')
dt_all_metro_lq_df

# COMMAND ----------

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

ranked_rvc_lq

# COMMAND ----------

plt.figure(figsize=(10, 15))
plt.barh(list(ranked_rvc_lq.keys()), ranked_rvc_lq.values, align='center')
plt.title("Downtowns Ranked by Greatest Business Activity Recovery")
plt.xlabel("August - December 2021 Business Activity Compared to 2019 Business Activity Across Selected Metros")

# COMMAND ----------

all_dt_to_local_lqs = get_table_as_pandas_df('all_cities_weekly_lq')
all_dt_to_local_lqs

# COMMAND ----------

# Create ranking of latest 8 week average of 5 week LQ rolling average
dt_all_metro_rolling_lq_df = all_dt_to_local_lqs.rolling(5, center=True).mean()

# Take the last rolling averages from 8 weeks of the year
dec_nov_metro_rolling_lqs = dt_all_metro_rolling_lq_df.tail(27).iloc[0:25,:]
# Average of last 8 weeks of rolling averages
mean_lq_of_dec_nov = dec_nov_metro_rolling_lqs.mean().sort_values(ascending=True)

all_cities = list(mean_lq_of_dec_nov.keys())
non_sink_cities = res = [i for i in all_cities if i not in ['Salt Lake City', 'Miami', 'Charlotte', 'Dallas', 'Denver']]

ranked_cities_by_latest_lqs_local = mean_lq_of_dec_nov.filter(items=non_sink_cities)

# COMMAND ----------

ranked_cities_by_latest_lqs_local

# COMMAND ----------

plt.figure(figsize=(10, 12))
plt.barh(list(ranked_cities_by_latest_lqs_local.keys()), ranked_cities_by_latest_lqs_local.values, align='center')
plt.title("Downtowns Ranked by Greatest Business Activity Recovery")
plt.xlabel("Last 6 Months of 2021 Business Activity Compared to 2019 Business Activity in Local Metro")

# COMMAND ----------

ranking_comp_df = pd.DataFrame()
ranking_comp_df['local_lq_ranking'] = list(ranked_cities_by_latest_lqs_local.keys())
ranking_comp_df['allmetro_lq_ranking'] = list(ranked_cities_by_latest_lqs_all.keys())
ranking_comp_descending_df = ranking_comp_df.loc[::-1]
ranking_comp_descending_df

# COMMAND ----------

local_lqs = list(ranked_cities_by_latest_lqs_local.keys())
all_lqs = list(ranked_cities_by_latest_lqs_all.keys())
rank_diff = []

for index, local_lq_city in enumerate(local_lqs):
    local_rank = index + 1
    all_rank = all_lqs.index(local_lq_city) + 1
    rank_diff.append([local_lq_city, local_rank - all_rank])
    
rank_diff.reverse()
rank_diff_df = pd.DataFrame(rank_diff, columns=['City', 'Rank Change'])
rank_diff_df['Change'] = rank_diff_df['Rank Change'].abs()
rank_diff_df['Direction'] = rank_diff_df['Rank Change'] > 0
rank_diff_df = rank_diff_df.replace(True, 'Increased')
rank_diff_df = rank_diff_df.replace(False, 'Decreased')
rank_diff_df.sort_values(by='Change', ascending=False)

# COMMAND ----------

rank_diff_bar_chart = rank_diff_df.sort_values(by='Rank Change', ascending=False)
plt.figure(figsize=(10, 12))
plt.barh(rank_diff_bar_chart['City'].values, rank_diff_bar_chart['Rank Change'].values, align='center')
plt.title("Changes in Ranking From Local Metro LQ to All Selected Metro LQ")
plt.xlabel("Ranking Change")

# COMMAND ----------

# Load lq df
dt_all_metro_lq_df = get_table_as_pandas_df('us_can_lqs_0317')

# Create ranking of latest 8 week average of 5 week LQ rolling average
dt_all_metro_lq_df['date_range_start_covid'] = pd.to_datetime(dt_all_metro_lq_df['date_range_start_covid'], format='%Y-%m-%d')

covid_time_series = dt_all_metro_lq_df[['date_range_start_covid', 'city', 'rvc_lq', 'nvt_lq', 'nvs_lq']]

# COMMAND ----------

import numpy as np
scope_cities=['Toronto','Mississauga','New York', 'San Francisco', 'Montréal', 'Quebec', 'London', 'Ottawa', 'Vancouver','Dallas', 'Seattle', 'Chicago', 'Washington DC', 'Halifax', 'Edmonton', 'Calgary', 'Winnipeg', 'Detroit', 'Los Angeles', 'Denver', 'Houston', 'Nashville', 'Atlanta']
covid_time_series = covid_time_series[covid_time_series['city'].isin(scope_cities)]
covid_time_series_cities = covid_time_series.groupby(by='city')['rvc_lq'].apply(list)
city_names = covid_time_series_cities.index
dates = covid_time_series['date_range_start_covid'].unique()
covid_time_series_cities= np.array([np.array(column) for column in covid_time_series_cities])
cities_time_series = pd.DataFrame(covid_time_series_cities)
cities_time_series = cities_time_series.T
cities_time_series.columns = city_names
# cities_time_series = cities_time_series.set_index(dates)
cities_time_series['dates'] = dates 

# COMMAND ----------

display(cities_time_series)

# COMMAND ----------

smoothened_lq_rankings = get_table_as_pandas_df('city_lq_rankings_smoothed_wide')
display(smoothened_lq_rankings)

# COMMAND ----------

