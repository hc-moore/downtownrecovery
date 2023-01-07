# Databricks notebook source
!pip install seaborn --upgrade

# COMMAND ----------

# quality of life 
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, date
# plotting
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns
# for fun
from scipy.fft import fft, fftfreq

# COMMAND ----------

def create_and_save_as_table(df, table_name):
    spark_df = spark.createDataFrame(df)
    spark_df.write.format('csv').saveAsTable(table_name)
    
def get_table_as_pandas_df(table_name):
    return sqlContext.sql("select * from " + table_name + " tables").toPandas()

# COMMAND ----------

weekly_lqs = get_table_as_pandas_df('all_weekly_lqs_csv')
display(weekly_lqs)

# COMMAND ----------



# COMMAND ----------

dt_all_metro_lq_df = get_table_as_pandas_df('us_can_lqs_0317')
display(dt_all_metro_lq_df)

# COMMAND ----------

dt_all_metro_lq_df = get_table_as_pandas_df('us_can_lqs_0317')

# Create ranking of latest 8 week average of 5 week LQ rolling average
dt_all_metro_lq_df['date_range_start_covid'] = pd.to_datetime(dt_all_metro_lq_df['date_range_start_covid'], format='%Y-%m-%d')
covid_time_series = dt_all_metro_lq_df[['date_range_start_covid', 'city', 'rvc_lq', 'nvt_lq', 'nvs_lq']]

# COMMAND ----------

scope_cities=['Toronto','Mississauga','New York', 'San Francisco', 'Montréal', 'Québec', 'London', 'Ottawa', 'Vancouver','Dallas', 'Seattle', 'Chicago', 'Washington DC', 'Halifax', 'Edmonton', 'Calgary', 'Winnipeg', 'Detroit', 'Los Angeles', 'Denver', 'Houston', 'Nashville', 'Atlanta']
covid_time_series = covid_time_series[covid_time_series['city'].isin(scope_cities)]
covid_time_series_cities = covid_time_series.groupby(by='city')['rvc_lq'].apply(list)
city_names = covid_time_series_cities.index
dates = covid_time_series['date_range_start_covid'].unique()
covid_time_series_cities = np.array([np.array(column) for column in covid_time_series_cities])
# cities time series is the rvc_lq for cities over time
cities_time_series = pd.DataFrame(covid_time_series_cities)
cities_time_series = cities_time_series.T
cities_time_series.columns = city_names
cities_time_series['dates'] = dates 

# COMMAND ----------

# some dfs of interest
# melt the df into longform
cities_time_series_melted = cities_time_series.melt(id_vars = "dates")
# in chronological order
cities_time_series_chrono = cities_time_series.sort_values(by = "dates")
cities_time_series_chrono = cities_time_series_chrono.set_index("dates")

# COMMAND ----------

dbutils.widgets.dropdown("rolling_window", "5", [str(x) for x in range(1, 26)])

# COMMAND ----------

dbutils.widgets.dropdown("to_plot", "rolling_rvc_lq", ["rolling_rvc_lq", "weekly_rank", "rank_change"])

# COMMAND ----------

rolling_window = int(dbutils.widgets.get("rolling_window"))
rolling_df = pd.DataFrame()
for label, content in cities_time_series_chrono.items():
    rolling_df = pd.concat([rolling_df, pd.DataFrame(data = {label : content.rolling(rolling_window, center = True).mean()}, index = cities_time_series_chrono.index)], axis = 1)
rolling_df = rolling_df.dropna(how = "all")
rolling_df = rolling_df.reset_index()

# for plotting
rolling_df_melted = rolling_df.melt(id_vars = "dates")
rolling_df_melted = rolling_df_melted.rename(columns = {"variable" : "city"})
rolling_df_melted = rolling_df_melted.rename(columns = {"value" : "rolling_rvc_lq"})
rolling_df = rolling_df.sort_values("dates")
weekly_ranks = {}
for idx, row in rolling_df.iterrows():
    weekly_ranks[row["dates"]] = row[1:].rank(method = "max")
    
ranks_df = pd.DataFrame(weekly_ranks)
ranks_change_df = ranks_df.diff(axis = 1).T.reset_index().rename(columns = {"index" : "dates"})

ranks_df_melted = ranks_df.T.reset_index().rename(columns = {"index" : "dates"}).melt(id_vars = "dates").rename(columns = {"variable" : "city", "value" : "weekly_rank"})
ranks_df_melted["dates"] = pd.to_datetime(ranks_df_melted["dates"])
ranks_change_df_melted = ranks_change_df.melt(id_vars = "dates").rename(columns = {"variable" : "city", "value" : "rank_change"})
ranks_change_df_melted["dates"] = pd.to_datetime(ranks_change_df_melted["dates"])

all_melted_dfs = rolling_df_melted.merge(ranks_df_melted, on = ["dates", "city"], how = "inner").merge(ranks_change_df_melted, on = ["dates", "city"], how = "inner")

avgs = all_melted_dfs.groupby(["dates"]).mean().reset_index()
avgs['avg_rank'] = (avgs['rank_change'] - np.nanmin(avgs['rank_change'])) / (np.nanmax(avgs['rank_change']) - np.nanmin(avgs['rank_change'])) * (np.nanmax(all_melted_dfs[dbutils.widgets.get("to_plot")]) - np.nanmin(all_melted_dfs[dbutils.widgets.get("to_plot")])) + np.nanmin(all_melted_dfs[dbutils.widgets.get("to_plot")])

sns.set_style('dark')
sns.set_context("talk")
fig = plt.figure(figsize = (18, 12))




ax = sns.lineplot(x = "dates",
             y = dbutils.widgets.get("to_plot"),
             hue = "city",
             style = "city",
             data = all_melted_dfs)
             #legend = None)

ax = sns.lineplot(x = "dates",
             y = "avg_rank",
             color = 'red',
             data = avgs,
             legend = None)

if (dbutils.widgets.get("to_plot") == "weekly_rank"):
    for line, city in zip(ax.lines, (all_melted_dfs["city"].unique()).tolist()):
        y_start = line.get_ydata()[0]
        x_start = line.get_xdata()[0] - 25
        y_end = line.get_ydata()[-1]
        x_end = line.get_xdata()[-1]
        if not np.isfinite(y_end):
            y_end = next(reversed(line.get_ydata()[~line.get_ydata().mask]),float("nan"))
            y_start = next(line.get_ydata()[~line.get_ydata().mask]),float("nan")
        if not np.isfinite(y_end) or not np.isfinite(x_end):
            continue     
        text_start = ax.annotate(city,
                           xy=(x_start, y_start),
                           xytext=(-25, 0),
                           color=line.get_color(),
                           xycoords=(ax.get_xaxis_transform(),
                                     ax.get_yaxis_transform()),
                           textcoords="offset points")
        text_end = ax.annotate(city,
                           xy=(x_end, y_end),
                           xytext=(0, 0),
                           color=line.get_color(),
                           xycoords=(ax.get_xaxis_transform(),
                                     ax.get_yaxis_transform()),
                           textcoords="offset points")
        text_width_start = (text_start.get_window_extent(
        fig.canvas.get_renderer()).transformed(ax.transData.inverted()).width)
        text_width_end = (text_end.get_window_extent(
        fig.canvas.get_renderer()).transformed(ax.transData.inverted()).width)
        if np.isfinite(text_width_start) | np.isfinite(text_width_end):
            ax.set_xlim(ax.get_xlim()[0], text_start.xy[0] - text_width_start * 1.05)
            ax.set_xlim(ax.get_xlim()[0], text_end.xy[0] + text_width_end * 1.05)
plt.xticks(rotation = 25)
ax.set(ylabel = None)
ax.tick_params(axis = 'y', pad = 20)
plt.title(str(rolling_window) + " week rolling average " + dbutils.widgets.get("to_plot"))
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
plt.show()

# COMMAND ----------

create_and_save_as_table(rolling_df_melted, 'city_lq_rankings_smoothed')

# COMMAND ----------

rolling_df_pivoted = rolling_df_melted.pivot(columns = "city", index = "dates", values = "rolling_rvc_lq").reset_index()

# COMMAND ----------

create_and_save_as_table(rolling_df_pivoted, 'city_lq_rankings_smoothed_wide')

# COMMAND ----------

display(rolling_df_pivoted)