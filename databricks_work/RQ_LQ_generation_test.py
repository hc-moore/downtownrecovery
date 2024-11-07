# Databricks notebook source
def create_replace_table(df, table_name):
    spark_df = spark.createDataFrame(df)
    spark_df.write.format('csv').saveAsTable(table_name, mode="overwrite")
    
def get_table_as_pandas_df(table_name):
    return sqlContext.sql("select * from " + table_name + " tables").toPandas()

def get_subset_as_pandas_df(table_name, cols):
    return sqlContext.sql("select " + cols + " from " + table_name + " tables").toPandas()


# COMMAND ----------

import pandas

city_index_full_df = get_subset_as_pandas_df("all_city_index", "postal_code, city, is_downtown")

cities_to_use = ["San Francisco", "Bakersfield"]

city_index_df = city_index_full_df
city_index_df

# COMMAND ----------

import pandas
placekey_df = get_subset_as_pandas_df("us_zipcode_placekey_202010", "postal_code, placekey")
placekey_df["postal_code"] = placekey_df["postal_code"].str.zfill(5)

placekey_df

# COMMAND ----------

counts_df_precovid = get_table_as_pandas_df("all_us_cities_dc").drop(columns="city")
counts_df_precovid = counts_df_precovid[counts_df_precovid["date_range_start"].str.contains("2019")]
counts_df_precovid

# COMMAND ----------

max(counts_df_precovid["date_range_start"])

# COMMAND ----------

counts_df_covid = get_table_as_pandas_df("safegraph_patterns_count")


# COMMAND ----------

counts_df_covid[counts_df_covid.duplicated()]

# COMMAND ----------

counts_df_covid_zipcode = (
    placekey_df.merge(counts_df_covid, on="placekey")
    .groupby(["postal_code", "date_range_start"], as_index=False)
    .agg("sum")
)

counts_df_covid_zipcode


# COMMAND ----------

a=counts_df_covid_zipcode[counts_df_covid_zipcode["date_range_start"].str.contains("2022-02")]
a

# COMMAND ----------

counts_df = pandas.concat([counts_df_precovid, a])
counts_df

# COMMAND ----------

formatted_df = counts_df.drop(
    columns=["raw_visit_counts", "normalized_visits_by_total_visits"]
).rename(columns={"normalized_visits_by_state_scaling": "visits"})
formatted_df["date_range_start"] = pandas.to_datetime(
    counts_df["date_range_start"], utc=True
)
formatted_df


# COMMAND ----------

full_df = city_index_df.merge(formatted_df)
bad_cities = ["Virginia Beach", "Mesa", "Arlington", "Aurora", "Long Beach"]
full_df = full_df[~full_df["city"].isin(bad_cities)]
full_df

# COMMAND ----------

aggregated_by_city_df = full_df.groupby(
    ["city", "is_downtown", "date_range_start"], as_index=False
).aggregate({"visits": "sum"})
aggregated_by_city_df


# COMMAND ----------

date_split_df = aggregated_by_city_df.drop("date_range_start", axis=1)
dates = aggregated_by_city_df["date_range_start"].dt.isocalendar()
date_split_df["date_range_start_year"] = dates.year
date_split_df["date_range_start_week"] = dates.week
date_split_df

# COMMAND ----------

precovid_df = date_split_df[
    (date_split_df["date_range_start_year"] == 2019)
    | (
        (date_split_df["date_range_start_year"] == 2020)
        & (date_split_df["date_range_start_week"] == 1)
    )
].drop("date_range_start_year", axis=1)
precovid_df


# COMMAND ----------

covid_df = date_split_df[
    (
        (date_split_df["date_range_start_year"] >= 2021)
        & (date_split_df["date_range_start_week"] >= 1)
    )
]

covid_df["city"].drop_duplicates()  # use normalized device counts!!!!!!!

# COMMAND ----------

city_list_df = covid_df.drop(["visits", "is_downtown"], axis=1).drop_duplicates(
    ignore_index=True
).reset_index(drop=True)

city_list_df[city_list_df["city"] == "Houston"]


# COMMAND ----------

combined_df = covid_df.merge(
    precovid_df,
    on=["city", "is_downtown", "date_range_start_week"],
    suffixes=["_covid", "_precovid"],
)

downtown_df = (
    combined_df[combined_df["is_downtown"]].reset_index(drop=True)
)

city_df = combined_df.groupby(
    ["city", "date_range_start_year", "date_range_start_week"], as_index=False
).aggregate({"visits_covid": "sum", "visits_precovid": "sum"})


combined_df


# COMMAND ----------

city_df.iloc[810:]

# COMMAND ----------

city_df[~city_df["city"].isin(downtown_df["city"])]

# COMMAND ----------

a=downtown_df
(a["visits_covid"] / a["visits_precovid"]).isna().sum()

# COMMAND ----------

rq_downtown_df = city_list_df.copy()

rq_downtown_df["metric"] = "downtown"
rq_downtown_df["weekly_lq"] = (
    downtown_df["visits_covid"] / downtown_df["visits_precovid"]
)


rq_downtown_df

# COMMAND ----------

existing = get_table_as_pandas_df("0407_downtown_rec_df_2")

# COMMAND ----------

existing

# COMMAND ----------

table = pandas.DataFrame(columns=["city", "week", "existing", "new", "difference"])
for i, entry in rq_downtown_df.iterrows():
    city = entry["city"]
    week = entry["date_range_start_week"]
    if week < 9:
        metric = entry["weekly_lq"]
        old_metric = existing[existing["city"] == city]["rec_2022.0" + str(week)].values[0]
        table = table.append({"city": city, "week": week, "existing": old_metric, "new": metric, "difference": abs(metric - old_metric)}, ignore_index=True)
#     print(old_metric - metric)
#     print("CITY {}: old downtown RQ is {}, new downtown RQ is {}, difference of {}")
print ("avg diff is " + str(table["difference"].mean()))
print ("avg diff no ny is " + str(table[table["city"] != "New York"]["difference"].mean()))


display(table)

# COMMAND ----------

rq_city_df = city_list_df.copy()

rq_city_df["metric"] = "metro"
rq_city_df["weekly_lq"] = city_df["visits_covid"] / city_df["visits_precovid"]

rq_city_df

# COMMAND ----------

lq_df = city_list_df.copy()
ratio_covid = downtown_df["visits_covid"] / city_df["visits_covid"]
ratio_precovid = downtown_df["visits_precovid"] / city_df["visits_precovid"]

lq_df["metric"] = "relative"
lq_df["weekly_lq"] = ratio_covid / ratio_precovid

lq_df

# COMMAND ----------

output_df = pandas.concat((rq_downtown_df, rq_city_df, lq_df))

# convert year + week back to date
output_df["week"] = pandas.to_datetime(
    output_df["date_range_start_year"] * 1000
    + output_df["date_range_start_week"] * 10
    + 1,
    format="%Y%U%w",
).dt.strftime("%Y-%m-%d")

output_df = output_df.drop(columns=["date_range_start_year", "date_range_start_week"])
output_df = output_df[output_df.columns[[0, 1, 3, 2]]]
output_df


# COMMAND ----------

create_replace_table(output_df, "processed_city_rq_lq_mar_june")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

a=get_table_as_pandas_df("processed_city_rq_lq_mar_june")


# COMMAND ----------



# COMMAND ----------

