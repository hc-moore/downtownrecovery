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
city_index_df["postal_code"] = city_index_df["postal_code"].astype("int64")
city_index_df

# COMMAND ----------

import pandas
placekey_df = pandas.concat((
    get_subset_as_pandas_df("us_zipcode_placekey_202010", "postal_code, placekey"),
    get_table_as_pandas_df("postal_code_placekey_can").drop(columns=["city", "is_downtown"])
))

placekey_df["postal_code"] = placekey_df["postal_code"].astype("int64")

placekey_df

# COMMAND ----------

counts_df_precovid = pandas.concat((
    get_table_as_pandas_df("all_us_cities_dc").drop(columns="city"),
    get_table_as_pandas_df("all_canada_device_count_agg_dt_0317").drop(columns="city")
))
counts_df_precovid = counts_df_precovid[counts_df_precovid["date_range_start"] < "2022-03"]
counts_df_precovid

# COMMAND ----------

counts_df_covid = get_table_as_pandas_df("safegraph_patterns_count")
counts_df_covid = counts_df_covid[counts_df_covid["date_range_start"] >= "2022-03"]
counts_df_covid

# COMMAND ----------

counts_df_covid_zipcode = (
    placekey_df.merge(counts_df_covid, on="placekey")
    .groupby(["postal_code", "date_range_start"], as_index=False)
    .agg("sum")
)

counts_df_covid_zipcode


# COMMAND ----------

counts_df = pandas.concat([counts_df_precovid, counts_df_covid_zipcode])
counts_df

# COMMAND ----------

# create_replace_table(counts_df.drop_duplicates(subset=["postal_code", "date_range_start"]), "0721_combined_counts_agg_postal_code")

# COMMAND ----------

TOTAL_VISITS = "normalized_visits_by_total_visits"
RAW_VISITS = "raw_visit_counts"
STATE_SCALING = "normalized_visits_by_state_scaling"
visit_metrics = [RAW_VISITS, TOTAL_VISITS, STATE_SCALING]

formatted_df = counts_df.copy()
formatted_df["date_range_start"] = pandas.to_datetime(
    counts_df["date_range_start"], utc=True
)
formatted_df["postal_code"] = formatted_df["postal_code"].astype("int64")

formatted_df


# COMMAND ----------

create_replace_table(formatted_df.drop_duplicates(subset=["postal_code", "date_range_start"]), "0721_combined_counts_agg_postal_code")

# COMMAND ----------

agg = get_table_as_pandas_df("0721_combined_counts_agg_postal_code")
agg

# COMMAND ----------

full_df = city_index_df.merge(formatted_df)
bad_cities = ["Virginia Beach", "Mesa", "Arlington", "Aurora", "Long Beach"]
full_df = full_df[~full_df["city"].isin(bad_cities)].drop(columns="postal_code")
full_df

# COMMAND ----------

aggregated_by_city_df = full_df.groupby(
    ["city", "is_downtown", "date_range_start"], as_index=False
).aggregate("sum")
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
        (date_split_df["date_range_start_year"] >= 2022)
        & (date_split_df["date_range_start_week"] >= 1)
    )
]

covid_df # use normalized device counts!!!!!!!

# COMMAND ----------

city_list_df = covid_df.drop_duplicates(
    subset=("city", "date_range_start_year", "date_range_start_week"),
    ignore_index=True,
).drop(columns=visit_metrics + ["is_downtown"])

city_list_df

# COMMAND ----------

combined_df = covid_df.merge(
    precovid_df,
    on=["city", "is_downtown", "date_range_start_week"],
    suffixes=["_covid", "_precovid"],
)

combined_df

# COMMAND ----------

c = combined_df["city"]
d = covid_df["city"]
e = precovid_df["city"]
d[~d.isin(e)]

# COMMAND ----------

downtown_df = combined_df[combined_df["is_downtown"]].reset_index(drop=True)

downtown_df

# COMMAND ----------

city_df = combined_df.groupby(
    ["city", "date_range_start_year", "date_range_start_week"], as_index=False
).aggregate("sum")

city_df

# COMMAND ----------

city_df[city_df["city"] == "Vancouver"]

# COMMAND ----------

rq_downtown_df = city_list_df.copy()

rq_downtown_df["metric"] = "downtown"
for metric in visit_metrics:
    rq_downtown_df[metric] = (downtown_df[metric + "_covid"] / downtown_df[metric + "_precovid"])
    
rq_downtown_df

# COMMAND ----------

rq_city_df = city_list_df.copy()

rq_city_df["metric"] = "metro"
for metric in visit_metrics:
    rq_city_df[metric] = (city_df[metric + "_covid"] / city_df[metric + "_precovid"])

rq_city_df

# COMMAND ----------

lq_df = city_list_df.copy()

lq_df["metric"] = "relative"
for metric in visit_metrics:
    ratio_covid = downtown_df[metric + "_covid"] / city_df[metric + "_covid"]
    ratio_precovid = downtown_df[metric + "_precovid"] / city_df[metric + "_precovid"]
    lq_df[metric] = ratio_covid / ratio_precovid

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
output_df


# COMMAND ----------

output_df[output_df["city"] == "Boston"]

# COMMAND ----------

create_replace_table(output_df, "processed_city_rq_lq_mar_june")

# COMMAND ----------

output_df = get_table_as_pandas_df("processed_city_rq_lq_mar_june")

# COMMAND ----------

output_df[output_df["city"] == "Boston"]

# COMMAND ----------

prev_rq_downtown = get_table_as_pandas_df("0713_downtown_rec_df_totaldevs")
prev_rq_downtown

# COMMAND ----------

prev_rq_city = get_table_as_pandas_df("0713_metro_rec_df_totaldevs")
prev_rq_city

# COMMAND ----------

prev_lq = get_table_as_pandas_df("0713_localized_lq_df_totaldevs")
prev_lq

# COMMAND ----------

cols = prev_rq_downtown.columns
cols[prev_rq_downtown.columns.get_loc("season_1"):]

# COMMAND ----------

def format_prev_df(prev_df, prefix, metric_name, metric_label, end_col):
    cols = prev_df.columns
    modified = (
        prev_df
            .drop(columns=cols[cols.get_loc(end_col):])
            .melt(id_vars=["city", "display_title"], var_name="date", value_name=metric_name)
    )
    modified["week"] = pandas.to_datetime(
        modified["date"].str.pad(width=7+len(prefix),side="right",fillchar="0")
        + ".1",
        format=prefix+"%Y.%U.%w",
    ).dt.strftime("%Y-%m-%d")
    modified["metric"] = metric_label

    return modified.drop(columns=["date", "display_title"])

prev_rq_downtown_formatted_tv = format_prev_df(prev_rq_downtown, "rec_", "normalized_visits_by_total_visits", "downtown", "season_1")
prev_rq_downtown_formatted_tv

# COMMAND ----------

prev_rq_city_formatted_tv = format_prev_df(prev_rq_city, "rec_", "normalized_visits_by_total_visits", "metro", "season_1")
prev_rq_city_formatted_tv

# COMMAND ----------

prev_lq_formatted_tv = format_prev_df(prev_lq, "llq_", "normalized_visits_by_total_visits", "relative", "season_1")
prev_lq_formatted_tv

# COMMAND ----------

prev_rq_downtown_ss = get_table_as_pandas_df("0407_downtown_rec_df_2")
prev_rq_city_ss = get_table_as_pandas_df("0407_metro_rec_df_2")
prev_lq_ss = get_table_as_pandas_df("0407_localized_lq_df_2")

prev_rq_downtown_ss

# COMMAND ----------

prev_rq_downtown_formatted_ss = format_prev_df(prev_rq_downtown_ss, "rec_", "normalized_visits_by_state_scaling", "downtown", "period_1")
prev_rq_city_formatted_ss = format_prev_df(prev_rq_city_ss, "rec_", "normalized_visits_by_state_scaling", "metro", "period_1")
prev_lq_formatted_ss = format_prev_df(prev_lq_ss, "llq_", "normalized_visits_by_state_scaling", "relative", "period_1")
prev_rq_downtown_formatted_ss

# COMMAND ----------

prev_rq_downtown_merged = prev_rq_downtown_formatted_ss.merge(prev_rq_downtown_formatted_tv)
prev_rq_city_merged = prev_rq_city_formatted_ss.merge(prev_rq_city_formatted_tv)
prev_lq_merged = prev_lq_formatted_ss.merge(prev_lq_formatted_tv)

prev_lq_merged

# COMMAND ----------

union_df = pandas.concat((output_df, prev_rq_downtown_merged, prev_rq_city_merged, prev_lq_merged))
union_df

# COMMAND ----------

create_replace_table(union_df, "0714_combined_metrics_df")

# COMMAND ----------

(union_df["week"] == "2022-02-28").sum()

# COMMAND ----------

union_df = get_table_as_pandas_df("0714_combined_metrics_df")

# COMMAND ----------

union_df

# COMMAND ----------

union_df[(union_df["city"] == "Boston") & (union_df["week"] < "2022") & (union_df["week"] > "2021")]

# COMMAND ----------

