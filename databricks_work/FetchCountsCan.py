# Databricks notebook source
pip install aiohttp

# COMMAND ----------

pip install gql


# COMMAND ----------

def create_replace_table(df, table_name):
    spark_df = spark.createDataFrame(df)
    spark_df.write.format('csv').saveAsTable(table_name, mode="overwrite")

def append_table(df, table_name):
    spark_df = spark.createDataFrame(df)
    spark_df.write.format('csv').saveAsTable(table_name, mode="append")
    
def get_table_as_pandas_df(table_name):
    return sqlContext.sql("select * from " + table_name + " tables").toPandas()

def get_subset_as_pandas_df(table_name, cols):
    return sqlContext.sql("select " + cols + " from " + table_name + " tables").toPandas()

# COMMAND ----------

import gql.transport.aiohttp

API_KEYS = ["Cf7EHS1B4njwwihTMHyUU4glL1m7mIVl", "0bo56aB5aqJGW7tLhfsZGgWkTeclF4EJ"]
# with open("API_KEYS.txt", "r") as keys:
#     API_KEYS = keys.read().splitlines()

transports = [
    gql.transport.aiohttp.AIOHTTPTransport(
        url="https://api.safegraph.com/v2/graphql/",
        headers={
            "Content-Type": "application/json",
            "apikey": key,
        },
    )
    for key in API_KEYS
]
transport = transports[0]

# COMMAND ----------

import pandas

city_index_full_df = get_subset_as_pandas_df("all_city_index", "postal_code, city" )

cities_blacklist = []
cities_whitelist = ["San Francisco", "Bakersfield"]

city_index_df = city_index_full_df[~city_index_full_df["city"].isin(cities_blacklist)]
city_index_df

# COMMAND ----------

placekey_df = get_table_as_pandas_df("postal_code_placekey_can").drop(columns="city")
placekey_df["postal_code"] = placekey_df["postal_code"].str.zfill(5)

placekey_df

# COMMAND ----------

key_city_df = (
    placekey_df
    .merge(city_index_df, on="postal_code")
)
key_city_df

# COMMAND ----------

key_city_df[key_city_df["city"] == "Vancouver"]["postal_code"].unique()

# COMMAND ----------

import asyncio
import datetime
import gql
import gql.transport

MAX_KEYS = 20

# fetch naics code from may (separate request), duration for whole month
# last 2 weeks of february until end of may

counts_query = gql.gql(
    """
    query getDeviceCounts($placekeys: [Placekey!], $start_date: DateTime!, $end_date: DateTime!) {
        batch_lookup(placekeys: $placekeys) {
            weekly_patterns (start_date: $start_date, end_date: $end_date) {
                placekey
                date_range_start
                raw_visit_counts
                normalized_visits_by_total_visits
                normalized_visits_by_state_scaling
            }
        }
    }
    """
)


async def fetch_keys(client: gql.client.AsyncClientSession, placekeys, start, end):
    try:
        responses = await client.execute(
            counts_query,
            variable_values={
                "start_date": start.strftime("%Y-%m-%d"),
                "end_date": end.strftime("%Y-%m-%d"),
                "placekeys": placekeys,
            },
        )
        return pandas.DataFrame.from_records(
            week
            for weekly in responses["batch_lookup"]
            if weekly["weekly_patterns"] is not None
            for week in weekly["weekly_patterns"]
        )
    except gql.transport.exceptions.TransportServerError:
        return await fetch_keys(client, placekeys, start, end)
        


def split_keys(placekeys, chunk_size):
    return (placekeys[i : i + chunk_size] for i in range(0, len(placekeys), chunk_size))


def split_keys_into(placekeys, num_divsions):
    length = len(placekeys)
    return (
        placekeys[i * length // num_divsions : (i + 1) * length // num_divsions]
        for i in range(num_divsions)
    )


async def fetch_all_sub(session, keys, start, end):\
    return pandas.concat(
        [
            await fetch_keys(session, key_group.tolist(), start, end)
            for key_group in split_keys(keys, MAX_KEYS)
        ]
    )


async def fetch_per_api_key(
    transport,
    keys,
    start,
    end,
):
    async with gql.Client(
        transport=transport, fetch_schema_from_transport=True, execute_timeout=None
    ) as session:
        NUM_SPLITS_PER_API_KEY = 2
        results = await asyncio.gather(
            *(
                fetch_all_sub(
                    session,
                    subset,
                    start,
                    end,
                )
                for subset in split_keys_into(keys, NUM_SPLITS_PER_API_KEY)
            )
        )
        return pandas.concat(results)


async def fetch_key_by_city(keys, start, end):
    num_transports = len(transports)
    results = pandas.concat(
        await asyncio.gather(
            *(
                fetch_per_api_key(
                    transports[i],
                    subset,
                    start,
                    end,
                )
                for i, subset in enumerate(split_keys_into(keys, num_transports))
            )
        )
    )
    results["date_range_start"] = results["date_range_start"].str.slice(stop=10)
    return results


async def fetch_key_all(keys_df, start, end):
    for city in keys_df["city"].drop_duplicates():
        print("FETCHING: {}".format(city))
        result = await fetch_key_by_city(keys_df[keys_df["city"] == city]["placekey"], start, end)
        print("FETCHED {} PLACEKEYS".format(result.shape[0]))
        append_table(result, "safegraph_patterns_count_merged_placekeys")
        print("SAVED DATA")
    

date = datetime.date.fromisocalendar(2022, 9, 1)
counts_df = await fetch_key_all(
    key_city_df, date, date + datetime.timedelta(weeks=18)
)
counts_df

# COMMAND ----------

# def reset_table(table_name):
#     return sqlContext.sql("drop table " + table_name)

# COMMAND ----------

# reset_table("safegraph_patterns_count")

# COMMAND ----------

a=get_table_as_pandas_df("safegraph_patterns_count")
a

# COMMAND ----------

a.drop_duplicates()

# COMMAND ----------

# create_replace_table(a.drop_duplicates(), "safegraph_patterns_count")

# COMMAND ----------

