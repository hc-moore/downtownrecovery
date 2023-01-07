# Databricks notebook source
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# COMMAND ----------

def create_and_save_as_table(df, table_name):
    spark_df = spark.createDataFrame(df)
    spark_df.write.format('csv').saveAsTable(table_name)
    
def get_table_as_pandas_df(table_name):
    return sqlContext.sql("select * from " + table_name + " tables").toPandas()

# COMMAND ----------

census_data = get_table_as_pandas_df("_0812_census_variables_4_csv")
weather_data = get_table_as_pandas_df("_0812_weather_3_csv")
political_data = get_table_as_pandas_df("_0812_political_leaning_1_csv")
oxford_data = get_table_as_pandas_df("_0812_oxford_4_csv")

# COMMAND ----------

full_model_data = census_data[census_data["city"]!="Hamilton"].replace({"Montréal":"Montreal","Québec":"Quebec"})
full_model_data = full_model_data.merge(weather_data[["City_","Climate_Name","Class","Type"]], how="left", left_on="city", right_on="City_").drop(columns=['City_'])
full_model_data = full_model_data.merge(political_data[["City_","%Liberal-Leaning","%Conservative-Leaning","%Other"]], how="left", left_on="city", right_on="City_").drop(columns=['City_'])
full_model_data = full_model_data.merge(oxford_data[["City_","Sum_of_C1_FULL", 'Sum_of_C2_FULL', 'Sum_of_C4_Large',
       'Sum_of_C4_FULL', 'Sum_of_C6_FULL', 'Sum_of_E1_FULL', 'Sum_of_H6_FULL']], how="left", left_on="city", right_on="City_").drop(columns=['City_'])

# COMMAND ----------

display(full_model_data)

# COMMAND ----------

full_model_data[['Climate_Cold Desert', 'Climate_Cold Semi-Arid',
       'Climate_Cold Semi-Arid ', 'Climate_Hot Desert', 'Climate_Hot Desert ',
       'Climate_Hot Semi-Arid ', 'Climate_Hot-Summer Mediterranean',
       'Climate_Humid Continental Climate - Dry Warm Summer',
       'Climate_Humid Continental Hot Summers With Year Around Precipitation',
       'Climate_Humid Continental Mild Summer, Wet All Year',
       'Climate_Humid Subtropical', 'Climate_Tropical Monsoon ',
       'Climate_Tropical Savanna', 'Climate_Warm-Summer Mediterranean']] = pd.get_dummies(full_model_data.Climate_Name, prefix='Climate')

# COMMAND ----------

full_model_data = full_model_data.drop(columns=["Climate_Name","Class","Type"])
full_model_data = full_model_data.rename(columns={"Sum_of_C1_FULL":"days_school_closing",
                                         "Sum_of_C2_FULL":"days_workplace_closing",
                                         "Sum_of_C4_Large":"days_cancel_large_events",
                                         "Sum_of_C4_FULL":"days_cancel_all_events",
                                         "Sum_of_C6_FULL":"days_stay_home_requirements",
                                         "Sum_of_E1_FULL":"days_income_support",
                                         "Sum_of_H6_FULL":"days_mask_mandates",
                                         "Climate_Humid Continental Mild Summer, Wet All Year":"Climate_Humid Continental Mild Summer - Wet All Year"})

# COMMAND ----------

create_and_save_as_table(full_model_data,"_0812_all_model_features_")

# COMMAND ----------

display(full_model_data)

# COMMAND ----------

