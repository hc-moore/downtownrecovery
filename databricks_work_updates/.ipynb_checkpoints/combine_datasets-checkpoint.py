# Databricks notebook source
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# COMMAND ----------
original_model_data = pd.read_csv('~/git/downtownrecovery/shinyapp/input_data/all_model_features_1015_weather.csv')


census_data =  pd.read_csv("~/data/downtownrecovery/model_input_datasets/0812_census_variables-4.csv")
weather_data =  pd.read_csv("~/data/downtownrecovery/model_input_datasets/weather_0812_3.csv")
political_data =  pd.read_csv("~/data/downtownrecovery/model_input_datasets/political_leaning_0812_1.csv")
oxford_data =  pd.read_csv("~/data/downtownrecovery/model_input_datasets/oxford_variables_0812_4.csv")

full_model_data = census_data[census_data["city"]!="Hamilton"].replace({"Montréal":"Montreal","Québec":"Quebec"})
full_model_data = full_model_data.merge(weather_data[["City ","Climate Name","Class","Type"]], how="left", left_on="city", right_on="City ").drop(columns=['City '])
full_model_data = full_model_data.merge(political_data[["City ","%Liberal-Leaning","%Conservative-Leaning","%Other"]], how="left", left_on="city", right_on="City ").drop(columns=['City '])
full_model_data = full_model_data.merge(oxford_data[["City ","Sum of C1_FULL", 'Sum of C2_FULL', 'Sum of C4_Large',
       'Sum of C4_FULL', 'Sum of C6_FULL', 'Sum of E1_FULL', 'Sum of H6_FULL']], how="left", left_on="city", right_on="City ").drop(columns=['City '])

full_model_data[['Climate_Cold Desert', 'Climate_Cold Semi-Arid',
       'Climate_Cold Semi-Arid ', 'Climate_Hot Desert', 'Climate_Hot Desert ',
       'Climate_Hot Semi-Arid ', 'Climate_Hot-Summer Mediterranean',
       'Climate_Humid Continental Climate - Dry Warm Summer',
       'Climate_Humid Continental Hot Summers With Year Around Precipitation',
       'Climate_Humid Continental Mild Summer, Wet All Year',
       'Climate_Humid Subtropical', 'Climate_Tropical Monsoon ',
       'Climate_Tropical Savanna', 'Climate_Warm-Summer Mediterranean']] = pd.get_dummies(full_model_data['Climate Name'], prefix='Climate')

full_model_data = full_model_data.drop(columns=["Climate Name","Class","Type"])
full_model_data = full_model_data.rename(columns={"Sum of C1_FULL":"days_school_closing",
                                         "Sum of C2_FULL":"days_workplace_closing",
                                         "Sum of C4_Large":"days_cancel_large_events",
                                         "Sum of C4_FULL":"days_cancel_all_events",
                                         "Sum of C6_FULL":"days_stay_home_requirements",
                                         "Sum of E1_FULL":"days_income_support",
                                         "Sum of H6_FULL":"days_mask_mandates",
                                         "Climate_Humid Continental Mild Summer, Wet All Year":"Climate_Humid Continental Mild Summer - Wet All Year"})

full_model_data[['city', 'pct_hisp_downtown', 'pct_hisp_city']]
# want to replace columns containing 'density' with updated columns
# business only exists at the downtown level and was computed separately in the US 
# it was computed correctly the first time
# but canadian densities were all off by a scaling factor of km2 <-> mi2
original_model_data.columns
us_densities.columns
can_city_index_with_features.columns
can_city_index_with_features.head()
can_densities = can_city_index_with_features.rename(columns = {"0": 'city'})[['city', 'population_density_downtown',
       'population_density_city', 'housing_density_downtown',
       'housing_density_city', 'employment_density_downtown']].rename(columns = { 'population_density_downtown' : 'population_density_downtown_km2',
       'population_density_city':'population_density_city_km2', 'housing_density_downtown':'housing_density_downtown_km2',
       'housing_density_city':'housing_density_city_km2', 'employment_density_downtown':'employment_density_downtown_km2'})

can_densities


# change them back to original variables so modeling is easier
all_densities = pd.concat([us_densities[can_densities.columns.values], can_densities]).rename(columns = {
    'population_density_downtown_km2' : 'population_density_downtown',
    'population_density_city_km2':'population_density_city',
    'housing_density_downtown_km2':'housing_density_downtown',
    'housing_density_city_km2':'housing_density_city',
    'employment_density_downtown_km2':'employment_density_downtown'})

# now join this to original model data

original_model_data_updated = original_model_data.drop(columns = ['population_density_downtown',

                                                                  'population_density_city',
                                                                  'housing_density_downtown',
                                                                  'housing_density_city',
                                                                  'employment_density_downtown']).merge(all_densities, how = "left", on = "city")

original_model_data_updated.columns

original_model_data_updated_subset = original_model_data_updated.drop(columns = ['normalized_visits_by_total_visits', 'week', 'metric', 'normalized Violent crimes based on population', 'Violent crimes per 100,000 people'])

original_model_data_updated_subset[['population_density_downtown',

                                                                  'population_density_city',
                                                                  'housing_density_downtown',
                                                                  'housing_density_city',
                                                                  'employment_density_downtown']] = original_model_data_updated_subset[['population_density_downtown',

                                                                  'population_density_city',
                                                                  'housing_density_downtown',
                                                                  'housing_density_city',
                                                                  'employment_density_downtown']] / 1000000

original_model_data_updated_subset[['population_density_downtown',

                                                                  'population_density_city',
                                                                  'housing_density_downtown',
                                                                  'housing_density_city',
                                                                  'employment_density_downtown']]

original_model_data_updated_subset.to_csv('~/data/downtownrecovery/curated_data/model_features_20230707.csv')



# COMMAND ----------

full_model_data = census_data[census_data["city"]!="Hamilton"].replace({"Montréal":"Montreal","Québec":"Quebec"})
full_model_data = full_model_data.merge(weather_data[["City_","Climate_Name","Class","Type"]], how="left", left_on="city", right_on="City_").drop(columns=['City_'])
full_model_data = full_model_data.merge(political_data[["City_","%Liberal-Leaning","%Conservative-Leaning","%Other"]], how="left", left_on="city", right_on="City_").drop(columns=['City_'])
full_model_data = full_model_data.merge(oxford_data[["City_","Sum of C1_FULL", 'Sum of C2_FULL', 'Sum of C4_Large',
       'Sum of C4_FULL', 'Sum of C6_FULL', 'Sum of E1_FULL', 'Sum of H6_FULL']], how="left", left_on="city", right_on="City_").drop(columns=['City_'])

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
full_model_data = full_model_data.rename(columns={"Sum of C1_FULL":"days_school_closing",
                                         "Sum of C2_FULL":"days_workplace_closing",
                                         "Sum of C4_Large":"days_cancel_large_events",
                                         "Sum of C4_FULL":"days_cancel_all_events",
                                         "Sum of C6_FULL":"days_stay_home_requirements",
                                         "Sum of E1_FULL":"days_income_support",
                                         "Sum of H6_FULL":"days_mask_mandates",
                                         "Climate_Humid Continental Mild Summer, Wet All Year":"Climate_Humid Continental Mild Summer - Wet All Year"})

# COMMAND ----------

create_and_save_as_table(full_model_data,"_0812_all_model_features_")

# COMMAND ----------

display(full_model_data)

# COMMAND ----------

