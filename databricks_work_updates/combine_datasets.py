# Databricks notebook source
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# COMMAND ----------
original_model_data = pd.read_csv('~/data/downtownrecovery/curated_data/one_week_metrics_cuebiq_update_hll_region_health__crime_april2023.csv')

us_densities = pd.read_csv('~/data/downtownrecovery/curated_data/fixed_us_density.csv')
# the densities are in km2
can_city_index_with_features = pd.read_csv('~/data/downtownrecovery/curated_data/can_city_index_with_features_20230705.csv')
weather_data =  pd.read_csv('~/data/downtownrecovery/model_input_datasets/weather_0812_3.csv')
political_data =  pd.read_csv('~/data/downtownrecovery/model_input_datasets/political_leaning_0812_1.csv')
oxford_data =  pd.read_csv('~/data/downtownrecovery/model_input_datasets/oxford_variables_0812_4.csv')

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

original_model_data_updated[['city',
                              #'population_density_downtown', 'population_density_downtown_km2',
                              'population_density_city',  'population_density_city_km2',
                              #'housing_density_downtown','housing_density_downtown_km2',
                              #'housing_density_city', 'housing_density_city_km2'
                              #'employment_density_downtown', 'employment_density_downtown_km2'
                              ]]




original_model_data_updated.to_csv('~/data/downtownrecovery/curated_data/model_data_20230705.csv')



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

