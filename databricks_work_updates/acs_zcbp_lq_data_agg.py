# Databricks notebook source
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# COMMAND ----------

# old

all_cities_weekly_df = pd.read_csv('~/data/downtownrecovery/metrics/all_weekly_metrics_20230701.csv')
all_cities_weekly_lq = all_cities_weekly_df.to_numpy()

# COMMAND ----------

# old

city_df = all_cities_weekly_df.transpose()

# COMMAND ----------

#Import Downtown Zipcodes
us_city_index = pd.read_csv('~/data/downtownrecovery/geographies/city_index_0119.csv')

us_downtowns = pd.read_csv('~/data/downtownrecovery/geographies/us_downtowns.csv', header = None)
us_city_index = us_city_index[~us_city_index["0"].isin(["Mesa", "Long Beach", "Virginia Beach", "Arlington", "Aurora"])]
us_city_index["_c2"] = us_city_index['0'].map(dict(zip(us_downtowns[0], us_downtowns[2])))
us_city_index["_c2"] = us_city_index["_c2"].apply(lambda x: x[1:-1].split(','))
us_city_index = us_city_index.rename(columns={"_c2":"downtown_zipcodes"})
us_city_index["downtown_zipcodes"] = us_city_index["downtown_zipcodes"].apply(lambda x: [int(i) for i in x])
us_city_index["zipcodes"] = us_city_index["zipcodes"].apply(lambda x: x.strip("][").replace("'","").split(', '))
us_city_index["zipcodes"] = us_city_index["zipcodes"].apply(lambda x: [int(i) for i in x])


us_city_index

# COMMAND ----------

import json
import requests
import censusdata
acs_url = f'https://api.census.gov/data/2019/acs/acs5?get=NAME,B25024_001E,B25024_002E,B25024_003E,B25024_004E,B25024_005E,B25024_006E,B25024_007E,B25024_008E,B25024_009E,B25024_010E,B25024_011E,B25003_001E,B25003_003E,B01002_001E,B15003_001E,B15003_022E,B15003_023E,B15003_024E,B15003_025E,B19013_001E,B25064_001E,B25002_001E,B25002_002E,B25002_003E,B03002_001E,B03002_003E,B03002_004E,B03002_006E,B03002_012E,B08301_001E,B08301_002E,B08301_010E,B08301_016E,B08301_017E,B08301_018E,B08301_019E,B08301_020E,B08301_021E,B08135_001E,B08303_001E&for=zip%20code%20tabulation%20area:*'
acs_response_all = requests.get(acs_url)
acs_results_all = acs_response_all.text

if "Sorry" in acs_results_all:
    print(acs_results_all)

acs_all = pd.DataFrame.from_records(json.loads(acs_results_all))
header = acs_all.iloc[0] 
acs_all = acs_all.iloc[1:] 
acs_all.columns = header
subj_industry = censusdata.download('acs5',2020,censusdata.censusgeo([('zip code tabulation area', '*')]),['S2403_C01_001E','S2403_C01_002E','S2403_C01_005E','S2403_C01_006E','S2403_C01_007E','S2403_C01_008E','S2403_C01_009E','S2403_C01_012E','S2403_C01_013E','S2403_C01_016E','S2403_C01_020E','S2403_C01_023E','S2403_C01_026E','S2403_C01_027E'],tabletype='subject')
new_subj_industry_indices = [int(i.geo[0][1]) for i in subj_industry.index]
indexed_subj_industry = subj_industry.set_index(pd.Index(new_subj_industry_indices))
indexed_subj_industry['zip_code'] = indexed_subj_industry.index.astype(int)
acs_all['zip code tabulation area'] = acs_all['zip code tabulation area'].astype(int)
acs_all = acs_all.merge(indexed_subj_industry, how='left', left_on='zip code tabulation area', right_on='zip_code')
acs_all

# COMMAND ----------

#removing 0 values
acs_all = acs_all.apply(pd.to_numeric, errors='coerce')
#acs_all["B19013_001E"] = acs_all["B19013_001E"].where(acs_all["B19013_001E"].astype(float)>0)
#acs_all["B25064_001E"] = acs_all["B25064_001E"].where(acs_all["B25064_001E"].astype(float)>0)
#acs_all["B01002_001E"] = acs_all["B01002_001E"].where(acs_all["B01002_001E"].astype(float)>0)
#acs_all["B01002_001E"] = acs_all["B01002_001E"].where(acs_all["B01002_001E"].astype(float)<100)
#acs_all["B01002_001E"] = acs_all["B01002_001E"].astype(float)
#acs_all["B01002_001E"] = acs_all["B01002_001E"].fillna(0)
#acs_all["B19013_001E"] = acs_all["B19013_001E"].astype(float)
#acs_all["B19013_001E"] = acs_all["B19013_001E"].where(acs_all["B19013_001E"].astype(float)>0)
#acs_all["B19013_001E"] = acs_all["B19013_001E"].fillna(0)
#acs_all["B25064_001E"] = acs_all["B25064_001E"].where(acs_all["B25064_001E"].astype(float)>0)
#acs_all["B25064_001E"] = acs_all["B25064_001E"].astype(float)
#acs_all["B25064_001E"] = acs_all["B25064_001E"].fillna(0)
#acs_all["B08135_001E"] = acs_all["B08135_001E"].where(acs_all["B08135_001E"].astype(float)>0)
#acs_all["B08135_001E"] = acs_all["B08135_001E"].astype(float)
#acs_all["B08135_001E"] = acs_all["B08135_001E"].fillna(0)
#acs_all["B08303_001E"] = acs_all["B08303_001E"].where(acs_all["B08303_001E"].astype(float)>0)
#acs_all["B08303_001E"] = acs_all["B08303_001E"].astype(float)
#acs_all["B08303_001E"] = acs_all["B08303_001E"].fillna(0)
for i in ["B19013_001E","B25064_001E","B01002_001E","B08135_001E","B08303_001E","B08301_001E","B08301_002E","B08301_010E","B08301_016E","B08301_017E","B08301_018E","B08301_019E","B08301_020E","B08301_021E","B08135_001E","B08303_001E",'B03002_001E','B25064_001E']:
    acs_all[i] = acs_all[i].astype(float)
    acs_all[i] = acs_all[i].clip(lower=0)
    acs_all[i] = acs_all[i].fillna(0)

acs_all.B25064_001E = acs_all.B25064_001E.clip(upper=90)

# COMMAND ----------

##city_df_cut["dwtn_zipcodes"] = city_df_cut["index"].map(us_city_index.set_index("0")["downtown_zipcodes"].to_dict())

# COMMAND ----------

# MAGIC %md
# MAGIC ##March 17 was rerun from here##

# COMMAND ----------

us_city_index = us_city_index[~us_city_index["0"].isin(["Mesa", "Long Beach", "Virginia Beach", "Arlington", "Aurora"])]

# COMMAND ----------

def div_0(n,d):
    try:
        return n/d
    except:
        return 0

# COMMAND ----------

def div_int_0(n,d):
    try:
        return int(n)/int(d)
    except:
        return 0

# COMMAND ----------

np.sum(acs_dwtn_data.B03002_001E * acs_dwtn_data.B01002_001E)/ np.sum(acs_dwtn_data.B01002_001E)

# COMMAND ----------

def get_acs_data(row):
    acs_dwtn_data = acs_all[acs_all["zip_code"].isin([int(i) for i in row["downtown_zipcodes"]])]
    acs_dwtn_sum = acs_dwtn_data.sum(axis=0)
    acs_city_data = acs_all[acs_all["zip_code"].isin([int(i) for i in row["zipcodes"]])]
    acs_city_sum = acs_city_data.sum(axis=0)
    zero_age = len(acs_dwtn_data[acs_dwtn_data["B01002_001E"]==0])
    zero_income = len(acs_dwtn_data[acs_dwtn_data["B25064_001E"]==0])
    zero_rent = len(acs_dwtn_data[acs_dwtn_data["B25002_003E"]==0])
    median_age_downtown = np.sum(acs_dwtn_data.B01002_001E * acs_dwtn_data.B03002_001E)/ np.sum(acs_dwtn_data.B03002_001E)
    median_age_city = np.sum(acs_city_data.B01002_001E * acs_city_data.B03002_001E)/ np.sum(acs_city_data.B03002_001E)
    median_income_downtown = np.sum(acs_dwtn_data.B19013_001E * acs_dwtn_data.B03002_001E)/ np.sum(acs_dwtn_data.B03002_001E)
    median_income_city = np.sum(acs_city_data.B19013_001E * acs_city_data.B03002_001E)/ np.sum(acs_city_data.B03002_001E)
    #median_rent_downtown = np.sum(acs_dwtn_data.B25002_003E * acs_dwtn_data.B03002_001E)/ np.sum(acs_dwtn_data.B03002_001E)
    #median_rent_city = np.sum(acs_city_data.B25002_003E * acs_city_data.B03002_001E)/ np.sum(acs_city_data.B03002_001E)
    return pd.Series([float(acs_dwtn_sum.B03002_001E), #total_pop_downtown
                      float(acs_city_sum.B03002_001E), #total_pop_city
                    div_0((float(acs_dwtn_sum.B25024_002E) + float(acs_dwtn_sum.B25024_003E)), float(acs_dwtn_sum.B25024_001E)) *100, #pct_singlefam_downtown
                    div_0((float(acs_city_sum.B25024_002E) + float(acs_city_sum.B25024_003E)), float(acs_city_sum.B25024_001E)) *100, #pct_singlefam_city 
                    div_0((float(acs_dwtn_sum.B25024_004E) + float(acs_dwtn_sum.B25024_005E) + float(acs_dwtn_sum.B25024_006E) + float(acs_dwtn_sum.B25024_007E) 
                          + float(acs_dwtn_sum.B25024_008E) + float(acs_dwtn_sum.B25024_009E)),float(acs_dwtn_sum.B25024_001E)) *100, #pct_multifam_downtown
                    div_0((float(acs_city_sum.B25024_004E) + float(acs_city_sum.B25024_005E) + float(acs_city_sum.B25024_006E) + float(acs_city_sum.B25024_007E) 
                          + float(acs_city_sum.B25024_008E) + float(acs_city_sum.B25024_009E)),float(acs_city_sum.B25024_001E)) *100, #pct_multifam_city
                    div_0((float(acs_dwtn_sum.B25024_010E) + float(acs_dwtn_sum.B25024_011E)),float(acs_dwtn_sum.B25024_001E)) *100, #pct_mobile_home_and_others_downtown
                    div_0((float(acs_city_sum.B25024_010E) + float(acs_city_sum.B25024_011E)),float(acs_city_sum.B25024_001E)) *100, #pct_mobile_home_and_others_city 
                    div_0(float(acs_dwtn_sum.B25003_003E), float(acs_dwtn_sum.B25003_001E)) *100, #pct_renter_downtown
                    div_0(float(acs_city_sum.B25003_003E), float(acs_city_sum.B25003_001E)) *100, #pct_renter_city
                    median_age_downtown, #median_age_downtown
                    median_age_city, #median_age_downtown
                    div_0((float(acs_dwtn_sum.B15003_022E) + float(acs_dwtn_sum.B15003_023E) + int(acs_dwtn_sum.B15003_024E) + float(acs_dwtn_sum.B15003_025E)),float(acs_dwtn_sum.B15003_001E)) *100, #bachelor_plus_downtown
                    div_0((float(acs_city_sum.B15003_022E) + float(acs_city_sum.B15003_023E) + int(acs_city_sum.B15003_024E) + float(acs_city_sum.B15003_025E)),float(acs_city_sum.B15003_001E)) *100, #bachelor_plus_city
                    median_income_downtown, #median_hhinc_downtown
                    median_income_city, #median_hhinc_city
                    div_0(float(acs_dwtn_sum.B25002_003E), len(acs_dwtn_data) - zero_rent), #median_rent_downtown
                    div_0(float(acs_city_sum.B25002_003E), len(acs_city_data) - zero_rent), #median_rent_city
                    div_0(float(acs_dwtn_sum.B25002_003E),float(acs_dwtn_sum.B25002_001E)) *100, #pct_vacant_downtown
                    div_0(float(acs_city_sum.B25002_003E),float(acs_city_sum.B25002_001E)) *100, #pct_vacant_city
                    div_0(float(acs_dwtn_sum.B03002_003E),float(acs_dwtn_sum.B03002_001E)) *100, #pct_nhwhite_downtown
                    div_0(float(acs_city_sum.B03002_003E),float(acs_city_sum.B03002_001E)) *100, #pct_nhwhite_city
                    div_0(float(acs_dwtn_sum.B03002_004E),float(acs_dwtn_sum.B03002_001E)) *100, #pct_nhblack_downtown
                    div_0(float(acs_city_sum.B03002_004E),float(acs_city_sum.B03002_001E)) *100, #pct_nhblack_city
                    div_0(float(acs_dwtn_sum.B03002_006E),float(acs_dwtn_sum.B03002_001E)) *100, #pct_nhasian_downtown
                    div_0(float(acs_city_sum.B03002_006E),float(acs_city_sum.B03002_001E)) *100, #pct_nhasian_city
                    div_0(float(acs_dwtn_sum.B03002_012E),float(acs_dwtn_sum.B03002_001E)) *100, #pct_hispanic_downtown
                    div_0(float(acs_dwtn_sum.B03002_012E),float(acs_dwtn_sum.B03002_001E)) *100, #pct_hispanic_city
                    div_0(float(acs_dwtn_sum.B08301_002E), float(acs_dwtn_sum.B08301_001E)) *100, #pct_commute_auto_downtown
                    div_0(float(acs_city_sum.B08301_002E), float(acs_city_sum.B08301_001E)) *100, #pct_commute_auto_city
                    div_0(float(acs_dwtn_sum.B08301_010E), float(acs_dwtn_sum.B08301_001E)) *100, #pct_commute_public_transit_downtown
                    div_0(float(acs_city_sum.B08301_010E), float(acs_city_sum.B08301_001E)) *100, #pct_commute_public_transit_city
                    div_0(float(acs_dwtn_sum.B08301_018E), float(acs_dwtn_sum.B08301_001E)) *100, #pct_commute_bicycle_downtown
                    div_0(float(acs_city_sum.B08301_018E), float(acs_city_sum.B08301_001E)) *100, #pct_commute_bicycle_city
                    div_0(float(acs_dwtn_sum.B08301_019E), float(acs_dwtn_sum.B08301_001E)) *100, #pct_commute_walk_downtown
                    div_0(float(acs_city_sum.B08301_019E), float(acs_city_sum.B08301_001E)) *100, #pct_commute_walk_city
                    div_0(float(acs_dwtn_sum.B08301_020E) + float(acs_dwtn_sum.B08301_017E) + float(acs_dwtn_sum.B08301_016E), float(acs_dwtn_sum.B08301_001E)) *100, #pct_commute_others_downtown
                    div_0(float(acs_city_sum.B08301_020E) + float(acs_city_sum.B08301_017E) + float(acs_city_sum.B08301_016E), float(acs_city_sum.B08301_001E)) *100, #pct_commute_city_downtown
                    float(acs_dwtn_sum.B25024_001E), #housing_units_downtown
                    float(acs_city_sum.B25024_001E), #housing_units_city
                    div_0(float(acs_dwtn_sum.B08135_001E), float(acs_dwtn_sum.B08303_001E)), #average_commute_time_downtown
                    div_0(float(acs_city_sum.B08135_001E), float(acs_city_sum.B08303_001E)) #average_commute_time_city
                     ])

# COMMAND ----------

us_city_index[['total_pop_downtown',
               'total_pop_city',
               'pct_singlefam_downtown',
               'pct_singlefam_city',
               'pct_multifam_downtown',
               'pct_multifam_city',
               'pct_mobile_home_and_others_downtown',
               'pct_mobile_home_and_others_city',
               'pct_renter_downtown',
               'pct_renter_city',
               'median_age_downtown',
               'median_age_city',
               'bachelor_plus_downtown',
               'bachelor_plus_city',
               'median_hhinc_downtown',
               'median_hhinc_city',
               'median_rent_downtown',
               'median_rent_city',
               'pct_vacant_downtown',
               'pct_vacant_city',
               'pct_nhwhite_downtown',
               'pct_nhwhite_city',
               'pct_nhblack_downtown',
               'pct_nhblack_city',
               'pct_nhasian_downtown',
               'pct_nhasian_city',
               'pct_hisp_downtown',
               'pct_hisp_city',
               'pct_commute_auto_downtown',
               'pct_commute_auto_city',
               'pct_commute_public_transit_downtown',
               'pct_commute_public_transit_city',
               'pct_commute_bicycle_downtown',
               'pct_commute_bicycle_city',
               'pct_commute_walk_downtown',
               'pct_commute_walk_city',
               'pct_commute_others_downtown',
               'pct_commute_others_city',
               'housing_units_downtown',
               'housing_units_city',
               'average_commute_time_downtown',
               'average_commute_time_city']] = us_city_index.apply(get_acs_data, axis=1)

# COMMAND ----------

us_city_index['pct_others_downtown'] = 100 - us_city_index['pct_hisp_downtown'] - us_city_index['pct_nhasian_downtown'] - us_city_index['pct_nhblack_downtown'] - us_city_index['pct_nhwhite_downtown']
us_city_index['pct_others_city'] = 100 - us_city_index['pct_hisp_city'] - us_city_index['pct_nhasian_city'] - us_city_index['pct_nhblack_city'] - us_city_index['pct_nhwhite_city']
us_city_index['pct_commute_others_downtown'] = 100 - us_city_index['pct_commute_auto_downtown'] - us_city_index['pct_commute_public_transit_downtown'] - us_city_index['pct_commute_bicycle_downtown'] - us_city_index['pct_commute_walk_downtown']
us_city_index['pct_commute_others_city'] = 100 - us_city_index['pct_commute_auto_city'] - us_city_index['pct_commute_public_transit_city'] - us_city_index['pct_commute_bicycle_city'] - us_city_index['pct_commute_walk_city']
us_city_index.head()

# COMMAND ----------

us_city_index[['0','median_rent_downtown','median_rent_city']]

# COMMAND ----------

us_city_index = us_city_index.rename(columns={"0":"city"})

# COMMAND ----------

# MAGIC %md ### Addition of LODES Data 5/2/22 ###

# COMMAND ----------

lodes = pd.read_csv('~/data/downtownrecovery/census_data/downtown_cbg_lodes.csv')
lodes = lodes.loc[:,["C000","CNS01","CNS02","CNS03","CNS04","CNS05","CNS06","CNS07","CNS08","CNS09","CNS10","CNS11","CNS12","CNS13",
                     "CNS14","CNS15","CNS16","CNS17","CNS18","CNS19","CNS20","city.y"]]
lodes.head()
lodes.dtypes
for i in range(len(lodes.columns)-1):
    lodes.iloc[:,i] = lodes.iloc[:,i].fillna(0)
    lodes.iloc[:,i] = lodes.iloc[:,i].astype(int)

def get_lodes_data(city):
    city_lodes = lodes[lodes["city.y"]==city]
    city_lodes_sum = city_lodes.sum(axis=0)
    return pd.Series([int(city_lodes_sum["C000"]),
                      div_int_0(city_lodes_sum["CNS01"],city_lodes_sum["C000"])*100,
                      div_int_0(city_lodes_sum["CNS02"],city_lodes_sum["C000"])*100,
                      div_int_0(city_lodes_sum["CNS03"],city_lodes_sum["C000"])*100,
                      div_int_0(city_lodes_sum["CNS04"],city_lodes_sum["C000"])*100,
                      div_int_0(city_lodes_sum["CNS05"],city_lodes_sum["C000"])*100,
                      div_int_0(city_lodes_sum["CNS06"],city_lodes_sum["C000"])*100,
                      div_int_0(city_lodes_sum["CNS07"],city_lodes_sum["C000"])*100,
                      div_int_0(city_lodes_sum["CNS08"],city_lodes_sum["C000"])*100,
                      div_int_0(city_lodes_sum["CNS09"],city_lodes_sum["C000"])*100,
                      div_int_0(city_lodes_sum["CNS10"],city_lodes_sum["C000"])*100,
                      div_int_0(city_lodes_sum["CNS11"],city_lodes_sum["C000"])*100,
                      div_int_0(city_lodes_sum["CNS12"],city_lodes_sum["C000"])*100,
                      div_int_0(city_lodes_sum["CNS13"],city_lodes_sum["C000"])*100,
                      div_int_0(city_lodes_sum["CNS14"],city_lodes_sum["C000"])*100,
                      div_int_0(city_lodes_sum["CNS15"],city_lodes_sum["C000"])*100,
                      div_int_0(city_lodes_sum["CNS16"],city_lodes_sum["C000"])*100,
                      div_int_0(city_lodes_sum["CNS17"],city_lodes_sum["C000"])*100,
                      div_int_0(city_lodes_sum["CNS18"],city_lodes_sum["C000"])*100,
                      div_int_0(city_lodes_sum["CNS19"],city_lodes_sum["C000"])*100,
                      div_int_0(city_lodes_sum["CNS20"],city_lodes_sum["C000"])*100])
        

#    city_lodes_stats["pct_jobs_agriculture_forestry_fishing_hunting"] = div_0(city_lodes_sum["CNS01"],city_lodes_sum["C000"])
#    city_lodes_stats["pct_jobs_mining_quarrying_oil_gas"] = div_0(city_lodes_sum["CNS02"],city_lodes_sum["C000"])
#    city_lodes_stats["pct_jobs_utilities"] = div_0(city_lodes_sum["CNS03"],city_lodes_sum["C000"])
#    city_lodes_stats["pct_jobs_construction"] = div_0(city_lodes_sum["CNS04"],city_lodes_sum["C000"])
#    city_lodes_stats["pct_jobs_manufacturing"] = div_0(city_lodes_sum["CNS05"],city_lodes_sum["C000"])
#    city_lodes_stats["pct_jobs_wholesale_trade"] = div_0(city_lodes_sum["CNS06"],city_lodes_sum["C000"])
#    city_lodes_stats["pct_jobs_retail_trade"] = div_0(city_lodes_sum["CNS07"],city_lodes_sum["C000"])
#    city_lodes_stats["pct_jobs_transport_warehouse"] = div_0(city_lodes_sum["CNS08"],city_lodes_sum["C000"])
#    city_lodes_stats["pct_jobs_information"] = div_0(city_lodes_sum["CNS09"],city_lodes_sum["C000"])
#    city_lodes_stats["pct_jobs_finance_insurance"] = div_0(city_lodes_sum["CNS10"],city_lodes_sum["C000"])
#    city_lodes_stats["pct_jobs_real_estate"] = div_0(city_lodes_sum["CNS11"],city_lodes_sum["C000"])
#    city_lodes_stats["pct_jobs_professional_science_techical"] = div_0(city_lodes_sum["CNS12"],city_lodes_sum["C000"])
#    city_lodes_stats["pct_jobs_management_of_companies_enterprises"] = div_0(city_lodes_sum["CNS13"],city_lodes_sum["C000"])
#    city_lodes_stats["pct_jobs_administrative_support_waste"] = div_0(city_lodes_sum["CNS14"],city_lodes_sum["C000"])
#    city_lodes_stats["pct_jobs_educational_services"] = div_0(city_lodes_sum["CNS15"],city_lodes_sum["C000"])
#    city_lodes_stats["pct_jobs_healthcare_social_assistance"] = div_0(city_lodes_sum["CNS16"],city_lodes_sum["C000"])
#    city_lodes_stats["pct_jobs_arts_entertainment_recreation"] = div_0(city_lodes_sum["CNS17"],city_lodes_sum["C000"])
#    city_lodes_stats["pct_jobs_accomodation_food_services"] = div_0(city_lodes_sum["CNS18"],city_lodes_sum["C000"])
#    city_lodes_stats["pct_jobs_other"] = div_0(city_lodes_sum["CNS19"],city_lodes_sum["C000"])
#    city_lodes_stats["pct_jobs_public_administration"] = div_0(city_lodes_sum["CNS20"],city_lodes_sum["C000"])
#    return city_lodes_stats

us_city_index[["total_jobs",
              "pct_jobs_agriculture_forestry_fishing_hunting",
              "pct_jobs_mining_quarrying_oil_gas",
              "pct_jobs_utilities",
              "pct_jobs_construction",
              "pct_jobs_manufacturing",
              "pct_jobs_wholesale_trade",
              "pct_jobs_retail_trade",
              "pct_jobs_transport_warehouse",
              "pct_jobs_information",
              "pct_jobs_finance_insurance",
              "pct_jobs_real_estate",
              "pct_jobs_professional_science_techical",
              "pct_jobs_management_of_companies_enterprises",
              "pct_jobs_administrative_support_waste",
              "pct_jobs_educational_services",
              "pct_jobs_healthcare_social_assistance",
              "pct_jobs_arts_entertainment_recreation",
              "pct_jobs_accomodation_food_services",
              "pct_jobs_other",
              "pct_jobs_public_administration"]] = us_city_index["city"].apply(get_lodes_data)

def calculate_entropy(row):
    entropy = 0
    for i in ["pct_jobs_agriculture_forestry_fishing_hunting",
              "pct_jobs_mining_quarrying_oil_gas",
              "pct_jobs_utilities",
              "pct_jobs_construction",
              "pct_jobs_manufacturing",
              "pct_jobs_wholesale_trade",
              "pct_jobs_retail_trade",
              "pct_jobs_transport_warehouse",
              "pct_jobs_information",
              "pct_jobs_finance_insurance",
              "pct_jobs_real_estate",
              "pct_jobs_professional_science_techical",
              "pct_jobs_management_of_companies_enterprises",
              "pct_jobs_administrative_support_waste",
              "pct_jobs_educational_services",
              "pct_jobs_healthcare_social_assistance",
              "pct_jobs_arts_entertainment_recreation",
              "pct_jobs_accomodation_food_services",
              "pct_jobs_other",
              "pct_jobs_public_administration"]:
        if row[i]>0:
            entropy = entropy + ((((row[i]/100)*row["total_jobs"])/row["total_jobs"])*np.log(((row[i]/100)*row["total_jobs"])/row["total_jobs"]))
        else:
            entropy = entropy
    return -entropy

us_city_index["employment_entropy"] = us_city_index.apply(lambda x: calculate_entropy(x), axis=1)

us_zcta_geo = pd.read_csv('~/data/downtownrecovery/census_data/us_zcta_geo.csv')
# us_zcta_geo and us_zcta_geo-1 are identical
us_zcta_geo.shape
us_zcta_geo.head()

us_zcta_geo_1 = pd.read_csv('~/data/downtownrecovery/census_data/us_zcta_geo-1.csv')
us_zcta_geo_1.shape
us_zcta_geo_1.head()
us_zcta_geo.equals(us_zcta_geo_1)

us_zcta_geo["GEOID10"] = us_zcta_geo["GEOID10"].astype(int)
# ALAND10 is in meters squared
us_zcta_geo["ALAND10"] = us_zcta_geo["ALAND10"].astype(int)
# to convert m2 -> km2; there are 1000 m in 1km. 1000 * 1000 = 1000000
us_zcta_geo["ALAND10km2"] = .000001 * us_zcta_geo["ALAND10"] 
us_zcta_geo["ALAND10km2"] = us_zcta_geo["ALAND10km2"]

def get_land_area_m2(zipcodes):
    us_zcta_geo_cut = us_zcta_geo[us_zcta_geo["GEOID10"].isin([int(i) for i in zipcodes])]
    return np.sum(us_zcta_geo_cut["ALAND10"])

def get_land_area_km2(zipcodes):
    us_zcta_geo_cut = us_zcta_geo[us_zcta_geo["GEOID10"].isin([int(i) for i in zipcodes])]
    return np.sum(us_zcta_geo_cut["ALAND10km2"])
    
us_city_index["land_area_downtown_m2"] = us_city_index["downtown_zipcodes"].apply(lambda x: get_land_area_m2(x))
us_city_index["land_area_city_m2"] = us_city_index["zipcodes"].apply(lambda x: get_land_area_m2(x))

us_city_index["land_area_downtown_km2"] = us_city_index["downtown_zipcodes"].apply(lambda x: get_land_area_km2(x))
us_city_index["land_area_city_km2"] = us_city_index["zipcodes"].apply(lambda x: get_land_area_km2(x))

us_city_index["population_density_downtown_m2"] = us_city_index["total_pop_downtown"]/us_city_index["land_area_downtown_m2"]
us_city_index["population_density_city_m2"] = us_city_index["total_pop_city"]/us_city_index["land_area_city_m2"]
us_city_index["employment_density_downtown_m2"] = us_city_index["total_jobs"]/us_city_index["land_area_downtown_m2"]
us_city_index["housing_density_downtown_m2"] = us_city_index["housing_units_downtown"]/us_city_index["land_area_downtown_m2"]
us_city_index["housing_density_city_m2"] = us_city_index["housing_units_city"]/us_city_index["land_area_city_m2"]

us_city_index["population_density_downtown_km2"] = us_city_index["total_pop_downtown"]/us_city_index["land_area_downtown_km2"]
us_city_index["population_density_city_km2"] = us_city_index["total_pop_city"]/us_city_index["land_area_city_km2"]
us_city_index["employment_density_downtown_km2"] = us_city_index["total_jobs"]/us_city_index["land_area_downtown_km2"]
us_city_index["housing_density_downtown_km2"] = us_city_index["housing_units_downtown"]/us_city_index["land_area_downtown_km2"]
us_city_index["housing_density_city_km2"] = us_city_index["housing_units_city"]/us_city_index["land_area_city_km2"]

us_city_index.head()

# COMMAND ----------

us_city_index_subset = us_city_index.drop(columns = [["1", "2", "3", "geo", "zipcodes", "downtown_zipcodes"]])

us_city_index = us_city_index.drop(columns=["1","2","3","geo","zipcodes","downtown_zipcodes"])

us_city_index.head()

us_city_index.to_csv('~/data/downtownrecovery/us_city_index_20230702.csv')

create_and_save_as_table(us_city_index, "us_city_index_0626")

# COMMAND ----------

us_rfr_factors_0626 = us_city_index[['city',
               'total_pop_downtown',
               'total_pop_city',
               'pct_singlefam_downtown',
               'pct_singlefam_city',
               'pct_multifam_downtown',
               'pct_multifam_city',
               'pct_mobile_home_and_others_downtown',
               'pct_mobile_home_and_others_city',
               'pct_renter_downtown',
               'pct_renter_city',
               'median_age_downtown',
               'median_age_city',
               'bachelor_plus_downtown',
               'bachelor_plus_city',
               'median_hhinc_downtown',
               'median_hhinc_city',
               'median_rent_downtown',
               'median_rent_city',
               'pct_vacant_downtown',
               'pct_vacant_city',
               'pct_nhwhite_downtown',
               'pct_nhwhite_city',
               'pct_nhblack_downtown',
               'pct_nhblack_city',
               'pct_nhasian_downtown',
               'pct_nhasian_city',
               'pct_hisp_downtown',
               'pct_hisp_city',
               'pct_commute_auto_downtown',
               'pct_commute_auto_city',
               'pct_commute_public_transit_downtown',
               'pct_commute_public_transit_city',
               'pct_commute_bicycle_downtown',
               'pct_commute_bicycle_city',
               'pct_commute_walk_downtown',
               'pct_commute_walk_city',
               'pct_commute_others_downtown',
               'pct_commute_others_city',
               'housing_units_downtown',
               'housing_units_city',
               'average_commute_time_downtown',
               'average_commute_time_city',
               "pct_jobs_agriculture_forestry_fishing_hunting",
               "pct_jobs_mining_quarrying_oil_gas",
               "pct_jobs_utilities",
               "pct_jobs_construction",
               "pct_jobs_manufacturing",
               "pct_jobs_wholesale_trade",
               "pct_jobs_retail_trade",
               "pct_jobs_transport_warehouse",
               "pct_jobs_information",
               "pct_jobs_finance_insurance",
               "pct_jobs_real_estate",
               "pct_jobs_professional_science_techical",
               "pct_jobs_management_of_companies_enterprises",
               "pct_jobs_administrative_support_waste",
               "pct_jobs_educational_services",
               "pct_jobs_healthcare_social_assistance",
               "pct_jobs_arts_entertainment_recreation",
               "pct_jobs_accomodation_food_services",
               "pct_jobs_other",
               "pct_jobs_public_administration",
               "employment_entropy",
               "population_density_downtown",
               "population_density_city",
               "employment_density_downtown",
               "housing_density_downtown",
               "housing_density_city"]]

create_and_save_as_table(us_rfr_factors_0626, "us_rfr_features_0626")

# COMMAND ----------

us_rfr_factors_0626

# COMMAND ----------

row = us_city_index.iloc[23]
zcbp_city = get_table_as_pandas_df('zcbp_'+row["city"]+'_0119_v1')
zcbp_city = zcbp_city.apply(pd.to_numeric, errors='coerce')
zcbp_dwtn_data = zcbp_city[zcbp_city["zipcode"].isin([int(i) for i in row["downtown_zipcodes"]])]
zcbp_dwtn_sum = zcbp_dwtn_data.sum(axis=0)
if "Utilities" not in zcbp_dwtn_data.sum().index.to_list():
    zcbp_dwtn_sum["Utilities"] = 0
if "Public Administration" not in zcbp_dwtn_data.sum().index.to_list():
    zcbp_dwtn_sum["Public Administration"] = 0
if "Agriculture Forestry Fishing and Hunting" not in zcbp_dwtn_data.sum().index.to_list():
    zcbp_dwtn_sum["Agriculture Forestry Fishing and Hunting"] = 0
if "Mining Quarrying and Oil and Gas Extraction" not in zcbp_dwtn_data.sum().index.to_list():
    zcbp_dwtn_sum["Mining Quarrying and Oil and Gas Extraction"] = 0
total_biz = np.sum(zcbp_dwtn_sum[["Accommodation and Food Services", "Administrative and Support and Waste Management and Remediation Services",
                                     "Agriculture Forestry Fishing and Hunting", "Arts Entertainment and Recreation", "Construction",
                                     "Educational Services", "Finance and Insurance", "Health Care and Social Assistance", "Information",
                                     "Management of Companies and Enterprises", "Manufacturing", "Management of Companies and Enterprises",
                                     "Manufacturing", "Mining Quarrying and Oil and Gas Extraction", "Other Services except Public Administration",
                                     "Professional Scientific and Technical Services", "Public Administration", "Real Estate and Rental and Leasing",
                                     "Retail Trade", "Transportation and Warehousing", "Utilities", "Wholesale Trade"]])
print(pd.Series(zcbp_dwtn_sum["ALAND10"]))

# COMMAND ----------

def get_zcbp_data(row):
    zcbp_city = get_table_as_pandas_df('zcbp_'+str(row["city"]).replace(" ","").lower()+'_0119_v1')
    zcbp_city = zcbp_city.apply(pd.to_numeric, errors='coerce')
    zcbp_dwtn_data = zcbp_city[zcbp_city["zipcode"].isin([int(i) for i in row["downtown_zipcodes"]])]
    zcbp_dwtn_sum = zcbp_dwtn_data.sum(axis=0)
    zcbp_city_data = zcbp_city[zcbp_city["zipcode"].isin([int(i) for i in row["zipcodes"]])]

    if "Utilities" not in zcbp_dwtn_data.sum().index.to_list():
        zcbp_dwtn_sum["Utilities"] = 0
    if "Public Administration" not in zcbp_dwtn_data.sum().index.to_list():
        zcbp_dwtn_sum["Public Administration"] = 0
    if "Agriculture Forestry Fishing and Hunting" not in zcbp_dwtn_data.sum().index.to_list():
        zcbp_dwtn_sum["Agriculture Forestry Fishing and Hunting"] = 0
    if "Mining Quarrying and Oil and Gas Extraction" not in zcbp_dwtn_data.sum().index.to_list():
        zcbp_dwtn_sum["Mining Quarrying and Oil and Gas Extraction"] = 0
    total_biz = np.sum(zcbp_dwtn_sum[["Accommodation and Food Services", "Administrative and Support and Waste Management and Remediation Services",
                                     "Agriculture Forestry Fishing and Hunting", "Arts Entertainment and Recreation", "Construction",
                                     "Educational Services", "Finance and Insurance", "Health Care and Social Assistance", "Information",
                                     "Management of Companies and Enterprises", "Manufacturing", "Management of Companies and Enterprises",
                                     "Manufacturing", "Mining Quarrying and Oil and Gas Extraction", "Other Services except Public Administration",
                                     "Professional Scientific and Technical Services", "Public Administration", "Real Estate and Rental and Leasing",
                                     "Retail Trade", "Transportation and Warehousing", "Utilities", "Wholesale Trade"]])
    return pd.Series([div_0(zcbp_dwtn_sum["Accommodation and Food Services"]*100,total_biz),
                      div_0(zcbp_dwtn_sum["Administrative and Support and Waste Management and Remediation Services"]*100,total_biz),
                      div_0(zcbp_dwtn_sum["Agriculture Forestry Fishing and Hunting"]*100,total_biz),
                      div_0(zcbp_dwtn_sum["Arts Entertainment and Recreation"]*100,total_biz),
                      div_0(zcbp_dwtn_sum["Construction"]*100,total_biz),
                      div_0(zcbp_dwtn_sum["Educational Services"]*100,total_biz),
                      div_0(zcbp_dwtn_sum["Finance and Insurance"]*100,total_biz),
                      div_0(zcbp_dwtn_sum["Health Care and Social Assistance"]*100,total_biz),
                      div_0(zcbp_dwtn_sum["Information"]*100,total_biz),
                      div_0(zcbp_dwtn_sum["Management of Companies and Enterprises"]*100,total_biz),
                      div_0(zcbp_dwtn_sum["Manufacturing"]*100,total_biz),
                      div_0(zcbp_dwtn_sum["Mining Quarrying and Oil and Gas Extraction"]*100,total_biz),
                      div_0(zcbp_dwtn_sum["Other Services except Public Administration"]*100,total_biz),
                      div_0(zcbp_dwtn_sum["Professional Scientific and Technical Services"]*100,total_biz),
                      div_0(zcbp_dwtn_sum["Public Administration"]*100,total_biz),
                      div_0(zcbp_dwtn_sum["Real Estate and Rental and Leasing"]*100,total_biz),
                      div_0(zcbp_dwtn_sum["Retail Trade"]*100,total_biz),
                      div_0(zcbp_dwtn_sum["Transportation and Warehousing"]*100,total_biz),
                      div_0(zcbp_dwtn_sum["Utilities"]*100,total_biz),
                      div_0(zcbp_dwtn_sum["Wholesale Trade"]*100,total_biz),
                      zcbp_dwtn_sum["ALAND10"],
                      total_biz])

# COMMAND ----------

us_city_index[['pct_biz_accom_food',
             'pct_biz_admin_waste',
             'pct_biz_agriculture',
             'pct_biz_arts_entertainment',
             'pct_biz_construction',
             'pct_biz_educational_svcs',
             'pct_biz_finance_insurance',
             'pct_biz_healthcare_social_assistance',
             'pct_biz_information',
             'pct_biz_mgmt_companies_enterprises',
             'pct_biz_manufacturing',
             'pct_biz_mining_gas',
             'pct_biz_other_except_pub_adm',
             'pct_biz_professional_sci_tech',
             'pct_biz_public_adm',
             'pct_biz_real_estate_leasing',
             'pct_biz_retail_trade',
             'pct_biz_transportation_warehousing',
             'pct_biz_utilities',
             'pct_biz_wholesale_trade',
             'land_area',
             'total_biz']] = us_city_index[["city","downtown_zipcodes"]].apply(lambda x: get_zcbp_data(x), axis=1)

# COMMAND ----------

display(us_city_index)

# COMMAND ----------

us_city_index.iloc[:,8:]

# COMMAND ----------

def calculate_entropy(row):
    entropy = 0
    housing = row['housing_units']
    biz = row['total_biz']
    for i in ['pct_biz_accom_food',
              'pct_biz_admin_waste',
              'pct_biz_arts_entertainment',
              'pct_biz_construction',
              'pct_biz_educational_svcs',
              'pct_biz_finance_insurance',
              'pct_biz_healthcare_social_assistance',
              'pct_biz_information',
              'pct_biz_mgmt_companies_enterprises',
              'pct_biz_manufacturing',
              'pct_biz_other_except_pub_adm',
              'pct_biz_professional_sci_tech',
              'pct_biz_public_adm',
              'pct_biz_retail_trade',
              'pct_biz_transportation_warehousing',
              'pct_biz_utilities',
              'pct_biz_wholesale_trade',
              'pct_biz_agriculture',
              'pct_biz_mining_gas',
              'pct_biz_real_estate_leasing']:
        try:
            entropy = entropy + (((row[i]/100*biz))/(biz+housing))*np.log(1/(((row[i]/100*biz))/(biz+housing)))
        except:
            entropy = entropy
    return entropy + ((housing/(housing+biz))*np.log(1/(housing/(housing+biz))))

# COMMAND ----------

us_city_index["population_density"] = us_city_index["total_pop"]/us_city_index["land_area"]
us_city_index["business_density"] = us_city_index["total_biz"]/us_city_index["land_area"]
us_city_index["housing_density"] = us_city_index["housing_units"]/us_city_index["land_area"]
us_city_index["land use entropy"] = us_city_index.apply(lambda x: calculate_entropy(x), axis=1)

# COMMAND ----------

us_city_index["country"] = [1]*len(us_city_index)

# COMMAND ----------

us_city_index

# COMMAND ----------

city_df_cut_rfr = us_city_index[['city',
 'pct_singlefam',
 'pct_renter',
 'pct_employment_natresource',
 'pct_employment_construction',
 'pct_employment_manufacturing',
 'pct_employment_wholesale',
 'pct_employment_retail',
 'pct_employment_transpowarehousingutil',
 'pct_employment_info',
 'pct_employment_financeinsre',
 'pct_employment_profscimgmtadminwaste',
 'pct_employment_eduhealthsocialassist',
 'pct_employment_artentrecaccommfood',
 'pct_employment_other',
 'pct_employment_pubadmin',
 'median_age',
 'bachelor_plus',
 'median_hhinc',
 'median_rent',
 'pct_vacant',
 'pct_nhwhite',
 'pct_nhblack',
 'pct_nhasian',
 'pct_hisp',
 'pct_biz_accom_food',
 'pct_biz_admin_waste',
 'pct_biz_arts_entertainment',
 'pct_biz_construction',
 'pct_biz_educational_svcs',
 'pct_biz_finance_insurance',
 'pct_biz_healthcare_social_assistance',
 'pct_biz_information',
 'pct_biz_mgmt_companies_enterprises',
 'pct_biz_manufacturing',
 'pct_biz_other_except_pub_adm',
 'pct_biz_professional_sci_tech',
 'pct_biz_public_adm',
 'pct_biz_retail_trade',
 'pct_biz_transportation_warehousing',
 'pct_biz_utilities',
 'pct_biz_wholesale_trade',
 'pct_biz_agriculture',
 'pct_biz_mining_gas',
 'pct_biz_real_estate_leasing',
 'population_density',
 'business_density',
 'housing_density',
 'land use entropy',
 'country']]

# COMMAND ----------

city_df_cut_rfr

# COMMAND ----------

create_and_save_as_table(city_df_cut_rfr, "0321_us_rfr_factors_3")

# COMMAND ----------

zcbp_ny = get_table_as_pandas_df('zcbp_'+str("sandiego").replace(" ","").lower()+'_0119_v1')
zcbp_ny["zipcode"] = zcbp_ny["zipcode"].astype(int)
zcbp_ny = zcbp_ny.apply(pd.to_numeric, errors='coerce')
#type(zcbp_ny.iloc[15]["Utilities"])
zcbp_ny
zcbp_dwtn_data = zcbp_ny[zcbp_ny["zipcode"].isin([int(i) for i in [92101]])]
sumtest = zcbp_dwtn_data.sum()
sumtest["a"] = 3
sumtest
#"Utilities" in zcbp_dwtn_data.sum().index.to_list()
#zcbp_dwtn_data

# COMMAND ----------

city_df_cut[["city","dwtn_zipcodes"]].iloc[:,1]

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

## Load ACS and ZCBP Data for all zipcodes in the US ##
# Table of business counts by industry per zipcode for a city
city_zcbp_df = get_table_as_pandas_df('zcbp_albuquerque_0119_v1') 
city_zcbp_df['zipcode'] = zcbp_df['zipcode'].astype(str)

# Table of all acs data
acs_df = get_table_as_pandas_df('acs_data_for_all_zipcodes')
acs_df = acs_df.rename(columns={'zip_code':'zipcode'})
acs_df['zipcode'] = acs_df['zipcode'].astype(str)
# Filter ACS zipcodes in the city_zcbp_df, may want to change to list of zipcodes only in downtown of the city
acs_df = acs_df[acs_df['zipcode'].isin(city_zcbp_df['zipcode'].tolist())]

city_zcbp_df.isna().sum()
# acs_df.head()

# COMMAND ----------

## Data Cleaning + Add necessary columns
zcbp_composite_metrics_df = pd.merge(city_zcbp_df,acs_df[['zipcode','housingunits']],on='zipcode', how='left')
zcbp_composite_metrics_df = zcbp_composite_metrics_df.set_index('zipcode')
zcbp_composite_metrics_df['Total Business Count'] = zcbp_composite_metrics_df.iloc[:,0:19].sum(axis=1)
zcbp_composite_metrics_df['Total Business and Residential Count'] = zcbp_composite_metrics_df['Total Business Count'] + zcbp_composite_metrics_df['housingunits'] 

# Reorder total business and residential count column to front
first_column = zcbp_composite_metrics_df.pop('Total Business and Residential Count')
zcbp_composite_metrics_df.insert(0, 'Total Business and Residential Count', first_column)
zcbp_composite_metrics_df.head()

# COMMAND ----------

## Generate Composite Metrics ##

# Calculate business entropy
def calc_percent_industry_in_zipcode(industry_count):
    return industry_count / total_biz_res_count

def calc_industry_entropy_product(percent_industry_in_zipcode):
    return percent_industry_in_zipcode * np.log(1/percent_industry_in_zipcode)

def set_percent_industry_row(row):
    return row.apply(lambda col:calc_percentage_industry_in_zipcode(col,total_biz_res_count), axis=0)

zcbp_industry_counts = zcbp_composite_metrics_df.iloc[:,0:19]
business_entropy = zcbp_industry_counts.apply()

## NOTES ##
# for industry_index in np.arange(0,19):
#     percent_industry_in_zipcode = zcbp_composite_metrics_df.iloc[:,industry_index]/zcbp_composite_metrics_df['Total Business and Residential Count']
#     industry_entropy_product = industry_mix_flat_whole_numbers["e_pct_"+str(i)] * np.log(1/industry_mix_flat_whole_numbers["e_pct_"+str(i)])
# industry_mix_flat_whole_numbers

## CODE TO BE USED, DON'T TOUCH ## 
# # Business Density = # of total business count / zipcode land area
# zcbp_composite_metrics_df['Business Density'] = zcbp_composite_metrics_df['Total Business Count'] / pd.to_numeric(zcbp_composite_metrics_df['ALAND10'])
# # Convert each industry business count to percentage of total business count
# for industry in zcbp_composite_metrics_df.columns[0:19]:
#     zcbp_composite_metrics_df[industry]=(zcbp_composite_metrics_df[industry]/zcbp_composite_metrics_df["Total Business Count"])*100
# zcbp_composite_metrics_df.head()

# COMMAND ----------

display(get_table_as_pandas_df("city_lqs_with_attributes_0322"))


# COMMAND ----------

