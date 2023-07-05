# Databricks notebook source
city_index = sqlContext.sql("select * from city_index_0119_csv tables").toPandas()
csa_index = sqlContext.sql("select * from csa_index_0119_csv tables").toPandas()
zcta_geo = sqlContext.sql("select * from us_zcta_geo_csv tables").toPandas()

# COMMAND ----------

!pip install censusdata

# COMMAND ----------

import censusdata
import pandas as pd
import json
import numpy as np
import matplotlib.pyplot as plt
import requests

# COMMAND ----------

def create_and_save_as_table(df, table_name):
    spark_df = spark.createDataFrame(df)
    spark_df.write.format('csv').saveAsTable(table_name)

# COMMAND ----------

maindf = sqlContext.sql("select * from main_df_cluster_labeled tables").toPandas()

# COMMAND ----------

display(maindf)

# COMMAND ----------

acs_url = f'https://api.census.gov/data/2019/acs/acs5?get=NAME,B25024_001E,B25024_002E,B25024_003E,B25024_004E,B25024_005E,B25024_006E,B25024_007E,B25024_008E,B25024_009E,B25024_010E,B25024_011E,B25003_001E,B25003_003E,B01002_001E,B15003_001E,B15003_022E,B15003_023E,B15003_024E,B15003_025E,B19013_001E,B25064_001E,B25002_001E,B25002_002E,B25002_003E,B03002_001E,B03002_003E,B03002_004E,B03002_006E,B03002_012E&for=zip%20code%20tabulation%20area:*'
acs_response_all = requests.get(acs_url)
acs_results_all = acs_response_all.text
acs_all = pd.DataFrame.from_records(json.loads(acs_results_all))
header = acs_all.iloc[0] 
acs_all = acs_all.iloc[1:] 
acs_all.columns = header
subj_industry = censusdata.download('acs5',2019,censusdata.censusgeo([('zip code tabulation area', '*')]),['S2403_C01_001E','S2403_C01_002E','S2403_C01_005E','S2403_C01_006E','S2403_C01_007E','S2403_C01_008E','S2403_C01_009E','S2403_C01_012E','S2403_C01_013E','S2403_C01_016E','S2403_C01_020E','S2403_C01_023E','S2403_C01_026E','S2403_C01_027E'],tabletype='subject')
new_subj_industry_indices = [int(i.geo[0][1]) for i in subj_industry.index]
indexed_subj_industry = subj_industry.set_index(pd.Index(new_subj_industry_indices))
indexed_subj_industry['zip_code'] = indexed_subj_industry.index.astype(int)
acs_all['zip code tabulation area'] = acs_all['zip code tabulation area'].astype(int)
acs_all = acs_all.merge(indexed_subj_industry, how='left', left_on='zip code tabulation area', right_on='zip_code')
acs_all

# COMMAND ----------

zcta_acs = acs_all
zcta_acs['total_pop'] = zcta_acs.B03002_001E.astype(int)
zcta_acs['pct_singlefam'] = ((zcta_acs.B25024_002E.astype(int) + zcta_acs.B25024_003E.astype(int))/zcta_acs.B25024_001E.astype(int)) *100
zcta_acs['pct_renter'] = zcta_acs.B25003_003E.astype(int) / zcta_acs.B25003_001E.astype(int) *100
zcta_acs['pct_employment_natresource'] = zcta_acs.S2403_C01_002E.astype(int) / zcta_acs.S2403_C01_001E.astype(int) *100
zcta_acs['pct_employment_construction'] = zcta_acs.S2403_C01_005E.astype(int) / zcta_acs.S2403_C01_001E.astype(int) *100
zcta_acs['pct_employment_manufacturing'] = zcta_acs.S2403_C01_006E.astype(int) / zcta_acs.S2403_C01_001E.astype(int) *100
zcta_acs['pct_employment_wholesale'] = zcta_acs.S2403_C01_007E.astype(int) / zcta_acs.S2403_C01_001E.astype(int) *100
zcta_acs['pct_employment_retail'] = zcta_acs.S2403_C01_008E.astype(int) / zcta_acs.S2403_C01_001E.astype(int) *100
zcta_acs['pct_employment_transpowarehousingutil'] = zcta_acs.S2403_C01_009E.astype(int) / zcta_acs.S2403_C01_001E.astype(int) *100
zcta_acs['pct_employment_info'] = zcta_acs.S2403_C01_012E.astype(int) / zcta_acs.S2403_C01_001E.astype(int) *100
zcta_acs['pct_employment_financeinsre'] = zcta_acs.S2403_C01_013E.astype(int) / zcta_acs.S2403_C01_001E.astype(int) *100
zcta_acs['pct_employment_profscimgmtadminwaste'] = zcta_acs.S2403_C01_016E.astype(int) / zcta_acs.S2403_C01_001E.astype(int) *100
zcta_acs['pct_employment_eduhealthsocialassist'] = zcta_acs.S2403_C01_020E.astype(int) / zcta_acs.S2403_C01_001E.astype(int) *100
zcta_acs['pct_employment_artentrecaccommfood'] = zcta_acs.S2403_C01_023E.astype(int) / zcta_acs.S2403_C01_001E.astype(int) *100
zcta_acs['pct_employment_other'] = zcta_acs.S2403_C01_026E.astype(int) / zcta_acs.S2403_C01_001E.astype(int) *100
zcta_acs['pct_employment_pubadmin'] = zcta_acs.S2403_C01_027E.astype(int) / zcta_acs.S2403_C01_001E.astype(int) *100
zcta_acs['median_age'] = zcta_acs.B01002_001E
zcta_acs['bach_plus'] = (zcta_acs.B15003_022E.astype(int) + zcta_acs.B15003_023E.astype(int) + zcta_acs.B15003_024E.astype(int) + zcta_acs.B15003_025E.astype(int))/zcta_acs.B15003_001E.astype(int) *100
zcta_acs['median_hhinc'] = zcta_acs.B19013_001E.astype(int)
zcta_acs['median_hhinc'] = zcta_acs['median_hhinc'].where(zcta_acs['median_hhinc']>0) #gets rid of negative values
zcta_acs['median_rent'] = zcta_acs.B25064_001E.astype(int)
zcta_acs['median_rent'] = zcta_acs['median_rent'].where(zcta_acs['median_rent']>0) #gets rid of negative values
zcta_acs['pct_vacant'] = zcta_acs.B25002_003E.astype(int) / zcta_acs.B25002_001E.astype(int) *100
zcta_acs['pct_nhwhite'] = zcta_acs.B03002_003E.astype(int)/zcta_acs.B03002_001E.astype(int) *100
zcta_acs['pct_nhblack'] = zcta_acs.B03002_004E.astype(int)/zcta_acs.B03002_001E.astype(int) *100
zcta_acs['pct_nhasian'] = zcta_acs.B03002_006E.astype(int)/zcta_acs.B03002_001E.astype(int) *100
zcta_acs['pct_hisp'] = zcta_acs.B03002_012E.astype(int)/zcta_acs.B03002_001E.astype(int) *100
zcta_acs['pct_others'] = 100 - zcta_acs['pct_hisp'] - zcta_acs['pct_nhasian'] - zcta_acs['pct_nhblack'] - zcta_acs['pct_nhwhite']
zcta_acs['housingunits'] = zcta_acs.B25024_001E.astype(int)

# COMMAND ----------

zcta_acs.iloc[:,46:]

# COMMAND ----------

create_and_save_as_table(zcta_acs.iloc[:,46:], "acs_data_for_all_zipcodes")

# COMMAND ----------

zipbiz_url = f'https://api.census.gov/data/2019/cbp?get=ZIPCODE,GEO_ID,NAICS2017_LABEL,NAICS2017,EMPSZES_LABEL,EMPSZES,ESTAB,EMP,PAYANN,LFO&for=zip%20code:*'
zipbiz_response_all = requests.get(zipbiz_url)
zipbiz_results_all = zipbiz_response_all.text
zipbiz_all = pd.DataFrame.from_records(zipbiz_results_all)
new_header_rest = zipbiz_all.iloc[0] 
zipbiz_all = zipbiz_all.iloc[1:] 
zipbiz_all.columns = new_header_rest
zipbiz_all

# COMMAND ----------

