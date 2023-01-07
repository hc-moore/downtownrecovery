# Databricks notebook source
city_index = sqlContext.sql("select * from city_index_0119_csv tables").toPandas()
csa_index = sqlContext.sql("select * from csa_index_0119_csv tables").toPandas()
zcta_geo = sqlContext.sql("select * from us_zcta_geo_csv tables").toPandas()

# COMMAND ----------

import pandas as pd
import numpy as np
import json      
import requests

# COMMAND ----------

def create_and_save_as_table(df, table_name):
    spark_df = spark.createDataFrame(df)
    spark_df.write.format('csv').saveAsTable(table_name)

# COMMAND ----------

city_zcbp = {}
for index, row in city_index.iterrows():
    zipbiz_url = f'https://api.census.gov/data/2019/cbp?get=ZIPCODE,GEO_ID,NAICS2017_LABEL,EMPSZES_LABEL,EMPSZES,ESTAB,EMP,PAYANN,LFO&for=zip%20code:'+str(row["zipcodes"][1:-1].replace("'",""))+'&NAICS2017=*'
    zipbiz_response_all = requests.get(zipbiz_url)
    zipbiz_results_all = zipbiz_response_all.text
    zipbiz_all = json.loads(zipbiz_results_all)
    zipbiz_city=pd.DataFrame.from_records(zipbiz_all)
    new_header_rest = zipbiz_city.iloc[0] 
    zipbiz_city = zipbiz_city.iloc[1:] 
    zipbiz_city.columns = new_header_rest
    zipbiz_city['Establishments']=pd.to_numeric(zipbiz_city['ESTAB'])
    zipbiz_city['Employment']=pd.to_numeric(zipbiz_city['EMP'])
    zipbiz_city = zipbiz_city[zipbiz_city['EMPSZES']== '001']
    conditions = [(zipbiz_city['NAICS2017']  == '11'), (zipbiz_city['NAICS2017']  == '21'),(zipbiz_city['NAICS2017']  == '22'),(zipbiz_city['NAICS2017']  == '23'),(zipbiz_city['NAICS2017']  == '31-33'),(zipbiz_city['NAICS2017']  == '42'), (zipbiz_city['NAICS2017']  == '44-45'), (zipbiz_city['NAICS2017']  == '48-49'), (zipbiz_city['NAICS2017']  == '51'), (zipbiz_city['NAICS2017']  == '52'), (zipbiz_city['NAICS2017']  == '53'), (zipbiz_city['NAICS2017']  == '54'), (zipbiz_city['NAICS2017']  == '55'), (zipbiz_city['NAICS2017']  == '56'), (zipbiz_city['NAICS2017']  == '61'), (zipbiz_city['NAICS2017']  == '62'), (zipbiz_city['NAICS2017']  == '71'), (zipbiz_city['NAICS2017']  == '72'), (zipbiz_city['NAICS2017']  == '81'),(zipbiz_city['NAICS2017']  == '99')]
    values = ['Agriculture Forestry Fishing and Hunting', 'Mining Quarrying and Oil and Gas Extraction', 'Utilities', 'Construction', 'Manufacturing', 'Wholesale Trade', 'Retail Trade', 'Transportation and Warehousing', 'Information', 'Finance and Insurance', 'Real Estate and Rental and Leasing', 'Professional Scientific and Technical Services', 'Management of Companies and Enterprises', 'Administrative and Support and Waste Management and Remediation Services', 'Educational Services', 'Health Care and Social Assistance', 'Arts Entertainment and Recreation', 'Accommodation and Food Services', 'Other Services except Public Administration', 'Public Administration']
    zipbiz_city['IND_category'] = np.select(conditions, values)
    industry_mix_flat = zipbiz_city.pivot_table(index=['ZIPCODE'], columns=['IND_category'], values=['Establishments']).reset_index()
    industry_mix_flat.columns = industry_mix_flat.columns.droplevel(0)
    industry_mix_flat = industry_mix_flat.reset_index().rename_axis(None, axis=1)
    industry_mix_flat = industry_mix_flat.drop(columns=['index','0'])
    industry_mix_flat = industry_mix_flat.fillna(0)
    industry_mix_flat = industry_mix_flat.rename(columns={industry_mix_flat.columns[0]: 'zipcode'})
    employment_by_zipcode = zipbiz_city[zipbiz_city["NAICS2017_LABEL"]=="Total for all sectors"][["ZIPCODE","Employment"]]
    industry_mix_flat = industry_mix_flat.merge(employment_by_zipcode, how="left", left_on="zipcode", right_on="ZIPCODE")
    industry_mix_flat = industry_mix_flat.merge(zcta_geo[["GEOID10", "ALAND10"]], how="left", left_on="zipcode", right_on="GEOID10")
    industry_mix_flat["employment_density"] = industry_mix_flat["Employment"]/industry_mix_flat["ALAND10"].astype(int)
    industry_mix_flat["percentile_employment"] = industry_mix_flat["employment_density"].rank(pct=True)
    industry_mix_flat = industry_mix_flat.drop(columns=["ZIPCODE", "GEOID10"])
    create_and_save_as_table(industry_mix_flat, "zcbp_"+str(row["0"]+"_0119_v1").replace(" ",""))
    print(str(row["0"])+" is done")
    city_zcbp[row["0"]] = industry_mix_flat

# COMMAND ----------

exclusions = []

downtown_zips_95 = []
for i in city_zcbp:
    downtown_zcbp = city_zcbp[i][city_zcbp[i]["percentile_employment"]>=0.95]
    for j in downtown_zcbp["zipcode"]:
        downtown_zips_95.append(j)

downtown_zips_90 = []
for i in city_zcbp:
    downtown_zcbp = city_zcbp[i][city_zcbp[i]["percentile_employment"]>=0.9]
    for j in downtown_zcbp["zipcode"]:
        downtown_zips_90.append(j)

downtown_zips_85 = []
for i in city_zcbp:
    downtown_zcbp = city_zcbp[i][city_zcbp[i]["percentile_employment"]>=0.85]
    for j in downtown_zcbp["zipcode"]:
        downtown_zips_85.append(j)
        
downtown_zips_80 = []
for i in city_zcbp:
    downtown_zcbp = city_zcbp[i][city_zcbp[i]["percentile_employment"]>=0.80]
    for j in downtown_zcbp["zipcode"]:
        downtown_zips_80.append(j)

# COMMAND ----------

downtown_zips_95

# COMMAND ----------

downtown_zips_90

# COMMAND ----------

downtown_zips_85

# COMMAND ----------

downtown_zips_80

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

csa_index

# COMMAND ----------

len(csa_index.iloc[0,4].strip("[]").replace("'","").split(", "))/2

# COMMAND ----------

NY_CSA = pd.DataFrame({"_c0": [52, 53, 54, 55, 56, 57], "CSA": ["NYA", "NYB", "NYC", "NYD", "NYE", "NYF"], "GEOID": ["NYA", "NYB", "NYC", "NYD", "NYE", "NYF"], "geo": ["NYA", "NYB", "NYC", "NYD", "NYE", "NYF"], "zipcodes": ["["+", ".join(csa_index.iloc[0,4].strip("[]").split(", ")[:218])+"]", "["+", ".join(csa_index.iloc[0,4].strip("[]").split(", ")[218:436])+"]", "["+", ".join(csa_index.iloc[0,4].strip("[]").split(", ")[436:654])+"]", "["+", ".join(csa_index.iloc[0,4].strip("[]").split(", ")[654:872])+"]", "["+", ".join(csa_index.iloc[0,4].strip("[]").split(", ")[872:1090])+"]", "["+", ".join(csa_index.iloc[0,4].strip("[]").split(", ")[1090:])+"]"]})
NY_CSA

# COMMAND ----------

csa_index_ny_split = csa_index.drop([0]).append(NY_CSA).reset_index()
csa_index_ny_split

# COMMAND ----------

csa_zcbp = {}
for index, row in csa_index_ny_split.iterrows():
    zipbiz_url = f'https://api.census.gov/data/2019/cbp?get=ZIPCODE,GEO_ID,NAICS2017_LABEL,EMPSZES_LABEL,EMPSZES,ESTAB,EMP,PAYANN,LFO&for=zip%20code:'+str(row["zipcodes"][1:-1]).replace("'","")+'&NAICS2017=*'
    zipbiz_response_all = requests.get(zipbiz_url)
    zipbiz_results_all = zipbiz_response_all.text
    zipbiz_all = json.loads(zipbiz_results_all)
    zipbiz_city=pd.DataFrame.from_records(zipbiz_all)
    new_header_rest = zipbiz_city.iloc[0] 
    zipbiz_city = zipbiz_city.iloc[1:] 
    zipbiz_city.columns = new_header_rest
    zipbiz_city['Establishments']=pd.to_numeric(zipbiz_city['ESTAB'])
    zipbiz_city['Employment']=pd.to_numeric(zipbiz_city['EMP'])
    zipbiz_city = zipbiz_city[zipbiz_city['EMPSZES']== '001']
    conditions = [(zipbiz_city['NAICS2017']  == '11'), (zipbiz_city['NAICS2017']  == '21'),(zipbiz_city['NAICS2017']  == '22'),(zipbiz_city['NAICS2017']  == '23'),(zipbiz_city['NAICS2017']  == '31-33'),(zipbiz_city['NAICS2017']  == '42'), (zipbiz_city['NAICS2017']  == '44-45'), (zipbiz_city['NAICS2017']  == '48-49'), (zipbiz_city['NAICS2017']  == '51'), (zipbiz_city['NAICS2017']  == '52'), (zipbiz_city['NAICS2017']  == '53'), (zipbiz_city['NAICS2017']  == '54'), (zipbiz_city['NAICS2017']  == '55'), (zipbiz_city['NAICS2017']  == '56'), (zipbiz_city['NAICS2017']  == '61'), (zipbiz_city['NAICS2017']  == '62'), (zipbiz_city['NAICS2017']  == '71'), (zipbiz_city['NAICS2017']  == '72'), (zipbiz_city['NAICS2017']  == '81'),(zipbiz_city['NAICS2017']  == '99')]
    values = ['Agriculture Forestry Fishing and Hunting', 'Mining Quarrying and Oil and Gas Extraction', 'Utilities', 'Construction', 'Manufacturing', 'Wholesale Trade', 'Retail Trade', 'Transportation and Warehousing', 'Information', 'Finance and Insurance', 'Real Estate and Rental and Leasing', 'Professional Scientific and Technical Services', 'Management of Companies and Enterprises', 'Administrative and Support and Waste Management and Remediation Services', 'Educational Services', 'Health Care and Social Assistance', 'Arts Entertainment and Recreation', 'Accommodation and Food Services', 'Other Services except Public Administration', 'Public Administration']
    zipbiz_city['IND_category'] = np.select(conditions, values)
    industry_mix_flat = zipbiz_city.pivot_table(index=['ZIPCODE'], columns=['IND_category'], values=['Establishments']).reset_index()
    industry_mix_flat.columns = industry_mix_flat.columns.droplevel(0)
    industry_mix_flat = industry_mix_flat.reset_index().rename_axis(None, axis=1)
    industry_mix_flat = industry_mix_flat.drop(columns=['index','0'])
    industry_mix_flat = industry_mix_flat.fillna(0)
    industry_mix_flat = industry_mix_flat.rename(columns={industry_mix_flat.columns[0]: 'zipcode'})
    employment_by_zipcode = zipbiz_city[zipbiz_city["NAICS2017_LABEL"]=="Total for all sectors"][["ZIPCODE","Employment"]]
    industry_mix_flat = industry_mix_flat.merge(employment_by_zipcode, how="left", left_on="zipcode", right_on="ZIPCODE")
    industry_mix_flat = industry_mix_flat.merge(zcta_geo[["GEOID10", "ALAND10"]], how="left", left_on="zipcode", right_on="GEOID10")
    industry_mix_flat["employment_density"] = industry_mix_flat["Employment"]/industry_mix_flat["ALAND10"].astype(int)
    industry_mix_flat["percentile_employment"] = industry_mix_flat["employment_density"].rank(pct=True)
    industry_mix_flat = industry_mix_flat.drop(columns=["ZIPCODE", "GEOID10"])
    create_and_save_as_table(industry_mix_flat, "zcbp_"+str(row["GEOID"]).strip(" ")+"_0119_v2")
    print(str(row["CSA"])+" is done")
    csa_zcbp[row["GEOID"]] = industry_mix_flat

# COMMAND ----------

csa_zcbp[408] = csa_zcbp["NYA"].append(csa_zcbp["NYB"]).append(csa_zcbp["NYC"]).append(csa_zcbp["NYD"]).append(csa_zcbp["NYE"]).append(csa_zcbp["NYF"]).drop(columns = ["percentile_employment"])
csa_zcbp[408]["percentile_employment"] = csa_zcbp[408]["employment_density"].rank(pct=True)
csa_zcbp.pop("NYA")
csa_zcbp.pop("NYB")
csa_zcbp.pop("NYC")
csa_zcbp.pop("NYD")
csa_zcbp.pop("NYE")
csa_zcbp.pop("NYF")
csa_zcbp[408]

# COMMAND ----------

csa_urban_core_zips_95 = []
for i in csa_zcbp:
    csa_urban_core_zips = csa_zcbp[i][csa_zcbp[i]["percentile_employment"]>=0.95]
    for j in csa_urban_core_zips["zipcode"]:
        csa_urban_core_zips_95.append(j)

csa_urban_core_zips_90 = []
for i in csa_zcbp:
    csa_urban_core_zips = csa_zcbp[i][csa_zcbp[i]["percentile_employment"]>=0.9]
    for j in csa_urban_core_zips["zipcode"]:
        csa_urban_core_zips_90.append(j)        

csa_urban_core_zips_85 = []
for i in csa_zcbp:
    csa_urban_core_zips = csa_zcbp[i][csa_zcbp[i]["percentile_employment"]>=0.85]
    for j in csa_urban_core_zips["zipcode"]:
        csa_urban_core_zips_85.append(j) 
        
csa_urban_core_zips_80 = []
for i in csa_zcbp:
    csa_urban_core_zips = csa_zcbp[i][csa_zcbp[i]["percentile_employment"]>=0.8]
    for j in csa_urban_core_zips["zipcode"]:
        csa_urban_core_zips_80.append(j)     

# COMMAND ----------

csa_urban_core_zips_95

# COMMAND ----------

csa_urban_core_zips_90

# COMMAND ----------

csa_urban_core_zips_85

# COMMAND ----------

csa_urban_core_zips_80

# COMMAND ----------

all_csa_zips = []
for index, row in csa_index.iterrows():
    for zipcode in row["zipcodes"].strip("[]").split(", "):
        all_csa_zips.append(zipcode[1:-1])

# COMMAND ----------

len(csa_urban_core_zips_80_85)

# COMMAND ----------

csa_urban_core_zips_80_85 = [x for x in csa_urban_core_zips_80 if x not in csa_urban_core_zips_85]
csa_urban_core_zips_85_90 = [x for x in csa_urban_core_zips_85 if x not in csa_urban_core_zips_90]
csa_urban_core_zips_90_95 = [x for x in csa_urban_core_zips_90 if x not in csa_urban_core_zips_95]
csa_non_core_zips = [x for x in all_csa_zips if x not in csa_urban_core_zips_80]
csa_non_core_zcta_0_80 = zcta_geo[zcta_geo["GEOID10"].isin(csa_non_core_zips)]
csa_urban_core_zcta_95_100 = zcta_geo[zcta_geo["GEOID10"].isin(csa_urban_core_zips_95)]
csa_urban_core_zcta_80_85 = zcta_geo[zcta_geo["GEOID10"].isin(csa_urban_core_zips_80_85)]
csa_urban_core_zcta_85_90 = zcta_geo[zcta_geo["GEOID10"].isin(csa_urban_core_zips_85_90)]
csa_urban_core_zcta_90_95 = zcta_geo[zcta_geo["GEOID10"].isin(csa_urban_core_zips_90_95)]
display(csa_urban_core_zcta_95_100)
display(csa_urban_core_zcta_80_85)
display(csa_urban_core_zcta_85_90)
display(csa_urban_core_zcta_90_95)
display(csa_non_core_zcta_0_80)

# COMMAND ----------

display(csa_urban_core_zcta_80_85)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

