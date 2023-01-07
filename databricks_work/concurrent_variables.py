# Databricks notebook source
!pip install seaborn --upgrade

# COMMAND ----------

import pandas as pd
import requests
# from bs4 import BeautifulSoup
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import sklearn as sk
import statsmodels.formula.api as smf
import statsmodels.api as sm
import warnings

# COMMAND ----------

def create_and_save_as_table(df, table_name):
    spark_df = spark.createDataFrame(df)
    spark_df.write.format('csv').saveAsTable(table_name)
    
def get_table_as_pandas_df(table_name):
    return sqlContext.sql("select * from " + table_name + " tables").toPandas()
  
def get_downtown_DAUIDs(city_name):
    dt_DAUIDs = [str.strip(dauid) for dauid in can_city_index_da[can_city_index_da["0"] == city_name]["downtown_da"].values[0].split(",")]
    dt_DAUIDs[0] = dt_DAUIDs[0][1:]
    dt_DAUIDs[-1] = dt_DAUIDs[-1][:-1]
    return dt_DAUIDs
    
def get_metro_DAUIDs(city_name):
    city_DAUIDs = [str.strip(dauid) for dauid in can_city_index_da[can_city_index_da["0"] == city_name]["CMA_da"].values[0].split(",")]
    city_DAUIDs[0] = city_DAUIDs[0][1:]
    city_DAUIDs[-1] = city_DAUIDs[-1][:-1]
    return city_DAUIDs

def get_CCS_DAUIDs(city_name):
    ccs_DAUIDs = [str.strip(dauid) for dauid in can_city_index_da[can_city_index_da["0"] == city_name]["ccs_da"].values[0].split(",")]
    ccs_DAUIDs[0] = ccs_DAUIDs[0][1:]
    ccs_DAUIDs[-1] = ccs_DAUIDs[-1][:-1]
    return ccs_DAUIDs


def get_downtown_zipcodes(city_name):
    dt_zipcodes = []
    dt_vc = pd.DataFrame()

    dt_vc = us_city_index
    dt_zipcodes = dt_vc[dt_vc['city'] == city_name]['dt_zipcodes'] 
    dt_zipcodes = dt_zipcodes.tolist()[0].strip('][').split(', ')
    return [zipcode.replace("'","") for zipcode in dt_zipcodes]

def get_metro_zipcodes(city_name):
    ma_zipcodes = []
    ma_vc = pd.DataFrame()
    
    ma_vc = metro_definition_df
    ma_zipcodes = ma_vc[ma_vc['city'] == city_name]['ma_zipcodes']
    ma_zipcodes = ma_zipcodes.tolist()[0].strip('][').split(', ')
    return [zipcode.replace("'","") for zipcode in ma_zipcodes]

# COMMAND ----------

metro_definition_df = get_table_as_pandas_df('city_index_0119_csv')
metro_definition_df = metro_definition_df.rename(columns={"zipcodes": "ma_zipcodes"})
us_city_index = get_table_as_pandas_df('city_csa_fips_codes___us_cities_downtowns__1__csv')
metro_definition_df = metro_definition_df.rename(columns={"0": "city"})

# COMMAND ----------

us_dc = get_table_as_pandas_df("all_us_cities_dc")
#can_dc = get_table_as_pandas_df("all_canada_device_count_agg_dt")

# COMMAND ----------

can_city_index_da = get_table_as_pandas_df('can_city_index_da_0406')

# COMMAND ----------

display(can_city_index_da)

# COMMAND ----------

all_study_DAUIDs = pd.DataFrame()
all_study_CCSUIDs = pd.DataFrame()
city = can_city_index_da["0"].unique()[0]
CMA_DAUIDs = pd.DataFrame(get_metro_DAUIDs(city)).drop_duplicates()
CMA_DAUIDs["CMA"] = city
downtown_DAUIDs = pd.DataFrame(get_downtown_DAUIDs(city)).drop_duplicates()

CCS_DAUIDs = pd.DataFrame(get_CCS_DAUIDs(city)).drop_duplicates()
CMA_DAUIDs["is_downtown"] =  CMA_DAUIDs[0].isin(downtown_DAUIDs[0])
CMA_DAUIDs["is_CCS"] =  CMA_DAUIDs[0].isin(CCS_DAUIDs[0])

# COMMAND ----------

downtown_DAUIDs[0].isin(CMA_DAUIDs[0]).sum() == len(downtown_DAUIDs)

# COMMAND ----------

CCS_DAUIDs[0].isin(CMA_DAUIDs[0]).sum() == len(CCS_DAUIDs)

# COMMAND ----------

all_study_DAUIDs = pd.DataFrame()

for city in can_city_index_da["0"].unique():
    CMA_DAUIDs = pd.DataFrame(get_metro_DAUIDs(city)).drop_duplicates()
    CMA_DAUIDs["CMA"] = city
    
    downtown_DAUIDs = pd.DataFrame(get_downtown_DAUIDs(city)).drop_duplicates()
    CCS_DAUIDs = pd.DataFrame(get_CCS_DAUIDs(city)).drop_duplicates()
    
    CMA_DAUIDs["is_downtown"] =  CMA_DAUIDs[0].isin(downtown_DAUIDs[0])
    CMA_DAUIDs["is_CCS"] =  CMA_DAUIDs[0].isin(CCS_DAUIDs[0])
    all_study_DAUIDs = all_study_DAUIDs.append(CMA_DAUIDs)
#all_study_DAUIDs = all_study_DAUIDs[~all_study_DAUIDs.duplicated(subset = 0)]
all_study_DAUIDs  = all_study_DAUIDs.rename(columns = {0 : "DAUID"})

# COMMAND ----------

all_study_DAUIDs = all_study_DAUIDs[all_study_DAUIDs["is_CCS"] == True]

# COMMAND ----------

display(us_city_index)

# COMMAND ----------

all_study_zips = pd.DataFrame()
city = us_city_index["city"].unique()[0]
metro_zips = pd.DataFrame(get_metro_zipcodes(city)).drop_duplicates()
metro_zips["city"] = city
downtown_zips = pd.DataFrame(get_downtown_zipcodes(city)).drop_duplicates()
metro_zips["is_downtown"] =  metro_zips[0].isin(downtown_zips[0])

# COMMAND ----------

metro_zips

# COMMAND ----------

all_study_zips = pd.DataFrame()

for city in us_city_index["city"].unique():
    metro_zips = pd.DataFrame(get_metro_zipcodes(city)).drop_duplicates()
    metro_zips["city"] = city
    
    downtown_zips = pd.DataFrame(get_downtown_zipcodes(city)).drop_duplicates()

    metro_zips["is_downtown"] =  metro_zips[0].isin(downtown_zips[0])
    all_study_zips = all_study_zips.append(metro_zips)
all_study_zips  = all_study_zips.rename(columns = {0 : "postal_code"})

# COMMAND ----------

display(all_study_zips)

# COMMAND ----------

all_study_zips = all_study_zips[~all_study_zips.duplicated(subset = 0)]

# COMMAND ----------

downtown_rec = get_table_as_pandas_df('0407_downtown_rec_df')
metro_rec = get_table_as_pandas_df('0407_metro_rec_df')
#relative_rec = get_table_as_pandas_df('relative_rec_df_0407')
#localized_lq = get_table_as_pandas_df('localized_lq_df_0407')

# COMMAND ----------

downtown_rec.iloc[:,2:] - metro_rec.iloc[:,2:]

# COMMAND ----------

downtown_rec.columns.values[2:][~np.all((downtown_rec.iloc[:,2:] - metro_rec.iloc[:,2:]) == 0, axis = 0)]

# COMMAND ----------

metro_rec.head()

# COMMAND ----------

downtown_rec.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Unemployment rate
# MAGIC ###### US cities: 
# MAGIC https://beta.bls.gov/dataViewer/view/7902ccef8ffe48ac998b21a4d222c377 <br>
# MAGIC US data is seasonally unadjusted and available in 1, 3, 6, and 12 month intervals.
# MAGIC ###### Canadian CMAs: 
# MAGIC https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1410038001 <br>
# MAGIC Canadian data is seasonally unadjusted and is available by month, as 3 month moving averages. <br>
# MAGIC cite:  Statistics Canada. Table 14-10-0380-01  Labour force characteristics, three-month moving average, seasonally adjusted

# COMMAND ----------

# MAGIC %md
# MAGIC Sources for closure/reopening dates <br>
# MAGIC Alberta: https://open.alberta.ca/publications/2020-21-school-re-entry-plan <br>
# MAGIC https://open.alberta.ca/dataset/61f54c09-d6d7-4a12-a5be-0bc663a02c31/resource/274d4a21-e863-4901-83a4-6e3f4df5cd45/download/covid19-alberta-relaunch-strategy.pdf

# COMMAND ----------

# MAGIC %md
# MAGIC Canadian case/vaxx data notes and sources: <br>
# MAGIC ### these do not exactly match the study area for CMAs or cities, nor is there a way to confirm by merging on DA
# MAGIC #### Vancouver (British Columbia): 
# MAGIC ##### cases 
# MAGIC http://www.bccdc.ca/Health-Info-Site/Documents/BC_COVID-19_Disclaimer_Data_Notes.pdf
# MAGIC #### Calgary & Edmonton (Alberta): 
# MAGIC ##### cases 
# MAGIC https://data.edmonton.ca/Community-Services/COVID-19-in-Alberta-Cases-by-Zone/jmcu-tz8y
# MAGIC #### Winnipeg (Manitoba): TBD, no easily accessible data on COVID-19 cases
# MAGIC #### Halifax (Nova Scotia): ^^^
# MAGIC #### London, Mississauga, Ottawa (Ontario), Toronto: 
# MAGIC ##### cases
# MAGIC https://www.publichealthontario.ca/en/data-and-analysis/infectious-disease/covid-19-data-surveillance/covid-19-data-tool?tab=trends <br>
# MAGIC Mississauga is grouped with Brampton and Caledon; the data is managed by Peel Public Health Unit.
# MAGIC ####  Montréal, Québec, Ottawa (Québec):
# MAGIC ####  cases
# MAGIC https://www.donneesquebec.ca/recherche/dataset/covid-19-portrait-quotidien-des-cas-confirmes <br>
# MAGIC Québec COVID-19 case data is only available at province level. <br>
# MAGIC https://covid19tracker.ca/vaccinationtracker.html is a province-level case/vaxx database maintained by volunteers from gov't and public health partners <br>
# MAGIC Another option available through statcan: https://health-infobase.canada.ca/covid-19/epidemiological-summary-covid-19-cases.html?redir=1

# COMMAND ----------

# MAGIC %md 
# MAGIC US case/vaxx data notes and sources:
# MAGIC #### San Francisco: 
# MAGIC ##### cases 
# MAGIC https://data.sfgov.org/COVID-19/COVID-19-Cases-Over-Time/gyr2-k29z 
# MAGIC ##### vaxx: 
# MAGIC https://data.sfgov.org/COVID-19/COVID-Vaccinations-Given-to-SF-Residents-Over-Time/bqge-2y7k
# MAGIC #### New York: 
# MAGIC ##### cases 
# MAGIC https://github.com/nychealth/coronavirus-data/blob/master/trends/data-by-day.csv
# MAGIC ##### vaxx 
# MAGIC https://raw.githubusercontent.com/nychealth/covid-vaccine-data/main/doses/doses-by-day.csv
# MAGIC #### Dallas: 
# MAGIC ##### cases 
# MAGIC https://covid-analytics-pccinnovation.hub.arcgis.com/
# MAGIC ##### vaxx 
# MAGIC https://covid-analytics-pccinnovation.hub.arcgis.com/ <br>
# MAGIC Dallas has county dashboards only, county data is available for downloaded at CDC's website.
# MAGIC #### Seattle: 
# MAGIC ##### cases 
# MAGIC https://kingcounty.gov/depts/health/covid-19/data/summary-dashboard.aspx
# MAGIC ##### vaxx 
# MAGIC https://kingcounty.gov/depts/health/covid-19/data/vaccination.aspx
# MAGIC #### Chicago:
# MAGIC ##### cases 
# MAGIC https://www.chicago.gov/city/en/sites/covid-19/home/covid-dashboard.html
# MAGIC ##### vaxx 
# MAGIC https://www.chicago.gov/city/en/sites/covid-19/home/covid-dashboard.html <br>
# MAGIC Chicago has covid data dashboards, but no option to download the data.
# MAGIC #### Washington DC:
# MAGIC ##### cases 
# MAGIC https://coronavirus.dc.gov/data
# MAGIC ##### vaxx 
# MAGIC https://coronavirus.dc.gov/data
# MAGIC #### Detroit:
# MAGIC ##### cases 
# MAGIC https://codtableau.detroitmi.gov/t/DHD/views/CityofDetroit-PublicCOVIDDashboard/TimelineCasesDashboard?%3AisGuestRedirectFromVizportal=y&%3Aembed=y
# MAGIC ##### vaxx 
# MAGIC https://codtableau.detroitmi.gov/t/DoIT-Data-Public/views/VaccinePublicDashboard/VaccineDashboard?:origin=card_share_link&:embed=y&:isGuestRedirectFromVizportal=y <br>
# MAGIC Detroit's tableau dashboards are visible, but data is unavailable for download.
# MAGIC #### Los Angeles:
# MAGIC ##### cases 
# MAGIC http://dashboard.publichealth.lacounty.gov/covid19_surveillance_dashboard/
# MAGIC ##### vaxx 
# MAGIC http://publichealth.lacounty.gov/media/Coronavirus/vaccine/vaccine-dashboard.htm <br>
# MAGIC Los Angeles has a warning about its data: "Please note that select data for ages 12-17 by city/community had errors prior to 7/28/2021.  The issue has been resolved but please discard any files with age 12-17 data downloaded prior to 7/28/2021.  All other age group and city/community data were not impacted by these issues."

# COMMAND ----------

unemployment_rates_canada = get_table_as_pandas_df("canadian_unemployment_rate_1_csv")
unemployment_rates_canada = unemployment_rates_canada[["REF_DATE", "GEO", "VALUE"]]
unemployment_rates_canada = unemployment_rates_canada.rename(columns = {"REF_DATE" : "month", "GEO" : "Region", "VALUE" : "unemployment_rate"})
unemployment_rates_canada["month"] = pd.to_datetime(unemployment_rates_canada["month"])

# COMMAND ----------

unemployment_rates_us = get_table_as_pandas_df("us_unemployment_rate_1_csv")
unemployment_rates_us["area_code"] = unemployment_rates_us["Series_ID"].str[3:-2]
unemployment_rates_us["month"] = pd.to_datetime(unemployment_rates_us["Label"])
bls_codes = get_table_as_pandas_df("us_bls_codes")
unemployment_rates_us = unemployment_rates_us.merge(bls_codes, on = "area_code", how = "left")
unemployment_rates_us = unemployment_rates_us[["month", "area_text", "Value"]]
unemployment_rates_us = unemployment_rates_us.rename(columns = {"area_text" : "Region", "Value" : "unemployment_rate"})
#unemployment_rates_us = unemployment_rates_us.pivot(index = "dates", columns = "Region", values = "unemployment_rate")

# COMMAND ----------

scope_cities=['Toronto','Mississauga','New York', 'San Francisco', 'Montréal', 'Québec', 'London', 'Ottawa', 'Vancouver','Dallas', 'Seattle', 'Chicago', 'Washington DC', 'Halifax', 'Edmonton', 'Calgary', 'Winnipeg', 'Detroit', 'Los Angeles', 'Denver', 'Houston', 'Nashville', 'Atlanta']

# COMMAND ----------

unemployment_rates_all = pd.concat([unemployment_rates_canada, unemployment_rates_us])
unemployment_rates_all["City"] = unemployment_rates_all["Region"].str.split(",", expand = True)[0]
unemployment_rates_all["City"] = unemployment_rates_all["City"].str.replace("Ottawa-Gatineau", "Ottawa")
unemployment_rates_all["City"] = unemployment_rates_all["City"].str.replace(" city", "")
unemployment_rates_all["City"] = unemployment_rates_all["City"].str.replace(" County/city", "")
unemployment_rates_all["City"] = unemployment_rates_all["City"].str.replace("Washington", "Washington DC")
all(unemployment_rates_all["City"].isin(scope_cities))

# COMMAND ----------

unemployment_rates_all = unemployment_rates_all.drop(columns = "Region")
unemployment_rates_all = unemployment_rates_all.rename(columns = {"month" : "date", "City" : "city"})
unemployment_rates_all["date"] = pd.to_datetime(unemployment_rates_all["date"])

# COMMAND ----------

# covid data availability varies WIDELY by scope_cities. just use a subset that actually has info and don't plot the others
# assume that covid cases in a zone == actual covid cases in that city. Ignore Unknown and Nones for the time being 
# alberta public health zones: ['Edmonton Zone', 'Calgary Zone', 'Central Zone', 'North Zone', 'South Zone', 'Unknown', None]
covid_cases_alberta = get_table_as_pandas_df("covid_cases_alberta_csv")
covid_cases_alberta = covid_cases_alberta[(covid_cases_alberta["Alberta_Health_Services_Zone"] == "Edmonton Zone") | (covid_cases_alberta["Alberta_Health_Services_Zone"] == "Calgary Zone")]
covid_cases_alberta = covid_cases_alberta.groupby(["Alberta_Health_Services_Zone", "Date_reported"]).size().reset_index().rename(columns = {"Alberta_Health_Services_Zone" : "city", "Date_reported" : "date", 0 : "cases"})
covid_cases_alberta["date"] = pd.to_datetime(covid_cases_alberta["date"])
covid_cases_alberta["city"] = covid_cases_alberta["city"].str.replace(" Zone", "")
covid_cases_alberta

# COMMAND ----------

# public health units are here: https://cdn.ymaws.com/alphaweb.site-ym.com/resource/resmgr/alpha_region_map_250320.jpg
# ontario public health units: 'Ontario', 'Algoma Public Health', 'Brant County Health Unit',
#       'Chatham-Kent Public Health',
#       'City of Hamilton Public Health Services',
#       'Durham Region Health Department ', 'Eastern Ontario Health Unit',
#       'Grey Bruce Health Unit', 'Haldimand-Norfolk Health Unit',
#       'Haliburton, Kawartha, Pine Ridge District Health Unit ',
#       'Halton Region Public Health',
#       'Hastings Prince Edward Public Health', 'Huron Perth Health Unit',
#       'Kingston, Frontenac and Lennox & Addington Public Health',
#       'Lambton Public Health',
#       'Leeds, Grenville & Lanark District Health Unit',
#       'Middlesex-London Health Unit', 'Niagara Region Public Health',
#       'North Bay Parry Sound District Health Unit',
#       'Northwestern Health Unit', 'Ottawa Public Health',
#       'Peel Public Health', 'Peterborough Public Health ',
#       'Porcupine Health Unit', 'Public Health Sudbury & Districts',
#       'Region of Waterloo Public Health and Emergency Services',
#       'Renfrew County and District Health Unit',
#       'Simcoe Muskoka District Health Unit ',
#       'Southwestern Public Health', 'Thunder Bay District Health Unit',
#       'Timiskaming Health Unit', 'Toronto Public Health',
#       'Wellington-Dufferin-Guelph Public Health',
#       'Windsor-Essex County Health Unit ', 'York Region Public Health'
# to keep things fair, just sum case totals by week for now. think about rolling later once they're all concatenated together
covid_cases_ontario = get_table_as_pandas_df("covid_cases_ontario_csv")
covid_cases_ontario = covid_cases_ontario[(covid_cases_ontario["Public_Health_Unit"] == "Toronto Public Health") | 
                                          (covid_cases_ontario["Public_Health_Unit"] == "Peel Public Health") |
                                          (covid_cases_ontario["Public_Health_Unit"] == "Middlesex-London Health Unit")]
covid_cases_ontario = covid_cases_ontario[["Date", "Public_Health_Unit", "Cases_by_reported_date", "7_day_rolling_average:_Cases_by_reported_date"]].rename(columns = {"Date" : "date", "Public_Health_Unit" : "city", "Cases_by_reported_date" : "cases", "7_day_rolling_average:_Cases_by_reported_date":"cases_7_day_rolling_avg"})
covid_cases_ontario["date"] = pd.to_datetime(covid_cases_ontario["date"])
covid_cases_ontario["city"] = covid_cases_ontario["city"].str.replace("Toronto Public Health", "Toronto")
covid_cases_ontario["city"] = covid_cases_ontario["city"].str.replace("Peel Public Health", "Mississauga")
covid_cases_ontario["city"] = covid_cases_ontario["city"].str.replace("Middlesex-London Health Unit", "London")
covid_cases_ontario

# COMMAND ----------

# ['All', 'Fraser', 'Interior', 'Northern', 'Vancouver Coastal','Vancouver Island', 'Out of Canada']
covid_cases_vancouver = get_table_as_pandas_df("covid_cases_vancouver_csv")
# vancouver's health authority regions: https://med-fom-midwifery.sites.olt.ubc.ca/files/2010/08/regionmap.gif
covid_cases_vancouver = covid_cases_vancouver[(covid_cases_vancouver["HA"] == "Fraser") | (covid_cases_vancouver["HA"] == "Vancouver Coastal") | (covid_cases_vancouver["HA"] == "Vancouver Island")]
covid_cases_vancouver = covid_cases_vancouver[["Date", "Cases_Reported", "Cases_Reported_Smoothed"]]
covid_cases_vancouver["city"] = "Vancouver"
covid_cases_vancouver = covid_cases_vancouver.rename(columns = {"Date" : "date", "Cases_Reported" : "cases", "Cases_Reported_Smoothed" : "cases_smoothed"})
covid_cases_vancouver["date"] = pd.to_datetime(covid_cases_vancouver["date"])


# COMMAND ----------

# just use cases for now since they all have that, some don't have smoothing or rolling avgs
covid_cases_canada = pd.concat([covid_cases_alberta, covid_cases_ontario, covid_cases_vancouver])
covid_cases_canada = covid_cases_canada[["city", "date", "cases"]]
covid_cases_canada

# COMMAND ----------

 # now get do all this for SF, NYC, and LA covid data
# sf's dates are by date of specimen collected 
covid_cases_sf = get_table_as_pandas_df("covid_cases_sf_csv")
covid_cases_sf = covid_cases_sf.rename(columns = {"Specimen_Collection_Date" : "date", "New_Cases" : "cases", "Cumulative_Cases" : "total_cases"})
covid_cases_sf["date"] = pd.to_datetime(covid_cases_sf["date"])
covid_cases_sf["city"] = "San Francisco"
covid_cases_sf

# COMMAND ----------

covid_cases_la = get_table_as_pandas_df("covid_cases_la_csv")
covid_cases_la = covid_cases_la.rename(columns = {"date_use" : "date", "new_case" : "cases"})
covid_cases_la["city"] = "Los Angeles"
covid_cases_la["date"] = pd.to_datetime(covid_cases_la["date"])
covid_cases_la

# COMMAND ----------

covid_cases_cali = pd.concat([covid_cases_la, covid_cases_sf])
covid_cases_cali

# COMMAND ----------

# cases will be the sum of probable and confirmed, to follow suit with alberta 
covid_cases_nyc = get_table_as_pandas_df("covid_cases_nyc_csv")
covid_cases_nyc["cases"] = covid_cases_nyc[["CASE_COUNT", "PROBABLE_CASE_COUNT"]].sum(axis = 1)
covid_cases_nyc = covid_cases_nyc.rename(columns = {"date_of_interest" : "date", "ALL_CASE_COUNT_7DAY_AVG" : "avg_cases"})
covid_cases_nyc["date"] = pd.to_datetime(covid_cases_nyc["date"])
covid_cases_nyc["city"] = "New York"
covid_cases_nyc = covid_cases_nyc[["date", "avg_cases", "cases", "city"]]
covid_cases_nyc

# COMMAND ----------

covid_cases_us = pd.concat([covid_cases_cali, covid_cases_nyc])
covid_cases_us = covid_cases_us[["date", "cases", "city"]]
covid_cases_us

# COMMAND ----------

covid_cases_all = pd.concat([covid_cases_canada, covid_cases_us])

# COMMAND ----------



# COMMAND ----------

# as a proof of concept, just do toronto for tonight
weekly_lqs_all = get_table_as_pandas_df("us_can_lqs_0317")
weekly_lqs_all = weekly_lqs_all[weekly_lqs_all["city"] != "Hamilton"]
weekly_lqs_all = weekly_lqs_all[["city", "date_range_start_covid", "rvc_lq"]]
weekly_lqs_all = weekly_lqs_all.rename(columns = {"date_range_start_covid" : "date"})

# COMMAND ----------

# melt them
unemployment_rates_all_melted = unemployment_rates_all.melt(id_vars = ["date", "city"])
covid_cases_all_melted = covid_cases_all.melt(id_vars = ["date", "city"])
weekly_lqs_all_melted = weekly_lqs_all.melt(id_vars = ["date", "city"])

# COMMAND ----------

unemployment_rates_all.dtypes

# COMMAND ----------

covid_cases_all.dtypes

# COMMAND ----------

weekly_lqs_all['date'] = pd.to_datetime(weekly_lqs_all['date'])

# COMMAND ----------

display(weekly_lqs_all.merge(covid_cases_all, on = ["date", "city"], how = "inner"))

# COMMAND ----------

display(unemployment_rates_all)

# COMMAND ----------

unemployment_rates_all

# COMMAND ----------



# COMMAND ----------

dbutils.widgets.dropdown(
  name = 'city',
  defaultValue = "Toronto",
  choices = ['New York', 'Los Angeles', 'San Francisco', 'Toronto', 'Calgary','Edmonton', 'Mississauga', 'Vancouver', 'London'],
  label = 'City'
)

dbutils.widgets.dropdown(
  name = 'weekly_roll',
  defaultValue = "5",
  choices = [str(x) for x in range(1, 26)],
  label = 'Weekly rolling window'
)

dbutils.widgets.dropdown(
  name = 'monthly_roll',
  defaultValue = "1",
  choices = [str(x) for x in range(1, 5)],
  label = 'Monthly rolling window'
)

scaler = sk.preprocessing.MinMaxScaler()

# COMMAND ----------

weekly_rolling_window = int(dbutils.widgets.get("weekly_roll"))
monthly_rolling_window = int(dbutils.widgets.get("monthly_roll"))
city = dbutils.widgets.get("city")

unemployment_rates_city_melted = unemployment_rates_all[unemployment_rates_all["city"] == city].melt(id_vars = ["date", "city"])
covid_cases_city_melted = covid_cases_all[covid_cases_all["city"] == city].melt(id_vars = ["date", "city"])
weekly_lqs_city_melted = weekly_lqs_all[weekly_lqs_all["city"] == city].melt(id_vars = ["date", "city"])

unemployment_rates_city_melted = unemployment_rates_city_melted.set_index("date")
weekly_lqs_city_melted = weekly_lqs_city_melted.set_index("date")
covid_cases_city_melted = covid_cases_city_melted.set_index("date")

unemployment_rates_city_melted = pd.concat([unemployment_rates_city_melted, pd.DataFrame(index = unemployment_rates_city_melted.index, data = {"scaled_value" : scaler.fit_transform(pd.DataFrame(unemployment_rates_city_melted["value"])).flatten()})], axis = 1)
covid_cases_city_melted = pd.concat([covid_cases_city_melted, pd.DataFrame(index = covid_cases_city_melted.index, data = {"scaled_value" : scaler.fit_transform(pd.DataFrame(covid_cases_city_melted["value"])).flatten()})], axis = 1)
weekly_lqs_city_melted = pd.concat([weekly_lqs_city_melted, pd.DataFrame(index = weekly_lqs_city_melted.index, data = {"scaled_value" : scaler.fit_transform(pd.DataFrame(weekly_lqs_city_melted["value"])).flatten()})], axis = 1)

plot_df = pd.concat([unemployment_rates_city_melted, covid_cases_city_melted, weekly_lqs_city_melted])
plot_df = plot_df.reset_index()
plot_df["date"] = pd.to_datetime(plot_df["date"])
plot_df = plot_df[(plot_df["date"] >= "2020-03-01") & 
                  (plot_df["date"] < "2022-01-01")]
sns.set_style('darkgrid')
sns.set_context('talk')
fig = plt.figure(figsize = (16, 9), dpi = 90)
ax = sns.lineplot(x = "date", 
             y = "scaled_value",
             hue = "variable",
             data = plot_df)
sns.move_legend(ax, "upper left",  bbox_to_anchor=(1, 1))
plt.xticks(rotation = 25)
plt.title(dbutils.widgets.get("city") + ": Scaled RVC LQ with COVID-19 cases and unemployment rate")
plt.show()