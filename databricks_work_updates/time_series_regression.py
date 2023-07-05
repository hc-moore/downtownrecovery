# Databricks notebook source
import numpy as np
import pandas as pd
import sklearn
import scipy
import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------

# utility functions
def get_downtown_city_zipcodes(city_name):
    dt_zipcodes = []
    dt_vc = pd.DataFrame()

    dt_vc = can_city_index[["city", "dt_zipcodes"]]# if city_name in us_cities else can_city_index[["city","dt_zipcodes"]]
    dt_zipcodes = dt_vc[dt_vc['city'] == city_name]['dt_zipcodes'] 
    dt_zipcodes = dt_zipcodes.tolist()[0].strip('][').split(', ')
    return [zipcode.replace("'","") for zipcode in dt_zipcodes]

def get_metro_zipcodes(city_name):
    ma_zipcodes = []
    ma_vc = pd.DataFrame()
    
    ma_vc = can_city_index[["city","ma_zipcodes"]]  #if city_name in us_cities else can_city_index[["city","ma_zipcodes"]]
    ma_zipcodes = ma_vc[ma_vc['city'] == city_name]['ma_zipcodes']
    ma_zipcodes = ma_zipcodes.tolist()[0].strip('][').split(', ')
    return [zipcode.replace("'","") for zipcode in ma_zipcodes]

def create_and_save_as_table(df, table_name):
    spark_df = spark.createDataFrame(df)
    spark_df.write.format('csv').saveAsTable(table_name)
    
def get_table_as_pandas_df(table_name):
    return sqlContext.sql("select * from " + table_name + " tables").toPandas()

# COMMAND ----------

# load in data that will fundamentally never change
# for help with plotting, grouping
regions_df = get_table_as_pandas_df('regions_csv')
regions_df['state'] = regions_df['display_title'].str[-2:].str.strip()

# raw feature space
raw_features = get_table_as_pandas_df('all_model_features_0823')

# calculated downtown rq, city rq, and lq
all_weekly_metrics = get_table_as_pandas_df('0714_combined_metrics_df')
all_weekly_metrics['city'] = all_weekly_metrics['city'].str.replace('é', 'e')
study_weeks = all_weekly_metrics.sort_values(by = 'week')['week'].unique()
all_weekly_metrics['week'] = pd.to_datetime(all_weekly_metrics['week'].values)
all_weekly_metrics = all_weekly_metrics.drop_duplicates(subset = ['metric', 'city', 'week'])

# for help with merging
all_city_index = get_table_as_pandas_df('all_city_index')
all_city_index['postal_code'] = all_city_index['postal_code'].fillna(0).astype(int)

# raw observations of mobile device count activity
us_dc = get_table_as_pandas_df("all_us_cities_dc")
us_dc['postal_code'] = us_dc['postal_code'].fillna(0).astype(int)
can_dc = get_table_as_pandas_df("all_canada_device_count_agg_dt")
can_dc['postal_code'] = can_dc['postal_code'].fillna(0).astype(int)
pre_march_2022 = us_dc.append(can_dc)
pre_march_2022['date_range_start'] = pd.to_datetime(pre_march_2022['date_range_start'].values)
pre_march_2022['city'] = pre_march_2022['city'].str.replace("é", "e")
pre_march_2022['postal_code'] = pre_march_2022['postal_code'].fillna(0).astype(int)



# COMMAND ----------

# curating raw data
metro_definition_df = get_table_as_pandas_df('city_index_0119_csv')
metro_definition_df = metro_definition_df.rename(columns={"zipcodes": "ma_zipcodes"})
can_city_index = get_table_as_pandas_df('can_city_index_da_0406')
can_city_index = can_city_index.rename(columns={"0": "city", "downtown_da": "dt_zipcodes", "ccs_da": "ma_zipcodes"})
metro_definition_df = metro_definition_df.rename(columns={"0": "city"})
can_city_index['city'] = can_city_index['city'].str.replace("é", "e")
can_cities = can_city_index['city'].tolist()

dt_postal_codes = pd.concat([pd.DataFrame({city : get_downtown_city_zipcodes(city)}) for city in can_cities], axis = 1)
dt_postal_codes['is_downtown'] = True
ma_postal_codes = pd.concat([pd.DataFrame({city : get_metro_zipcodes(city)}) for city in can_cities], axis = 1)
ma_postal_codes['is_downtown'] = False

all_can_postal_codes = pd.concat([dt_postal_codes.melt(id_vars = 'is_downtown').dropna(), ma_postal_codes.melt(id_vars = 'is_downtown').dropna()])
all_can_postal_codes = all_can_postal_codes.rename(columns = {'variable' : 'city', 'value' : 'postal_code'})
all_can_postal_codes['postal_code'] = all_can_postal_codes['postal_code'].fillna(0).astype(int)
all_postal_codes = pd.concat([all_city_index, all_can_postal_codes]).drop_duplicates()

all_post_feb2022 = get_table_as_pandas_df("safegraph_patterns_count_agg_postal")
all_post_feb2022['date_range_start'] = pd.to_datetime(all_post_feb2022['date_range_start'].values)
all_post_feb2022['city'] =  all_post_feb2022['city'].str.replace("é", "e")
all_post_feb2022['postal_code'] = all_post_feb2022['postal_code'].fillna(0).astype(int)

all_sampled_records = pd.concat([pre_march_2022, all_post_feb2022[all_post_feb2022['date_range_start'].values > np.max(pre_march_2022['date_range_start'].values)]]) # ensures no duplicate dates
all_sampled_records = all_sampled_records.merge(regions_df[['city', 'state']], how = 'inner', on = 'city').merge(all_postal_codes[['postal_code', 'is_downtown']], on = 'postal_code', how = 'inner')
all_sampled_records.shape

# COMMAND ----------

# start widgets
#dbutils.widgets.removeAll()
dbutils.widgets.dropdown('period', '1', ['1', '2', 'change'])
dbutils.widgets.dropdown('metric', 'downtown', ['downtown', 'metro', 'relative'])
dbutils.widgets.dropdown('state', 'CA', np.unique(regions_df['state']).tolist())
dbutils.widgets.dropdown('week_start', '2020-03-09', study_weeks.tolist())
dbutils.widgets.dropdown('week_end', '2020-06-01', study_weeks.tolist())

# COMMAND ----------

# MAGIC %md
# MAGIC # Sampling Canada
# MAGIC 
# MAGIC 1. Safegraph's API allows for searches by postal code, city, state, country. The API page response is limited to 500 rows per page per request.
# MAGIC 2. Given that the API limit allows for 500 pages per request and submitting such requests incurs a great cost in time, money, and resources, the best course of action would be to make the fewest number of requests that return the most POIs to the spatial extent with the greatest amount of alignment with our defined study boundaries.
# MAGIC 3. We have the Census Dissemination Area IDs and 'city' names of where we want to sample, but our study boundaries do not necessarily line up with what Safegraph has listed as a POI's city- in the case of Toronto alone, there are subdivisions upon subdivisions of 'cities' that are within the City of Toronto's boundary but do not necessarily share a name. The more fragmented a study city is by its city name in Safegraph, the greater the chance of it having missing or incomplete data relative to other study regions sampled with this method. 
# MAGIC 4. The best course of action would be to sample on Census Dissemination Area IDs over all weeks of the study period, but the Safegraph API does not allow for requests by `poi:cbg`. 
# MAGIC 5. The next best course of action would be to sample on postal codes, but a Canadian postal code <-> Dissemination Area ID relationship file does not exist. Moreover, Canadian postal codes cover a very small area in space, to the extent that a single-digit *number* of POIs are returned with API request. This would require a much higher volume of API requests which is not ideal, since they have a hard cap of 1000 requests/minute.
# MAGIC 6. The next best course of action (and ultimately what we did) is to first compile a list of all eligible placekeys in the study area. This was done by purchasing the Places dataset for all of Canada, which has a column `placekey` but does not have a column for `poi:cbg`. To avoid pulling unnecessary placekeys, we purchased a week of Patterns data from October 2020, which has columns for both `placekey` and `poi:cbg`. We joined the Patterns placekey information to our list of Census Dissemination Areas in the study by the `poi:cbg` column to have a list of placekeys we knew were in the study area. 
# MAGIC 7. This set of placekeys (about 123k total) was requested for each week of the study period in batches of 20. 
# MAGIC 8. The average weekly presence of placekeys for the entirety of Canada was about 8k. 
# MAGIC 9. Placekey coverage was consistent across time and space with the exception of Hamilton, Ontario, which had spikes in placekey presence during the last weeks of 2019, 2020, and 2021.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC # Definitions
# MAGIC 
# MAGIC \\(C\\): the unordered set of US and Canadian cities in the study. It is 62 string elements. 
# MAGIC 
# MAGIC $$ C = \\{ c \\in \\text{ All cities in the US and Canada } \| c \\ni \\{\\text{ Albuquerque, Atlanta, ..., Wichita, Winnipeg }\\}\\} $$
# MAGIC 
# MAGIC \\(W\\): the ordered set of all ISO week numbers. Each week in the study is mapped to an ISO week number following the convention used in cmd 10 of [this notebook](https://dbc-8e5b4d4c-5814.cloud.databricks.com/?o=3181607526002649#notebook/3801644626179230/command/3801644626179239). Its elements are \\(\\{1, ..., 52\\}\\).  
# MAGIC 
# MAGIC $$ W = \\{ w \\in \mathbb{Z} \| w \\in \\{1, ..., 52\\} $$
# MAGIC 
# MAGIC ## Random variables
# MAGIC 
# MAGIC \\(V\\) is a discrete random variable describing the raw visit counts to a POI. The values it can take on are integers from 0 to \\(M\\), where \\(M\\) is the total number of visitors registered by Safegraph for a given week. Obviously, the likelihood of this maximum ever being observed is very low but theoretically, that would be its upper bound. 
# MAGIC 
# MAGIC Every individual \\(V_i)\\) can be normalized by the sum of all \\(V_i)\\) in the state of city \\(c\\) for every unique combination of year and week number. 
# MAGIC 
# MAGIC $$ \frac{V_i}{\sum_i V_i\}$$
# MAGIC 
# MAGIC \\(X\\) is a continuous random variable describing the sum of *normalized visits by total visits* to the \\(j\\)th `postal_code` of all postal codes in city \\(c_k\\) for a week \\(n\\) in year \\(y\\). The \\(j\\)th `postal_code` can have geography \\(g\\) 'downtown' or 'not downtown'.  \\(X\\) is the sum of all sampled, normalized \\(V\\) in a postal code. Given that the eligible POIs to sample follow a normal distribution and if we assume all POIs had an equal chance of being sampled, the distribution of the sample mean will follow a normal dsitrbution, as wil the sample sum. 
# MAGIC 
# MAGIC $$ X(c, j, w, y) = \sum_{c, j} \frac{V(c, j, w, y)}{\sum_{c \in S}\sum_{j \in c}V(c, j, w, y)} $$
# MAGIC $$ X(c, j, w, y) = \sum_{c, j} \frac{V(c, j, w, y)}{\mathcal{S} + V(c, j, w, y)} $$
# MAGIC 
# MAGIC \\(Y\\) is a function of random variables \\( (X_1, X_2, X_3, ... , X_n) \\). \\(Y\\) can be conditioned on week and geography. 
# MAGIC 
# MAGIC $$ Y(c, g, w, y) = \sum_g X(c, g, w, y) $$
# MAGIC 
# MAGIC Each \\(Y\\) in 2020, 2021, and 2022 is divided by the corresponding \\(Y\\) by week in 2019. The expected value is the recovery metric for a given week, year, city, and geography. 
# MAGIC 
# MAGIC $$ R(w, y, c, g) = E(\frac{Y(w, y, c, g)}{Y(w, y - 1, c, g)}) $$
# MAGIC 
# MAGIC In other words, \\(R\\) is the expected value of the ratio of normally distributed random variables \\(Y_1\\) and \\(Y_0\\), which themselves are sums of normally distributed random variables \\(X(c, j, w, y)\\).
# MAGIC 
# MAGIC The probability distribution of \\(R\\) is the distribution of the ratio of random variables \\(Y_1\\) and \\(Y_0\\). 
# MAGIC 
# MAGIC We can create a rejection region as a means to identify which cities may be outliers in their sampling distribution. 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Definition from Safegraph
# MAGIC 
# MAGIC > The `normalized_by_total_visits` version adjusts for changes in the overall number of visits in the data. This helps adjust for the amount of activity SafeGraph is seeing in total, which also will adjust for actual aggregate changes in foot traffic activity. Do you want your analysis to smooth out the fact that foot traffic in general spikes on Black Friday? `normalized_by_total_visits` will help adjust for that. The result is “the share of all observed visits that were to this POI.”
# MAGIC 
# MAGIC ### What does that mean for the study?
# MAGIC The way the data was collected was to request information on every placekey that had a `cbg:poi` field that corresponded to a `postal_code` in the study. The same set of placekeys were used each week, in the same order. Not all placekeys were present in each week, attributable to failed API requests in batches of up to 1000 placekeys or a placekey simply not yet existing yet. POIs are reported if its `raw_vistor_counts` is greater than 3.  
# MAGIC 
# MAGIC Even if a different set of placekeys were pulled during week X of 2019, `normalized_visits_by_total_visits` comes in pre-scaled to relative to every POI in the state's contribution for that week. like you did pull every possible POI for a given what this POI contributed to the sum total for all visits in the state, whether or not it is included in the comparison week, because that week is already comprised of its own `normalized_visits_by_total_visits` that report a `postal_code`'s total contribution, assuming that all placekeys were recorded in its own week. 
# MAGIC 
# MAGIC You don't need to see the same POI in a week- the POIs reported value has been normalized by all possible POIs in that state. Any variation in sampling distribution was discussed above. 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Assumptions
# MAGIC 1. With the `normalized_visits_by_total_visits` field, we can aggregate the observed POIs by `postal_code` under the assumption they are i.i.d.
# MAGIC 2. The POIs were sampled fairly from the postal_codes they were aggregated to. Each individual POI has an equal likelihood of being observed. If that turns out not to be the case, we can define what the allowable variation is in sampling likelihood such that we can trust this is a reliable approximation of the sort of activity you would see in this postal code on a given week. 
# MAGIC 3. The `normalized_visits_by_total_vists` is the share of all observed **visits** that were to this POI.
# MAGIC 4. The distribution of visits to a unique placekey can be modeled as a beta distribution- there will be some popular POIs, especially around major holidays, festivals, or large gatherings to a single location, but most of them will have about the same number of daily visitors. 
# MAGIC 5. The same set of placekeys were requested each week, for all weeks.
# MAGIC 4. Cities are entirely distinct, indpendent observations. What happens in one does not influence another. 
# MAGIC 5. 'Business as usual' means the normalized visits in a week of 202X would be identical
# MAGIC 
# MAGIC ### Questions
# MAGIC 
# MAGIC How far away from 1.0 should a weekly recovery metric be for it to be considered significantly different from the previous year to not be 'business as usual'?
# MAGIC 
# MAGIC What can changes between weekly recovery metrics tell us about the overall recovery pattern of a city?

# COMMAND ----------

# MAGIC %md
# MAGIC # Applying clusters to raw data

# COMMAND ----------

# load in clusters and subset raw feature space according to desired metric
# filter and load clusters by selected metric and period
if dbutils.widgets.get('metric') == 'downtown':
    features = ['total_pop_downtown', 'pct_singlefam_downtown', 'pct_multifam_downtown','pct_renter_downtown','median_age_downtown','bachelor_plus_downtown','median_hhinc_downtown','median_rent_downtown','pct_vacant_downtown','pct_commute_auto_city','pct_commute_public_transit_city','pct_commute_bicycle_city','pct_commute_walk_city','housing_units_downtown','average_commute_time_city','pct_jobs_agriculture_forestry_fishing_hunting','pct_jobs_mining_quarrying_oil_gas', 'pct_jobs_utilities','pct_jobs_construction', 'pct_jobs_manufacturing','pct_jobs_wholesale_trade', 'pct_jobs_retail_trade','pct_jobs_transport_warehouse', 'pct_jobs_information','pct_jobs_finance_insurance','pct_jobs_real_estate','pct_jobs_professional_science_techical','pct_jobs_management_of_companies_enterprises','pct_jobs_administrative_support_waste','pct_jobs_educational_services','pct_jobs_healthcare_social_assistance','pct_jobs_arts_entertainment_recreation','pct_jobs_accomodation_food_services','pct_jobs_public_administration', 'employment_entropy', 'population_density_downtown', 'employment_density_downtown', 'housing_density_downtown','days_school_closing', 'days_workplace_closing','days_cancel_large_events', 'days_cancel_all_events','days_stay_home_requirements', 'days_income_support','days_mask_mandates', 'winter_avg_temp', 'spring_avg_temp', 'summer_avg_temp', 'fall_avg_temp']
    if dbutils.widgets.get('period') == '1':
        metric_clusters = get_table_as_pandas_df("rq_dwtn_clusters_0823_period_1") 
        metric_clusters['description'] = metric_clusters['cluster'].map({'0':'>=1 sd above',
                                                                         '1':'=1 sd below',
                                                                         '2':'average, then decline',
                                                                         '3': 'average, then uptick',
                                                                         '4':'<1 sd above',
                                                                         '5': '>=1 sd above'})
    elif dbutils.widgets.get('period') == '2':
        metric_clusters = get_table_as_pandas_df("rq_dwtn_clusters_0823_period_2") 
        metric_clusters['description'] = metric_clusters['cluster'].map({'0':'=1 sd below',
                                                                         '1':'>=1 sd above',
                                                                         '2':'<1 sd above',
                                                                         '3': '<1 sd below',
                                                                         '4':'<1 sd above',
                                                                         '5': '>=1 sd above'})
    elif dbutils.widgets.get('period') == 'cluster':
        metric_clusters = get_table_as_pandas_df("movement_clusters_downtown_csv") 
        metric_clusters['description'] = metric_clusters['cluster'].map({'0':'no change',
                                                                         '1':'improvement',
                                                                         '2':'decline'})
    
elif dbutils.widgets.get('metric') == 'metro':
    
    features = ['total_pop_city', 'pct_singlefam_city', 'pct_multifam_city','pct_renter_city','median_age_city','bachelor_plus_city','median_hhinc_city','median_rent_city','pct_vacant_city','pct_commute_auto_city','pct_commute_public_transit_city','pct_commute_bicycle_city','pct_commute_walk_city','housing_units_city','average_commute_time_city','pct_jobs_agriculture_forestry_fishing_hunting','pct_jobs_mining_quarrying_oil_gas', 'pct_jobs_utilities','pct_jobs_construction', 'pct_jobs_manufacturing','pct_jobs_wholesale_trade', 'pct_jobs_retail_trade','pct_jobs_transport_warehouse', 'pct_jobs_information','pct_jobs_finance_insurance','pct_jobs_real_estate','pct_jobs_professional_science_techical','pct_jobs_management_of_companies_enterprises','pct_jobs_administrative_support_waste','pct_jobs_educational_services','pct_jobs_healthcare_social_assistance','pct_jobs_arts_entertainment_recreation','pct_jobs_accomodation_food_services','pct_jobs_public_administration', 'employment_entropy', 'population_density_city', 'employment_density_downtown', 'housing_density_city', 'days_school_closing', 'days_workplace_closing','days_cancel_large_events', 'days_cancel_all_events','days_stay_home_requirements', 'days_income_support','days_mask_mandates', 'winter_avg_temp', 'spring_avg_temp', 'summer_avg_temp', 'fall_avg_temp']
    
    
    
elif dbutils.widgets.get('metric') == 'relative':
    
    features = ['total_pop_downtown', 'total_pop_city','pct_singlefam_downtown', 'pct_singlefam_city', 'pct_multifam_downtown','pct_multifam_city','pct_mobile_home_and_others_city', 'pct_renter_downtown','pct_renter_city', 'median_age_downtown', 'median_age_city','bachelor_plus_downtown', 'bachelor_plus_city', 'median_hhinc_downtown','median_hhinc_city', 'median_rent_downtown', 'median_rent_city','pct_vacant_downtown', 'pct_vacant_city', 'pct_commute_auto_downtown', 'pct_commute_auto_city','pct_commute_public_transit_downtown','pct_commute_public_transit_city', 'pct_commute_bicycle_downtown','pct_commute_bicycle_city', 'pct_commute_walk_downtown','pct_commute_walk_city','pct_commute_others_city', 'housing_units_downtown','housing_units_city', 'average_commute_time_downtown','average_commute_time_city','pct_jobs_agriculture_forestry_fishing_hunting','pct_jobs_mining_quarrying_oil_gas', 'pct_jobs_utilities','pct_jobs_construction', 'pct_jobs_manufacturing','pct_jobs_wholesale_trade', 'pct_jobs_retail_trade','pct_jobs_transport_warehouse', 'pct_jobs_information','pct_jobs_finance_insurance', 'pct_jobs_real_estate','pct_jobs_professional_science_techical','pct_jobs_management_of_companies_enterprises','pct_jobs_administrative_support_waste','pct_jobs_educational_services','pct_jobs_healthcare_social_assistance','pct_jobs_arts_entertainment_recreation','pct_jobs_accomodation_food_services','pct_jobs_public_administration', 'employment_entropy','population_density_downtown', 'population_density_city','employment_density_downtown', 'housing_density_downtown','housing_density_city', 'days_school_closing', 'days_workplace_closing','days_cancel_large_events', 'days_cancel_all_events','days_stay_home_requirements', 'days_income_support','days_mask_mandates', 'winter_avg_temp', 'spring_avg_temp', 'summer_avg_temp', 'fall_avg_temp']
    
    #TODO replicate periods assignment for lq like downtown
    if dbutils.widgets.get('period') == '1':
        metric_clusters = get_table_as_pandas_df("rq_dwtn_clusters_0823_period_1") 
        metric_clusters['description'] = metric_clusters['cluster'].map({'0':'>=1 sd above',
                                                                         '1':'=1 sd below',
                                                                         '2':'average, then decline',
                                                                         '3': 'average, then uptick',
                                                                         '4':'<1 sd above',
                                                                         '5': '>=1 sd above'})
    elif dbutils.widgets.get('period') == '2':
        metric_clusters = get_table_as_pandas_df("rq_dwtn_clusters_0823_period_2") 
        metric_clusters['description'] = metric_clusters['cluster'].map({'0':'=1 sd below',
                                                                         '1':'>=1 sd above',
                                                                         '2':'<1 sd above',
                                                                         '3': '<1 sd below',
                                                                         '4':'<1 sd above',
                                                                         '5': '>=1 sd above'})
# for consistency's sake- sorry québécois
metric_clusters = metric_clusters.replace('Québec','Quebec')
metric_clusters = metric_clusters.replace('Montréal','Montreal')

# create rfc_table by metric
rfc_table = raw_features[['city'] + features]
rfc_table = rfc_table[~rfc_table['city'].isin(['Dallas','Orlando','Mississauga','Hamilton','Oklahoma City'])]
rfc_table = rfc_table.replace('Québec','Quebec')
rfc_table = rfc_table.replace('Montréal','Montreal')
rfc_table.loc[:,'cluster'] = rfc_table.loc[:,'city'].map(dict(zip(metric_clusters['city'], metric_clusters['cluster'])))

all_sampled_records_w_clusters = all_sampled_records.merge(metric_clusters, on = 'city', how = 'inner')
all_sampled_records_w_clusters.head()

# COMMAND ----------

#X_wide = X.pivot_table(columns = 'city', index = ['week_num', 'is_downtown'], values = 'total_state_visits_est').reset_index()
#X_wide = X_wide.rename(columns = {'week_city' : 'city', 'week_postal_code' : 'postal_code', 'week_is_downtown' : 'is_downtown'}).drop(columns = 'postal_code')
#X_wide.groupby(['city', 'is_downtown']).transform(lambda x : (x - x.mean()) / x.std()).reset_index() # now look at their normalized values - how different are they from the expected errors 
#X_wide

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Recovery metric and current weekly diff

# COMMAND ----------

# pretty
sns.set_style('darkgrid')
sns.set_context('talk')
# each city should have had about the same estimation for their weekly total visits to the state normalization constant
# there is some variation between postal_codes, placekey presence from week to week, but the VAST majority of samples consistently estimate the same normalizing constant (population param - mean)for a given state
X = all_sampled_records
X_w = X.sort_values(by = 'date_range_start')[['city', 'postal_code', 'date_range_start', 'is_downtown', 'raw_visit_counts', 'normalized_visits_by_total_visits']]
X_w = X.groupby(['date_range_start', 'city', 'is_downtown']).sum().reset_index() # sums all vals seen per postal_code per city per week per downtown status
#X_w['total_state_visits_est'] = (X_w['raw_visit_counts'] / X_w['normalized_visits_by_total_visits']) -  X_w['raw_visit_counts'] # this guess should be the same for all cities in the state. let's allow normal errors- are there any cities with estimates that are excessively off? if so, the degree to which they are off compared to other estimations of the same value may expose some sampling inconsistencies 
X_w = X_w.groupby(['date_range_start', 'city', 'is_downtown']).mean().reset_index()

hue_order = np.unique(X_w['city'].sort_values())
X_w['total_state_visits_diff'] = X_w.sort_values(by = 'date_range_start').groupby(['city', 'postal_code', 'is_downtown'])['normalized_visits_by_total_visits'].transform(lambda x : np.diff(x, 1, prepend = 0))
X_w = X_w[X_w['date_range_start'] > pd.to_datetime(dbutils.widgets.get('week_selector'))]
g_w1 = sns.relplot(x = 'date_range_start', y = 'total_state_visits_est', hue = 'city', style = 'city', hue_order = hue_order, ci = 'sd', col = 'is_downtown', palette = 'colorblind', facet_kws = {'sharey':True}, markers = True, kind = 'line', alpha = .5, data = X_w, height = 10, aspect = 1.5);
g_w1.set_titles("State : " + dbutils.widgets.get('state'));
#g_w2 = sns.relplot(x = 'date_range_start', y = 'total_state_visits_diff', kind = 'line', ci = 'sd',  hue = 'city',style = 'city', palette = 'colorblind', hue_order = hue_order, col = 'is_downtown', markers = True, facet_kws = {'sharey':True}, alpha = .5, data = X_w, height = 10, aspect = 1.5);
g_w2.set_titles("State : " + dbutils.widgets.get('state'));
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # State-level comparisons 
# MAGIC (states with a low number of cities are included in the dropdown, but not really worth exploring in depth here, nor are comparisons between cities with very few downtown postal_code observations)

# COMMAND ----------

from sklearn.preprocessing import MinMaxScaler

# COMMAND ----------

all_weekly_metrics = get_table_as_pandas_df('all_weekly_metrics')
all_weekly_metrics

# COMMAND ----------

all_sampled_records.head()

# COMMAND ----------


def update_X(X, week, state, s):
    
    scaler = MinMaxScaler()
    X_w = X.sort_values(by = 'date_range_start')[['city', 'state', 'is_downtown', 'postal_code', 'date_range_start', 'raw_visit_counts', 'normalized_visits_by_total_visits']]
    X_w = X_w.groupby(['date_range_start', 'city', 'is_downtown']).sum().reset_index().drop(columns = 'postal_code') # sums all vals seen per postal_code per city per week per downtown status
    X_w = X_w.merge(regions_df, on = 'city', how = 'inner')
    #X_w['total_state_visits_est'] = (X_w['raw_visit_counts'] / X_w['normalized_visits_by_total_visits']) -  X_w['raw_visit_counts'] # this guess should be the same for all cities in the state. let's allow normal errors- are there any cities with estimates that are excessively off? if so, the degree to which they are off compared to other estimations of the same value may expose some sampling inconsistencies 
    X_w = X_w[X_w['is_downtown'] == True]
    X_t = X_w
    X_t['est_0'] = X_t['normalized_visits_by_total_visits']
    X_t['est_scaled'] = X_t.sort_values(by = 'date_range_start').groupby(['city'])['normalized_visits_by_total_visits'].transform(lambda x : scipy.ndimage.gaussian_filter1d(x, sigma = s, order = 0))
    X_t['diff_scaled'] = X_t.sort_values(by = 'date_range_start').groupby(['city'])['normalized_visits_by_total_visits'].transform(lambda x : scipy.ndimage.gaussian_filter1d(x, sigma = s, order = 1))
    X_t['diff2_scaled'] = X_t.sort_values(by = 'date_range_start').groupby(['city'])['normalized_visits_by_total_visits'].transform(lambda x : scipy.ndimage.gaussian_filter1d(x, sigma = s, order = 2))
    #X_t['total_state_visits_diff'] = X_t.sort_values(by = 'date_range_start').groupby(['city', 'postal_code', 'is_downtown'])['total_state_visits_est'].transform(lambda x : np.diff(x, 1, prepend = 0))
    #X_t['total_state_visits_diff2'] = X_t.sort_values(by = 'date_range_start').groupby(['city', 'postal_code', 'is_downtown'])['total_state_visits_est'].transform(lambda x : np.diff(x, 2, prepend = [0, 0]))
    
    # first, minmax scale estimate relative to city's guesses over all time
    # for this city, how high was this estimate, AFTER the selected time period?
    
    if state != 'all':
        X_t = X_t[(X_t['date_range_start'] > week) & (X_t['state'] == state)]
    else:
        X_t = X_t[(X_t['date_range_start'] > week)]
    #arr_0 = X_t.groupby(['city', 'is_downtown'])['est_scaled'].transform(lambda x : x.values).to_numpy().reshape(-1, 1)
    #arr_1 = X_t.groupby(['city', 'is_downtown'])['diff_scaled'].transform(lambda x : x.values).to_numpy().reshape(-1,1)
    #arr_2 = X_t.groupby(['city', 'is_downtown'])['diff2_scaled'].transform(lambda x : x.values).to_numpy().reshape(-1,1)
    #X_t['est_scaled'] = scaler.fit_transform(arr_0)
    #X_t['diff_scaled'] = scaler.fit_transform(arr_1)
    #X_t['diff2_scaled'] =  scaler.fit_transform(arr_2)
    
    # now scale across week
    #X_t['est_scaled'] = X_t.groupby(['date_range_start', 'is_downtown'])['est_city_scaled'].transform(lambda x : -1 * (x - np.nanmean(x)) / np.nanstd(x)) # compared to other cities in the state this week, what was your guess 
    #X_t['diff_scaled'] = X_t.groupby(['date_range_start', 'is_downtown'])['diff_city_scaled'].transform(lambda x : -1 * (x - np.nanmean(x)) / np.nanstd(x))
    #X_t['diff2_scaled'] =  X_t.groupby(['date_range_start', 'is_downtown'])['diff2_city_scaled'].transform(lambda x : -1 * (x - np.nanmean(x)) / np.nanstd(x))
    X_t = X_t.drop(columns = 'normalized_visits_by_total_visits')
    return X_t

# COMMAND ----------

X_w = X.sort_values(by = 'date_range_start')[['city', 'state', 'is_downtown', 'postal_code', 'date_range_start', 'raw_visit_counts', 'normalized_visits_by_total_visits']]
X_w = X_w.groupby(['date_range_start', 'city', 'is_downtown']).sum().reset_index() # sums all vals seen per postal_code per city per week per downtown status
    #X_w['total_state_visits_est'] = (X_w['raw_visit_counts'] / X_w['normalized_visits_by_total_visits']) -  X_w['raw_visit_counts'] # this guess should be the same for all cities in the state. let's allow normal errors- are there any cities with estimates that are excessively off? if so, the degree to which they are off compared to other estimations of the same value may expose some sampling inconsistencies 
X_w = X_w.merge(regions_df, on = 'city', how = 'inner').drop(columns = 'postal_code')
X_w = X_w[X_w['is_downtown'] == True]
X_t = X_w
#X_t['est_scaled'] = X_t.sort_values(by = 'date_range_start').groupby(['city'])['normalized_visits_by_total_visits'].transform(lambda x : scipy.ndimage.gaussian_filter1d(x, sigma = 2, order = 0))
#X_t['diff_scaled'] = X_t.sort_values(by = 'date_range_start').groupby(['city'])['normalized_visits_by_total_visits'].transform(lambda x : scipy.ndimage.gaussian_filter1d(x, sigma = 2, order = 1))
#X_t['diff2_scaled'] = X_t.sort_values(by = 'date_range_start').groupby(['city'])['normalized_visits_by_total_visits'].transform(lambda x : scipy.ndimage.gaussian_filter1d(x, sigma = 2, order = 2))
    #X_t['total_state_visits_diff'] = X_t.sort_values(by = 'date_range_start').groupby(['city', 'postal_code', 'is_downtown'])['total_state_visits_est'].transform(lambda x : np.diff(x, 1, prepend = 0))
    #X_t['total_state_visits_diff2'] = X_t.sort_values(by = 'date_range_start').groupby(['city', 'postal_code', 'is_downtown'])['total_state_visits_est'].transform(lambda x : np.diff(x, 2, prepend = [0, 0]))
    
    # first, minmax scale estimate relative to city's guesses over all time
    # for this city, how high was this estimate, AFTER the selected time period?
#X_t = X_t[(X_t['date_range_start'] > week) & (X_t['state'] == state)]
X_tX_w = X.sort_values(by = 'date_range_start')[['city', 'state', 'is_downtown', 'postal_code', 'date_range_start', 'raw_visit_counts', 'normalized_visits_by_total_visits']]
X_w = X_w.groupby(['date_range_start', 'city', 'is_downtown']).sum().reset_index() # sums all vals seen per postal_code per city per week per downtown status
    #X_w['total_state_visits_est'] = (X_w['raw_visit_counts'] / X_w['normalized_visits_by_total_visits']) -  X_w['raw_visit_counts'] # this guess should be the same for all cities in the state. let's allow normal errors- are there any cities with estimates that are excessively off? if so, the degree to which they are off compared to other estimations of the same value may expose some sampling inconsistencies 
X_w = X_w.merge(regions_df, on = 'city', how = 'inner').drop(columns = 'postal_code')
X_w = X_w[X_w['is_downtown'] == True]
X_t = X_w
#X_t['est_scaled'] = X_t.sort_values(by = 'date_range_start').groupby(['city'])['normalized_visits_by_total_visits'].transform(lambda x : scipy.ndimage.gaussian_filter1d(x, sigma = 2, order = 0))
#X_t['diff_scaled'] = X_t.sort_values(by = 'date_range_start').groupby(['city'])['normalized_visits_by_total_visits'].transform(lambda x : scipy.ndimage.gaussian_filter1d(x, sigma = 2, order = 1))
#X_t['diff2_scaled'] = X_t.sort_values(by = 'date_range_start').groupby(['city'])['normalized_visits_by_total_visits'].transform(lambda x : scipy.ndimage.gaussian_filter1d(x, sigma = 2, order = 2))
    #X_t['total_state_visits_diff'] = X_t.sort_values(by = 'date_range_start').groupby(['city', 'postal_code', 'is_downtown'])['total_state_visits_est'].transform(lambda x : np.diff(x, 1, prepend = 0))
    #X_t['total_state_visits_diff2'] = X_t.sort_values(by = 'date_range_start').groupby(['city', 'postal_code', 'is_downtown'])['total_state_visits_est'].transform(lambda x : np.diff(x, 2, prepend = [0, 0]))
    
    # first, minmax scale estimate relative to city's guesses over all time
    # for this city, how high was this estimate, AFTER the selected time period?
#X_t = X_t[(X_t['date_range_start'] > week) & (X_t['state'] == state)]
X_t

# COMMAND ----------

metrics = get_table_as_pandas_df('0714_combined_metrics_df')
all_clusters['city'] = all_clusters['city'].str.replace('é', 'e')
metrics['city'] = metrics['city'].str.replace('é', 'e')
metrics['week'] = pd.to_datetime(metrics['week'].values)
metrics_w_clusters = metrics.merge(all_clusters, on = ['city', 'metric'], how = 'inner').drop_duplicates(subset = ['city', 'week', 'metric']).drop(columns = ['raw_visit_counts', 'normalized_visits_by_state_scaling'])
metrics_w_clusters

# COMMAND ----------

X = all_sampled_records
X = X[~X['city'].isin(["Dallas", "Mississauga", "Orlando", "Oklahoma City", "Hamilton"])]
X_t = update_X(X, pd.to_datetime(dbutils.widgets.get('week_selector')), 'all', 3)
X_c = X_t.merge(metrics_w_clusters[metrics_w_clusters['metric'] == dbutils.widgets.get('metric')], left_on = ['city', 'date_range_start'], right_on = ['city', 'week'],  how = 'inner')

hue_order_t = np.unique(X_c['cluster'].sort_values())
ax_t = sns.relplot(x = 'date_range_start', y = 'normalized_visits_by_total_visits', data = X_c, kind = 'line', palette = 'colorblind',  hue = 'cluster', style = 'cluster', hue_order = hue_order_t, alpha = .5, height = 10, aspect = 1.5);
ax_t = sns.relplot(x = 'date_range_start', y = 'est_0', data = X_c, kind = 'line', palette = 'colorblind',  hue = 'cluster', style = 'cluster', hue_order = hue_order_t, alpha = .5, height = 10, aspect = 1.5);
ax_t = sns.relplot(x = 'date_range_start', y = 'est_scaled', data = X_c, kind = 'line', palette = 'colorblind', hue = 'cluster', style = 'cluster', hue_order = hue_order_t, alpha = .5, height = 10, aspect = 1.5);
#ax_t = sns.relplot(x = 'date_range_start', y = 'diff_scaled', data = X_c, kind = 'line', palette = 'colorblind', hue = 'cluster', style = 'cluster', hue_order = hue_order_t, alpha = .5, height = 10, aspect = 1.5);
ax_w = sns.displot(x =  'normalized_visits_by_total_visits',  data = X_c, palette = 'colorblind', kind = 'kde', hue = 'cluster', hue_order = hue_order_t, height = 10, aspect = 1);
ax_w = sns.displot(x =  'est_scaled',  data = X_c, palette = 'colorblind', kind = 'kde', hue = 'cluster', hue_order = hue_order_t, height = 10, aspect = 1);
ax_w = sns.displot(x =  'diff_scaled',  data = X_c, palette = 'colorblind', kind = 'kde', hue = 'cluster', hue_order = hue_order_t, height = 10, aspect = 1);
plt.show();

# COMMAND ----------

X = all_sampled_records
X = X[~X['city'].isin(["Dallas", "Mississauga", "Orlando", "Oklahoma City", "Hamilton"])]
X_t = update_X(X, pd.to_datetime(dbutils.widgets.get('week_selector')), 'all', 3)
X_c = X_t.merge(metrics_w_clusters[metrics_w_clusters['metric'] == dbutils.widgets.get('metric')], left_on = ['city', 'date_range_start'], right_on = ['city', 'week'],  how = 'inner')
hue_order_t = np.unique(X_c['cluster'].sort_values())
ax_t = sns.relplot(x = 'est_scaled', y = 'diff_scaled', kind = 'scatter', data = X_c, palette = 'colorblind', size = 'normalized_visits_by_total_visits', hue = 'cluster', style = 'cluster', hue_order = hue_order_t, alpha = .5, height = 10, aspect = 1.5);
ax_w = sns.displot(x =  'normalized_visits_by_total_visits',  kind = 'ecdf', data = X_c, palette = 'colorblind',  hue = 'cluster', hue_order = hue_order_t, height = 16, aspect = 1);
plt.show();

# COMMAND ----------

X_t

# COMMAND ----------

# MAGIC %md
# MAGIC # Distribution of population parameter estimates by week

# COMMAND ----------

X = all_sampled_records[all_sampled_records['state'] == dbutils.widgets.get('state')]
#X = X[~X['city'].isin(["El Paso", "Ottawa", "San Diego", "Columbus",  "Nashville", "Orlando", "Oklahoma City", "Hamilton"])]
X_t = update_X(X, pd.to_datetime(dbutils.widgets.get('week_selector')), dbutils.widgets.get('state'), 3)
hue_order_t = np.unique(X_t['city'].sort_values())
ax_t = sns.relplot(x = 'est_scaled', y = 'diff_scaled', data = X_t, palette = 'colorblind', hue = 'city', style = 'city', hue_order = hue_order_t, height = 10, aspect = 1.5);
plt.show();

# COMMAND ----------

X = all_sampled_records[all_sampled_records['state'] == dbutils.widgets.get('state')]
#X = X[~X['city'].isin(["El Paso", "Ottawa", "San Diego", "Columbus",  "Nashville", "Orlando", "Oklahoma City", "Hamilton"])]
X_t = update_X(X, pd.to_datetime(dbutils.widgets.get('week_selector')), dbutils.widgets.get('state'), 3)
hue_order_t = np.unique(X_t['city'].sort_values())

#ax_t2 = sns.relplot(x = 'diff_scaled', y = 'diff2_scaled', data = X_t, palette = 'colorblind', hue = 'city', hue_order = hue_order_t, height = 10, aspect = 1.5);
ax_t1 = sns.jointplot(x = 'diff_scaled',y = 'diff2_scaled', data = X_t, palette = 'colorblind', hue = 'city', hue_order = hue_order_t, height = 14);
plt.show();

# COMMAND ----------

# MAGIC %md
# MAGIC # Distribution of scaled estimates for all cities

# COMMAND ----------

#X_s = all_sampled_records[~all_sampled_records['city'].isin(["El Paso", "Ottawa", "San Diego", "Columbus",  "Dallas", "Nashville", "Orlando", "Oklahoma City", "Hamilton"])]
X_s = all_sampled_records
X_s = X_s.sort_values(by = 'date_range_start')[['city', 'postal_code', 'state', 'date_range_start', 'is_downtown', 'raw_visit_counts', 'normalized_visits_by_total_visits']]
X_w = X.groupby(['date_range_start', 'city', 'is_downtown']).sum().reset_index() # sums all vals seen per postal_code per city per week per downtown status
#X_s['total_state_visits_est'] = (X_s['raw_visit_counts'] / X_s['normalized_visits_by_total_visits']) -  X_s['raw_visit_counts'] # this guess should be the same for all cities in the state. let's allow normal errors- are there any cities with estimates that are excessively off? if so, the degree to which they are off compared to other estimations of the same value may expose some sampling inconsistencies 
X_s = X_s[X_s['is_downtown'] == True]
X_t = X_s
X_t['est_scaled'] = X_t.sort_values(by = 'date_range_start').groupby(['city'])['normalized_visits_by_total_visits'].transform(lambda x : scipy.ndimage.gaussian_filter1d(x, sigma = 3, order = 0))
X_t['diff_scaled'] = X_t.sort_values(by = 'date_range_start').groupby(['city'])['normalized_visits_by_total_visits'].transform(lambda x : scipy.ndimage.gaussian_filter1d(x, sigma = 3, order = 1))
X_t['diff2_scaled'] = X_t.sort_values(by = 'date_range_start').groupby(['city'])['normalized_visits_by_total_visits'].transform(lambda x : scipy.ndimage.gaussian_filter1d(x, sigma = 3, order = 2))


#X_t['total_state_visits_diff'] = X_t.sort_values(by = 'date_range_start').groupby(['city', 'postal_code', 'is_downtown'])['total_state_visits_est'].transform(lambda x : np.diff(x, 1, prepend = 0))
#X_t['total_state_visits_diff2'] = X_t.sort_values(by = 'date_range_start').groupby(['city', 'postal_code', 'is_downtown'])['total_state_visits_est'].transform(lambda x : np.diff(x, 2, prepend = [0, 0]))
#X_t['est_scaled'] = X_t.groupby(['date_range_start', 'city', 'is_downtown'])['total_state_visits_est'].transform(lambda x : (x - np.nanmean(x)) / np.nanstd(x)) # compared to other cities in the state this week, what was your guess 
#X_t['diff_scaled'] = X_t.groupby(['date_range_start', 'city', 'is_downtown'])['total_state_visits_diff'].transform(lambda x : (x - np.nanmean(x)) / np.nanstd(x))
#X_t['diff2_scaled'] =  X_t.groupby(['date_range_start', 'city', 'is_downtown'])['total_state_visits_diff2'].transform(lambda x : (x - np.nanmean(x)) / np.nanstd(x))

X_plot = X_t[X_t['date_range_start'] > pd.to_datetime(dbutils.widgets.get('week_selector'))]
X_plot['max'] = X_plot.groupby('city')['est_scaled'].transform('max')
hue_order_plot = X_plot.sort_values(['max', 'city', 'est_scaled'], ascending = False).drop('max', axis = 1)['city'].unique()
ax_t3 = sns.relplot(x = 'date_range_start', y = 'est_scaled', hue = 'city', style = 'city',  hue_order = hue_order_plot, units = 'city', data = X_plot, height = 12, aspect = 1);
plt.show();

# COMMAND ----------

X_plot = X_t[X_t['date_range_start'] > pd.to_datetime(dbutils.widgets.get('week_selector'))]
X_plot['max'] = X_plot.groupby('city')['diff2_scaled'].transform('max')

hue_order_plot = X_plot.sort_values(['max', 'city', 'diff2_scaled'], ascending = False).drop('max', axis = 1)['city'].unique()
ax_t3 = sns.relplot(x = 'est_scaled', y = 'diff_scaled', hue = 'city', style = 'city',  units = 'city', hue_order = hue_order_plot, data = X_plot, height = 12, aspect = 1);
plt.show();

# COMMAND ----------

# who are these cities
est_scaled_thresh = .02
#diff_scaled_thresh = .002

# lo and behold, it's Washington DC, Las Vegas, Omaha, Honolulu, Albuquerque, Salt Lake City, Oklahoma City 
X_plot['max'] = X_plot.groupby('city')['est_scaled'].transform('max')
X_plot.sort_values(['max', 'city', 'est_scaled'], ascending = False).drop('max', axis = 1)['city'].unique()
X_group = X_plot[(X_plot['est_scaled'] > est_scaled_thresh)]
ax_t3 = sns.relplot(x = 'diff_scaled', y = 'est_scaled', hue = 'city', hue_order = X_group.sort_values(by = 'est_scaled')['city'].unique(), data = X_group, height = 10);
plt.show();

# COMMAND ----------

#X_s = all_sampled_records[~all_sampled_records['city'].isin(["Dallas", "Orlando", "Oklahoma City", "Hamilton"])]
X_s = all_sampled_records
X_s = X_s.sort_values(by = 'date_range_start')[['city', 'postal_code', 'state', 'date_range_start', 'is_downtown', 'raw_visit_counts', 'normalized_visits_by_total_visits']]
X_w = X.groupby(['date_range_start', 'city', 'is_downtown']).sum().reset_index() # sums all vals seen per postal_code per city per week per downtown status
#X_s['total_state_visits_est'] = (X_s['raw_visit_counts'] / X_s['normalized_visits_by_total_visits']) -  X_s['raw_visit_counts'] # this guess should be the same for all cities in the state. let's allow normal errors- are there any cities with estimates that are excessively off? if so, the degree to which they are off compared to other estimations of the same value may expose some sampling inconsistencies 
X_s = X_s[X_s['is_downtown'] == True]
X_t = X_s
X_t['est_scaled'] = X_t.sort_values(by = 'date_range_start').groupby(['city'])['normalized_visits_by_total_visits'].transform(lambda x : scipy.ndimage.gaussian_filter1d(x, sigma = 2, order = 0))
X_t['diff_scaled'] = X_t.sort_values(by = 'date_range_start').groupby(['city'])['normalized_visits_by_total_visits'].transform(lambda x : scipy.ndimage.gaussian_filter1d(x, sigma = 2, order = 1))
X_t['diff2_scaled'] = X_t.sort_values(by = 'date_range_start').groupby(['city'])['normalized_visits_by_total_visits'].transform(lambda x : scipy.ndimage.gaussian_filter1d(x, sigma = 2, order = 2))


#X_t['total_state_visits_diff'] = X_t.sort_values(by = 'date_range_start').groupby(['city', 'postal_code', 'is_downtown'])['total_state_visits_est'].transform(lambda x : np.diff(x, 1, prepend = 0))
#X_t['total_state_visits_diff2'] = X_t.sort_values(by = 'date_range_start').groupby(['city', 'postal_code', 'is_downtown'])['total_state_visits_est'].transform(lambda x : np.diff(x, 2, prepend = [0, 0]))
#X_t['est_scaled'] = X_t.groupby(['date_range_start', 'city', 'is_downtown'])['total_state_visits_est'].transform(lambda x : (x - np.nanmean(x)) / np.nanstd(x)) # compared to other cities in the state this week, what was your guess 
#X_t['diff_scaled'] = X_t.groupby(['date_range_start', 'city', 'is_downtown'])['total_state_visits_diff'].transform(lambda x : (x - np.nanmean(x)) / np.nanstd(x))
#X_t['diff2_scaled'] =  X_t.groupby(['date_range_start', 'city', 'is_downtown'])['total_state_visits_diff2'].transform(lambda x : (x - np.nanmean(x)) / np.nanstd(x))

X_t = X_t[X_t['date_range_start'] > pd.to_datetime(dbutils.widgets.get('week_selector'))]
#hue_order_t = np.unique(X_t['city'].sort_values())

# COMMAND ----------

# downtowns
downtown_clusters = get_table_as_pandas_df('rq_dwtn_clusters_0822')
downtown_cluster_centers = get_table_as_pandas_df('rq_dwtn_cluster_centers_0822')

# relative
lq_clusters = get_table_as_pandas_df('lq_clusters_0822')
lq_cluster_centers = get_table_as_pandas_df('lq_cluster_centers_0822')

# full city
city_clusters = get_table_as_pandas_df('rq_city_clusters_0822')
city_cluster_centers = get_table_as_pandas_df('rq_city_cluster_centers_0822')

# COMMAND ----------

lq_clusters = get_table_as_pandas_df("lq_clusters_0822") 
rq_dwtn_clusters = get_table_as_pandas_df("rq_dwtn_clusters_0822") 
rq_city_clusters = get_table_as_pandas_df("rq_city_clusters_0822") 

# COMMAND ----------

downtown_clusters['metric'] = 'downtown'
lq_clusters['metric'] = 'relative'
city_clusters['metric'] = 'metro'

all_clusters = pd.concat([downtown_clusters, lq_clusters, city_clusters])
all_clusters

# COMMAND ----------

def plot_cluster_centroids(df):
    df.plot()
    return df

# COMMAND ----------

downtown_cluster_centers.shape

# COMMAND ----------

centers = get_table_as_pandas_df('thirty_week_centroids_2')
test = centers.sort_values('cluster_index')
test.set_index('cluster_index').plot()

# COMMAND ----------

metrics = get_table_as_pandas_df('0714_combined_metrics_df')
all_clusters['city'] = all_clusters['city'].str.replace('é', 'e')
metrics['city'] = metrics['city'].str.replace('é', 'e')
metrics_w_clusters = metrics.merge(all_clusters, on = ['city', 'metric'], how = 'inner')
metrics_w_clusters

# COMMAND ----------

# MAGIC %md
# MAGIC # Weekly downtown RQ observations

# COMMAND ----------

def update_X_metrics(X, week, state, s):
    
    X_w = X.sort_values(by = 'week')[['city', 'week', 'normalized_visits_by_total_visits', 'cluster']]
    #X_w = X_w.groupby(['week', 'city']).sum().reset_index() # sums all vals seen per postal_code per city per week per downtown status
    #X_w['total_state_visits_est'] = (X_w['raw_visit_counts'] / X_w['normalized_visits_by_total_visits']) -  X_w['raw_visit_counts'] # this guess should be the same for all cities in the state. let's allow normal errors- are there any cities with estimates that are excessively off? if so, the degree to which they are off compared to other estimations of the same value may expose some sampling inconsistencies 
    X_w = X_w.merge(regions_df, on = 'city', how = 'inner')
    X_t = X_w

    X_t['est_scaled'] = X_t.sort_values(by = 'week').groupby(['city'])['normalized_visits_by_total_visits'].transform(lambda x : scipy.ndimage.gaussian_filter1d(x, sigma = s, order = 0))
    X_t['diff_scaled'] = X_t.sort_values(by = 'week').groupby([ 'city'])['normalized_visits_by_total_visits'].transform(lambda x : scipy.ndimage.gaussian_filter1d(x, sigma = s, order = 1))
    X_t['diff2_scaled'] = X_t.sort_values(by = 'week').groupby(['city'])['normalized_visits_by_total_visits'].transform(lambda x : scipy.ndimage.gaussian_filter1d(x, sigma = s, order = 2))
    #X_t['diff_scaled'] = X_t.sort_values(by = 'week').groupby([ 'city', 'cluster'])['est_scaled'].transform(lambda x : np.diff(x, 1, prepend = 0))
    #X_t['est_scaled'] = X_t.sort_values(by = 'week').groupby(['city'])['normalized_visits_by_total_visits'].transform(lambda x : x.rolling(window=9).mean().shift(-4))
    #X_t['diff_scaled'] = X_t.sort_values(by = 'week').groupby([ 'city'])['est_scaled'].transform(lambda x : np.diff(x, 1, prepend = 0))
    X_t['week'] = pd.to_datetime(X_t['week'])
    if state != 'all':
        X_t = X_t[(X_t['week'] > week) & (X_t['state'] == state)]
    else:
        X_t = X_t[(X_t['week'] > week)]
    return X_t

# COMMAND ----------

#metrics = all_weekly_metrics

def format_ts_numpy(metric, scaling, drop_cities, s):
    rq_df = metrics.copy()
    rq_df = metrics[metrics['metric'] == metric][['city', scaling, 'week']]
    rq_df = rq_df.sort_values(by = 'week')[['city', 'week', 'normalized_visits_by_total_visits']]
    rq_df = rq_df.dropna()
    rq_df = rq_df.fillna(0)
    rq_df['normalized_visits_by_total_visits'] = rq_df.sort_values(by = 'week').groupby(['city'])['normalized_visits_by_total_visits'].transform(lambda x : scipy.ndimage.gaussian_filter1d(x, sigma = s, order = 0))
    #rq_df['diff_scaled'] = rq_df.sort_values(by = 'week').groupby(['city'])['normalized_visits_by_total_visits'].transform(lambda x : scipy.ndimage.gaussian_filter1d(x, sigma = s, order = 1))
    #rq_df['diff2_scaled'] = rq_df.sort_values(by = 'week').groupby(['city'])['normalized_visits_by_total_visits'].transform(lambda x : scipy.ndimage.gaussian_filter1d(x, sigma = s, order = 2))
    cols_to_drop = drop_cities + ['Hamilton', 'Montreal', 'Quebec']
    rq_df = rq_df.pivot_table(columns = 'city', index = 'week', values = 'normalized_visits_by_total_visits').reset_index()
    rq_df = rq_df.drop(columns=cols_to_drop) 
    
    return rq_df, rq_df
ts_format_dt_rq, rq_df = format_ts_numpy('downtown', 'normalized_visits_by_total_visits', ['Dallas', 'Orlando', 'Oklahoma City',  'Hamilton'], 3)
df = pd.DataFrame(ts_format_dt_rq)
df[df.columns[1:5]].plot()

# COMMAND ----------

X_t

# COMMAND ----------

X_s = metrics_w_clusters[metrics_w_clusters['metric'] == dbutils.widgets.get('metric')]
X_s = X_s[~X_s['city'].isin(["Hamilton", "Dallas", "Orlando", "Oklahoma City"])]
X_t = update_X_metrics(X_s, pd.to_datetime(dbutils.widgets.get('week_selector')), 'all', 3)
keep_clusters = X_t.groupby('cluster').size().sort_values() / len(X_t['week'].unique()) >= 5
X_t = X_t[X_t['cluster'].isin(keep_clusters[keep_clusters].index.values)]
#X_t[['est_scaled', 'diff_scaled', 'diff2_scaled']] = X_t[['est_scaled', 'diff_scaled', 'diff2_scaled']].fillna(0).astype(float)
#X_t['approx'] = X_t[['est_scaled', 'diff_scaled', 'diff2_scaled']].sum(axis = 1)
#X_t['cluster'] = X_t.groupby('week')['approx'].transform(lambda x : (100 * x) % 3 )
#X_t['max'] = X_t.groupby('cluster')['est_scaled'].transform('max')
#hue_order = X_t.sort_values(['max', 'cluster', 'est_scaled'], ascending = False).drop('max', axis = 1)['cluster'].unique()
ax_t0 = sns.jointplot(x = 'est_scaled',  y = 'diff_scaled', palette = 'colorblind',  hue = 'cluster',  data = X_t,  height = 16);
#ax_t1 = sns.displot(x = 'diff2_scaled',  palette = 'colorblind', hue = 'cluster', data = X_t, height = 16);
ax_t0.fig.suptitle('Joint distribution of ' + dbutils.widgets.get('metric') + ' recovery metric and its first difference',  y = 1.01)
plt.show();

# COMMAND ----------

all_weekly_metrics['week'].max()

# COMMAND ----------

dbutils.widgets.dropdown('metric', 'downtown', ['downtown', 'metro', 'relative'])

# COMMAND ----------

X_plot

# COMMAND ----------

#import plotly.express as px


#X_s = metrics_w_clusters[metrics_w_clusters['metric'] == dbutils.widgets.get('metric')]
#X_s = X_s[~X_s['city'].isin(["Hamilton", "Dallas", "Orlando", "Oklahoma City"])]
#X_t = update_X_metrics(X_s, pd.to_datetime(dbutils.widgets.get('week_selector')), dbutils.widgets.get('state'), 3)
#X_t = update_X_metrics(X_s, pd.to_datetime(dbutils.widgets.get('week_selector')), 'all', 3) # change this to dbutils.widgets.get('state') to look at it by state
#X_t['max'] = X_t.groupby('city')['est_scaled'].transform('max')
#hue_order = X_t.sort_values(['max', 'city', 'est_scaled'], ascending = False).drop('max', axis = 1)['city'].unique()
#X_plot = X_s[X_s['cluster'] == int(dbutils.widgets.get('cluster_select'))]
#X_plot = X_plot.sort_values(by = 'week')[['city', 'cluster', 'week', 'normalized_visits_by_total_visits']]
#X_plot = X_plot.drop_duplicates(subset = ['city', 'week'])
#X_plot = X_plot.dropna()
#X_plot['nvt_rolling'] = X_plot.groupby('city')['normalized_visits_by_total_visits'].transform(lambda x : x.rolling(10, min_periods=1, center=True).mean())
#ax_t3 = sns.relplot(x = 'week', y = 'normalized_visits_by_total_visits', units = 'city', kind = 'line', estimator = None, data = X_plot, hue = 'city',  style = 'city', palette = 'colorblind', height = 10, aspect = 1.5);
#X_s = X_s.sort_values(by = 'week')
#fig = px.line(X_plot, x="week", y="nvt_rolling", line_group = 'city', color='city', title = 'Cluster ' + dbutils.widgets.get('cluster_select') + ' - ' + dbutils.widgets.get('metric'))
#fig.show();

# COMMAND ----------

X_s = all_weekly_metrics[all_weekly_metrics['metric'] == 'downtown']
X_s = X_s[~X_s['city'].isin(["Hamilton", "Dallas", "Orlando", "Oklahoma City"])]
X_t = update_X_metrics(X_s, pd.to_datetime(dbutils.widgets.get('week_selector')), dbutils.widgets.get('state'), 3)

ax_t = sns.relplot(x = 'week', y = 'est_scaled', units = 'city', data = X_t, hue = 'diff_scaled', size = 'diff_scaled', palette = 'coolwarm', height = 10,  aspect = 1.5);
plt.show()

# COMMAND ----------

X_s = all_weekly_metrics[all_weekly_metrics['metric'] == 'downtown']
X_s = X_s[~X_s['city'].isin(["Hamilton", "Dallas", "Orlando", "Oklahoma City"])]
X_t = update_X_metrics(X_s, pd.to_datetime(dbutils.widgets.get('week_selector')), 'all', 3)
X_t['max'] = X_t.groupby('city')['est_scaled'].transform('max')
hue_order = X_t.sort_values(['max', 'city', 'est_scaled'], ascending = False).drop('max', axis = 1)['city'].unique()
ax_t0 = sns.relplot(x = 'diff_scaled', y = 'diff2_scaled', units = 'city', estimator = None,  data = X_t, hue = 'est_scaled', height = 10, aspect = 1.5);
#ax_t1 = sns.relplot(x = 'est_scaled', y = 'diff2_scaled', units = 'postal_code', estimator = None, data = X_t, palette = 'coolwarm', hue = 'est_scaled', size =  'est_scaled', height = 10, aspect = 1.5);
plt.show();

# COMMAND ----------

X_s = all_weekly_metrics[all_weekly_metrics['metric'] == 'downtown']
X_s = X_s[~X_s['city'].isin(["Hamilton", "Dallas", "Mississauga", "Orlando", "Oklahoma City"])]
X_t = update_X_metrics(X_s, pd.to_datetime(dbutils.widgets.get('week_selector')), 'all', 3)
X_t[['est_scaled', 'diff_scaled', 'diff2_scaled']] = X_t[['est_scaled', 'diff_scaled', 'diff2_scaled']].fillna(0).astype(float)
X_t['approx'] = X_t[['est_scaled', 'diff_scaled', 'diff2_scaled']].sum(axis = 1)
#X_t['cluster'] = X_t.groupby('week')['approx'].transform(lambda x : (100 * x) % 3 )
X_t['max'] = X_t.groupby('city')['est_scaled'].transform('max')
hue_order = X_t.sort_values(['max', 'city', 'est_scaled'], ascending = False).drop('max', axis = 1)['city'].unique()
ax_t0 = sns.jointplot(x = 'est_scaled', y = 'diff_scaled', palette = 'colorblind', hue = 'city', hue_order = hue_order, data = X_t,  height = 16, legend = False);
ax_t1 = sns.jointplot(x = 'diff2_scaled', y = 'diff_scaled', palette = 'colorblind', hue = 'city',hue_order = hue_order, data = X_t, height = 16, legend = False);
#ax_t0._legend.remove()
plt.show();

# COMMAND ----------

X_wide = X_t.pivot_table(columns = 'city', index = 'week', values = 'normalized_visits_by_total_visits').reset_index()
X_wide

# COMMAND ----------

clusters['city'] = clusters['city'].str.replace("é", "e")
clusters['metric'] = 'downtown'

all_weekly_metrics_w_clusters = all_weekly_metrics.merge(clusters, on = ['metric', 'city'], how = 'inner')
all_weekly_metrics_w_clusters

# COMMAND ----------

canada_post_feb2022 = canada_post_feb2022.merge(sampled_placekeys_can, on = 'placekey', how = 'inner')
canada_post_feb2022['postal_code'] = canada_post_feb2022['postal_code'].fillna(0).astype(int)

# COMMAND ----------

# MAGIC %md
# MAGIC # Distribution of raw_visit_counts for all POI in Canada, for all time

# COMMAND ----------

# select city
dbutils.widgets.dropdown("canadian_cities", "Toronto", np.unique(sampled_placekeys_can['city']).tolist())

# COMMAND ----------

canada_pre_feb2022.head()

# COMMAND ----------

np.shape(canada_pre_feb2022)

# COMMAND ----------

all_canada_device_counts = pd.concat([canada_pre_feb2022.merge(all_city_index, on = 'postal_code', how = 'inner'), canada_post_feb2022[canada_post_feb2022['date_range_start'] > np.max(canada_pre_feb2022['date_range_start'])]])

# COMMAND ----------



# COMMAND ----------

# each week had len(can_placekeys) / 20 searches. the placekeys were requested in the order they appeared in the places dataset- no shuffling or reordering was done
# ~6k samples per week
# draw a sample of size 20 six-thousand times
# the sample is not necessarily always the same order
# there were some requests with empty results, but the sampling was repeated 3 more times on the placekeys that weren't seen that week 
# more unqiue placekeys per postal code == popular 
# pretend every POI had exactly one visitor
weekly_unique_placekey_count = all_canada_device_counts.groupby(['city', 'postal_code', 'date_range_start', 'is_downtown']).size().reset_index()
weekly_unique_placekey_count = weekly_unique_placekey_count.rename(columns = {0 : 'unique_placekey_counts'})
display(weekly_unique_placekey_count)

# COMMAND ----------

weekly_unique_placekey_count['week_num'] = pd.to_datetime(weekly_unique_placekey_count['date_range_start'].values).week.astype(int)
weekly_unique_placekey_count['year'] = pd.to_datetime(weekly_unique_placekey_count['date_range_start'].values).year
weekly_unique_placekey_count.loc[weekly_unique_placekey_count['year'] == 2020, 'week_num'] = weekly_unique_placekey_count.loc[weekly_unique_placekey_count['year'] == 2020, 'week_num'] - 1

# COMMAND ----------

study_weeks = np.unique(weekly_unique_placekey_count.loc[(weekly_unique_placekey_count['city'] == dbutils.widgets.get('canadian_cities')), 'date_range_start'].dt.strftime('%Y-%m-%d')).tolist()

dbutils.widgets.dropdown('week_selector', '2019-01-07', study_weeks)

# COMMAND ----------

# entirety of canada for this week
weekly_unique_placekey_count.loc[weekly_unique_placekey_count['date_range_start'] == dbutils.widgets.get('week_selector'), ['city', 'unique_placekey_counts']].describe().T

# COMMAND ----------

# MAGIC %md
# MAGIC # Number of unique placekeys seen per postal code by year and geography

# COMMAND ----------

# they pretty much always look like this heavy tailed dist
sns.displot(x = 'unique_placekey_counts', col = 'year', row = 'city', discrete = True, multiple = 'stack', hue = 'is_downtown',  bins = 10, kind = 'hist', data = weekly_unique_placekey_count, log_scale = True, stat="probability", facet_kws = {'sharex': False, 'sharey': False})

# COMMAND ----------

ax = sns.relplot(x = 'week_num', y = 'unique_placekey_counts', col = 'year', row = 'city', hue = 'is_downtown', data = weekly_unique_placekey_count, facet_kws = {'sharex': False, 'sharey': False})

ax.set(yscale="log")

# COMMAND ----------



# COMMAND ----------

single_week_counts = weekly_unique_placekey_count.loc[(weekly_unique_placekey_count['date_range_start'] == dbutils.widgets.get('week_selector')), ['city', 'unique_placekey_counts']]

# COMMAND ----------

# the vectors are of varying length so it's a bit hard to plot them together sensibly
single_week_counts.groupby('city').apply(lambda x : x.describe().T)

# COMMAND ----------

city_weekly_counts =  weekly_unique_placekey_count.groupby(['city', 'date_range_start']).sum().reset_index()
city_weekly_counts['week'] = (city_weekly_counts['date_range_start'] - pd.to_datetime('2019-01-07')).dt.days / 7

# COMMAND ----------

city_weekly_counts['week']

# COMMAND ----------



# COMMAND ----------

# one of these cities is not like the others, one of these cities is hamilton
import matplotlib.dates as md
plt.figure(figsize=(32,24))

ax = sns.relplot(x = 'week', y = 'unique_placekey_counts', hue = 'city', col = 'city', col_wrap = 4, facet_kws = {'sharex':False, 'sharey':False}, data = city_weekly_counts)

# COMMAND ----------

X_counts = city_weekly_counts[city_weekly_counts['city'] != 'Hamilton'].drop(columns = ['postal_code']).pivot(index = 'week', columns = 'city', values = 'unique_placekey_counts')
X_counts

# COMMAND ----------

X_std = (X - X.min(axis=0)) / (X.max(axis=0) - X.min(axis=0))
X_scaled = X_std.reset_index().melt(id_vars='week')

# COMMAND ----------

# weekly change in distribution # of eligible POI to appear in a given week?
sns.relplot(x = 'week', y = 'value', hue = 'city', col = 'city', col_wrap = 4, data = X_scaled);
plt.show()

# COMMAND ----------

# but just in general, not in respect to time? 
sns.displot(x = 'value', hue = 'city', data = X_scaled, height = 8, aspect = 2);
plt.show()

# COMMAND ----------

# each week had len(can_placekeys) / 20 searches. the placekeys were requested in the order they appeared in the places dataset- no shuffling or reordering was done
# ~6k samples per week
# draw a sample of size 20 six-thousand times
# the sample is not necessarily always the same order
# there were some requests with empty results, but the sampling was repeated 3 more times on the placekeys that weren't seen that week 
# more unqiue placekeys per postal code == popular 
# pretend every POI had exactly one visitor
weekly_sums = all_canada_device_counts.groupby(['city', 'postal_code', 'date_range_start', 'is_downtown']).sum().reset_index()
display(weekly_sums)

# COMMAND ----------

city_weekly_sums =  weekly_sums.groupby(['city', 'date_range_start', 'is_downtown']).mean().reset_index()
city_weekly_sums['week'] = city_weekly_sums['date_range_start'].dt.week
city_weekly_sums['year'] = city_weekly_sums['date_range_start'].dt.year

# COMMAND ----------

city_weekly_sums

# COMMAND ----------

# this will OOM if you don't subset on a single city
#single_city_sums = city_weekly_sums[city_weekly_sums['city'] == dbutils.widgets.get("canadian_cities")]
sns.displot(x = 'week', y = 'normalized_visits_by_total_visits', kind = 'hist', col = 'year', hue = 'is_downtown', row = 'city', data = city_weekly_sums, facet_kws = {'sharex': False, 'sharey': False})

# COMMAND ----------

plt.figure(figsize=(32,24))

ax = sns.relplot(x = 'week', y = 'normalized_visits_by_total_visits', hue = 'city', col = 'city', col_wrap = 4, facet_kws = {'sharex':False, 'sharey':False}, data = city_weekly_sums)

# COMMAND ----------

X_nvt = city_weekly_sums[city_weekly_sums['city'] != 'Hamilton'].drop(columns = ['postal_code']).pivot(index = 'week', columns = 'city', values = 'normalized_visits_by_total_visits')
X_nvt

# COMMAND ----------

X_nvt_std = ((X_nvt - X_nvt.min(axis=0)) / (X_nvt.max(axis=0) - X_nvt.min(axis=0))) / X_counts
X_nvt_scaled = X_nvt_std.reset_index().melt(id_vars='week')

# COMMAND ----------

# weekly change in distribution # of eligible POI to appear in a given week?
sns.relplot(x = 'week', y = 'value', hue = 'city', col = 'city', col_wrap = 4, data = X_nvt_scaled);
plt.show()

# COMMAND ----------



# COMMAND ----------

# but overall, we can see that these are very heavy tailed distributions 
# fit them to... poisson? Poisson as the sum itself of independent Poissons
single_week_counts.loc[single_week_counts['city'] == dbutils.widgets.get("canadian_cities"), 'unique_placekey_counts'].describe()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

sampled_placekeys_us = sampled_placekeys_us.merge(all_city_index, on = 'postal_code', how = 'inner')

# COMMAND ----------

all_sampled_placekeys = pd.concat([sampled_placekeys_us, sampled_placekeys_can])

# COMMAND ----------

display(all_sampled_placekeys)

# COMMAND ----------

# pct of postal_codes present in all_sampled_placekeys / postal_codes present in all_city_index
len(np.unique(all_sampled_placekeys['postal_code'])) / len(np.unique(all_city_index[all_city_index['city'].isin(np.unique(all_sampled_placekeys['city']))]['postal_code']))

# COMMAND ----------

# the number of each city's unique postal codes included in the study
# all city index has info on old cities, no longer included in the study- but the postal codes have been subset for what should be present
display(pd.DataFrame(all_city_index.groupby('city').size().sort_values().reset_index()))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We don't know specifically which placekeys are present in each week by postal code, but we DO know how many there 'should be' each week if we got what we asked for every single time. This obviously will not be the case, since week to week activity changes and some placekeys might simply not be present or had fewer than the total number of visitors to make it eligible for reporting. 

# COMMAND ----------

# one last look pre-aggregation
display(all_sampled_placekeys)

# COMMAND ----------

all_sampled_placekeys[all_sampled_placekeys['city'] == dbutils.widgets.get('city')]

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Number of eligible placekeys sampled by postal code
# MAGIC 
# MAGIC Of the placekeys we requested, how many unique placekeys would you expect to see per postal code, over the entire study period?

# COMMAND ----------

placekeys_by_postal_code = all_sampled_placekeys.groupby(['city', 'postal_code', 'is_downtown']).size().reset_index()
placekeys_by_postal_code = placekeys_by_postal_code.rename(columns = {0 : 'unique_placekey_counts'})
display(placekeys_by_postal_code)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Raw weekly device count by week

# COMMAND ----------

# actual raw placekey distribution cannot be plotted- OOM error
# so agg by postal_code
# zero variance- no variation in expected placekey presence across postal_codes
sns.displot(x = 'unique_placekey_counts', col = 'city', kind = 'kde', facet_kws = {'sharey' : False, 'sharex' : False}, col_wrap = 4, data = placekeys_by_postal_code)

# COMMAND ----------

us_dc = get_table_as_pandas_df("all_us_cities_dc")
can_dc = get_table_as_pandas_df("all_canada_device_count_agg_dt")
pre_march_2022 = us_dc.append(can_dc)
pre_march_2022['date_range_start'] = pd.to_datetime(pre_march_2022['date_range_start'].values)
display(pre_march_2022)

# COMMAND ----------

post_march_2022 = get_table_as_pandas_df("safegraph_patterns_count_merged_placekeys")
post_march_2022['date_range_start'] = pd.to_datetime(post_march_2022['date_range_start'].values)
post_march_2022 = post_march_2022[post_march_2022['date_range_start'] > max(pre_march_2022['date_range_start'])]
post_march_2022['city'] = post_march_2022['city'].str.replace("é", "e") 
display(post_march_2022)

# COMMAND ----------



# COMMAND ----------

# raw_visit_counts and normalized_visits_by_total_visits aggregated by week, by postal code. aka 'G'
us_dc = get_table_as_pandas_df("all_us_cities_dc")
can_dc = get_table_as_pandas_df("all_canada_device_count_agg_dt")
us_can_dc = us_dc.append(can_dc)
us_can_dc["postal_code"] = us_can_dc["postal_code"].astype(int)
us_can_dc['month'] = pd.to_datetime(us_can_dc['date_range_start'].values).month
us_can_dc['year'] = pd.to_datetime(us_can_dc['date_range_start'].values).year
us_can_dc['week_num'] =  pd.to_datetime(us_can_dc['date_range_start'].values).week
us_can_dc.loc[us_can_dc['year'].astype(int) == 2020, 'week_num'] = us_can_dc.loc[us_can_dc['year'].astype(int) == 2020, 'week_num'] - 1 # to replicate original week_uid definition
# just do this now to save some headaches / time later
us_can_dc['postal_code'] = us_can_dc['postal_code'].astype(int) # for postal_code with leading zeros
all_placekey_index['postal_code'] = all_placekey_index['postal_code'].astype(int) # for postal_code with leading zeros
us_can_dc['city'] = us_can_dc['city'].str.replace("é", "e") # for cities with characters that can get corrupted across reading/writing

# COMMAND ----------

us_dc = get_table_as_pandas_df("all_us_cities_dc")
us_dc["postal_code"] = us_dc["postal_code"].astype(int)
can_dc = get_table_as_pandas_df("all_canada_device_count_agg_dt")
can_dc["postal_code"] = can_dc["postal_code"].astype(int)
can_dc = can_dc.drop(columns = 'city').merge(all_city_index, on = 'postal_code', how = 'inner')
us_dc = us_dc.drop(columns = 'city').merge(all_city_index, on = 'postal_code', how = 'inner')

# COMMAND ----------

us_can_dc = us_dc.append(can_dc)

# COMMAND ----------

us_can_dc.shape

# COMMAND ----------

agg_dc_post_feb = get_table_as_pandas_df('safegraph_patterns_count_agg_postal')
agg_dc_post_feb["postal_code"] = agg_dc_post_feb["postal_code"].astype(int)
agg_dc_post_feb['city'] = agg_dc_post_feb['city'].str.replace("é", "e") 
agg_dc_post_feb = agg_dc_post_feb.merge(all_city_index, on = ['city', 'postal_code'], how = 'inner')
agg_dc_post_feb.shape

# COMMAND ----------

agg_dc_post_feb

# COMMAND ----------

agg_dc_post_feb['date_range_start'] = pd.to_datetime(agg_dc_post_feb['date_range_start'])
us_can_dc['date_range_start'] = pd.to_datetime(us_can_dc['date_range_start'])

# COMMAND ----------

all_agg_dc = us_can_dc.append(agg_dc_post_feb[agg_dc_post_feb['date_range_start'] > np.max(us_can_dc['date_range_start'])])
all_agg_dc['city'] = all_agg_dc['city'].str.replace('é', 'e')

# COMMAND ----------

regions_df = get_table_as_pandas_df('regions_csv')
regions_df['state'] = regions_df['display_title'].str[-2:].str.strip()
dbutils.widgets.dropdown('state', 'CA', np.unique(regions_df['state']).tolist())

# COMMAND ----------

all_agg_dc.shape

# COMMAND ----------

np.unique(all_agg_dc.groupby('city').size().reset_index()['city']) # there are some cities in here that aren't in the study area, merge with all_city_index to remove

# COMMAND ----------

all_agg_dc_state = all_agg_dc.merge(regions_df[['city', 'state', 'region', 'metro_size']], on = 'city', how = 'inner')
all_agg_dc_state['date_range_start'] = pd.to_datetime(all_agg_dc_state['date_range_start'])


# COMMAND ----------

np.unique(all_agg_dc_state.groupby('city').size().reset_index()['city'])

# COMMAND ----------

# MAGIC %md
# MAGIC # Population parameter estimation
# MAGIC The `normalized_by_total_visits` value is the sum of the raw visits for a POI divided by all visits across the state for that same week. This distribution should be the same for all cities in a given state- further from the rest inidicate something went wrong with its sampling. 

# COMMAND ----------



# COMMAND ----------

# bimodal distributions because of the massive drop in overall visits due to covid
X = all_agg_dc_state[all_agg_dc_state['state'] == dbutils.widgets.get('state')].groupby(['city', 'is_downtown', 'date_range_start']).sum().reset_index()
X['total_state_visits_est'] = X['raw_visit_counts'] / X['normalized_visits_by_total_visits']
X['week_num'] = (X['date_range_start'] - pd.to_datetime(np.min(X['date_range_start']))).dt.days / 7
X = X.pivot(columns = 'city', index = ['is_downtown', 'week_num'], values = 'total_state_visits_est').reset_index()

X = X.drop(columns = ['is_downtown'])
X

# COMMAND ----------

sns.pairplot(X, height = 4, aspect = 1, hue = 'week_num')

# COMMAND ----------

# bimodal distributions because of the massive drop in overall visits due to covid
state_est = all_agg_dc_state[all_agg_dc_state['state'] == dbutils.widgets.get('state')]#.groupby(['city', 'is_downtown', 'date_range_start']).mean().reset_index()
# each city should have had about the same estimation for their weekly total visits to the state normalization constant
sns.catplot(x = 'date_range_start', y = 'city',  palette = "colorblind", hue = 'total_state_visits_est', data = state_est, height = 10, aspect = 2, alpha = .5);

# COMMAND ----------



# COMMAND ----------

# bimodal distributions because of the massive drop in overall visits due to covid
state_est = all_agg_dc_state[all_agg_dc_state['state'] == dbutils.widgets.get('state')]#.groupby(['city', 'is_downtown', 'date_range_start']).mean().reset_index()
# each city should have had about the same estimation for their weekly total visits to the state normalization constant
sns.displot(x = 'date_range_start', y = 'total_state_visits_est', kind = 'kde', row = 'is_downtown', palette = "colorblind", ci = 'sd', hue = 'city', data = state_est, height = 8, aspect = 2, facet_kws = {'sharey' : True});

# COMMAND ----------

# bimodal distributions because of the massive drop in overall visits due to covid
all_states_est = all_agg_dc_state.groupby(['state', 'city', 'is_downtown', 'date_range_start']).mean().reset_index()
# each city should have had about the same estimation for their weekly total visits to the state normalization constant
sns.relplot(x = 'date_range_start', y = 'total_state_visits_est', hue = 'city', kind = 'line', ci = 'sd', alpha = .25, style = 'is_downtown', row = 'state', data = all_states_est, height = 8, aspect = 2, facet_kws = {'sharey' : False});

# COMMAND ----------

state_est[['date_range_start', 'total_state_visits_est', 'city', 'is_downtown']]

# COMMAND ----------

sns.pairplot(state_est, hue = 'city', diag_kind = 'hist')

# COMMAND ----------

all_weekly_info = all_can_device_count.merge(all_placekey_index, how = 'inner', on = ['placekey', 'postal_code'])
all_weekly_info['year'] = pd.to_datetime(all_weekly_info['date_range_start'].values).year
all_weekly_info['week_num'] = pd.to_datetime(all_weekly_info['date_range_start'].values).week
all_weekly_info.loc[all_weekly_info['year'].astype(int) == 2020, 'week_num'] = all_weekly_info.loc[all_weekly_info['year'].astype(int) == 2020, 'week_num'] - 1
all_weekly_info

# COMMAND ----------

# keeps only placekeys that appeared in at least two years
placekey_counts = all_weekly_info[all_weekly_info.duplicated(subset = ['placekey', 'week_num'], keep = False)].groupby(['city', 'week_num', 'year', 'postal_code'])['placekey'].size().reset_index()
placekey_counts

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # All weekly metrics

# COMMAND ----------

# weekly metrics
all_weekly_metrics = get_table_as_pandas_df('all_weekly_metrics')
all_weekly_metrics = all_weekly_metrics[~all_weekly_metrics["city"].isin(outlier_cities)]
all_weekly_metrics['month'] = pd.to_datetime(all_weekly_metrics['week'].values).month
all_weekly_metrics['year'] = pd.to_datetime(all_weekly_metrics['week'].values).year
all_weekly_metrics['week_num'] = pd.to_datetime(all_weekly_metrics['week'].values).week
all_weekly_metrics.loc[all_weekly_metrics['year'].astype(int) == 2020, 'week_num'] = all_weekly_metrics.loc[all_weekly_metrics['year'].astype(int) == 2020, 'week_num'] - 1
all_weekly_metrics = all_weekly_metrics[pd.to_datetime(all_weekly_metrics['week']) > pd.to_datetime("2020-03-16")] # strictly after shelter in place

# COMMAND ----------

# distribution of placekey appearances by week_num, regardless of year
# this took 27 minutes to run - don't even bother with this again
# city_ax = sns.displot(x = 'week_num', y = 'placekey', hue = 'city', kind = 'kde', col = 'city', col_wrap = 4, facet_kws={'sharey': False, 'sharex':False}, data = placekey_counts)

# COMMAND ----------

# filter on some threshold of appearances
# sums placekey appearances for all postal_codes in a city, grouped by year and week_num
weekly_placekey_counts = placekey_counts.groupby(['city', 'week_num', 'year'])['placekey'].sum().reset_index()
weekly_placekey_counts

# COMMAND ----------

# MAGIC %md
# MAGIC # Distribution of total placekey presence by week per city

# COMMAND ----------

# distribution of placekey presence by week
# don't take too long, could be fun to look at. measures placekey counts by week, by year, by city
# can help detect major outliers and overall trends if each one is scaled to the total placekeys present per city
by_week_ax = sns.displot(x = 'week_num', y = 'placekey', hue = 'year', kind = 'kde', col = 'city', palette='colorblind',col_wrap = 4, facet_kws={'sharey': False, 'sharex':False}, data = weekly_placekey_counts)

# COMMAND ----------

# distribution of placekey presence by week
# don't take too long, could be fun to look at. measures placekey counts by week, by year, by city
# can help detect major outliers and overall trends if each one is scaled to the total placekeys present per city
by_week_ax_blob = sns.displot(x = 'week_num', y = 'placekey', hue = 'year', kind = 'kde', col = 'city',palette='colorblind', col_wrap = 4, fill=True, facet_kws={'sharey': False, 'sharex':False}, data = weekly_placekey_counts)

# COMMAND ----------

total_placekeys_by_week_by_city = all_weekly_info.groupby(['date_range_start', 'city'])['placekey'].size().reset_index()

# COMMAND ----------

total_placekeys_by_week_by_city['year'] = pd.to_datetime(total_placekeys_by_week_by_city['date_range_start'].values).year.astype(str)
total_placekeys_by_week_by_city['week_num'] = pd.to_datetime(total_placekeys_by_week_by_city['date_range_start'].values).week
total_placekeys_by_week_by_city.loc[total_placekeys_by_week_by_city['year'].astype(int) == 2020, 'week_num'] = total_placekeys_by_week_by_city.loc[total_placekeys_by_week_by_city['year'].astype(int) == 2020, 'week_num'] - 1

# COMMAND ----------

total_placekeys_by_week_by_city

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC # Average weekly unique placekey appearance in Canadian data

# COMMAND ----------

# average weekly placekey appearances for all of canada
single_year_ax = sns.relplot(x = 'week_num', y = 'placekey', kind = 'line', hue = 'year', col = 'city', col_wrap = 4, palette='colorblind',facet_kws={'sharey': False},  data = total_placekeys_by_week_by_city)

# COMMAND ----------

total_placekeys_by_week_by_city

# COMMAND ----------

# add postal_code and city to us_can_dc
us_can_dc = us_can_dc.merge(all_city_index, on = ['city', 'postal_code'], how = 'inner')

# COMMAND ----------

# create some widgets
dbutils.widgets.dropdown("metric", "downtown", ["downtown", "city", "relative"]) # default to downtown
dbutils.widgets.dropdown("city", "San Francisco", np.unique(all_weekly_metrics['city']).tolist())
dbutils.widgets.dropdown("gaussian_order", "2", [str(x) for x in np.arange(0, 3)]) # filter degree- start with a 2nd order sum
dbutils.widgets.dropdown("sigma", "2.5", [str(x) for x in np.arange(1.0, 5.0, .5)])
#dbutils.widgets.dropdown("state", "CA", np.unique(all_weekly_metrics['display_title'].str[-2:].str.strip()).tolist())

# COMMAND ----------

# MAGIC %md
# MAGIC # Week-agnostic distribution of normalized_visits_by_total_visits by year
# MAGIC Given that each week's recovery metric is a ratio of how the selected geography for each city compared to that same region in space for the same week of 2019, if the pandemic had never happened, we would expect to see deviations- no week will match up exactly, but the distribution of errors should follow a normal distribution.
# MAGIC 
# MAGIC No minmax scaling on weekly recovery metric (yet). This is agnostic to how a city did relative to *other* cities during same time period. This recovery is strictly comparing a week in 2020, 2021, 2022, to that same week in 2019 for each specific city. 
# MAGIC 
# MAGIC If the cities were performing 'business as usual', you would expect to see normal distributions (or whatever distribution you assume for how far from 1 the comparison is)

# COMMAND ----------

# they're all very flat if they share a y axis due to the handful of cities that did well compared to the majority
# the rug plots + values on each city's axis shows how it did relative to itself

# business as usual- the expected value of X_202x / X_2019 should be alpha+/- 1. (we should define this- how about .05)
from scipy.stats import norm

expected_values = norm.rvs(0, 1, size = len(np.unique(all_weekly_metrics['week'])))

observed_values = all_weekly_metrics[(all_weekly_metrics['metric'] == dbutils.widgets.get('metric')) &
                                     (all_weekly_metrics['city'] == dbutils.widgets.get('city'))]
# Fit a normal distribution to the observed values:
mu, std = norm.fit(observed_values['normalized_visits_by_total_visits'])

# Plot the histogram.
plt.hist(expected_values, bins=25, density=True, alpha=0.6, color='g')

# Plot the PDF.
xmin, xmax = plt.xlim()
x = np.linspace(xmin, xmax, 100)
p = norm.pdf(x)
plt.plot(x, p, 'k', linewidth=2)
title = "Fit results: mu = %.2f,  std = %.2f" % (mu, std)
plt.title(title)

plt.show()

# COMMAND ----------

# they're all very flat if they share a y axis due to the handful of cities that did well compared to the majority
# the rug plots + values on each city's axis shows how it did relative to itself
all_ax = sns.displot(x = 'normalized_visits_by_total_visits', palette='colorblind', hue = 'year', kind = 'kde', col = 'city', col_wrap=4, rug=True,  facet_kws={'sharey': True, 'sharex':False}, data = all_weekly_metrics.query("metric == @dbutils.widgets.get('metric')"));

for axis in all_ax.axes.flat:
    axis.tick_params(labelleft=True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Accounting for different recovery ranges across cities

# COMMAND ----------

all_ax = sns.displot(x = 'normalized_visits_by_total_visits', hue='year', palette='colorblind', kind = 'kde', col = 'city', col_wrap=4, rug=True,  facet_kws={'sharey': True, 'sharex':False}, data = all_weekly_metrics.query("metric == @dbutils.widgets.get('metric')"));

# COMMAND ----------

# they're all very flat if they share a y axis due to the handful of cities that did well compared to the majority
# the rug plots + values on each city's axis shows how it did relative to itself
all_ax = sns.displot(x = 'week_num', y = 'normalized_visits_by_total_visits', hue='year', palette='colorblind', kind = 'kde', col = 'city', col_wrap=5, rug=True,  facet_kws={'sharey': False, 'sharex':True}, data = all_weekly_metrics.query("metric == @dbutils.widgets.get('metric')"));

# COMMAND ----------

sns.displot(x = 'week_num', y = 'normalized_visits_by_total_visits', hue = 'year',palette='colorblind', kind = 'kde', col = 'city', fill=True, rug=True,col_wrap=5,  facet_kws={'sharey': False, 'sharex':False}, data = all_weekly_metrics.query("metric == @dbutils.widgets.get('metric')"));

# COMMAND ----------

# create the minmax scaler
from sklearn.preprocessing import MinMaxScaler
scaler_all = MinMaxScaler()

# COMMAND ----------

scalers = all_weekly_metrics.sort_values(by = 'week').groupby(['metric', 'year', 'week_num', 'city'])['normalized_visits_by_total_visits'].transform(lambda x : scaler_all.fit(x.to_numpy().reshape(-1,1)))

# COMMAND ----------

# number of gaussian filters to apply with gaussian_order
# from this cell onward, you have to manually rerun if you change the metric. it was too complex otherwise
selected_weekly_metric = all_weekly_metrics.loc[all_weekly_metrics['metric'] == dbutils.widgets.get('metric'),
                                               ['city', 'normalized_visits_by_total_visits', 'week', 'year', 'week_num']]
for i in np.arange(0, int(dbutils.widgets.get('gaussian_order')) + 1):
    selected_weekly_metric['gaussian_' + str(i)] = selected_weekly_metric.sort_values(by = 'week').groupby(['city'])['normalized_visits_by_total_visits'].transform(lambda x : scipy.ndimage.gaussian_filter1d(x, sigma = float(dbutils.widgets.get('sigma')), order = i))
    
selected_weekly_metric['approx_order_' + dbutils.widgets.get('gaussian_order')] = selected_weekly_metric[['gaussian_' + str(x) for x in np.arange(0, int(dbutils.widgets.get('gaussian_order')) + 1)]].sum(axis =1)
filter_df_approx = selected_weekly_metric[['city', 'normalized_visits_by_total_visits', 'week', 'year', 'week_num', 'gaussian_0', 'gaussian_1', 'gaussian_2']].sort_values(by = 'week').groupby(['city', 'year', 'week_num', 'normalized_visits_by_total_visits'])[['gaussian_' + str(x) for x in np.arange(0, int(dbutils.widgets.get('gaussian_order')) + 1)]].sum().reset_index()
filter_df_approx['approx_order_' + dbutils.widgets.get('gaussian_order')] = filter_df_approx[['gaussian_' + str(x) for x in np.arange(0, int(dbutils.widgets.get('gaussian_order')) + 1)]].sum(axis =1)
filter_df_approx

# COMMAND ----------

melted_filter_df = filter_df_approx.melt(id_vars = ['city', 'week_num', 'year'])
melted_filter_df['value'] = melted_filter_df['value'].astype(float)
melted_filter_df

# COMMAND ----------

dbutils.widgets.dropdown("variable", np.unique(melted_filter_df['variable']).tolist()[-1], np.unique(melted_filter_df['variable']).tolist())

# COMMAND ----------

# they're all very flat if they share a y axis due to the handful of cities that did well compared to the majority
# the rug plots + values on each city's axis shows how it did relative to itself

# business as usual- the expected value of X_202x / X_2019 should be alpha+/- 1. (we should define this- how about .05)
from scipy.stats import norm

expected_values = norm.rvs(0, 1, size = len(np.unique(selected_weekly_metric['week'])))

observed_values = selected_weekly_metric[selected_weekly_metric['city'] == dbutils.widgets.get('city')]

# Fit a normal distribution to the observed values:
mu, std = norm.fit(observed_values[dbutils.widgets.get('variable')])

# Plot the histogram.
plt.hist(expected_values, bins=25, density=True, alpha=0.6, color='g')

# Plot the PDF.
xmin, xmax = plt.xlim()
x = np.linspace(xmin, xmax, 100)
p = norm.pdf(x)
plt.plot(x, p, 'k', linewidth=2)
title = "Fit results: mu = %.2f,  std = %.2f" % (mu, std)
plt.title(title)

plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Relplot

# COMMAND ----------

selected_weekly_metric['minmax_scaled' + dbutils.widgets.get("variable")] = selected_weekly_metric.sort_values(by = 'week').groupby(['city'])[dbutils.widgets.get("variable")].transform(lambda X : (X - X.min(axis=0)) / (X.max(axis=0) - X.min(axis=0)))
sns.relplot(x = 'week_num', y = 'minmax_scaled' + dbutils.widgets.get("variable"), palette='colorblind', kind = 'line', hue='year', col = 'city', col_wrap=4, data = selected_weekly_metric);

# COMMAND ----------

selected_weekly_metric['minmax_scaled' + dbutils.widgets.get("variable")] = selected_weekly_metric.sort_values(by = 'week').groupby(['city'])[dbutils.widgets.get("variable")].transform(lambda X : (X - X.min(axis=0)) / (X.max(axis=0) - X.min(axis=0)))
sns.relplot(x = 'week_num', y = 'minmax_scaled' + dbutils.widgets.get("variable"), hue = 'year', palette='colorblind', kind = 'line', data = selected_weekly_metric, height = 8, aspect = 2);

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Jointplot

# COMMAND ----------

selected_weekly_metric['minmax_scaled' + dbutils.widgets.get("variable")] = selected_weekly_metric.sort_values(by = 'week').groupby(['city'])[dbutils.widgets.get("variable")].transform(lambda X : (X - X.min(axis=0)) / (X.max(axis=0) - X.min(axis=0)))
sns.jointplot(x = 'minmax_scaledgaussian_0', y = dbutils.widgets.get("variable"), hue = 'year', palette='colorblind', kind = 'kde', data = selected_weekly_metric.query('city == @dbutils.widgets.get("city")'));

# COMMAND ----------

display(selected_weekly_metric)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Displot

# COMMAND ----------



# COMMAND ----------

sns.displot(x = 'week_num', y = 'minmax_scaled' + dbutils.widgets.get("variable"), hue = 'year', palette='colorblind', kind = 'kde', col = 'city', col_wrap=5, rug=True, data = selected_weekly_metric);

# COMMAND ----------

sns.displot(x = 'week_num', y = 'minmax_scaled' + dbutils.widgets.get("variable"), hue = 'year',palette='colorblind', kind = 'kde', col = 'city', col_wrap=5, rug=True, fill=True, data = selected_weekly_metric);

# COMMAND ----------

sns.relplot(x = 'week_num', y = 'gaussian_0', hue = dbutils.widgets.get('variable'), col = 'year', row = 'city', facet_kws = {'sharey': 'row'}, data = selected_weekly_metric);

# COMMAND ----------

# model it as a sum of gaussians
!pip install tslearn



# COMMAND ----------

selected_weekly_metric

# COMMAND ----------



# COMMAND ----------

ts_array[0].shape

# COMMAND ----------

from sklearn.model_selection import ShuffleSplit

# COMMAND ----------



# COMMAND ----------

from tslearn.clustering import KernelKMeans
shuffler = ShuffleSplit(n_splits=2, test_size = .20, random_state = 0)
ts_array = selected_weekly_metric[['city', 'gaussian_0', 'gaussian_1', 'gaussian_2']].set_index('city').groupby('city').apply(pd.DataFrame.to_numpy).to_numpy()
kkm = KernelKMeans(n_clusters = 4, kernel='gak')

fig, ax = plt.subplots(4, 1, figsize=(24,24))
fig.tight_layout()

reg


for train, test in shuffler.split(ts_array):
    X_train = TimeSeriesScalerMeanVariance().fit_transform(ts_array[train].tolist())
    X_test = TimeSeriesScalerMeanVariance().fit_transform(ts_array[test].tolist())
    sz = X_train.shape[1]
    y_pred = kkm.fit_predict(X_train)
    
    for yi in range(4):
        for xx in X_train[y_pred == yi]:
            ax[yi].plot(xx[:,0], "k-", alpha=.2)
            ax[yi].plot(xx[:,1], "g-", alpha=.2),
            ax[yi].plot(xx[:,2], "b-", alpha=.2)
            ax[yi].plot(km.cluster_centers_[yi].ravel(), "r-")
            ax[yi].set_xlim(0, sz)
            #plt.text(0.55, 0.85,'Cluster %d' % (yi + 1),
            #     transform=plt.gca().transAxes)
            #if yi == 1:
            #    plt.title("DBA $k$-means")
        
#plt.figure(figsize=(16,12))
plt.show()

# COMMAND ----------



# COMMAND ----------

X_train[y_pred == yi]

# COMMAND ----------

ts_array[train][:50].tolist()

# COMMAND ----------

import numpy
from sklearn.metrics import accuracy_score

from tslearn.generators import random_walk_blobs
from tslearn.preprocessing import TimeSeriesScalerMinMax, \
    TimeSeriesScalerMeanVariance
from tslearn.neighbors import KNeighborsTimeSeriesClassifier, \
    KNeighborsTimeSeries

numpy.random.seed(0)
n_ts_per_blob, sz, d, n_blobs = 20, 100, 1, 2

# Prepare data
X, y = random_walk_blobs(n_ts_per_blob=n_ts_per_blob,
                         sz=sz,
                         d=d,
                         n_blobs=n_blobs)
scaler = TimeSeriesScalerMinMax(value_range=(0., 1.))  # Rescale time series
X_scaled = scaler.fit_transform(X)

indices_shuffle = numpy.random.permutation(n_ts_per_blob * n_blobs)
X_shuffle = X_scaled[indices_shuffle]
y_shuffle = y[indices_shuffle]

X_train = X_shuffle[:n_ts_per_blob * n_blobs // 2]
X_test = X_shuffle[n_ts_per_blob * n_blobs // 2:]

# COMMAND ----------

X_train[0]

# COMMAND ----------

# 
from scipy import signal
from scipy.fft import fftshift

# COMMAND ----------

# run the bart model on each week with a composite 'improvement' score
# update variable importance by week

# COMMAND ----------

# MAGIC %md
# MAGIC Parameterizing weekly recovery metric as a function of year and week number for each city mapping. 
# MAGIC 
# MAGIC $$ R(w,y) = f_c(w,y)$$
# MAGIC $$ \frac{dR}{dw} = \frac{df_c}{dw}$$
# MAGIC $$ \frac{dR}{dy} = \frac{df_c}{dy}$$
# MAGIC 
# MAGIC can find the gradient

# COMMAND ----------

# number of gaussian filters to apply with gaussian_order
# from this cell onward, you have to manually rerun if you change the metric. it was too complex otherwise
selected_weekly_metric = all_weekly_metrics.loc[all_weekly_metrics['metric'] == dbutils.widgets.get('metric'),
                                               ['city', 'normalized_visits_by_total_visits', 'week', 'year', 'week_num']]
for i in np.arange(0, int(dbutils.widgets.get('gaussian_order')) + 1):
    selected_weekly_metric['gaussian_' + str(i)] = selected_weekly_metric.sort_values(by = 'week').groupby(['city'])['normalized_visits_by_total_visits'].transform(lambda x : scipy.ndimage.gaussian_filter1d(x, sigma = float(dbutils.widgets.get('sigma')), order = i))
    
selected_weekly_metric['approx_order_' + dbutils.widgets.get('gaussian_order')] = selected_weekly_metric[['gaussian_' + str(x) for x in np.arange(0, int(dbutils.widgets.get('gaussian_order')) + 1)]].sum(axis =1)
filter_df_approx = selected_weekly_metric[['city', 'normalized_visits_by_total_visits', 'week', 'year', 'week_num', 'gaussian_0', 'gaussian_1', 'gaussian_2']].sort_values(by = 'week').groupby(['city', 'year', 'week_num', 'normalized_visits_by_total_visits'])[['gaussian_' + str(x) for x in np.arange(0, int(dbutils.widgets.get('gaussian_order')) + 1)]].sum().reset_index()
filter_df_approx['approx_order_' + dbutils.widgets.get('gaussian_order')] = filter_df_approx[['gaussian_' + str(x) for x in np.arange(0, int(dbutils.widgets.get('gaussian_order')) + 1)]].sum(axis =1)
filter_df_approx

# COMMAND ----------

selected_weekly_metric['minmax_scaled' + 'gaussian_' + str(i)] = selected_weekly_metric.sort_values(by = 'week').groupby(['city'])['gaussian_' + str(i)].transform(lambda X : (X - X.min(axis=0)) / (X.max(axis=0) - X.min(axis=0)))
selected_weekly_metric['minmax_scaled' + 'approx_order_' + dbutils.widgets.get('gaussian_order')] = selected_weekly_metric.sort_values(by = 'week').groupby(['city'])['approx_order_' + dbutils.widgets.get('gaussian_order')].transform(lambda X : (X - X.min(axis=0)) / (X.max(axis=0) - X.min(axis=0)))

# COMMAND ----------

melted_filter_df = filter_df_approx.melt(id_vars = ['city', 'week_num', 'year'])
melted_filter_df['value'] = melted_filter_df['value'].astype(float)
melted_filter_df

# COMMAND ----------

dbutils.widgets.dropdown("variable", np.unique(melted_filter_df['variable']).tolist()[-1], np.unique(melted_filter_df['variable']).tolist())

# COMMAND ----------

city_weekly_metric = selected_weekly_metric[selected_weekly_metric['city'] == dbutils.widgets.get('city')]
city_weekly_metric['minmax_scaled' + 'gaussian_' + str(i)] = city_weekly_metric.sort_values('week').groupby(['city'])['gaussian_' + str(i)].transform(lambda X : (X - X.min(axis=0)) / (X.max(axis=0) - X.min(axis=0)))
city_weekly_metric

# COMMAND ----------



# COMMAND ----------

# this figure is interesting, but not necessarily saying anything new and takes a bit to run
#plt.figure(figsize=(8, 8))
# sort by week num, then by year.
# then convolve on 2020 -> 2021 -> 2022???
#scalers = melted_filter_df.sort_values(by = ['year', 'week_num', ]).groupby(['year'])[dbutils.widgets.get('sg_measurement')].transform(lambda x : scaler_all.fit(x.to_numpy().reshape(-1,1)))
#scalers = all_weekly_metrics.sort_values(by = 'week').groupby(['metric', 'year', 'week_num', 'city'])[dbutils.widgets.get('variable')].transform(lambda x : scaler.fit(x.to_numpy().reshape(-1,1)))
#ax = sns.relplot(x='week_num', y=dbutils.widgets.get('variable'), palette = 'colorblind', hue = "year", kind = "line", col="city",  col_wrap=6, 
#                 height=8, data=filter_df_approx);
# plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
#ax._legend.remove()
#plt.show()

# COMMAND ----------

melted_filter_df = filter_df_approx.melt(id_vars = ['city', 'week_num', 'year'])
melted_filter_df['value'] = melted_filter_df['value'].astype(float)
melted_filter_df

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Empirical joint density function of weekly selected variable and week number, conditioned by year
# MAGIC ## Single city selection

# COMMAND ----------

# bivariate contour plot for one city at a time
plt.figure(figsize=(8, 8))
# sort by week num, then by year.
# then convolve on 2020 -> 2021 -> 2022???
#scalers = melted_filter_df.sort_values(by = ['year', 'week_num', ]).groupby(['year'])[dbutils.widgets.get('sg_measurement')].transform(lambda x : scaler_all.fit(x.to_numpy().reshape(-1,1)))
#scalers = all_weekly_metrics.sort_values(by = 'week').groupby(['metric', 'year', 'week_num', 'city'])[dbutils.widgets.get('variable')].transform(lambda x : scaler.fit(x.to_numpy().reshape(-1,1)))
sns.displot(x='week_num', y = "value", palette = 'colorblind', kind = "kde", hue='year', col = 'variable', rug=True, facet_kws={'sharey': False, 'sharex': True},
                 data=melted_filter_df.query("city == @dbutils.widgets.get('city')"));

sns.displot(x = 'week_num', y = 'value', palette = 'colorblind', hue = "year", kind="kde", col='variable', rug=True, fill=True, facet_kws={'sharey': False, 'sharex': True}, data=melted_filter_df.query("city == @dbutils.widgets.get('city')"));

# COMMAND ----------

# bivariate contour plot for one city at a time

#plt.figure(figsize=(8, 8))
# sort by week num, then by year.
# then convolve on 2020 -> 2021 -> 2022???
#scalers = melted_filter_df.sort_values(by = ['year', 'week_num', ]).groupby(['year'])[dbutils.widgets.get('sg_measurement')].transform(lambda x : scaler_all.fit(x.to_numpy().reshape(-1,1)))
#scalers = all_weekly_metrics.sort_values(by = 'week').groupby(['metric', 'year', 'week_num', 'city'])[dbutils.widgets.get('variable')].transform(lambda x : scaler.fit(x.to_numpy().reshape(-1,1)))
#ax = sns.relplot(x='week_num', y='value', palette = 'colorblind', hue = "year", kind = "line", col="city",  col_wrap=6, 
#                 height=8, data=melted_filter_df.query('variable == @dbutils.widgets.get("variable")'));

# replace labels
#new_labels = ['2022', '2021', '2020']
#for t, l in zip(ax._legend.texts, new_labels):
#    t.set_text(l)
# plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
#plt.show()

# COMMAND ----------

# is it possible to group the cities by how similar these joint distributions are? K-means on their deviance from the expected 'business as usual' average?
# finding the inverse of the transform that would take this back to isotropic gaussian?

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## All cities

# COMMAND ----------

# now look at them all
plt.figure(figsize=(24, 16))
# allowing the cities their own axis scales demonstrates how similar they are if you account for a scaling constant
# sort by week num, then by year.
# then convolve on 2020 -> 2021 -> 2022???
#scalers = melted_filter_df.sort_values(by = ['year', 'week_num', ]).groupby(['year'])[dbutils.widgets.get('sg_measurement')].transform(lambda x : scaler_all.fit(x.to_numpy().reshape(-1,1)))
#scalers = all_weekly_metrics.sort_values(by = 'week').groupby(['metric', 'year', 'week_num', 'city'])[dbutils.widgets.get('variable')].transform(lambda x : scaler.fit(x.to_numpy().reshape(-1,1)))
#ax = sns.relplot(x='week_num', y='value', palette = 'colorblind', hue = "year", kind = "line", col="city",  col_wrap=6, 
#                 height=8, data=melted_filter_df.query('variable == @dbutils.widgets.get("variable")'));
ax = sns.displot(x = 'week_num', y = dbutils.widgets.get("variable"), palette = 'colorblind', hue = "year", kind = "kde", rug=True, col="city", col_wrap=4, 
                 height=8, facet_kws={'sharex': True, 'sharey': False,'legend_out' : True}, data=filter_df_approx);
    
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
plt.show()

# COMMAND ----------

create_and_save_as_table(filter_df_approx, 'bart_input')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Establishing periodicity
# MAGIC 
# MAGIC What would this look like had the pandemic never happened? There is an allowable amount of change that can take place for it to not be 'significantly' different from expected device count measurements. 'Normal' is the device count activity metric in 2019. 
# MAGIC 
# MAGIC If this is the case, then each reported weekly average should be somewhere about 1. The larger the deviation from 1, the greater the change in expected weekly activity. 
# MAGIC 
# MAGIC Given that the POIs have been scaled by all POIs observed for the week in that state, doing much more to the observations relative to their geography won't accomplish much. Aggregating by `postal_code` and examining the change in city level reported metrics over time as well as comparisons to the expected 'business as usual' metrics can help distinguish which features are relevant to estimate a change in \\(F(t)\\)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Single city filtering

# COMMAND ----------

# how severe is the difference between observed activity and expected activity?
# we have N discrete samples of a continuous signal y_i(t), where t is between [2020-03-02, 2022-06-06] and i is between [0, 61]. 'i' is the city index
# the order of i does not matter, but the order of t matters
# now what if the weekly samples were grouped by their common denominator - each week_num of 2020, 2021, 2022 is scaled by its corresponding week_num in 2019
# 
# figure out what you want to solve, use DTFT to set up system of eqs and solve- can do feature selection and coefficient estimation (in the frequency domain??))
# maybe some visualizations of this 
# gaussian filter 'rewards' sustained changes over some period of time. The window of time is determined by the filter size and the magnitude of the reward/penalty is adjusted with sigma. A higher sigma means a more generous threshold of what defines 'change', less reward/penalty between types of change. Imagine the derivative of this filter as a way to determine the magnitude of the change. High magnitude observations correspond to a high frequency in the signal. If that's all you want to know- at which points does this signal experience the largest change, apply a low pass filter to it. (aka the gaussian filter lol). 

# pdf as a function of jointly distributed independent variables- i know y, i want to fit for x. and i also know that some regularization applied to y[n + 1] can lead y[n] if i get good coefficient estimates and apply the right regularization.

# applying a gaussian filter == ridge regression == MAP with gaussian prior on complex part

# applying a laplacian filter == lasso == MAP beta prior on complex part



# no filter, straight to OLS == no regularization == plain ol MLE == MAP with uniform prior

# MAP - maximum a posteriori estimation, aka regularized MLE

# Imagine the infinite area below sigma as it tends towards infinity. A lower sigma means a tighter, high risk/high reward type of estimations of the true signal. As sigma approaches 0, this gaussian filter turns into a delta 

# group by city- apply the same low pass filter to every city
# Q: but what if some cities have different periods than other cities and the low pass filter that works for one doesn't work for the other and you get a garbage signal?
# A: use some flavor of cross validation to determine the best hyperparameter or take the naive route if it's not too bad and . you are basically assuming a gaussian distribution on the errors and trying to account for the potential differences in periods for the 
# gaussian filter to do what it does well- be a low pass filter and isolate high frequencies
single_city_filter_df = all_weekly_metrics[(all_weekly_metrics["metric"] == dbutils.widgets.get('metric'))]

for i in np.arange(0, int(dbutils.widgets.get('gaussian_order')) + 1):
    single_city_filter_df['gaussian_' + str(i)] = single_city_filter_df.sort_values(by = 'week').groupby(['metric', 'year', 'week_num', 'city'])['normalized_visits_by_total_visits'].transform(lambda x : scipy.ndimage.gaussian_filter1d(x, sigma = float(dbutils.widgets.get('sigma')), order = i))
single_city_filter_df_approx = single_city_filter_df.sort_values(by = 'week').groupby(['city', 'week_num', 'year', 'metric'])[['gaussian_' + str(x) for x in np.arange(0, int(dbutils.widgets.get('gaussian_order')) + 1)]].sum().reset_index()

# COMMAND ----------

single_city_filter_df_approx['approx_order_' + dbutils.widgets.get('gaussian_order')] = single_city_filter_df_approx[['gaussian_' + str(x) for x in np.arange(0, int(dbutils.widgets.get('gaussian_order')) + 1)]].sum(axis =1)
single_city_melted_filter_df = single_city_filter_df_approx.melt(id_vars = ['city', 'week_num', 'year', 'metric'])
single_city_melted_filter_df['value'] = single_city_melted_filter_df['value'].astype(float)
dbutils.widgets.dropdown("variable", np.unique(single_city_melted_filter_df['variable']).tolist()[0], np.unique(single_city_melted_filter_df['variable']).tolist())

# COMMAND ----------

plt.figure(figsize=(16, 16))
# sort by week num, then by year.
# then convolve on 2020 -> 2021 -> 2022???


plot_df = single_city_melted_filter_df.loc[(single_city_melted_filter_df["variable"] == dbutils.widgets.get("variable")) &
                                      (single_city_melted_filter_df["city"] == dbutils.widgets.get("city")) &
                                     (single_city_melted_filter_df["metric"] == dbutils.widgets.get("metric")), ["year", "week_num", "value"]]#.pivot(index = 'week_num', columns = 'year').add_prefix('year_')



#ax = sns.lineplot(x='week_num', y = 'minmax_scaled' + dbutils.widgets.get("variable"), palette = 'colorblind', hue = "year", data=single_city_melted_filter_df);
#ax = sns.lineplot(x='week_num', y = 'value', palette = 'colorblind', hue = "year", data=plot_df);

#plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
#plt.show()
# average, mins, and maxes of each week's second order approximation?

# COMMAND ----------

scalers = plot_df.apply(lambda x : scaler.fit(x.values.reshape(-1,1)), axis = 1)

scaled_df = pd.DataFrame(plot_df.apply(lambda x : scaler.transform(x.values.reshape(3,1)).flatten(), axis = 1).apply(lambda x : {'scaled_2020' : x[0], 'scaled_2021' : x[1], 'scaled_2022' : x[2]}).to_dict()).T.reset_index().melt(id_vars = 'index')

#ax = sns.lineplot(x='week_num', y = 'minmax_scaled' + dbutils.widgets.get("variable"), palette = 'colorblind', hue = "year", data=single_city_melted_filter_df);
ax = sns.lineplot(x='index', y = 'value', palette = 'colorblind', hue = "variable", data=plot_df);

plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
plt.show()
# average, mins, and maxes of each week's second order approximation?

# COMMAND ----------

#plot_df.apply(lambda x : scaler.transform(x.values.reshape(3,1)).flatten(), axis = 1)

# COMMAND ----------

scaled_df = pd.DataFrame(plot_df.apply(lambda x : scaler.transform(x.values.reshape(3,1)).flatten(), axis = 1).apply(lambda x : {'scaled_2020' : x[0], 'scaled_2021' : x[1], 'scaled_2022' : x[2]}).to_dict()).T

# COMMAND ----------

#scaled_df.reset_index().melt(id_vars = 'index')

# COMMAND ----------

#plot_df

# COMMAND ----------

# maybe fit on sum of 2020, 2021, and 2022 second order approx and try to pay extra attention to how 2022 differs /is similar from earlier years in the same week_num
# 3d distance function for each week?
# similarity score between 2021 and 2020, then one between 2022 and 2021 AND 2022, (similarity of 2021, 2020) 
# get year by year changes and if yearly benchmarks are being met, slowing down, staying the same for each week
# how about a ratio of 2022 - 2021 / 2021 - 2020
# rewards big improvments between 2022 and 2021
# i KNOW they would both be worse in 2021 and 2022 during 2020 prepandemic months- so it doesn't matter there's missing values. all that really matters is how 2022 did compared to 2021, given that our benchmark is 2019
# if you're still on early 2020 shutdown levels in 2022, that's not good :(
# look at the 2022 (2021) 2020 sandwich of March through May. steady recovery? slowin down? doin worse? pickin back up?

# COMMAND ----------

# see if phase shifting any of the sluggish performers in 2022 aligns nicely with a strong rebound in early/mid 2021 or strong initial resilience in mid 2020
# second order approx encodes position, velocity, and accel of recovery for a given week num for each year
# want to know best guess for jun-aug of 2022

# are other cities following similar recovery patterns that aren't readily apparent because they're delayed a few weeks?

# group based on 2022 second order approx first, then look at how they got here

# COMMAND ----------

#downtown_f = downtown.groupby('city')['normalized_visits_by_total_visits'].apply(lambda x : np.fft.fft(x))
#downtown_f

# COMMAND ----------

# optional - clear the widgets to start the next session fresh
dbutils.widgets.removeAll()

# COMMAND ----------

