# Databricks notebook source
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestClassifier
from sklearn.datasets import make_classification
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import RepeatedStratifiedKFold
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn import metrics 

def create_and_save_as_table(df, table_name):
    spark_df = spark.createDataFrame(df)
    spark_df.write.format('csv').saveAsTable(table_name)
    
def get_table_as_pandas_df(table_name):
    return sqlContext.sql("select * from " + table_name + " tables").toPandas()

# COMMAND ----------

rfc_data = get_table_as_pandas_df("_0812_all_model_features_")
lq_clusters = get_table_as_pandas_df("lq_clusters_0822") 
rq_dwtn_clusters = get_table_as_pandas_df("rq_dwtn_clusters_0822") 
rq_city_clusters = get_table_as_pandas_df("rq_city_clusters_0822") 
rq_dwtn_clusters_period_1 = get_table_as_pandas_df("rq_dwtn_clusters_0823_period_1") 
rq_dwtn_clusters_period_2 = get_table_as_pandas_df("rq_dwtn_clusters_0823_period_2") 

# COMMAND ----------

def get_weather(cityname):
    weather_lookup = json.loads(requests.get('http://api.worldweatheronline.com/premium/v1/weather.ashx?key=dc3dc812f3b74f8aa83224814222208&q='+cityname+'&fx=no&cc=no&mca=yes&format=json').text)
    jan_min = float(weather_lookup['data']['ClimateAverages'][0]['month'][0]['avgMinTemp_F'])
    feb_min = float(weather_lookup['data']['ClimateAverages'][0]['month'][1]['avgMinTemp_F'])
    dec_min = float(weather_lookup['data']['ClimateAverages'][0]['month'][11]['avgMinTemp_F'])
    jan_max = float(weather_lookup['data']['ClimateAverages'][0]['month'][0]['absMaxTemp_F'])
    feb_max = float(weather_lookup['data']['ClimateAverages'][0]['month'][1]['absMaxTemp_F'])
    dec_max = float(weather_lookup['data']['ClimateAverages'][0]['month'][11]['absMaxTemp_F'])
    winter_avg = (((jan_min+jan_max)/2) + ((feb_min+feb_max)/2) + ((dec_min+dec_max)/2))/3
    
    mar_min = float(weather_lookup['data']['ClimateAverages'][0]['month'][2]['avgMinTemp_F'])
    apr_min = float(weather_lookup['data']['ClimateAverages'][0]['month'][3]['avgMinTemp_F'])
    may_min = float(weather_lookup['data']['ClimateAverages'][0]['month'][4]['avgMinTemp_F'])
    mar_max = float(weather_lookup['data']['ClimateAverages'][0]['month'][2]['absMaxTemp_F'])
    apr_max = float(weather_lookup['data']['ClimateAverages'][0]['month'][3]['absMaxTemp_F'])
    may_max = float(weather_lookup['data']['ClimateAverages'][0]['month'][4]['absMaxTemp_F'])
    spring_avg = (((mar_min+mar_max)/2) + ((apr_min+apr_max)/2) + ((may_min+may_max)/2))/3
    
    jun_min = float(weather_lookup['data']['ClimateAverages'][0]['month'][5]['avgMinTemp_F'])
    jul_min = float(weather_lookup['data']['ClimateAverages'][0]['month'][6]['avgMinTemp_F'])
    aug_min = float(weather_lookup['data']['ClimateAverages'][0]['month'][7]['avgMinTemp_F'])
    jun_max = float(weather_lookup['data']['ClimateAverages'][0]['month'][5]['absMaxTemp_F'])
    jul_max = float(weather_lookup['data']['ClimateAverages'][0]['month'][6]['absMaxTemp_F'])
    aug_max = float(weather_lookup['data']['ClimateAverages'][0]['month'][7]['absMaxTemp_F'])
    summer_avg = (((jun_min+jun_max)/2) + ((jul_min+jul_max)/2) + ((aug_min+aug_max)/2))/3
        
    sep_min = float(weather_lookup['data']['ClimateAverages'][0]['month'][8]['avgMinTemp_F'])
    oct_min = float(weather_lookup['data']['ClimateAverages'][0]['month'][9]['avgMinTemp_F'])
    nov_min = float(weather_lookup['data']['ClimateAverages'][0]['month'][10]['avgMinTemp_F'])
    sep_max = float(weather_lookup['data']['ClimateAverages'][0]['month'][8]['absMaxTemp_F'])
    oct_max = float(weather_lookup['data']['ClimateAverages'][0]['month'][9]['absMaxTemp_F'])
    nov_max = float(weather_lookup['data']['ClimateAverages'][0]['month'][10]['absMaxTemp_F'])
    fall_avg = (((sep_min+sep_max)/2) + ((oct_min+oct_max)/2) + ((nov_min+nov_max)/2))/3
    
    return pd.Series([winter_avg,spring_avg,summer_avg,fall_avg])

# COMMAND ----------

rfc_data['display_name'] = rfc_data['city'].map({'Boston': "Boston, MA",
 'Portland': "Portland, OR",
 'Las Vegas': "Las Vegas, NV",
 'Detroit': "Detroit, MI",
 'New York': "New York, NY",
 'Los Angeles': "Los Angeles, CA",
 'Chicago': "Chicago, IL",
 'Houston': "Houston, TX",
 'Phoenix': "Phoenix, AZ",
 'Philadelphia': "Philadelphia, PA",
 'Vancouver': "Vancouver, BC",
 'Montreal': "Montreal, QC",
 'Calgary': "Calgary, AB",
 'Halifax': "Halifax, NS",
 'London': "London, ON",
 'Edmonton': "Edmonton, AB",
 'Mississauga': "Missisauga, ON",
 'Ottawa': "Ottawa, ON",
 'Winnipeg': "Winnipeg, MB",
 'Toronto': "Toronto, ON",
 'Quebec': "Quebec, QC",
 'Cleveland': "Cleveland, OH",
 'Honolulu': "Honolulu, HI",
 'Cincinnati': "Cincinnati, OH",
 'Pittsburgh': "Pittsburgh, PA",
 'Salt Lake City': "Salt Lake City, UT",
 'Fort Worth': "Forth Worth, TX",
 'Columbus': "Columbus, OH",
 'Indianapolis': "Indianapolis, IN",
 'Charlotte': "Charlotte, NC",
 'San Francisco': "San Francisco, CA",
 'Seattle': "Seattle, WA",
 'Denver': "Denver, CO",
 'Washington DC': "Washington DC",
 'Sacramento': "Sacramento, CA",
 'Kansas City': "Kansas City, MO",
 'Atlanta': "Atlanta, GA",
 'Omaha': "Omaha, NB",
 'Colorado Springs': "Colorado Springs, CO",
 'Raleigh': "Raleigh, NC",
 'Miami': "Miami, FL",
 'Memphis': "Memphis, TN",
 'St Louis': "St Louis, MO",
 'Orlando': "Orlando, FL",
 'San Antonio': "San Antonio, TX",
 'San Diego': "San Diego, CA",
 'Dallas': "Dallas, TX",
 'San Jose': "San Jose, CA",
 'Austin': "Austin, TX",
 'Jacksonville': "Jacksonville, FL",
 'Tulsa': "Tulsa, OK",
 'Bakersfield': "Bakersfield, CA",
 'Wichita': "Wichita, KS",
 'Tampa': "Tampa, FL",
 'New Orleans': "New Orleans, LA",
 'Nashville': "Nashville, TN",
 'Oklahoma City': "Oklahoma City, OK",
 'El Paso': "El Paso, TX",
 'Louisville': "Louisville, KY",
 'Baltimore': "Baltimore, MD",
 'Milwaukee': "Milwaukee, WI",
 'Albuquerque': "Albuquerque, NM",
 'Tucson': "Tucson, AZ",
 'Fresno': "Fresno, CA",
 'Oakland': "Oakland, CA",
 'Minneapolis':"Minneapolis, MN"})

import json
import requests
rfc_data[['winter_avg_temp','spring_avg_temp','summer_avg_temp','fall_avg_temp']] = rfc_data['display_name'].apply(lambda x: get_weather(x))

# COMMAND ----------

create_and_save_as_table(rfc_data.drop(columns=['display_name','Climate_Cold Desert', 'Climate_Cold Semi-Arid',
       'Climate_Cold Semi-Arid ', 'Climate_Hot Desert', 'Climate_Hot Desert ',
       'Climate_Hot Semi-Arid ', 'Climate_Hot-Summer Mediterranean',
       'Climate_Humid Continental Climate - Dry Warm Summer',
       'Climate_Humid Continental Hot Summers With Year Around Precipitation',
       'Climate_Humid Continental Mild Summer - Wet All Year',
       'Climate_Humid Subtropical', 'Climate_Tropical Monsoon ',
       'Climate_Tropical Savanna', 'Climate_Warm-Summer Mediterranean']), 'all_model_features_0823')

# COMMAND ----------

rfc_data = get_table_as_pandas_df("all_model_features_0823")

# COMMAND ----------

downtown_rfc_table = rfc_data[['city','total_pop_downtown', 'pct_singlefam_downtown', 'pct_multifam_downtown','pct_renter_downtown','median_age_downtown','bachelor_plus_downtown','median_hhinc_downtown','median_rent_downtown','pct_vacant_downtown','pct_commute_auto_city','pct_commute_public_transit_city','pct_commute_bicycle_city','pct_commute_walk_city','housing_units_downtown','average_commute_time_city','pct_jobs_agriculture_forestry_fishing_hunting','pct_jobs_mining_quarrying_oil_gas', 'pct_jobs_utilities','pct_jobs_construction', 'pct_jobs_manufacturing','pct_jobs_wholesale_trade', 'pct_jobs_retail_trade','pct_jobs_transport_warehouse', 'pct_jobs_information','pct_jobs_finance_insurance','pct_jobs_real_estate','pct_jobs_professional_science_techical','pct_jobs_management_of_companies_enterprises','pct_jobs_administrative_support_waste','pct_jobs_educational_services','pct_jobs_healthcare_social_assistance','pct_jobs_arts_entertainment_recreation','pct_jobs_accomodation_food_services','pct_jobs_public_administration', 'employment_entropy', 'population_density_downtown', 'employment_density_downtown', 'housing_density_downtown','days_school_closing', 'days_workplace_closing','days_cancel_large_events', 'days_cancel_all_events','days_stay_home_requirements', 'days_income_support','days_mask_mandates', 'winter_avg_temp', 'summer_avg_temp']]

#'pct_nhwhite_downtown','pct_nhblack_downtown','pct_nhasian_downtown','pct_hisp_downtown',

city_rfc_table = rfc_data[['city','total_pop_city', 'pct_singlefam_city', 'pct_multifam_city','pct_renter_city','median_age_city','bachelor_plus_city','median_hhinc_city','median_rent_city','pct_vacant_city','pct_commute_auto_city','pct_commute_public_transit_city','pct_commute_bicycle_city','pct_commute_walk_city','housing_units_city','average_commute_time_city','pct_jobs_agriculture_forestry_fishing_hunting','pct_jobs_mining_quarrying_oil_gas', 'pct_jobs_utilities','pct_jobs_construction', 'pct_jobs_manufacturing','pct_jobs_wholesale_trade', 'pct_jobs_retail_trade','pct_jobs_transport_warehouse', 'pct_jobs_information','pct_jobs_finance_insurance','pct_jobs_real_estate','pct_jobs_professional_science_techical','pct_jobs_management_of_companies_enterprises','pct_jobs_administrative_support_waste','pct_jobs_educational_services','pct_jobs_healthcare_social_assistance','pct_jobs_arts_entertainment_recreation','pct_jobs_accomodation_food_services','pct_jobs_public_administration', 'employment_entropy', 'population_density_city', 'employment_density_downtown', 'housing_density_city', 'days_school_closing', 'days_workplace_closing','days_cancel_large_events', 'days_cancel_all_events','days_stay_home_requirements', 'days_income_support','days_mask_mandates', 'winter_avg_temp', 'summer_avg_temp']]

#'pct_nhwhite_city','pct_nhblack_city','pct_nhasian_city','pct_hisp_city',

lq_rfc_table = rfc_data[['city','total_pop_downtown', 'pct_singlefam_downtown', 'pct_multifam_downtown','pct_renter_downtown','median_age_downtown','bachelor_plus_downtown','median_hhinc_downtown','median_rent_downtown','pct_vacant_downtown','pct_commute_auto_city','pct_commute_public_transit_city','pct_commute_bicycle_city','pct_commute_walk_city','housing_units_downtown','average_commute_time_city','pct_jobs_agriculture_forestry_fishing_hunting','pct_jobs_mining_quarrying_oil_gas', 'pct_jobs_utilities','pct_jobs_construction', 'pct_jobs_manufacturing','pct_jobs_wholesale_trade', 'pct_jobs_retail_trade','pct_jobs_transport_warehouse', 'pct_jobs_information','pct_jobs_finance_insurance','pct_jobs_real_estate','pct_jobs_professional_science_techical','pct_jobs_management_of_companies_enterprises','pct_jobs_administrative_support_waste','pct_jobs_educational_services','pct_jobs_healthcare_social_assistance','pct_jobs_arts_entertainment_recreation','pct_jobs_accomodation_food_services','pct_jobs_public_administration', 'employment_entropy', 'population_density_downtown', 'employment_density_downtown', 'housing_density_downtown','days_school_closing', 'days_workplace_closing','days_cancel_large_events', 'days_cancel_all_events','days_stay_home_requirements', 'days_income_support','days_mask_mandates', 'winter_avg_temp', 'summer_avg_temp']]

#lq_rfc_table = rfc_data[['city', 'total_pop_downtown', 'total_pop_city','pct_singlefam_downtown', 'pct_singlefam_city', 'pct_multifam_downtown','pct_multifam_city','pct_mobile_home_and_others_city', 'pct_renter_downtown','pct_renter_city', 'median_age_downtown', 'median_age_city','bachelor_plus_downtown', 'bachelor_plus_city', 'median_hhinc_downtown','median_hhinc_city', 'median_rent_downtown', 'median_rent_city','pct_vacant_downtown', 'pct_vacant_city', 'pct_commute_auto_downtown', 'pct_commute_auto_city','pct_commute_public_transit_downtown','pct_commute_public_transit_city', 'pct_commute_bicycle_downtown','pct_commute_bicycle_city', 'pct_commute_walk_downtown','pct_commute_walk_city','pct_commute_others_city', 'housing_units_downtown','housing_units_city', 'average_commute_time_downtown','average_commute_time_city','pct_jobs_agriculture_forestry_fishing_hunting','pct_jobs_mining_quarrying_oil_gas', 'pct_jobs_utilities','pct_jobs_construction', 'pct_jobs_manufacturing','pct_jobs_wholesale_trade', 'pct_jobs_retail_trade','pct_jobs_transport_warehouse', 'pct_jobs_information','pct_jobs_finance_insurance', 'pct_jobs_real_estate','pct_jobs_professional_science_techical','pct_jobs_management_of_companies_enterprises','pct_jobs_administrative_support_waste','pct_jobs_educational_services','pct_jobs_healthcare_social_assistance','pct_jobs_arts_entertainment_recreation','pct_jobs_accomodation_food_services','pct_jobs_public_administration', 'employment_entropy','population_density_downtown', 'population_density_city','employment_density_downtown', 'housing_density_downtown','housing_density_city', 'days_school_closing', 'days_workplace_closing','days_cancel_large_events', 'days_cancel_all_events','days_stay_home_requirements', 'days_income_support','days_mask_mandates', 'winter_avg_temp', 'summer_avg_temp']]

#'pct_nhwhite_downtown','pct_nhwhite_city', 'pct_nhblack_downtown', 'pct_nhblack_city','pct_nhasian_downtown', 'pct_nhasian_city', 'pct_hisp_downtown','pct_hisp_city', 

#lq_rfc_table['population_lq'] = lq_rfc_table['total_pop_downtown']/lq_rfc_table['total_pop_city']

# COMMAND ----------

rq_dwtn_clusters = rq_dwtn_clusters.replace('Québec','Quebec')
rq_dwtn_clusters = rq_dwtn_clusters.replace('Montréal','Montreal')
downtown_rfc_table = downtown_rfc_table[~downtown_rfc_table['city'].isin(['Dallas','Orlando','Mississauga','Hamilton','Oklahoma City'])]
downtown_rfc_table.loc[:,'cluster'] = downtown_rfc_table.loc[:,'city'].map(dict(zip(rq_dwtn_clusters['city'], rq_dwtn_clusters['cluster'])))

# COMMAND ----------

rq_city_clusters = rq_city_clusters.replace('Québec','Quebec')
rq_city_clusters = rq_city_clusters.replace('Montréal','Montreal')
city_rfc_table = city_rfc_table[~city_rfc_table['city'].isin(['Dallas','Orlando','Mississauga','Hamilton','Oklahoma City'])]
city_rfc_table.loc[:,'cluster'] = city_rfc_table.loc[:,'city'].map(dict(zip(rq_city_clusters['city'], rq_city_clusters['cluster'])))

# COMMAND ----------

lq_clusters = rq_city_clusters.replace('Québec','Quebec')
lq_clusters = rq_city_clusters.replace('Montréal','Montreal')
lq_rfc_table = lq_rfc_table[~lq_rfc_table['city'].isin(['Dallas','Orlando','Mississauga','Hamilton','Oklahoma City'])].copy()
lq_rfc_table.loc[:,'cluster'] = lq_rfc_table.loc[:,'city'].map(dict(zip(lq_clusters['city'], lq_clusters['cluster'])))

# COMMAND ----------

rq_dwtn_clusters_period_1 = rq_dwtn_clusters_period_1.replace('Québec','Quebec')
rq_dwtn_clusters_period_1 = rq_dwtn_clusters_period_1.replace('Montréal','Montreal')
downtown_rfc_table = downtown_rfc_table[~downtown_rfc_table['city'].isin(['Dallas','Orlando','Mississauga','Hamilton','Oklahoma City'])]
downtown_rfc_table.loc[:,'cluster_period_1'] = downtown_rfc_table.loc[:,'city'].map(dict(zip(rq_dwtn_clusters_period_1['city'], rq_dwtn_clusters_period_1['cluster'])))

# COMMAND ----------

rq_dwtn_clusters_period_2 = rq_dwtn_clusters_period_2.replace('Québec','Quebec')
rq_dwtn_clusters_period_2 = rq_dwtn_clusters_period_2.replace('Montréal','Montreal')
downtown_rfc_table = downtown_rfc_table[~downtown_rfc_table['city'].isin(['Dallas','Orlando','Mississauga','Hamilton','Oklahoma City'])]
downtown_rfc_table.loc[:,'cluster_period_2'] = downtown_rfc_table.loc[:,'city'].map(dict(zip(rq_dwtn_clusters_period_2['city'], rq_dwtn_clusters_period_2['cluster'])))

# COMMAND ----------

from sklearn.model_selection import RandomizedSearchCV
# Number of trees in random forest
n_estimators = [int(x) for x in np.linspace(start = 200, stop = 2000, num = 10)]
# Number of features to consider at every split
max_features = ['auto', 'sqrt']
# Maximum number of levels in tree
max_depth = [int(x) for x in np.linspace(10, 110, num = 11)]
max_depth.append(None)
# Minimum number of samples required to split a node
min_samples_split = [2, 5, 10]
# Minimum number of samples required at each leaf node
min_samples_leaf = [1, 2, 4]
# Method of selecting samples for training each tree
bootstrap = [True, False]

# Create the random grid
random_grid = {'n_estimators': n_estimators,
               'max_features': max_features,
               'max_depth': max_depth,
               'min_samples_split': min_samples_split,
               'min_samples_leaf': min_samples_leaf,
               'bootstrap': bootstrap}

X = downtown_rfc_table.drop(columns=['city','cluster','cluster_period_1','cluster_period_2']).to_numpy()
y = downtown_rfc_table['cluster_period_2'].to_numpy()
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.3)

rf = RandomForestClassifier()
rf_random = RandomizedSearchCV(estimator = rf, param_distributions = random_grid, n_iter = 100, cv = 3, verbose=2, random_state=42, n_jobs = -1)
rf_random.fit(X_train, y_train)
rf_random.best_params_

# COMMAND ----------

def generate_random_forest_classifier(X, y, indextable):
    sc = StandardScaler()
    X = sc.fit_transform(X)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.001)
    model = RandomForestClassifier(n_estimators = 400,
                                   min_samples_split = 10,
                                   min_samples_leaf = 4,
                                   max_features = 'sqrt',
                                   max_depth = 90,
                                   bootstrap = True) 
    model.fit(X_train, y_train)
    train_score = model.score(X_train, y_train)
    test_score = model.score(X_test, y_test)
    #feature_importances = pd.Series(model.feature_importances_, index = indextable.drop(columns=['city','cluster','cluster_period_1','cluster_period_2']).columns).sort_values(ascending = False)
    feature_importances = pd.Series(model.feature_importances_, index = indextable.drop(columns=['city','cluster']).columns).sort_values(ascending = False)
    return feature_importances, train_score, test_score

# COMMAND ----------

#Generate RFC Downtown
X_dwtn = downtown_rfc_table.drop(columns=['city','cluster','cluster_period_1','cluster_period_2']).to_numpy()
y_dwtn = downtown_rfc_table['cluster'].to_numpy()
feature_importances, train_score, test_score = generate_random_forest_classifier(X_dwtn,y_dwtn, downtown_rfc_table)
print('train score: '+str(train_score))
print('test score: '+str(test_score))
feature_importances[0:60]

# COMMAND ----------

#Generate RFC City
X_city = city_rfc_table.drop(columns=['city','cluster']).to_numpy()
y_city = city_rfc_table['cluster'].to_numpy()
feature_importances, train_score = generate_random_forest_classifier(X_city,y_city, city_rfc_table)
print('train score: '+str(train_score))
feature_importances[0:60]

# COMMAND ----------

#Generate RFC LQ
X_lq = lq_rfc_table.drop(columns=['city','cluster']).to_numpy()
y_lq = lq_rfc_table['cluster'].to_numpy()
feature_importances, train_score = generate_random_forest_classifier(X_lq,y_lq, lq_rfc_table)
print('train score: '+str(train_score))
feature_importances[0:60]

# COMMAND ----------

#Generate RFC Downtown Period 1 Jun 20 - May 21
X_dwtn = downtown_rfc_table.drop(columns=['city','cluster','cluster_period_1','cluster_period_2','pct_singlefam_downtown']).to_numpy()
y_dwtn = downtown_rfc_table['cluster_period_1'].to_numpy()
feature_importances, train_score, test_score = generate_random_forest_classifier(X_dwtn,y_dwtn, downtown_rfc_table.drop(columns=['pct_singlefam_downtown']))
print('train score: '+str(train_score))
print('test score: '+str(test_score))
display(pd.DataFrame(feature_importances.reset_index()))

# COMMAND ----------

#Generate RFC Downtown Period 2 Jun 21 - May 22
X_dwtn = downtown_rfc_table.drop(columns=['city','cluster','cluster_period_1','cluster_period_2','pct_singlefam_downtown']).to_numpy()
y_dwtn = downtown_rfc_table['cluster_period_2'].to_numpy()
feature_importances, train_score, test_score = generate_random_forest_classifier(X_dwtn,y_dwtn, downtown_rfc_table.drop(columns=['pct_singlefam_downtown']))
print('train score: '+str(train_score))
print('test score: '+str(test_score))
display(pd.DataFrame(feature_importances.reset_index()))

# COMMAND ----------

#Generate RFC LQ
X_dwtn = lq_rfc_table.drop(columns=['city','cluster','pct_singlefam_downtown']).to_numpy()
y_dwtn = lq_rfc_table['cluster'].to_numpy()
feature_importances, train_score, test_score = generate_random_forest_classifier(X_dwtn,y_dwtn, lq_rfc_table.drop(columns=['pct_singlefam_downtown']))
print('train score: '+str(train_score))
print('test score: '+str(test_score))
display(pd.DataFrame(feature_importances.reset_index()))

# COMMAND ----------

normalized_visits = get_table_as_pandas_df('0714_combined_metrics_df')[['city','metric','normalized_visits_by_total_visits','week']].replace('Montréal','Montreal').replace('Québec','Quebec')
normalized_visits = normalized_visits[~normalized_visits["city"].isin(['Dallas','Orlando','Mississauga','Hamilton','Oklahoma City'])].copy()
normalized_visits_city = normalized_visits[normalized_visits["metric"]=="metro"].copy()
normalized_visits_city['cluster'] = normalized_visits_city['city'].map(dict(zip(rq_city_clusters['city'], rq_city_clusters['cluster']))).astype(int)

# COMMAND ----------

city_rfc_table = city_rfc_table[~city_rfc_table['city'].isin(['Dallas','Orlando','Mississauga','Hamilton','Oklahoma City'])]
city_rfc_table.loc[:,'cluster'] = city_rfc_table.loc[:,'city'].map(dict(zip(rq_city_clusters['city'], rq_city_clusters['cluster'])))

# COMMAND ----------

mean_city = normalized_visits_city.groupby(['cluster','week']).mean()['normalized_visits_by_total_visits'].reset_index()

# COMMAND ----------

max_city = normalized_visits_city.groupby(['cluster','week']).max()['normalized_visits_by_total_visits'].reset_index()

# COMMAND ----------

min_city = normalized_visits_city.groupby(['cluster','week']).min()['normalized_visits_by_total_visits'].reset_index()

# COMMAND ----------

mean_city["week"] = pd.to_datetime(mean_city["week"])

# COMMAND ----------

mean_city_rolling = pd.DataFrame()
for i in mean_city["cluster"].unique():
    mean_city_rolling_cluster = mean_city[mean_city["cluster"]==i].copy()
    mean_city_rolling_cluster['rolling'] = mean_city_rolling_cluster['normalized_visits_by_total_visits'].rolling(window=9).mean().shift(-4)
    mean_city_rolling = pd.concat([mean_city_rolling,mean_city_rolling_cluster])

# COMMAND ----------

import seaborn as sns
#sns.set_style('darkgrid')
sns.set(rc={'figure.figsize':(14,8)})
ax = sns.lineplot(data=mean_city_rolling, x ='week', y = 'rolling', hue='cluster', palette='tab10')
plt.ylabel('City RQ')
plt.xlabel('Date')
plt.show()

# COMMAND ----------

import datetime
import seaborn as sns
datetime.date(2020, 6, 1)

# COMMAND ----------

normalized_visits_dwtn = normalized_visits[normalized_visits["metric"]=="downtown"].copy()
normalized_visits_dwtn['cluster'] = normalized_visits_dwtn['city'].map(dict(zip(rq_dwtn_clusters['city'], rq_dwtn_clusters['cluster']))).astype(int)
#mean_dwtn = normalized_visits_dwtn.groupby(['cluster','week']).mean()['normalized_visits_by_total_visits'].reset_index()
normalized_visits_dwtn["week"] = pd.to_datetime(normalized_visits_dwtn["week"]).dt.date
normalized_visits_dwtn[normalized_visits_dwtn.isnull()]

# COMMAND ----------

 grouped.loc[:,('rolling', 'median')]

# COMMAND ----------

normalized_visits_dwtn = normalized_visits[normalized_visits["metric"]=="downtown"].copy()
normalized_visits_dwtn['cluster'] = normalized_visits_dwtn['city'].map(dict(zip(rq_dwtn_clusters['city'], rq_dwtn_clusters['cluster']))).astype(int)
#mean_dwtn = normalized_visits_dwtn.groupby(['cluster','week']).mean()['normalized_visits_by_total_visits'].reset_index()
normalized_visits_dwtn["week"] = pd.to_datetime(normalized_visits_dwtn["week"]).dt.date
normalized_visits_dwtn = normalized_visits_dwtn.dropna()
normalized_visits_dwtn = normalized_visits_dwtn[normalized_visits_dwtn["week"]>datetime.date(2020, 6, 1)]
normalized_visits_dwtn['rolling'] = normalized_visits_dwtn.sort_values(by = 'week').groupby(['city'])['normalized_visits_by_total_visits'].transform(lambda x : x.rolling(window=19).mean().shift(-9))
normalized_visits_dwtn = normalized_visits_dwtn.dropna()

# compute the min, median and max
grouped = normalized_visits_dwtn.groupby(["cluster", "week"]).agg({'rolling': ['min', 'median', 'max']}).unstack("cluster")
grouped = grouped.dropna()

# plot medians
ax = grouped.loc[:,('rolling', 'median')].plot(figsize = (16, 12), sharey = True)

# Getting the color palette used
palette = sns.color_palette()

# index is now the number of clusters and indexes into grouped by cluster #
index = 0
for index in np.arange(0, len(normalized_visits_dwtn['cluster'].unique())):
        if index < 10: 
            ax.fill_between(grouped.index, grouped.loc[:,('rolling', 'median', index)], 
                    grouped.loc[:,('rolling', 'max', index)], alpha=.2, color=palette[index])
            ax.fill_between(grouped.index, 
                    grouped.loc[:,('rolling', 'min', index)] , grouped.loc[:,('rolling', 'median', index)], alpha=.2, color=palette[index])
plt.ylabel('Downtown RQ')
plt.xlabel('Date')
ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
plt.title('Downtown recovery cluster extrema')
plt.show()

# COMMAND ----------

normalized_visits_dwtn = normalized_visits[normalized_visits["metric"]=="downtown"].copy()
normalized_visits_dwtn['cluster'] = normalized_visits_dwtn['city'].map(dict(zip(rq_dwtn_clusters['city'], rq_dwtn_clusters['cluster']))).astype(int)
mean_dwtn = normalized_visits_dwtn.groupby(['cluster','week']).mean()['normalized_visits_by_total_visits'].reset_index()
mean_dwtn["week"] = pd.to_datetime(mean_dwtn["week"]).dt.date
mean_dwtn = mean_dwtn[mean_dwtn["week"]>datetime.date(2020, 6, 1)]
mean_dwtn['rolling'] = mean_dwtn.groupby('cluster')['normalized_visits_by_total_visits'].transform(lambda x : x.rolling(window=19).mean().shift(-9))
sns.set(rc={'figure.figsize':(14,10)})
ax = sns.lineplot(data=mean_dwtn, x ='week', y = 'rolling', hue='cluster', palette='tab10')
plt.ylabel('Downtown RQ')
plt.xlabel('Date')
plt.show()

# COMMAND ----------

normalized_visits_dwtn[normalized_visits_dwtn["week"]==datetime.date(2020,12,7)]

# COMMAND ----------

clusters = get_table_as_pandas_df('rq_dwtn_clusters_0823_period_2').replace('Montréal','Montreal').replace('Québec','Quebec')
normalized_visits_dwtn = normalized_visits[normalized_visits["metric"]=="downtown"].copy()
normalized_visits_dwtn['cluster'] = normalized_visits_dwtn['city'].map(dict(zip(clusters['city'], clusters['cluster']))).astype(int)
normalized_visits_dwtn['rolling'] = normalized_visits_dwtn.groupby('city')['normalized_visits_by_total_visits'].transform(lambda x : x.rolling(window=9).mean().shift(-4))
normalized_visits_dwtn["week"] = pd.to_datetime(normalized_visits_dwtn["week"]).dt.date
normalized_visits_dwtn.week.unique()

# COMMAND ----------

clusters = get_table_as_pandas_df('rq_dwtn_clusters_0823_period_2').replace('Montréal','Montreal').replace('Québec','Quebec')
normalized_visits_dwtn = normalized_visits[normalized_visits["metric"]=="downtown"].copy()
normalized_visits_dwtn['cluster'] = normalized_visits_dwtn['city'].map(dict(zip(clusters['city'], clusters['cluster']))).astype(int)
normalized_visits_dwtn['rolling'] = normalized_visits_dwtn.groupby('city')['normalized_visits_by_total_visits'].transform(lambda x : x.rolling(window=5).mean().shift(-3))
normalized_visits_dwtn["week"] = pd.to_datetime(mean_dwtn["week"]).dt.date
#mean_dwtn = normalized_visits_dwtn.groupby(['cluster','week']).mean()['normalized_visits_by_total_visits'].reset_index()
#mean_dwtn["week"] = pd.to_datetime(mean_dwtn["week"]).dt.date
#mean_dwtn = mean_dwtn[mean_dwtn["week"]>datetime.date(2020, 6, 1)]
#mean_dwtn['rolling'] = mean_dwtn.groupby('cluster')['normalized_visits_by_total_visits'].transform(lambda x : x.rolling(window=9).mean().shift(-4))
sns.set(rc={'figure.figsize':(14,10)})
ax = sns.lineplot(data=normalized_visits_dwtn, x ='week', y = 'rolling', hue='cluster', palette='tab10')
plt.ylabel('Downtown RQ')
plt.xlabel('Date')
plt.show()

# COMMAND ----------

clusters = get_table_as_pandas_df('rq_dwtn_clusters_0823_period_2').replace('Montréal','Montreal').replace('Québec','Quebec')
normalized_visits_dwtn = normalized_visits[normalized_visits["metric"]=="downtown"].copy()
normalized_visits_dwtn['cluster'] = normalized_visits_dwtn['city'].map(dict(zip(clusters['city'], clusters['cluster']))).astype(int)
normalized_visits_dwtn['rolling'] = normalized_visits_dwtn.groupby('city')['normalized_visits_by_total_visits'].transform(lambda x : x.rolling(window=5).mean().shift(-3))
#normalized_visits_dwtn["week"] = pd.to_datetime(mean_dwtn["week"]).dt.date
normalized_visits_dwtn["color"] = normalized_visits_dwtn["cluster"].map({0:'blue', 1:'green',2:'orange',3:'red',4:'pink',5:'black'})
sns.set(rc={'figure.figsize':(14,10)})
for city in normalized_visits_dwtn['city'].unique():
    city_data = normalized_visits_dwtn[normalized_visits_dwtn['city']==city]
    ax = plt.plot(city_data['week'], city_data['rolling'], color=city_data['color'].iloc[0], linewidth=0.5)
plt.ylabel('Downtown RQ')
plt.xlabel('Date')
plt.show()

# COMMAND ----------

import seaborn as sns

# COMMAND ----------

clusters = get_table_as_pandas_df('rq_dwtn_clusters_0823_period_1').replace('Montréal','Montreal').replace('Québec','Quebec')
normalized_visits = get_table_as_pandas_df('0714_combined_metrics_df').replace('Montréal','Montreal').replace('Québec','Quebec')
normalized_visits = normalized_visits[~normalized_visits['city'].isin(['Orlando','Dallas','Hamilton','Mississauga','Oklahoma City'])]
normalized_visits_dwtn = normalized_visits[normalized_visits["metric"]=="downtown"].copy()
normalized_visits_dwtn['cluster'] = normalized_visits_dwtn['city'].map(dict(zip(clusters['city'], clusters['cluster']))).astype(int)
normalized_visits_dwtn['rolling'] = normalized_visits_dwtn.groupby('city')['normalized_visits_by_total_visits'].transform(lambda x : x.rolling(window=9).mean().shift(-4))
normalized_visits_dwtn = normalized_visits_dwtn.sort_values(by='week')
normalized_visits_dwtn = normalized_visits_dwtn[normalized_visits_dwtn['week']<'2022-06-01']
normalized_visits_dwtn["week"] = pd.to_datetime(normalized_visits_dwtn["week"].values)
sns.set(rc={'figure.figsize':(14,10)})
ax = sns.lineplot(data=normalized_visits_dwtn, x ='week', y = 'rolling', hue='cluster', palette='tab10', units='city', alpha=0.3, estimator=None)#, ci='sd')
plt.ylabel('Downtown RQ')
plt.xlabel('Date')
plt.show()

# COMMAND ----------

clusters = get_table_as_pandas_df('rq_dwtn_clusters_0823_period_2').replace('Montréal','Montreal').replace('Québec','Quebec')
normalized_visits = get_table_as_pandas_df('0714_combined_metrics_df').replace('Montréal','Montreal').replace('Québec','Quebec')
normalized_visits = normalized_visits[~normalized_visits['city'].isin(['Orlando','Dallas','Hamilton','Mississauga','Oklahoma City'])]
normalized_visits_dwtn = normalized_visits[normalized_visits["metric"]=="downtown"].copy()
normalized_visits_dwtn['cluster'] = normalized_visits_dwtn['city'].map(dict(zip(clusters['city'], clusters['cluster']))).astype(int)
normalized_visits_dwtn['rolling'] = normalized_visits_dwtn.groupby('city')['normalized_visits_by_total_visits'].transform(lambda x : x.rolling(window=9).mean().shift(-4))
normalized_visits_dwtn = normalized_visits_dwtn.sort_values(by='week')
normalized_visits_dwtn = normalized_visits_dwtn[normalized_visits_dwtn['week']<'2022-06-01']
normalized_visits_dwtn["week"] = pd.to_datetime(normalized_visits_dwtn["week"].values)
#mean_dwtn = normalized_visits_dwtn.groupby(['cluster','week']).mean()['normalized_visits_by_total_visits'].reset_index()
#mean_dwtn["week"] = pd.to_datetime(mean_dwtn["week"]).dt.date
#mean_dwtn = mean_dwtn[mean_dwtn["week"]>datetime.date(2020, 6, 1)]
#mean_dwtn['rolling'] = mean_dwtn.groupby('cluster')['normalized_visits_by_total_visits'].transform(lambda x : x.rolling(window=9).mean().shift(-4))
sns.set(rc={'figure.figsize':(14,10)})
ax = sns.lineplot(data=normalized_visits_dwtn, x ='week', y = 'rolling', hue='cluster', palette='tab10', units='city', alpha=0.3, estimator=None)#, ci='sd')
plt.ylabel('Downtown RQ')
plt.xlabel('Date')
plt.show()

# COMMAND ----------

normalized_visits = get_table_as_pandas_df('0714_combined_metrics_df')[['city','metric','normalized_visits_by_total_visits','week']].replace('Montréal','Montreal').replace('Québec','Quebec')
normalized_visits = normalized_visits[~normalized_visits["city"].isin(['Dallas','Orlando','Mississauga','Hamilton','Oklahoma City'])].copy()
normalized_visits_lq = normalized_visits[normalized_visits["metric"]=="relative"].copy()
normalized_visits_lq['cluster'] = normalized_visits_city['city'].map(dict(zip(rq_city_clusters['city'], rq_city_clusters['cluster']))).astype(int)

# COMMAND ----------

import seaborn as sns
sns.set_style("whitegrid")

# COMMAND ----------

clusters = get_table_as_pandas_df('rq_dwtn_clusters_0823_period_1').replace('Montréal','Montreal').replace('Québec','Quebec')
normalized_visits = get_table_as_pandas_df('0714_combined_metrics_df').replace('Montréal','Montreal').replace('Québec','Quebec')
normalized_visits = normalized_visits[~normalized_visits['city'].isin(['Orlando','Dallas','Hamilton','Mississauga','Oklahoma City'])]
normalized_visits_dwtn = normalized_visits[normalized_visits["metric"]=="relative"].copy()
normalized_visits_dwtn['cluster'] = normalized_visits_dwtn['city'].map(dict(zip(clusters['city'], clusters['cluster']))).astype(int)
normalized_visits_dwtn['rolling'] = normalized_visits_dwtn.groupby('city')['normalized_visits_by_total_visits'].transform(lambda x : x.rolling(window=15).mean().shift(-7))
normalized_visits_dwtn = normalized_visits_dwtn.sort_values(by='week')
normalized_visits_dwtn = normalized_visits_dwtn[normalized_visits_dwtn['week']<'2022-06-01']
normalized_visits_dwtn["week"] = pd.to_datetime(normalized_visits_dwtn["week"].values)
#mean_dwtn = normalized_visits_dwtn.groupby(['cluster','week']).mean()['normalized_visits_by_total_visits'].reset_index()
#mean_dwtn["week"] = pd.to_datetime(mean_dwtn["week"]).dt.date
#mean_dwtn = mean_dwtn[mean_dwtn["week"]>datetime.date(2020, 6, 1)]
#mean_dwtn['rolling'] = mean_dwtn.groupby('cluster')['normalized_visits_by_total_visits'].transform(lambda x : x.rolling(window=9).mean().shift(-4))
sns.set(rc={'figure.figsize':(14,10)})
ax = sns.lineplot(data=normalized_visits_dwtn, x ='week', y = 'rolling', hue='cluster', palette='tab10', units='city', alpha=0.3, estimator=None)#, ci='sd')
plt.ylabel('LQ')
plt.xlabel('Date')
plt.show()

# COMMAND ----------

clusters = get_table_as_pandas_df('rq_dwtn_clusters_0823_period_2').replace('Montréal','Montreal').replace('Québec','Quebec')
normalized_visits = get_table_as_pandas_df('0714_combined_metrics_df').replace('Montréal','Montreal').replace('Québec','Quebec')
normalized_visits = normalized_visits[~normalized_visits['city'].isin(['Orlando','Dallas','Hamilton','Mississauga','Oklahoma City'])]
normalized_visits_dwtn = normalized_visits[normalized_visits["metric"]=="relative"].copy()
normalized_visits_dwtn['cluster'] = normalized_visits_dwtn['city'].map(dict(zip(clusters['city'], clusters['cluster']))).astype(int)
normalized_visits_dwtn['rolling'] = normalized_visits_dwtn.groupby('city')['normalized_visits_by_total_visits'].transform(lambda x : x.rolling(window=15).mean().shift(-7))
normalized_visits_dwtn = normalized_visits_dwtn.sort_values(by='week')
normalized_visits_dwtn = normalized_visits_dwtn[normalized_visits_dwtn['week']<'2022-06-01']
normalized_visits_dwtn["week"] = pd.to_datetime(normalized_visits_dwtn["week"].values)
#mean_dwtn = normalized_visits_dwtn.groupby(['cluster','week']).mean()['normalized_visits_by_total_visits'].reset_index()
#mean_dwtn["week"] = pd.to_datetime(mean_dwtn["week"]).dt.date
#mean_dwtn = mean_dwtn[mean_dwtn["week"]>datetime.date(2020, 6, 1)]
#mean_dwtn['rolling'] = mean_dwtn.groupby('cluster')['normalized_visits_by_total_visits'].transform(lambda x : x.rolling(window=9).mean().shift(-4))
sns.set(rc={'figure.figsize':(14,10)})
ax = sns.lineplot(data=normalized_visits_dwtn, x ='week', y = 'rolling', hue='cluster', palette='tab10', units='city', alpha=0.3, estimator=None)#, ci='sd')
plt.ylabel('LQ')
plt.xlabel('Date')
plt.show()

# COMMAND ----------

clusters = get_table_as_pandas_df('lq_clusters_0824_period_2_new').replace('Montréal','Montreal').replace('Québec','Quebec')
normalized_visits = get_table_as_pandas_df('0714_combined_metrics_df').replace('Montréal','Montreal').replace('Québec','Quebec')
normalized_visits = normalized_visits[~normalized_visits['city'].isin(['Orlando','Dallas','Hamilton','Mississauga','Oklahoma City'])]
normalized_visits_dwtn = normalized_visits[normalized_visits["metric"]=="relative"].copy()
normalized_visits_dwtn['cluster'] = normalized_visits_dwtn['city'].map(dict(zip(clusters['city'], clusters['cluster']))).astype(int)
normalized_visits_dwtn['rolling'] = normalized_visits_dwtn.groupby('city')['normalized_visits_by_total_visits'].transform(lambda x : x.rolling(window=15).mean().shift(-7))
normalized_visits_dwtn = normalized_visits_dwtn.sort_values(by='week')
normalized_visits_dwtn = normalized_visits_dwtn[normalized_visits_dwtn['week']<'2022-06-01']
normalized_visits_dwtn["week"] = pd.to_datetime(normalized_visits_dwtn["week"].values)
#mean_dwtn = normalized_visits_dwtn.groupby(['cluster','week']).mean()['normalized_visits_by_total_visits'].reset_index()
#mean_dwtn["week"] = pd.to_datetime(mean_dwtn["week"]).dt.date
#mean_dwtn = mean_dwtn[mean_dwtn["week"]>datetime.date(2020, 6, 1)]
#mean_dwtn['rolling'] = mean_dwtn.groupby('cluster')['normalized_visits_by_total_visits'].transform(lambda x : x.rolling(window=9).mean().shift(-4))
sns.set(rc={'figure.figsize':(14,10)})
ax = sns.lineplot(data=normalized_visits_dwtn, x ='week', y = 'rolling', hue='cluster', palette='tab10', units='city', alpha=0.3, estimator=None)#, ci='sd')
plt.ylabel('LQ')
plt.xlabel('Date')
plt.show()

# COMMAND ----------

sns.set_theme(style="whitegrid")
clusters = get_table_as_pandas_df('lq_clusters_single_period_4').replace('Montréal','Montreal').replace('Québec','Quebec')
normalized_visits = get_table_as_pandas_df('0714_combined_metrics_df').replace('Montréal','Montreal').replace('Québec','Quebec')
normalized_visits = normalized_visits[~normalized_visits['city'].isin(['Orlando','Dallas','Hamilton','Mississauga','Oklahoma City'])]
normalized_visits_dwtn = normalized_visits[normalized_visits["metric"]=="relative"].copy()
normalized_visits_dwtn = normalized_visits_dwtn[normalized_visits_dwtn["normalized_visits_by_total_visits"].notna()]
normalized_visits_dwtn['cluster'] = normalized_visits_dwtn['city'].map(dict(zip(clusters['city'], clusters['cluster']))).astype(int)
normalized_visits_dwtn['rolling'] = normalized_visits_dwtn.groupby('city')['normalized_visits_by_total_visits'].transform(lambda x : x.rolling(window=13).mean().shift(-6))
normalized_visits_dwtn = normalized_visits_dwtn.sort_values(by='week')
#normalized_visits_dwtn = normalized_visits_dwtn[normalized_visits_dwtn['week']>'2020-11-01']
normalized_visits_dwtn = normalized_visits_dwtn[normalized_visits_dwtn['week']<'2022-06-01']
normalized_visits_dwtn["week"] = pd.to_datetime(normalized_visits_dwtn["week"])
normalized_visits_dwtn_avg = normalized_visits_dwtn.groupby(['week','cluster']).mean()['rolling'].reset_index()
sns.set(rc={'figure.figsize':(16,12)})
sns.set(font_scale = 1.15)
sns.set_style("whitegrid")
sns.lineplot(data=normalized_visits_dwtn, x ='week', y = 'rolling', hue='cluster', palette='tab10', units='city', alpha=0.25, estimator=None, linewidth=1.5)#, ci='sd')
sns.lineplot(data=normalized_visits_dwtn_avg, x ='week', y = 'rolling', hue='cluster', palette='tab10', units='cluster', alpha=0.8, estimator=None, linewidth=2.5)#, ci='sd')
plt.ylabel('Location Quotient')
plt.xlabel('Date (Year-Month)')
plt.legend(bbox_to_anchor=(1.02, 1), loc='upper left', borderaxespad=0)
plt.show()

# COMMAND ----------

sns.set_theme(style="whitegrid")
clusters = get_table_as_pandas_df('rq_dwtn_clusters_0823_period_1').replace('Montréal','Montreal').replace('Québec','Quebec')
normalized_visits = get_table_as_pandas_df('0714_combined_metrics_df').replace('Montréal','Montreal').replace('Québec','Quebec')
normalized_visits = normalized_visits[~normalized_visits['city'].isin(['Orlando','Dallas','Hamilton','Mississauga','Oklahoma City'])]
normalized_visits_dwtn = normalized_visits[normalized_visits["metric"]=="downtown"].copy()
normalized_visits_dwtn = normalized_visits_dwtn[normalized_visits_dwtn["normalized_visits_by_total_visits"].notna()]
normalized_visits_dwtn['cluster'] = normalized_visits_dwtn['city'].map(dict(zip(clusters['city'], clusters['cluster']))).astype(int)
normalized_visits_dwtn = normalized_visits_dwtn.sort_values(by='week')
normalized_visits_dwtn['rolling'] = normalized_visits_dwtn.groupby('city')['normalized_visits_by_total_visits'].transform(lambda x : x.rolling(window=15).mean().shift(-7))
normalized_visits_dwtn['cluster'] = normalized_visits_dwtn['cluster'].map({0:0,1:1,2:2,3:3,4:4,5:0})
normalized_visits_dwtn = normalized_visits_dwtn[normalized_visits_dwtn['week']>'2020-06-01']
normalized_visits_dwtn = normalized_visits_dwtn[normalized_visits_dwtn['week']<'2021-06-01']
normalized_visits_dwtn["week"] = pd.to_datetime(normalized_visits_dwtn["week"])
normalized_visits_dwtn_avg = normalized_visits_dwtn.groupby(['week','cluster']).mean()['rolling'].reset_index()
sns.set(rc={'figure.figsize':(7.5,12)})
sns.set(font_scale = 1.15)
sns.set_style("whitegrid")
sns.lineplot(data=normalized_visits_dwtn, x ='week', y = 'rolling', hue='cluster', palette='tab10', units='city', alpha=0.25, estimator=None, linewidth=1.5)#, ci='sd')
sns.lineplot(data=normalized_visits_dwtn_avg, x ='week', y = 'rolling', hue='cluster', palette='tab10', units='cluster', alpha=0.8, estimator=None, linewidth=2.5)#, ci='sd')
plt.ylabel('Location Quotient')
plt.xlabel('Date (Year-Month)')
plt.ylim(0.1,1.6)
plt.legend(bbox_to_anchor=(1.02, 1), loc='upper left', borderaxespad=0)
plt.show()

# COMMAND ----------

sns.set_theme(style="whitegrid")
clusters = get_table_as_pandas_df('rq_dwtn_clusters_0823_period_2').replace('Montréal','Montreal').replace('Québec','Quebec')
normalized_visits = get_table_as_pandas_df('0714_combined_metrics_df').replace('Montréal','Montreal').replace('Québec','Quebec')
normalized_visits = normalized_visits[~normalized_visits['city'].isin(['Orlando','Dallas','Hamilton','Mississauga','Oklahoma City'])]
normalized_visits_dwtn = normalized_visits[normalized_visits["metric"]=="downtown"].copy()
normalized_visits_dwtn = normalized_visits_dwtn[normalized_visits_dwtn["normalized_visits_by_total_visits"].notna()]
normalized_visits_dwtn['cluster'] = normalized_visits_dwtn['city'].map(dict(zip(clusters['city'], clusters['cluster']))).astype(int)
normalized_visits_dwtn["week"] = pd.to_datetime(normalized_visits_dwtn["week"])
normalized_visits_dwtn = normalized_visits_dwtn.sort_values(by='week')
normalized_visits_dwtn['rolling'] = normalized_visits_dwtn.groupby('city')['normalized_visits_by_total_visits'].transform(lambda x : x.rolling(window=15).mean().shift(-7))
normalized_visits_dwtn['cluster'] = normalized_visits_dwtn['cluster'].map({0:0,1:1,2:2,3:3,4:4,5:1})
normalized_visits_dwtn = normalized_visits_dwtn[normalized_visits_dwtn['week']>'2021-06-01']
normalized_visits_dwtn = normalized_visits_dwtn[normalized_visits_dwtn['week']<'2022-05-15']
normalized_visits_dwtn["week"] = pd.to_datetime(normalized_visits_dwtn["week"])
normalized_visits_dwtn_avg = normalized_visits_dwtn.groupby(['week','cluster']).mean()['rolling'].reset_index()
sns.set(rc={'figure.figsize':(7.5,12)})
sns.set(font_scale = 1.15)
sns.set_style("whitegrid")
sns.lineplot(data=normalized_visits_dwtn, x ='week', y = 'rolling', hue='cluster', palette='husl', units='city', alpha=0.25, estimator=None, linewidth=1.5)#, ci='sd')
sns.lineplot(data=normalized_visits_dwtn_avg, x ='week', y = 'rolling', hue='cluster', palette='husl', units='cluster', alpha=0.8, estimator=None, linewidth=2.5)#, ci='sd')

plt.ylabel('Location Quotient')
plt.xlabel('Date (Year-Month)')
plt.ylim(0.1,1.6)
plt.legend(bbox_to_anchor=(1.02, 1), loc='upper left', borderaxespad=0)
plt.show()

# COMMAND ----------

sns.set_theme(style="whitegrid")
clusters = get_table_as_pandas_df('rq_dwtn_clusters_0823_period_2').replace('Montréal','Montreal').replace('Québec','Quebec')
normalized_visits = get_table_as_pandas_df('0714_combined_metrics_df').replace('Montréal','Montreal').replace('Québec','Quebec')
normalized_visits = normalized_visits[~normalized_visits['city'].isin(['Orlando','Dallas','Hamilton','Mississauga','Oklahoma City'])]
normalized_visits_dwtn = normalized_visits[normalized_visits["metric"]=="downtown"].copy()
normalized_visits_dwtn = normalized_visits_dwtn[~normalized_visits_dwtn["normalized_visits_by_total_visits"].isna()]
normalized_visits_dwtn = normalized_visits_dwtn[normalized_visits_dwtn['week']>'2021-06-01']
normalized_visits_dwtn = normalized_visits_dwtn[normalized_visits_dwtn['week']<'2022-05-31']
normalized_visits_dwtn["week"] = pd.to_datetime(normalized_visits_dwtn["week"])
normalized_visits_dwtn = normalized_visits_dwtn.sort_values(by='week')
normalized_visits_dwtn['cluster'] = normalized_visits_dwtn['city'].map(dict(zip(clusters['city'], clusters['cluster']))).astype(int)
normalized_visits_dwtn['normalized_visits_by_total_visits'] = normalized_visits_dwtn['normalized_visits_by_total_visits'].astype(float)
normalized_visits_dwtn['rolling'] = normalized_visits_dwtn.groupby('city')['normalized_visits_by_total_visits'].transform(lambda x : x.rolling(window=15).mean().shift(-7))
normalized_visits_dwtn['cluster'] = normalized_visits_dwtn['cluster'].map({0:0,1:1,2:2,3:3,4:4,5:1})
normalized_visits_dwtn_avg = normalized_visits_dwtn.groupby(['week','cluster']).mean()['rolling'].reset_index()
sns.set(rc={'figure.figsize':(7.5,12)})
sns.set(font_scale = 1.15)
sns.set_style("whitegrid")
sns.lineplot(data=normalized_visits_dwtn, x ='week', y = 'rolling', hue='cluster', palette='husl', units='city', alpha=0.25, estimator=None, linewidth=1.5)#, ci='sd')
sns.lineplot(data=normalized_visits_dwtn_avg, x ='week', y = 'rolling', hue='cluster', palette='husl', units='cluster', alpha=0.8, estimator=None, linewidth=2.5)#, ci='sd')
plt.ylabel('Location Quotient')
plt.xlabel('Date (Year-Month)')
plt.ylim(0.1,1.6)
plt.legend(bbox_to_anchor=(1.02, 1), loc='upper left', borderaxespad=0)
plt.show()

# COMMAND ----------

import seaborn as sns
sns.set_theme(style="whitegrid")
clusters = get_table_as_pandas_df('rq_dwtn_clusters_0831_all_2').replace('Montréal','Montreal').replace('Québec','Quebec')
normalized_visits = get_table_as_pandas_df('0714_combined_metrics_df').replace('Montréal','Montreal').replace('Québec','Quebec')
normalized_visits = normalized_visits[~normalized_visits['city'].isin(['Orlando','Dallas','Hamilton','Mississauga','Oklahoma City'])]
normalized_visits_dwtn = normalized_visits[normalized_visits["metric"]=="downtown"].copy()
normalized_visits_dwtn = normalized_visits_dwtn[normalized_visits_dwtn["normalized_visits_by_total_visits"].notna()]
normalized_visits_dwtn['cluster'] = normalized_visits_dwtn['city'].map(dict(zip(clusters['city'], clusters['cluster']))).astype(int)
normalized_visits_dwtn = normalized_visits_dwtn.sort_values(by='week')
normalized_visits_dwtn['rolling'] = normalized_visits_dwtn.groupby('city')['normalized_visits_by_total_visits'].transform(lambda x : x.rolling(window=15).mean().shift(-7))
normalized_visits_dwtn['cluster'] = normalized_visits_dwtn['cluster'].map({0:0,1:1,2:2,3:3,4:4,5:0})
normalized_visits_dwtn = normalized_visits_dwtn[normalized_visits_dwtn['week']>'2020-06-01']
normalized_visits_dwtn = normalized_visits_dwtn[normalized_visits_dwtn['week']<'2021-06-01']
normalized_visits_dwtn["week"] = pd.to_datetime(normalized_visits_dwtn["week"])
normalized_visits_dwtn_avg = normalized_visits_dwtn.groupby(['week','cluster']).mean()['rolling'].reset_index()
sns.set(rc={'figure.figsize':(15,12)})
sns.set(font_scale = 1.15)
sns.set_style("whitegrid")
sns.lineplot(data=normalized_visits_dwtn, x ='week', y = 'rolling', hue='cluster', palette='tab10', units='city', alpha=0.25, estimator=None, linewidth=1.5)#, ci='sd')
sns.lineplot(data=normalized_visits_dwtn_avg, x ='week', y = 'rolling', hue='cluster', palette='tab10', units='cluster', alpha=0.8, estimator=None, linewidth=2.5)#, ci='sd')
plt.ylabel('Location Quotient')
plt.xlabel('Date (Year-Month)')
plt.ylim(0.1,1.6)
plt.legend(bbox_to_anchor=(1.02, 1), loc='upper left', borderaxespad=0)
plt.show()

# COMMAND ----------

display(normalized_visits_dwtn)

# COMMAND ----------

normalized_visits_dwtn.groupby(['week','cluster']).mean()['rolling'].reset_index()

# COMMAND ----------

clusters = get_table_as_pandas_df('rq_dwtn_clusters_0823_period_1').replace('Montréal','Montreal').replace('Québec','Quebec')
normalized_visits_dwtn = normalized_visits[normalized_visits["metric"]=="downtown"].copy()
normalized_visits_dwtn['cluster'] = normalized_visits_dwtn['city'].map(dict(zip(clusters['city'], clusters['cluster']))).astype(int)
mean_dwtn = normalized_visits_dwtn.groupby(['cluster','week']).mean()['normalized_visits_by_total_visits'].reset_index()
mean_dwtn["week"] = pd.to_datetime(mean_dwtn["week"]).dt.date
mean_dwtn = mean_dwtn[mean_dwtn["week"]>datetime.date(2020, 6, 1)]
mean_dwtn['rolling'] = mean_dwtn.groupby('cluster')['normalized_visits_by_total_visits'].transform(lambda x : x.rolling(window=9).mean().shift(-4))
sns.set(rc={'figure.figsize':(14,10)})
ax = sns.lineplot(data=mean_dwtn, x ='week', y = 'rolling', hue='cluster', palette='tab10')
plt.ylabel('Downtown RQ')
plt.xlabel('Date')
plt.show()

# COMMAND ----------

clusters = get_table_as_pandas_df('rq_dwtn_clusters_0823_period_2').replace('Montréal','Montreal').replace('Québec','Quebec')
normalized_visits_dwtn = normalized_visits[normalized_visits["metric"]=="downtown"].copy()
normalized_visits_dwtn['cluster'] = normalized_visits_dwtn['city'].map(dict(zip(clusters['city'], clusters['cluster']))).astype(int)
mean_dwtn = normalized_visits_dwtn.groupby(['cluster','week']).mean()['normalized_visits_by_total_visits'].reset_index()
mean_dwtn["week"] = pd.to_datetime(mean_dwtn["week"]).dt.date
mean_dwtn = mean_dwtn[mean_dwtn["week"]>datetime.date(2020, 6, 1)]
mean_dwtn['rolling'] = mean_dwtn.groupby('cluster')['normalized_visits_by_total_visits'].transform(lambda x : x.rolling(window=9).mean().shift(-4))
sns.set(rc={'figure.figsize':(14,10)})
ax = sns.lineplot(data=mean_dwtn, x ='week', y = 'rolling', hue='cluster', palette='tab10')
plt.ylabel('Downtown RQ')
plt.xlabel('Date')
plt.show()

# COMMAND ----------

