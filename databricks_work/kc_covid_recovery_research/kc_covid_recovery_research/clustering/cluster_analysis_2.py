# Databricks notebook source
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
from scipy import stats
from datetime import datetime, timedelta, date
import matplotlib.pyplot as plt
import seaborn as sns
import statsmodels.api as sm
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import cross_val_score

# COMMAND ----------

 def get_table_as_pandas_df(table_name):
    return sqlContext.sql("select * from " + table_name + " tables").toPandas()

# COMMAND ----------

#dbutils.widgets.removeAll()
rfc_data = pd.read_csv('E:\\data\downtownrecovery/s3_objects/all_model_features_1015_weather.csv')
display(rfc_data)



# COMMAND ----------

rq_dwtn_clusters_period_1 = pd.read_csv('E:\\data\downtownrecovery/s3_objects/rq_dwtn_clusters_0823_period_1.csv')
rq_dwtn_clusters_period_2 = pd.read_csv('E:\\data\downtownrecovery/s3_objects/rq_dwtn_clusters_1015_period_2.csv')
# COMMAND ----------

lq_clusters = pd.read_csv('E:\\data\downtownrecovery/s3_objects/lq_clusters_single_period4.csv')
      
downtown_rfc_table = rfc_data[['city','total_pop_downtown', 'pct_singlefam_downtown', 'pct_multifam_downtown','pct_renter_downtown','median_age_downtown','bachelor_plus_downtown','median_hhinc_downtown','median_rent_downtown','pct_vacant_downtown','median_year_structure_built','median_no_rooms','pct_commute_auto_city','pct_commute_public_transit_city','pct_commute_bicycle_city','pct_commute_walk_city','housing_units_downtown','average_commute_time_city','pct_jobs_agriculture_forestry_fishing_hunting','pct_jobs_mining_quarrying_oil_gas', 'pct_jobs_utilities','pct_jobs_construction', 'pct_jobs_manufacturing','pct_jobs_wholesale_trade', 'pct_jobs_retail_trade','pct_jobs_transport_warehouse', 'pct_jobs_information','pct_jobs_finance_insurance','pct_jobs_real_estate','pct_jobs_professional_science_techical','pct_jobs_management_of_companies_enterprises','pct_jobs_administrative_support_waste','pct_jobs_educational_services','pct_jobs_healthcare_social_assistance','pct_jobs_arts_entertainment_recreation','pct_jobs_accomodation_food_services','pct_jobs_public_administration', 'employment_entropy', 'population_density_downtown', 'employment_density_downtown', 'housing_density_downtown','days_school_closing', 'days_workplace_closing','days_cancel_large_events', 'days_cancel_all_events','days_stay_home_requirements', 'days_income_support','days_mask_mandates', 'winter_avg_temp', 'summer_avg_temp']]

#'pct_nhwhite_downtown','pct_nhblack_downtown','pct_nhasian_downtown','pct_hisp_downtown',

city_rfc_table = rfc_data[['city','total_pop_city', 'pct_singlefam_city', 'pct_multifam_city','pct_renter_city','median_age_city','bachelor_plus_city','median_hhinc_city','median_rent_city','pct_vacant_city','pct_commute_auto_city','pct_commute_public_transit_city','pct_commute_bicycle_city','pct_commute_walk_city','housing_units_city','average_commute_time_city','pct_jobs_agriculture_forestry_fishing_hunting','pct_jobs_mining_quarrying_oil_gas', 'pct_jobs_utilities','pct_jobs_construction', 'pct_jobs_manufacturing','pct_jobs_wholesale_trade', 'pct_jobs_retail_trade','pct_jobs_transport_warehouse', 'pct_jobs_information','pct_jobs_finance_insurance','pct_jobs_real_estate','pct_jobs_professional_science_techical','pct_jobs_management_of_companies_enterprises','pct_jobs_administrative_support_waste','pct_jobs_educational_services','pct_jobs_healthcare_social_assistance','pct_jobs_arts_entertainment_recreation','pct_jobs_accomodation_food_services','pct_jobs_public_administration', 'employment_entropy', 'population_density_city', 'employment_density_downtown', 'housing_density_city', 'days_school_closing', 'days_workplace_closing','days_cancel_large_events', 'days_cancel_all_events','days_stay_home_requirements', 'days_income_support','days_mask_mandates', 'winter_avg_temp', 'summer_avg_temp']]

#'pct_nhwhite_city','pct_nhblack_city','pct_nhasian_city','pct_hisp_city',

lq_rfc_table = rfc_data[['city','total_pop_downtown', 'pct_singlefam_downtown', 'pct_multifam_downtown','pct_renter_downtown','median_age_downtown','bachelor_plus_downtown','median_hhinc_downtown','median_rent_downtown','pct_vacant_downtown','median_year_structure_built','median_no_rooms','pct_commute_auto_city','pct_commute_public_transit_city','pct_commute_bicycle_city','pct_commute_walk_city','housing_units_downtown','average_commute_time_city','pct_jobs_agriculture_forestry_fishing_hunting','pct_jobs_mining_quarrying_oil_gas', 'pct_jobs_utilities','pct_jobs_construction', 'pct_jobs_manufacturing','pct_jobs_wholesale_trade', 'pct_jobs_retail_trade','pct_jobs_transport_warehouse', 'pct_jobs_information','pct_jobs_finance_insurance','pct_jobs_real_estate','pct_jobs_professional_science_techical','pct_jobs_management_of_companies_enterprises','pct_jobs_administrative_support_waste','pct_jobs_educational_services','pct_jobs_healthcare_social_assistance','pct_jobs_arts_entertainment_recreation','pct_jobs_accomodation_food_services','pct_jobs_public_administration', 'employment_entropy', 'population_density_downtown', 'employment_density_downtown', 'housing_density_downtown','days_school_closing', 'days_workplace_closing','days_cancel_large_events', 'days_cancel_all_events','days_stay_home_requirements', 'days_income_support','days_mask_mandates', 'winter_avg_temp', 'summer_avg_temp']]

subset_features = ['days_school_closing', 'days_workplace_closing','days_cancel_large_events', 'days_cancel_all_events','days_stay_home_requirements', 'days_income_support','days_mask_mandates']
downtown_rfc_table = downtown_rfc_table[~downtown_rfc_table['city'].isin(['Dallas','Orlando','Mississauga','Hamilton','Oklahoma City'])]
rq_dwtn_clusters_period_1 = rq_dwtn_clusters_period_1.replace('Québec','Quebec')
rq_dwtn_clusters_period_1 = rq_dwtn_clusters_period_1.replace('Montréal','Montreal')
rq_dwtn_clusters_period_1['cluster_map'] = rq_dwtn_clusters_period_1['cluster'].map({0:"R1.1",1:"R1.5",2:"R1.3",3:"R1.4",4:"R1.2",5:"R1.1"})
downtown_rfc_table.loc[:,'cluster_period_1'] = downtown_rfc_table.loc[:,'city'].map(dict(zip(rq_dwtn_clusters_period_1['city'], rq_dwtn_clusters_period_1['cluster_map'])))         

rq_dwtn_clusters_period_2 = rq_dwtn_clusters_period_2.replace('Québec','Quebec')
rq_dwtn_clusters_period_2 = rq_dwtn_clusters_period_2.replace('Montréal','Montreal')
rq_dwtn_clusters_period_2['cluster_map'] = rq_dwtn_clusters_period_2['cluster'].map({0:"R2.5",1:"R2.1",2:"R2.3",3:"R2.1",4:"R2.4",5:"R2.2"})
downtown_rfc_table.loc[:,'cluster_period_2'] = downtown_rfc_table.loc[:,'city'].map(dict(zip(rq_dwtn_clusters_period_2['city'], rq_dwtn_clusters_period_2['cluster_map'])))

city_rfc_table = city_rfc_table[~city_rfc_table['city'].isin(['Dallas','Orlando','Mississauga','Hamilton','Oklahoma City'])]

oxford_vars = downtown_rfc_table[subset_features]

oxford_vars.corr()

ax_downtown_1 = sns.pairplot(data = downtown_rfc_table[['city', 'cluster_period_1'] + subset_features], hue = 'cluster_period_1', palette = 'colorblind')
ax_downtown_1.fig.suptitle('Oxford variable pairplot: downtown recovery period 1', y = 1.01)
plt.show()


ax_downtown_2 = sns.pairplot(data = downtown_rfc_table[['city', 'cluster_period_2'] + subset_features], hue = 'cluster_period_2', palette = 'colorblind')
ax_downtown_2.fig.suptitle('Oxford variable pairplot: downtown recovery period 2', y = 1.01)
plt.show()


lq_rfc_table = lq_rfc_table[~lq_rfc_table['city'].isin(['Dallas','Orlando','Mississauga','Hamilton','Oklahoma City'])].copy()
lq_clusters = lq_clusters.replace('Québec','Quebec')
lq_clusters = lq_clusters.replace('Montréal','Montreal')
lq_clusters['cluster_map'] = lq_clusters['cluster'].map({0:"L1",1:"L3",2:"L2",3:"L4",4:"L1",5:"L1"})
lq_rfc_table.loc[:,'cluster'] = lq_rfc_table.loc[:,'city'].map(dict(zip(lq_clusters['city'], lq_clusters['cluster_map'])))


ax_lq = sns.pairplot(data = lq_rfc_table[['city', 'cluster'] + subset_features], hue = 'cluster', palette = 'colorblind')
ax_lq.fig.suptitle('Oxford variable pairplot: polycentricity', y = 1.01)
plt.show()

# create features df
features_df = rfc_table.melt(id_vars = 'city', var_name = 'x_feature')
features_df['value'] = np.round(features_df['value'].astype(float), 2)

# features_df['minmax_scaled'] = features_df.groupby('x_feature')['value'].transform(lambda x : scaler.fit_transform(x.values.reshape(-1,1)).flatten())

#hue_order = rfc_table.sort_values(by = 'cluster', ascending = True)['cluster'].unique()

import plotly.express as px
import plotly.graph_objects as go
#display(features_df)

#features_df = features_df.sort_values(by = 'rank')

fig = px.box(features_df, x = 'value', y = 'description', color = 'description',
               color_discrete_map=color_map,
               #labels={"cluster":"Cluster", "city": "City"},
               hover_name = 'city', 
               points = 'all',
               #box=True,
               category_orders={"description": rank},
               #hover_data = subset_features, 
               facet_row_spacing = .015, 
               facet_col_spacing = .035,
               facet_col = 'x_feature', 
               facet_col_wrap = 3,
               height = 120 * len(subset_features), 
               width = 25 * len(subset_features))

fig.update_layout(hovermode="closest", 
                  margin=dict(l=20, r=20, t=20, b=20),
                  showlegend = False,
                  font=dict(
                      #family="Courier New, monospace",
                      size=14
                  ),
                  #legend = {'traceorder':'reversed'},
                  hoverlabel=dict(
                      font_size=14
                  ))
fig.update_traces(width = .35,
                  legendgroup = 'description',
                 hoveron = 'points',
                 pointpos=0)
fig.update_xaxes(matches = None)
fig.update_yaxes(title= None, type = 'category')
fig.for_each_xaxis(lambda xaxis: xaxis.update(showticklabels=True))
fig.show()#config = {'displayModeBar':False})

# COMMAND ----------

# filter and load clusters by selected metric and period
if dbutils.widgets.get('metric') == 'downtown':
    all_features = ['total_pop_downtown', 'pct_singlefam_downtown', 'pct_multifam_downtown','pct_renter_downtown','median_age_downtown','bachelor_plus_downtown','median_hhinc_downtown','median_rent_downtown','pct_vacant_downtown','pct_commute_auto_city','pct_commute_public_transit_city','pct_commute_bicycle_city','pct_commute_walk_city','housing_units_downtown','average_commute_time_city','pct_jobs_agriculture_forestry_fishing_hunting','pct_jobs_mining_quarrying_oil_gas', 'pct_jobs_utilities','pct_jobs_construction', 'pct_jobs_manufacturing','pct_jobs_wholesale_trade', 'pct_jobs_retail_trade','pct_jobs_transport_warehouse', 'pct_jobs_information','pct_jobs_finance_insurance','pct_jobs_real_estate','pct_jobs_professional_science_techical','pct_jobs_management_of_companies_enterprises','pct_jobs_administrative_support_waste','pct_jobs_educational_services','pct_jobs_healthcare_social_assistance','pct_jobs_arts_entertainment_recreation','pct_jobs_accomodation_food_services','pct_jobs_public_administration', 'employment_entropy', 'population_density_downtown', 'employment_density_downtown', 'housing_density_downtown','days_school_closing', 'days_workplace_closing','days_cancel_large_events', 'days_cancel_all_events','days_stay_home_requirements', 'days_income_support','days_mask_mandates', 'winter_avg_temp', 'spring_avg_temp', 'summer_avg_temp', 'fall_avg_temp']
    if dbutils.widgets.get('period') == '1':
        metric_clusters = get_table_as_pandas_df("rq_dwtn_clusters_0823_period_1") 
        metric_clusters['description'] = metric_clusters['cluster'].map({0:'Cluster 1.1',
                                                                         5: 'Cluster 1.1',
                                                                         4:'Cluster 1.2',
                                                                         2:'Cluster 1.3',
                                                                         3: 'Cluster 1.4',
                                                                         1:'Cluster 1.5'                
                                                                         })
        rank = ['Cluster 1.1', 'Cluster 1.2','Cluster 1.3', 'Cluster 1.4', 'Cluster 1.5']
        color_map ={
                   'Cluster 1.1':'#1f78b4',
                   'Cluster 1.2':'#6a3d9a',
                   'Cluster 1.3':'#33a02c',
                   'Cluster 1.4': '#e31a1c',
                   'Cluster 1.5':'#fdbf6f'}
        rfc_important_features = all_features # not yet supported
        
    elif dbutils.widgets.get('period') == '2':
        metric_clusters = get_table_as_pandas_df("rq_dwtn_clusters_0823_period_2") 
        metric_clusters['description'] = metric_clusters['cluster'].map({5: 'Cluster 2.1',
                                                                         1:'Cluster 2.1',
                                                                         4:'Cluster 2.2',
                                                                         2:'Cluster 2.3',
                                                                         3: 'Cluster 2.4',
                                                                         0:'Cluster 2.5'     
                                                                         })
        
        rank = ['Cluster 2.1','Cluster 2.2','Cluster 2.3','Cluster 2.4','Cluster 2.5']
        color_map ={
                   'Cluster 2.1':'#b3de69',
                   'Cluster 2.2':'#fb8072',
                   'Cluster 2.3':'#8dd3c7',
                   'Cluster 2.4':'#bebada',
                   'Cluster 2.5':'#e7298a'}
        rfc_important_features = all_features # not yet supported
        
elif dbutils.widgets.get('metric') == 'relative':
    
    all_features = ['total_pop_downtown', 'total_pop_city','pct_singlefam_downtown', 'pct_singlefam_city', 'pct_multifam_downtown','pct_multifam_city','pct_mobile_home_and_others_city', 'pct_renter_downtown','pct_renter_city', 'median_age_downtown', 'median_age_city','bachelor_plus_downtown', 'bachelor_plus_city', 'median_hhinc_downtown','median_hhinc_city', 'median_rent_downtown', 'median_rent_city','pct_vacant_downtown', 'pct_vacant_city', 'pct_commute_auto_downtown', 'pct_commute_auto_city','pct_commute_public_transit_downtown','pct_commute_public_transit_city', 'pct_commute_bicycle_downtown','pct_commute_bicycle_city', 'pct_commute_walk_downtown','pct_commute_walk_city','pct_commute_others_city', 'housing_units_downtown','housing_units_city', 'average_commute_time_downtown','average_commute_time_city','pct_jobs_agriculture_forestry_fishing_hunting','pct_jobs_mining_quarrying_oil_gas', 'pct_jobs_utilities','pct_jobs_construction', 'pct_jobs_manufacturing','pct_jobs_wholesale_trade', 'pct_jobs_retail_trade','pct_jobs_transport_warehouse', 'pct_jobs_information','pct_jobs_finance_insurance', 'pct_jobs_real_estate','pct_jobs_professional_science_techical','pct_jobs_management_of_companies_enterprises','pct_jobs_administrative_support_waste','pct_jobs_educational_services','pct_jobs_healthcare_social_assistance','pct_jobs_arts_entertainment_recreation','pct_jobs_accomodation_food_services','pct_jobs_public_administration', 'employment_entropy','population_density_downtown', 'population_density_city','employment_density_downtown', 'housing_density_downtown','housing_density_city', 'days_school_closing', 'days_workplace_closing','days_cancel_large_events', 'days_cancel_all_events','days_stay_home_requirements', 'days_income_support','days_mask_mandates', 'winter_avg_temp', 'spring_avg_temp', 'summer_avg_temp', 'fall_avg_temp']
    
    metric_clusters = get_table_as_pandas_df("lq_clusters_single_period_4") 
    metric_clusters['description'] = metric_clusters['cluster'].map({1:'Cluster 1',
                                                                     2:'Cluster 2',
                                                                     0:'Cluster 3',
                                                                     3:'Cluster 4'             
                                                                    })
    rank = ['Cluster 1', 'Cluster 2', 'Cluster 3', 'Cluster 4']
    color_map ={
        'Cluster 1':'#ffff99',
        'Cluster 2':'#7fc97f',
        'Cluster 3':'#1f78b4',
        'Cluster 4': '#e41a1c'}
    
    rfc_important_features = ['employment_density_downtown', 'pct_jobs_information',
                              'pct_multifam_downtown','pct_multifam_city',
                              'bachelor_plus_downtown',
                              'pct_jobs_retail_trade',
                              'pct_renter_downtown','pct_renter_city',
                              'pct_commute_auto_city', 'pct_commute_auto_downtown',
                              'days_stay_home_requirements',
                              'pct_commute_bicycle_downtown','pct_commute_bicycle_city',
                              'days_cancel_large_events',
                              'pct_jobs_accomodation_food_services',
                              'pct_commute_walk_downtown','pct_commute_walk_city',
                              'pct_commute_public_transit_downtown','pct_commute_public_transit_city',
                              'population_density_downtown', 'population_density_city', 
                              'pct_vacant_downtown', 'pct_vacant_city',
                              'average_commute_time_downtown','average_commute_time_city',
                              'pct_jobs_agriculture_forestry_fishing_hunting',
                              'days_workplace_closing',
                              # the rest are unordered
                              'total_pop_downtown', 'total_pop_city','pct_singlefam_downtown', 'pct_singlefam_city', 'pct_mobile_home_and_others_city',  'median_age_downtown', 'median_age_city', 'bachelor_plus_city', 'median_hhinc_downtown','median_hhinc_city', 'median_rent_downtown', 'median_rent_city',    'pct_commute_others_city', 'housing_units_downtown','housing_units_city', 'pct_jobs_mining_quarrying_oil_gas', 'pct_jobs_utilities','pct_jobs_construction', 'pct_jobs_manufacturing','pct_jobs_wholesale_trade','pct_jobs_transport_warehouse', 'pct_jobs_finance_insurance', 'pct_jobs_real_estate','pct_jobs_professional_science_techical','pct_jobs_management_of_companies_enterprises','pct_jobs_administrative_support_waste','pct_jobs_educational_services','pct_jobs_healthcare_social_assistance','pct_jobs_arts_entertainment_recreation','pct_jobs_public_administration', 'employment_entropy','housing_density_downtown','housing_density_city', 'days_school_closing', 'days_cancel_all_events', 'days_income_support','days_mask_mandates', 'winter_avg_temp', 'spring_avg_temp', 'summer_avg_temp', 'fall_avg_temp']
    
    
       
#for consistency's sake- sorry québécois
metric_clusters = metric_clusters.replace('Québec','Quebec')
metric_clusters = metric_clusters.replace('Montréal','Montreal')

# create rfc_table by metric

import random
if dbutils.widgets.get('subset') == 'LEHD':
    subset_features = ['pct_jobs_agriculture_forestry_fishing_hunting','pct_jobs_mining_quarrying_oil_gas', 'pct_jobs_utilities','pct_jobs_construction', 'pct_jobs_manufacturing','pct_jobs_retail_trade', 'pct_jobs_information','pct_jobs_finance_insurance','pct_jobs_real_estate','pct_jobs_professional_science_techical','pct_jobs_management_of_companies_enterprises','pct_jobs_administrative_support_waste','pct_jobs_educational_services','pct_jobs_healthcare_social_assistance','pct_jobs_arts_entertainment_recreation','pct_jobs_accomodation_food_services','pct_jobs_public_administration', 'employment_entropy']
elif dbutils.widgets.get('subset') == 'Oxford':
    subset_features = ['days_school_closing', 'days_workplace_closing','days_cancel_large_events', 'days_cancel_all_events','days_stay_home_requirements', 'days_income_support','days_mask_mandates']
    
elif dbutils.widgets.get('subset') == 'custom':
    subset_features = ['pct_jobs_construction', 'pct_jobs_manufacturing','pct_jobs_retail_trade', 'pct_multifam_downtown'] # change as you see fit
elif dbutils.widgets.get('subset') == 'important':
    subset_features = rfc_important_features
else:
    subset_features = all_features


rfc_table = rfc_data[['city'] + subset_features]

rfc_table = rfc_table[~rfc_table['city'].isin(['Dallas','Orlando','Mississauga','Hamilton','Oklahoma City'])]
rfc_table = rfc_table.replace('Québec','Quebec')
rfc_table = rfc_table.replace('Montréal','Montreal')
rfc_table.loc[:,'description'] = rfc_table.loc[:,'city'].map(dict(zip(metric_clusters['city'], metric_clusters['description'])))
rfc_table.loc[:,'cluster'] = rfc_table.loc[:,'city'].map(dict(zip(metric_clusters['city'], metric_clusters['cluster'])))
rfc_table['cluster'] = rfc_table['cluster'].astype(str)
rfc_table[subset_features] = rfc_table[subset_features].astype(float)
scaler = MinMaxScaler()

# create features df
features_df = rfc_table.melt(id_vars = ['city', 'description', 'cluster'], var_name = 'x_feature')
features_df['value'] = np.round(features_df['value'].astype(float), 2)
display(features_df.groupby(['description', 'x_feature']).describe().reset_index())

# COMMAND ----------

# filter and load clusters by selected metric and period
if dbutils.widgets.get('metric') == 'downtown':
    all_features = ['total_pop_downtown', 'pct_singlefam_downtown', 'pct_multifam_downtown','pct_renter_downtown','median_age_downtown','bachelor_plus_downtown','median_hhinc_downtown','median_rent_downtown','pct_vacant_downtown','pct_commute_auto_city','pct_commute_public_transit_city','pct_commute_bicycle_city','pct_commute_walk_city','housing_units_downtown','average_commute_time_city','pct_jobs_agriculture_forestry_fishing_hunting','pct_jobs_mining_quarrying_oil_gas', 'pct_jobs_utilities','pct_jobs_construction', 'pct_jobs_manufacturing','pct_jobs_wholesale_trade', 'pct_jobs_retail_trade','pct_jobs_transport_warehouse', 'pct_jobs_information','pct_jobs_finance_insurance','pct_jobs_real_estate','pct_jobs_professional_science_techical','pct_jobs_management_of_companies_enterprises','pct_jobs_administrative_support_waste','pct_jobs_educational_services','pct_jobs_healthcare_social_assistance','pct_jobs_arts_entertainment_recreation','pct_jobs_accomodation_food_services','pct_jobs_public_administration', 'employment_entropy', 'population_density_downtown', 'employment_density_downtown', 'housing_density_downtown','days_school_closing', 'days_workplace_closing','days_cancel_large_events', 'days_cancel_all_events','days_stay_home_requirements', 'days_income_support','days_mask_mandates', 'winter_avg_temp', 'spring_avg_temp', 'summer_avg_temp', 'fall_avg_temp']
    if dbutils.widgets.get('period') == '1':
        metric_clusters = get_table_as_pandas_df("rq_dwtn_clusters_0823_period_1") 
        metric_clusters['description'] = metric_clusters['cluster'].map({0:'Cluster 1.1',
                                                                         5: 'Cluster 1.1',
                                                                         4:'Cluster 1.2',
                                                                         2:'Cluster 1.3',
                                                                         3: 'Cluster 1.4',
                                                                         1:'Cluster 1.5'                
                                                                         })
        rank = ['Cluster 1.1', 'Cluster 1.2','Cluster 1.3', 'Cluster 1.4', 'Cluster 1.5']
        color_map ={
                   'Cluster 1.1':'#1f78b4',
                   'Cluster 1.2':'#6a3d9a',
                   'Cluster 1.3':'#33a02c',
                   'Cluster 1.4': '#e31a1c',
                   'Cluster 1.5':'#fdbf6f'}
        rfc_important_features = all_features # not yet supported
        
    elif dbutils.widgets.get('period') == '2':
        metric_clusters = get_table_as_pandas_df("rq_dwtn_clusters_0823_period_2") 
        metric_clusters['description'] = metric_clusters['cluster'].map({5: 'Cluster 2.1',
                                                                         1:'Cluster 2.1',
                                                                         4:'Cluster 2.2',
                                                                         2:'Cluster 2.3',
                                                                         3: 'Cluster 2.4',
                                                                         0:'Cluster 2.5'     
                                                                         })
        
        rank = ['Cluster 2.1','Cluster 2.2','Cluster 2.3','Cluster 2.4','Cluster 2.5']
        color_map ={
                   'Cluster 2.1':'#b3de69',
                   'Cluster 2.2':'#fb8072',
                   'Cluster 2.3':'#8dd3c7',
                   'Cluster 2.4':'#bebada',
                   'Cluster 2.5':'#e7298a'}
        rfc_important_features = all_features # not yet supported
        
elif dbutils.widgets.get('metric') == 'relative':
    
    all_features = ['total_pop_downtown', 'total_pop_city','pct_singlefam_downtown', 'pct_singlefam_city', 'pct_multifam_downtown','pct_multifam_city','pct_mobile_home_and_others_city', 'pct_renter_downtown','pct_renter_city', 'median_age_downtown', 'median_age_city','bachelor_plus_downtown', 'bachelor_plus_city', 'median_hhinc_downtown','median_hhinc_city', 'median_rent_downtown', 'median_rent_city','pct_vacant_downtown', 'pct_vacant_city', 'pct_commute_auto_downtown', 'pct_commute_auto_city','pct_commute_public_transit_downtown','pct_commute_public_transit_city', 'pct_commute_bicycle_downtown','pct_commute_bicycle_city', 'pct_commute_walk_downtown','pct_commute_walk_city','pct_commute_others_city', 'housing_units_downtown','housing_units_city', 'average_commute_time_downtown','average_commute_time_city','pct_jobs_agriculture_forestry_fishing_hunting','pct_jobs_mining_quarrying_oil_gas', 'pct_jobs_utilities','pct_jobs_construction', 'pct_jobs_manufacturing','pct_jobs_wholesale_trade', 'pct_jobs_retail_trade','pct_jobs_transport_warehouse', 'pct_jobs_information','pct_jobs_finance_insurance', 'pct_jobs_real_estate','pct_jobs_professional_science_techical','pct_jobs_management_of_companies_enterprises','pct_jobs_administrative_support_waste','pct_jobs_educational_services','pct_jobs_healthcare_social_assistance','pct_jobs_arts_entertainment_recreation','pct_jobs_accomodation_food_services','pct_jobs_public_administration', 'employment_entropy','population_density_downtown', 'population_density_city','employment_density_downtown', 'housing_density_downtown','housing_density_city', 'days_school_closing', 'days_workplace_closing','days_cancel_large_events', 'days_cancel_all_events','days_stay_home_requirements', 'days_income_support','days_mask_mandates', 'winter_avg_temp', 'spring_avg_temp', 'summer_avg_temp', 'fall_avg_temp']
    
    metric_clusters = get_table_as_pandas_df("lq_clusters_single_period_4") 
    metric_clusters['description'] = metric_clusters['cluster'].map({1:'Cluster 1',
                                                                     2:'Cluster 2',
                                                                     0:'Cluster 3',
                                                                     3:'Cluster 4'             
                                                                    })
    rank = ['Cluster 1', 'Cluster 2', 'Cluster 3', 'Cluster 4']
    color_map ={
        'Cluster 1':'#ffff99',
        'Cluster 2':'#7fc97f',
        'Cluster 3':'#1f78b4',
        'Cluster 4': '#e41a1c'}
    
    rfc_important_features = ['employment_density_downtown', 'pct_jobs_information',
                              'pct_multifam_downtown','pct_multifam_city',
                              'bachelor_plus_downtown',
                              'pct_jobs_retail_trade',
                              'pct_renter_downtown','pct_renter_city',
                              'pct_commute_auto_city', 'pct_commute_auto_downtown',
                              'days_stay_home_requirements',
                              'pct_commute_bicycle_downtown','pct_commute_bicycle_city',
                              'days_cancel_large_events',
                              'pct_jobs_accomodation_food_services',
                              'pct_commute_walk_downtown','pct_commute_walk_city',
                              'pct_commute_public_transit_downtown','pct_commute_public_transit_city',
                              'population_density_downtown', 'population_density_city', 
                              'pct_vacant_downtown', 'pct_vacant_city',
                              'average_commute_time_downtown','average_commute_time_city',
                              'pct_jobs_agriculture_forestry_fishing_hunting',
                              'days_workplace_closing',
                              # the rest are unordered
                              'total_pop_downtown', 'total_pop_city','pct_singlefam_downtown', 'pct_singlefam_city', 'pct_mobile_home_and_others_city',  'median_age_downtown', 'median_age_city', 'bachelor_plus_city', 'median_hhinc_downtown','median_hhinc_city', 'median_rent_downtown', 'median_rent_city',    'pct_commute_others_city', 'housing_units_downtown','housing_units_city', 'pct_jobs_mining_quarrying_oil_gas', 'pct_jobs_utilities','pct_jobs_construction', 'pct_jobs_manufacturing','pct_jobs_wholesale_trade','pct_jobs_transport_warehouse', 'pct_jobs_finance_insurance', 'pct_jobs_real_estate','pct_jobs_professional_science_techical','pct_jobs_management_of_companies_enterprises','pct_jobs_administrative_support_waste','pct_jobs_educational_services','pct_jobs_healthcare_social_assistance','pct_jobs_arts_entertainment_recreation','pct_jobs_public_administration', 'employment_entropy','housing_density_downtown','housing_density_city', 'days_school_closing', 'days_cancel_all_events', 'days_income_support','days_mask_mandates', 'winter_avg_temp', 'spring_avg_temp', 'summer_avg_temp', 'fall_avg_temp']
    
    
       
#for consistency's sake- sorry québécois
metric_clusters = metric_clusters.replace('Québec','Quebec')
metric_clusters = metric_clusters.replace('Montréal','Montreal')

# create rfc_table by metric

subset_feats = ['pct_jobs_information','pct_jobs_manufacturing', 'pct_jobs_finance_insurance']
rfc_table = rfc_data[['city'] + subset_feats]
rfc_table = rfc_table[~rfc_table['city'].isin(['Dallas','Orlando','Mississauga','Hamilton','Oklahoma City'])]
rfc_table = rfc_table.replace('Québec','Quebec')
rfc_table = rfc_table.replace('Montréal','Montreal')
rfc_table.loc[:,'description'] = rfc_table.loc[:,'city'].map(dict(zip(metric_clusters['city'], metric_clusters['description'])))
rfc_table.loc[:,'color'] = rfc_table.loc[:,'city'].map(dict(zip(metric_clusters['city'], metric_clusters['color'])))
rfc_table.loc[:,'cluster'] = rfc_table.loc[:,'city'].map(dict(zip(metric_clusters['city'], metric_clusters['cluster'])))

scaler = MinMaxScaler()
rfc_table[subset_feats] = rfc_table[subset_feats].astype(float)
#rfc_table['employment_entropy_log'] = np.log(rfc_table['employment_entropy'])
dbutils.widgets.dropdown('x_feature', subset_feats[0], subset_feats)

#rfc_table['employment_entropy_log'] = np.log(rfc_table(['employment_entropy']))
# create features df



features_df = rfc_table.melt(id_vars = ['city', 'description', 'color', 'cluster'], var_name = 'x_feature')
features_df['value'] = np.round(features_df['value'].astype(float), 2)

import plotly.express as px
import plotly.graph_objects as go

fig = px.box(data_frame = features_df, x = 'value', y = 'description', color = 'cluster', 
               color_discrete_map={
                   0:'#386cb0',
                   1:'#ffff99',
                   2:'#7fc97f',
                   3: '#fdc086',
                   4:'#e78ac3',
                   5: '#a6611a'},
               hover_name = 'city', 
              # hoverinfo = '%{city}',
               #labels = 'city',
               points = 'all',
               #hover_data = ['city'], 
               #box = True,
             #hover_data = subset_features, 
               facet_row_spacing = .01, 
               facet_col_spacing = .02,
               facet_col = 'x_feature',
              # facet_col_wrap = 4,
               height = 600, 
               width = 1600
               )

fig.update_traces(marker_line_outliercolor ='rgba(10, 10, 0, 0)',
                 
                  hoveron = 'points',
                  pointpos=0,
                  text = '%{city}'
                 )


fig.update_layout(hovermode="x", 
                  margin=dict(l=20, r=20, t=20, b=20),
                  showlegend=False,
                  hoverlabel=dict(
                      font_size=14
                  ),
                  boxgroupgap = .1,
                 #title=dict(font_size=12),
                 )
fig.update_xaxes(matches = None)
fig.update_yaxes(title = None,   type = 'category')

fig.show()#config = {'displayModeBar':False})

# COMMAND ----------

# filter and load clusters by selected metric and period
if dbutils.widgets.get('metric') == 'downtown':
    features = ['total_pop_downtown', 'pct_singlefam_downtown', 'pct_multifam_downtown','pct_renter_downtown','median_age_downtown','bachelor_plus_downtown','median_hhinc_downtown','median_rent_downtown','pct_vacant_downtown','pct_commute_auto_city','pct_commute_public_transit_city','pct_commute_bicycle_city','pct_commute_walk_city','housing_units_downtown','average_commute_time_city','pct_jobs_agriculture_forestry_fishing_hunting','pct_jobs_mining_quarrying_oil_gas', 'pct_jobs_utilities','pct_jobs_construction', 'pct_jobs_manufacturing','pct_jobs_wholesale_trade', 'pct_jobs_retail_trade','pct_jobs_transport_warehouse', 'pct_jobs_information','pct_jobs_finance_insurance','pct_jobs_real_estate','pct_jobs_professional_science_techical','pct_jobs_management_of_companies_enterprises','pct_jobs_administrative_support_waste','pct_jobs_educational_services','pct_jobs_healthcare_social_assistance','pct_jobs_arts_entertainment_recreation','pct_jobs_accomodation_food_services','pct_jobs_public_administration', 'employment_entropy', 'population_density_downtown', 'employment_density_downtown', 'housing_density_downtown','days_school_closing', 'days_workplace_closing','days_cancel_large_events', 'days_cancel_all_events','days_stay_home_requirements', 'days_income_support','days_mask_mandates', 'winter_avg_temp', 'spring_avg_temp', 'summer_avg_temp', 'fall_avg_temp']
    if dbutils.widgets.get('period') == '1':
        metric_clusters = get_table_as_pandas_df("rq_dwtn_clusters_0823_period_1") 
        metric_clusters['description'] = metric_clusters['cluster'].map({0:'Exceptional Performer',
                                                                         1:'Well Below Average',
                                                                         2:'Average Then Stagnant',
                                                                         3: 'Below Average, Then Improve',
                                                                         4:'Above Average',
                                                                         5: 'Exceptional Performer'})
        metric_clusters['color'] = metric_clusters['cluster'].map({0:'#386cb0',
                                                                   1:'#ffff99',
                                                                   2:'#7fc97f',
                                                                   3: '#fdc086',
                                                                   4:'#e78ac3',
                                                                   5: '#a6611a'})
        subset_features = ['pct_singlefam_downtown','pct_multifam_downtown','pct_renter_downtown', 'pct_commute_auto_city', 'pct_commute_public_transit_city', 'average_commute_time_city','pct_singlefam_city','pct_multifam_city']

        
    elif dbutils.widgets.get('period') == '2':
        metric_clusters = get_table_as_pandas_df("rq_dwtn_clusters_0823_period_2") 
        metric_clusters['description'] = metric_clusters['cluster'].map({0:'Well below average',
                                                                         1:'Very high performers',
                                                                         2:'Slightly above average',
                                                                         3: 'Slightly below average ',
                                                                         4:'Well above average',
                                                                         5: 'Very high performers'})
        
        metric_clusters['color'] = metric_clusters['cluster'].map({0:'#377eb8',
                                                                   1:'#ffff99',
                                                                   2:'#66a61e',
                                                                   3: '#d95f02',
                                                                   4:'#e7298a',
                                                                   5: '#ffff99'})
        subset_features = ['pct_singlefam_downtown','pct_multifam_downtown','pct_renter_downtown', 'pct_commute_auto_city', 'pct_commute_public_transit_city', 'average_commute_time_city','pct_singlefam_city','pct_multifam_city']
                           #'pct_jobs_mining_quarrying_oil_gas', 'pct_jobs_utilities','pct_jobs_construction', 'pct_jobs_manufacturing','pct_jobs_wholesale_trade', 'pct_jobs_retail_trade','pct_jobs_transport_warehouse', 'pct_jobs_information','pct_jobs_finance_insurance','pct_jobs_real_estate','pct_jobs_professional_science_techical','pct_jobs_management_of_companies_enterprises','pct_jobs_administrative_support_waste','pct_jobs_educational_services','pct_jobs_healthcare_social_assistance','pct_jobs_arts_entertainment_recreation','pct_jobs_accomodation_food_services','pct_jobs_public_administration']
              

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
       
#for consistency's sake- sorry québécois
metric_clusters = metric_clusters.replace('Québec','Quebec')
metric_clusters = metric_clusters.replace('Montréal','Montreal')

# create rfc_table by metric
rfc_table = rfc_data[['city'] + subset_features]
rfc_table[subset_features] = rfc_table[subset_features].astype(float)
#rfc_table = rfc_table[['city'] + [subset_features[i] for i in [0, 1]]]
rfc_table = rfc_table[~rfc_table['city'].isin(['Dallas','Orlando','Mississauga','Hamilton','Oklahoma City'])]
rfc_table = rfc_table.replace('Québec','Quebec')
rfc_table = rfc_table.replace('Montréal','Montreal')
rfc_table.loc[:,'description'] = rfc_table.loc[:,'city'].map(dict(zip(metric_clusters['city'], metric_clusters['description'])))
rfc_table.loc[:,'color'] = rfc_table.loc[:,'city'].map(dict(zip(metric_clusters['city'], metric_clusters['color'])))
rfc_table.loc[:,'cluster'] = rfc_table.loc[:,'city'].map(dict(zip(metric_clusters['city'], metric_clusters['cluster'])))
rfc_table['cluster'] = rfc_table['cluster'].astype(str)

scaler = MinMaxScaler()

#rfc_table['employment_entropy_log'] = np.log(rfc_table(['employment_entropy']))
# create features df
features_df = rfc_table.melt(id_vars = ['city', 'description', 'color', 'cluster'], var_name = 'x_feature')
features_df['value'] = np.round(features_df['value'].astype(float), 2)

# features_df['minmax_scaled'] = features_df.groupby('x_feature')['value'].transform(lambda x : scaler.fit_transform(x.values.reshape(-1,1)).flatten())

#hue_order = rfc_table.sort_values(by = 'cluster', ascending = True)['cluster'].unique()

import plotly.express as px
import plotly.graph_objects as go
#display(features_df)

fig = px.scatter_matrix(rfc_table, dimensions = subset_features, color = 'cluster',
               color_discrete_map={
                   '0':'#386cb0',
                   '1':'#ffff99',
                   '2':'#7fc97f',
                   '3': '#fdc086',
                   '4':'#e78ac3',
                   '5': '#a6611a'},
               #text = 'city', 
                 
              # points = 'all',
              # box=True,
              
               #hover_data = subset_features, 
              # facet_row_spacing = .01, 
               #facet_col_spacing = .02,
               #facet_col = 'x_feature', 
               #facet_col_wrap = 4,
               height = 1200, 
               width = 1200)


# plot
#ax_feats = sns.displot(data = features_df, x = 'value', col = 'x_feature', hue = 'cluster', kind = 'kde', palette = 'colorblind', hue_order = hue_order, label = 'cluster', facet_kws = {'sharey':False, 'legend_out' : True, 'sharex' : False},  col_wrap = 3, height = 6, aspect = 1)

# maybe: use KS to test empirical cdfs by cluster to measure how different they are from each other
#for axes in ax_feats.axes.ravel():
#    h,l = axes.get_legend_handles_labels()

fig.update_layout(hovermode="closest", 
                  margin=dict(l=20, r=20, t=20, b=20),
                  hoverlabel=dict(
                      font_size=14
                  ),
              
              
                 showlegend = False)
#fig.update_traces(#width = .5,
                 #hoveron = 'points'#,
                 #pointpos=0
#)
fig.update_xaxes(matches = None)
#fig.update_yaxes(matches=None, title= None)
fig.for_each_xaxis(lambda xaxis: xaxis.update(showticklabels=True))
fig.show(config = {'displayModeBar':False})

# COMMAND ----------

# look at some really interesting vars
import plotly.express as px
# filter and load clusters by selected metric and period
if dbutils.widgets.get('metric') == 'downtown':
    features = ['total_pop_downtown', 'pct_singlefam_downtown', 'pct_multifam_downtown','pct_renter_downtown','median_age_downtown','bachelor_plus_downtown','median_hhinc_downtown','median_rent_downtown','pct_vacant_downtown','pct_commute_auto_city','pct_commute_public_transit_city','pct_commute_bicycle_city','pct_commute_walk_city','housing_units_downtown','average_commute_time_city','pct_jobs_agriculture_forestry_fishing_hunting','pct_jobs_mining_quarrying_oil_gas', 'pct_jobs_utilities','pct_jobs_construction', 'pct_jobs_manufacturing','pct_jobs_wholesale_trade', 'pct_jobs_retail_trade','pct_jobs_transport_warehouse', 'pct_jobs_information','pct_jobs_finance_insurance','pct_jobs_real_estate','pct_jobs_professional_science_techical','pct_jobs_management_of_companies_enterprises','pct_jobs_administrative_support_waste','pct_jobs_educational_services','pct_jobs_healthcare_social_assistance','pct_jobs_arts_entertainment_recreation','pct_jobs_accomodation_food_services','pct_jobs_public_administration', 'employment_entropy', 'population_density_downtown', 'employment_density_downtown', 'housing_density_downtown','days_school_closing', 'days_workplace_closing','days_cancel_large_events', 'days_cancel_all_events','days_stay_home_requirements', 'days_income_support','days_mask_mandates', 'winter_avg_temp', 'spring_avg_temp', 'summer_avg_temp', 'fall_avg_temp']
    if dbutils.widgets.get('period') == '1':
        metric_clusters = get_table_as_pandas_df("rq_dwtn_clusters_0823_period_1") 
        metric_clusters['description'] = metric_clusters['cluster'].map({0:'Exceptional Performer',
                                                                         1:'Well Below Average',
                                                                         2:'Average Then Stagnant',
                                                                         3: 'Below Average, Then Improve',
                                                                         4:'Above Average',
                                                                         5: 'Exceptional Performer'})
        metric_clusters['color'] = metric_clusters['cluster'].map({0:'#386cb0',
                                                                   1:'#ffff99',
                                                                   2:'#7fc97f',
                                                                   3: '#fdc086',
                                                                   4:'#e78ac3',
                                                                   5: '#a6611a'})
        
    elif dbutils.widgets.get('period') == '2':
        metric_clusters = get_table_as_pandas_df("rq_dwtn_clusters_0823_period_2") 
        metric_clusters['description'] = metric_clusters['cluster'].map({0:'Well below average (>1 SD below)',
                                                                         1:'Very high performers',
                                                                         2:'Slightly above average (0-1 SD above)',
                                                                         3: 'Slightly below average (0-1 SD below)',
                                                                         4:'Well above average (1+ SD above)',
                                                                         5: 'Very high performers'})
        
        metric_clusters['color'] = metric_clusters['cluster'].map({0:'#377eb8',
                                                                   1:'#ffff99',
                                                                   2:'#66a61e',
                                                                   3: '#d95f02',
                                                                   4:'#e7298a',
                                                                   5: '#ffff99'})
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
       
#for consistency's sake- sorry québécois
metric_clusters = metric_clusters.replace('Québec','Quebec')
metric_clusters = metric_clusters.replace('Montréal','Montreal')

# create rfc_table by metric
rfc_table = rfc_data[['city'] + features]
rfc_table = rfc_table[~rfc_table['city'].isin(['Dallas','Orlando','Mississauga','Hamilton','Oklahoma City'])]
rfc_table = rfc_table.replace('Québec','Quebec')
rfc_table = rfc_table.replace('Montréal','Montreal')
rfc_table.loc[:,'description'] = rfc_table.loc[:,'city'].map(dict(zip(metric_clusters['city'], metric_clusters['description'])))
rfc_table.loc[:,'color'] = rfc_table.loc[:,'city'].map(dict(zip(metric_clusters['city'], metric_clusters['color'])))
rfc_table.loc[:,'cluster'] = rfc_table.loc[:,'city'].map(dict(zip(metric_clusters['city'], metric_clusters['cluster'])))
scaler = MinMaxScaler()
rfc_table[features] = rfc_table[features].astype(float)
dbutils.widgets.dropdown('x_feature', features[0], features)

fig = sns.catplot(data = rfc_table, 
                  x = dbutils.widgets.get('x_feature'),
                  y = 'cluster',
                  
                  hue = 'cluster', 
                  kind = 'swarm',
                  palette = {
                      0:'#386cb0',
                      1:'#ffff99',
                      2:'#7fc97f',
                      3: '#fdc086',
                      4:'#e78ac3',
                      5: '#a6611a'},
                  legend_out = True,
                
                  height = 6,
                  aspect = 2)

def label_point(x, y, val, ax):
    a = pd.concat({'x': x, 'y': y, 'val': val}, axis=1)
    for i, point in a.iterrows():
        fig.text(point['x']+.02, point['y'], str(point['val']))

label_point(rfc_table[dbutils.widgets.get('x_feature')], rfc_table.cluster, rfc_table['cluster'], plt.gca()) 

plt.show()

# COMMAND ----------



# COMMAND ----------

# joint distribution of features by simple random sample- change to high ranking set of vars from rfc(?)
import random
# look at some really interesting vars
import plotly.express as px
# filter and load clusters by selected metric and period
if dbutils.widgets.get('metric') == 'downtown':
    features = ['total_pop_downtown', 'pct_singlefam_downtown', 'pct_multifam_downtown','pct_renter_downtown','median_age_downtown','bachelor_plus_downtown','median_hhinc_downtown','median_rent_downtown','pct_vacant_downtown','pct_commute_auto_city','pct_commute_public_transit_city','pct_commute_bicycle_city','pct_commute_walk_city','housing_units_downtown','average_commute_time_city','pct_jobs_agriculture_forestry_fishing_hunting','pct_jobs_mining_quarrying_oil_gas', 'pct_jobs_utilities','pct_jobs_construction', 'pct_jobs_manufacturing','pct_jobs_wholesale_trade', 'pct_jobs_retail_trade','pct_jobs_transport_warehouse', 'pct_jobs_information','pct_jobs_finance_insurance','pct_jobs_real_estate','pct_jobs_professional_science_techical','pct_jobs_management_of_companies_enterprises','pct_jobs_administrative_support_waste','pct_jobs_educational_services','pct_jobs_healthcare_social_assistance','pct_jobs_arts_entertainment_recreation','pct_jobs_accomodation_food_services','pct_jobs_public_administration', 'employment_entropy', 'population_density_downtown', 'employment_density_downtown', 'housing_density_downtown','days_school_closing', 'days_workplace_closing','days_cancel_large_events', 'days_cancel_all_events','days_stay_home_requirements', 'days_income_support','days_mask_mandates', 'winter_avg_temp', 'spring_avg_temp', 'summer_avg_temp', 'fall_avg_temp']
    if dbutils.widgets.get('period') == '1':
        metric_clusters = get_table_as_pandas_df("rq_dwtn_clusters_0823_period_1") 
        metric_clusters['description'] = metric_clusters['cluster'].map({0:'Exceptional Performer',
                                                                         1:'Well Below Average',
                                                                         2:'Average Then Stagnant',
                                                                         3: 'Below Average, Then Improve',
                                                                         4:'Above Average',
                                                                         5: 'Exceptional Performer'})
        metric_clusters['color'] = metric_clusters['cluster'].map({0:'#386cb0',
                                                                   1:'#ffff99',
                                                                   2:'#7fc97f',
                                                                   3: '#fdc086',
                                                                   4:'#e78ac3',
                                                                   5: '#a6611a'})
        
    elif dbutils.widgets.get('period') == '2':
        metric_clusters = get_table_as_pandas_df("rq_dwtn_clusters_0823_period_2") 
        metric_clusters['description'] = metric_clusters['cluster'].map({0:'Well below average (>1 SD below)',
                                                                         1:'Very high performers',
                                                                         2:'Slightly above average (0-1 SD above)',
                                                                         3: 'Slightly below average (0-1 SD below)',
                                                                         4:'Well above average (1+ SD above)',
                                                                         5: 'Very high performers'})
        
        metric_clusters['color'] = metric_clusters['cluster'].map({0:'#377eb8',
                                                                   1:'#ffff99',
                                                                   2:'#66a61e',
                                                                   3: '#d95f02',
                                                                   4:'#e7298a',
                                                                   5: '#ffff99'})
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
       
#for consistency's sake- sorry québécois
metric_clusters = metric_clusters.replace('Québec','Quebec')
metric_clusters = metric_clusters.replace('Montréal','Montreal')

# create rfc_table by metric

# subset features here
subset_features = ['total_pop_downtown', 'pct_singlefam_downtown', 'pct_multifam_downtown','pct_renter_downtown','median_age_downtown','bachelor_plus_downtown','median_hhinc_downtown','median_rent_downtown','pct_vacant_downtown','pct_commute_auto_city','pct_commute_public_transit_city','pct_commute_bicycle_city','pct_commute_walk_city','housing_units_downtown','average_commute_time_city','pct_jobs_agriculture_forestry_fishing_hunting','pct_jobs_mining_quarrying_oil_gas', 'pct_jobs_utilities','pct_jobs_construction', 'pct_jobs_manufacturing','pct_jobs_wholesale_trade', 'pct_jobs_retail_trade','pct_jobs_transport_warehouse', 'pct_jobs_information','pct_jobs_finance_insurance','pct_jobs_real_estate','pct_jobs_professional_science_techical','pct_jobs_management_of_companies_enterprises','pct_jobs_administrative_support_waste','pct_jobs_educational_services','pct_jobs_healthcare_social_assistance','pct_jobs_arts_entertainment_recreation','pct_jobs_accomodation_food_services','pct_jobs_public_administration', 'employment_entropy', 'population_density_downtown', 'employment_density_downtown', 'housing_density_downtown','days_school_closing', 'days_workplace_closing','days_cancel_large_events', 'days_cancel_all_events','days_stay_home_requirements', 'days_income_support','days_mask_mandates', 'winter_avg_temp', 'summer_avg_temp']

rfc_table = rfc_data[['city'] + subset_features]
rfc_table = rfc_table[~rfc_table['city'].isin(['Dallas','Orlando','Mississauga','Hamilton','Oklahoma City'])]
rfc_table = rfc_table.replace('Québec','Quebec')
rfc_table = rfc_table.replace('Montréal','Montreal')
rfc_table.loc[:,'description'] = rfc_table.loc[:,'city'].map(dict(zip(metric_clusters['city'], metric_clusters['description'])))
rfc_table.loc[:,'color'] = rfc_table.loc[:,'city'].map(dict(zip(metric_clusters['city'], metric_clusters['color'])))
rfc_table.loc[:,'cluster'] = rfc_table.loc[:,'city'].map(dict(zip(metric_clusters['city'], metric_clusters['cluster'])))
scaler = MinMaxScaler()
rfc_table[subset_features] = rfc_table[subset_features].astype(float)
#dbutils.widgets.dropdown('x_feature', subset_features[0], subset_features)

ax_feats = sns.pairplot(data = rfc_table[['city', 'cluster', 'description'] + subset_features[:4]], hue = 'cluster', palette = 'colorblind')
ax_feats.fig.suptitle('Joint distribution of selected features for ' + dbutils.widgets.get('metric') + ' period ' + dbutils.widgets.get('period') + ' recovery clusters', y = 1.01)
plt.show()

# COMMAND ----------

rfc_table

# COMMAND ----------

#normalized_visits_dwtn = normalized_visits[normalized_visits["metric"]=="downtown"].copy()
#ormalized_visits_dwtn['cluster'] = normalized_visits_dwtn['city'].map(dict(zip(rq_dwtn_clusters['city'], rq_dwtn_clusters['cluster']))).astype(int)
#mean_dwtn = normalized_visits_dwtn.groupby(['cluster','week']).mean()['normalized_visits_by_total_visits'].reset_index()
#mean_dwtn["week"] = pd.to_datetime(mean_dwtn["week"])
#mean_dwtn_rolling = pd.DataFrame()
#for i in mean_city["cluster"].unique():
#    mean_dwtn_rolling_cluster = mean_dwtn[mean_dwtn["cluster"]==i].copy()
#    mean_dwtn_rolling_cluster['rolling'] = mean_dwtn_rolling_cluster['normalized_visits_by_total_visits'].rolling(window=9).mean().shift(-4)
#    mean_dwtn_rolling = pd.concat([mean_dwtn_rolling,mean_dwtn_rolling_cluster])
#sns.set(rc={'figure.figsize':(14,10)})
#ax = sns.lineplot(data=mean_dwtn_rolling, x ='week', y = 'rolling', hue='cluster', palette='tab10')
#plt.ylabel('Downtown RQ')
#plt.xlabel('Date')
#plt.show()

# COMMAND ----------

#downtown_rfc_table = rfc_data[['city'] + downtown_features]

#'pct_nhwhite_downtown','pct_nhblack_downtown','pct_nhasian_downtown','pct_hisp_downtown',

#city_rfc_table = rfc_data[['city'] + city_features]

#'pct_nhwhite_city','pct_nhblack_city','pct_nhasian_city','pct_hisp_city',

#lq_rfc_table = rfc_data[['city'] + lq_features]

#'pct_nhwhite_downtown','pct_nhwhite_city', 'pct_nhblack_downtown', 'pct_nhblack_city','pct_nhasian_downtown', 'pct_nhasian_city', 'pct_hisp_downtown','pct_hisp_city', 

#lq_rfc_table['population_lq'] = lq_rfc_table['total_pop_downtown']/lq_rfc_table['total_pop_city']

# COMMAND ----------

#cluster_features_df = pd.merge(rq_dwtn_clusters, features, on='city')
#cluster_features_df = cluster_features_df.drop('city', axis=1)
#cluster_features_df

# COMMAND ----------

dbutils.widgets.dropdown('metric', 'downtown', ['downtown', 'metro', 'relative'])


# set the cutoff at > X cities. X * number of unique x_feature == cutoff
keep_clusters = (features_df.groupby(['cluster']).size().sort_values(ascending = False) / len(features_df['x_feature'].unique())) >= 5
features_df = features_df[features_df['cluster'].isin(keep_clusters[keep_clusters].index.values)]

ax_feats = sns.displot(data = features_df, x = 'value', col = 'x_feature', hue = 'cluster', kind = 'kde', palette = 'colorblind',  facet_kws = {'sharey':False, 'sharex' : False}, col_wrap = 3, height = 6, aspect = 1)
ax_feats.fig.suptitle('Distribution of features for ' + dbutils.widgets.get('metric') + ' period ' + dbutils.widgets.get('period') + ' recovery clusters', y = 1.01)
plt.show()

# COMMAND ----------

# filter by widget selected metric
# get subset of features so pairplot is comprehensible
import random
get_cols = ['city', 'cluster']
if dbutils.widgets.get('metric') == 'downtown':
    feature_cols = random.sample(set(downtown_features), k = 5)
    flat_cols = get_cols + feature_cols
    features_df_pairplots = downtown_rfc_table
elif dbutils.widgets.get('metric') == 'metro':
    feature_cols = random.sample(set(city_features), k = 5)
    flat_cols = get_cols + feature_cols
    features_df_pairplots = city_rfc_table
elif dbutils.widgets.get('metric') == 'relative':
    feature_cols = random.sample(set(lq_features), k = 5)
    flat_cols = get_cols + feature_cols
    features_df_pairplots = lq_rfc_table
features_df_pairplots[feature_cols] = features_df_pairplots[feature_cols].astype(float)
features_df_pairplots = features_df_pairplots[flat_cols]

keep_clusters = features_df_pairplots.groupby(['cluster']).size().sort_values(ascending = False) >= 5
features_df_pairplots = features_df_pairplots[features_df_pairplots['cluster'].isin(keep_clusters[keep_clusters].index.values)]
ax_feats = sns.pairplot(data = features_df_pairplots, hue = 'cluster', palette = 'colorblind')
ax_feats.fig.suptitle('Joint distribution of selected features for ' + dbutils.widgets.get('metric') + ' period ' + dbutils.widgets.get('period') + ' recovery clusters', y = 1.01)
plt.show()

# COMMAND ----------

#cluster_features_df = pd.merge(rq_dwtn_clusters, features, on='city')
#cluster_features_df = cluster_features_df.drop('city', axis=1)
#cluster_features_df

# COMMAND ----------

# importing the required libraries
from sklearn import datasets
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
%matplotlib inline

# COMMAND ----------

cluster_features_df['cluster'] = cluster_features_df['cluster'].astype('string')

# COMMAND ----------

cluster_features_df

# COMMAND ----------

# Plot Downtown Features
for feature in downtown_features:
    cluster_features_df[feature] = cluster_features_df[feature].astype(float)
    sns.kdeplot(data = cluster_features_df, x=feature, hue='cluster')
  
    plt.xlabel(feature)
    plt.ylabel('Probability Density')

# COMMAND ----------

#old
# Feature Engineering
MIN_CLUSTER_SIZE = 5
## Season 1
### Identify clusters with more than 5 instances
cluster_labeled = pd.merge(per1_clusters, features, on='city', how='outer')
# .drop(columns="vaccination_policy")
# season_1_filter = cluster_labeled['Season'].isin(['Season_1', 'Season_2', 'Season_3'])
low_cluster_count_filter = ~cluster_labeled['cluster'].isin(cluster_sizes[cluster_sizes["city"] < MIN_CLUSTER_SIZE]["cluster"])
per_1_feats = cluster_labeled[low_cluster_count_filter].groupby('city').mean()
per_1_features = per_1_feats.columns.tolist()
per_1_features.remove('cluster')
per_1_feats.display()

# COMMAND ----------

MIN_CLUSTER_SIZE = 2


# COMMAND ----------

def in_qrange(ser, q):
    return ser.between(*ser.quantile(q=q))


# COMMAND ----------

feats_long = per_1_feats.melt(id_vars="cluster", var_name="feature")

feats_long

# COMMAND ----------

t = MinMaxScaler()

res = pd.DataFrame()
for feature in per_1_features:
    group = feats_long[feats_long["feature"] == feature].drop(columns="feature")
    a=pd.DataFrame(t.fit_transform(group[group["value"].transform(in_qrange, q=[0.1, 0.9])])).set_axis(labels=["cluster", "value"], axis="columns")
    a["cluster"] = a["cluster"] * 7
    a["feature"] = feature
#     a.display()
    res = pd.concat([res, a])

res

# COMMAND ----------


# feats_normalized = pd.DataFrame(
#     t. fit_transform(x.drop(columns="cluster"))
# ).set_axis(labels=per_1_features, axis="columns").assign(cluster=per_1_feats["cluster"].values)

# feats_normalized

# COMMAND ----------

# feats_long_normalized

# COMMAND ----------

quantiled = (
    res
        .groupby(["cluster", "feature"], as_index=False)
        .quantile([0.1, 0.25, 0.5, 0.75, 0.9])
        .reset_index(level=1)
        .pivot(index=("cluster", "feature"), columns="level_1", values="value").reset_index()
)

quantiled

# COMMAND ----------

iqr = quantiled.assign(iqr=lambda df: df[0.9] - df[0.1])
iqr.display()

# COMMAND ----------

IQR_THRESHOLD = 0.3
CLUSTER_THRESHOLD = 3
low_deviation = iqr[iqr["iqr"] <= IQR_THRESHOLD].groupby(["feature"], as_index=False).agg({"cluster": "count", "iqr": "mean"})
low_deviation = low_deviation[low_deviation["cluster"] >= CLUSTER_THRESHOLD]
low_deviation

# COMMAND ----------

low_deviation.plot.bar(x="feature", y="iqr")

# COMMAND ----------

# for feature in low_deviation
# per_1_feats.hist(by="cluster", column="pct_jobs_mining_quarrying_oil_gas", figsize=(20, 10), sharex=True, subplots=True)

for feature in per_1_features:
    fig = plt.figure()
    ax = fig.add_subplot(title=feature)

    for cluster in cluster_sizes[cluster_sizes["city"] >= MIN_CLUSTER_SIZE]["cluster"]:
        p = per_1_feats[per_1_feats["cluster"] == cluster][feature]

        p.plot(kind='kde', ax=ax, label=cluster)
    ax.legend()


# y.plot(kind='kde', ax=ax1, color='red')

# COMMAND ----------

cluster = 0
iqr[iqr["cluster"] == cluster].sort_values(by="iqr", axis="index")

# COMMAND ----------

