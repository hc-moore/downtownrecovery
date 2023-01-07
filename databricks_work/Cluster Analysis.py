# Databricks notebook source


# COMMAND ----------



# COMMAND ----------


# COMMAND ----------

# pretty-fy labels - all vars are downtown. mention this in the text so as not to use up precious text space
# shading plot boxes:
# transit is light blue
# LODES is yellow
# housing is green
# covid-19 restrictions is red
# socio-economic is grey
# weather is purple



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# filter and load clusters by selected metric and period


# COMMAND ----------

# filter and load clusters by selected metric and period
if dbutils.widgets.get('metric') == 'downtown':
    features = ['total_pop_downtown', 'pct_singlefam_downtown', 'pct_multifam_downtown','pct_renter_downtown','median_age_downtown','bachelor_plus_downtown','median_hhinc_downtown','median_rent_downtown','pct_vacant_downtown','pct_commute_auto_city','pct_commute_public_transit_city','pct_commute_bicycle_city','pct_commute_walk_city','housing_units_downtown','average_commute_time_city','pct_jobs_agriculture_forestry_fishing_hunting','pct_jobs_mining_quarrying_oil_gas', 'pct_jobs_utilities','pct_jobs_construction', 'pct_jobs_manufacturing','pct_jobs_wholesale_trade', 'pct_jobs_retail_trade','pct_jobs_transport_warehouse', 'pct_jobs_information','pct_jobs_finance_insurance','pct_jobs_real_estate','pct_jobs_professional_science_techical','pct_jobs_management_of_companies_enterprises','pct_jobs_administrative_support_waste','pct_jobs_educational_services','pct_jobs_healthcare_social_assistance','pct_jobs_arts_entertainment_recreation','pct_jobs_accomodation_food_services','pct_jobs_public_administration', 'employment_entropy', 'population_density_downtown', 'employment_density_downtown', 'housing_density_downtown','days_school_closing', 'days_workplace_closing','days_cancel_large_events', 'days_cancel_all_events','days_stay_home_requirements', 'days_income_support','days_mask_mandates', 'winter_avg_temp', 'spring_avg_temp', 'summer_avg_temp', 'fall_avg_temp']
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
        important_features = ['pct_jobs_information', 'pct_jobs_finance_insurance', 'pct_jobs_professional_science_techical',
                              'pct_commute_public_transit_city', 'pct_jobs_arts_entertainment_recreation', 'pct_jobs_manufacturing',
                              'pct_jobs_healthcare_social_assistance', 'employment_entropy', 'average_commute_time_city', 
                              'bachelor_plus_downtown', 'pct_renter_downtown', 'pct_multifam_downtown', 
                              'days_stay_home_requirements', 'days_cancel_large_events', 'winter_avg_temp']
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
        important_features = ['pct_jobs_professional_science_techical', 'pct_jobs_finance_insurance', 'pct_jobs_arts_entertainment_recreation',
                              'pct_jobs_accomodation_food_services', 'days_stay_home_requirements', 'days_cancel_all_events',
                              'pct_multifam_downtown', 'bachelor_plus_downtown', 'median_hhinc_downtown',
                              'winter_avg_temp', 'summer_avg_temp', 'average_commute_time_city', 
                              'pct_commute_public_transit_city','pct_jobs_educational_services', 'employment_entropy']  

elif dbutils.widgets.get('metric') == 'relative':
    
    features = ['total_pop_downtown', 'total_pop_city','pct_singlefam_downtown', 'pct_singlefam_city', 'pct_multifam_downtown','pct_multifam_city','pct_mobile_home_and_others_city', 'pct_renter_downtown','pct_renter_city', 'median_age_downtown', 'median_age_city','bachelor_plus_downtown', 'bachelor_plus_city', 'median_hhinc_downtown','median_hhinc_city', 'median_rent_downtown', 'median_rent_city','pct_vacant_downtown', 'pct_vacant_city', 'pct_commute_auto_downtown', 'pct_commute_auto_city','pct_commute_public_transit_downtown','pct_commute_public_transit_city', 'pct_commute_bicycle_downtown','pct_commute_bicycle_city', 'pct_commute_walk_downtown','pct_commute_walk_city','pct_commute_others_city', 'housing_units_downtown','housing_units_city', 'average_commute_time_downtown','average_commute_time_city','pct_jobs_agriculture_forestry_fishing_hunting','pct_jobs_mining_quarrying_oil_gas', 'pct_jobs_utilities','pct_jobs_construction', 'pct_jobs_manufacturing','pct_jobs_wholesale_trade', 'pct_jobs_retail_trade','pct_jobs_transport_warehouse', 'pct_jobs_information','pct_jobs_finance_insurance', 'pct_jobs_real_estate','pct_jobs_professional_science_techical','pct_jobs_management_of_companies_enterprises','pct_jobs_administrative_support_waste','pct_jobs_educational_services','pct_jobs_healthcare_social_assistance','pct_jobs_arts_entertainment_recreation','pct_jobs_accomodation_food_services','pct_jobs_public_administration', 'employment_entropy','population_density_downtown', 'population_density_city','employment_density_downtown', 'housing_density_downtown','housing_density_city', 'days_school_closing', 'days_workplace_closing','days_cancel_large_events', 'days_cancel_all_events','days_stay_home_requirements', 'days_income_support','days_mask_mandates', 'winter_avg_temp', 'spring_avg_temp', 'summer_avg_temp', 'fall_avg_temp']
    
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
    important_features = ['pct_jobs_information', 'pct_jobs_professional_science_techical', 'pct_jobs_finance_insurance', 
                          'pct_jobs_healthcare_social_assistance', 'employment_density_downtown', 'pct_jobs_public_administration', 
                          'average_commute_time_city', 'pct_commute_public_transit_downtown', 'bachelor_plus_downtown',
                          'median_hhinc_downtown', 'median_rent_downtown', 'pct_vacant_downtown',
                          'pct_renter_downtown', 'days_cancel_all_events', 'winter_avg_temp']
    
       
#for consistency's sake- sorry québécois
metric_clusters = metric_clusters.replace('Québec','Quebec')
metric_clusters = metric_clusters.replace('Montréal','Montreal')



# features subset here
dbutils.widgets.dropdown('subset', 'LEHD', ['Oxford', 'LEHD', 'commute', 'all', 'final'])
import random
if dbutils.widgets.get('subset') == 'LEHD':
    subset_features = ['pct_jobs_agriculture_forestry_fishing_hunting','pct_jobs_mining_quarrying_oil_gas', 'pct_jobs_utilities','pct_jobs_construction', 'pct_jobs_manufacturing','pct_jobs_retail_trade', 'pct_jobs_information','pct_jobs_finance_insurance','pct_jobs_real_estate','pct_jobs_professional_science_techical','pct_jobs_management_of_companies_enterprises','pct_jobs_administrative_support_waste','pct_jobs_educational_services','pct_jobs_healthcare_social_assistance','pct_jobs_arts_entertainment_recreation','pct_jobs_accomodation_food_services','pct_jobs_public_administration', 'employment_entropy']
elif dbutils.widgets.get('subset') == 'Oxford':
    subset_features = ['days_school_closing', 'days_workplace_closing','days_cancel_large_events', 'days_cancel_all_events','days_stay_home_requirements', 'days_income_support','days_mask_mandates']
elif dbutils.widgets.get('subset') == 'commute':
    subset_features = ['pct_commute_auto_city','pct_commute_public_transit_city','pct_commute_bicycle_city','pct_commute_walk_city','average_commute_time_city']
elif dbutils.widgets.get('subset') == 'final':
    # the final subset
    subset_features = important_features
else:
    subset_features = features

# create rfc_table by metric
rfc_table = rfc_data[['city'] + subset_features]
rfc_table[subset_features] = rfc_table[subset_features].astype(float)
rfc_table = rfc_table[['city'] + [subset_features[i] for i in [0, 1]]] # OR manually input two features to compare
rfc_table.loc[:,'description'] = rfc_table.loc[:,'city'].map(dict(zip(metric_clusters['city'], metric_clusters['description'])))
rfc_table.loc[:,'cluster'] = rfc_table.loc[:,'city'].map(dict(zip(metric_clusters['city'], metric_clusters['cluster'])))
rfc_table['cluster'] = rfc_table['cluster'].astype(str)

scaler = MinMaxScaler()

#rfc_table['employment_entropy_log'] = np.log(rfc_table(['employment_entropy']))
# create features df
features_df = rfc_table.melt(id_vars = ['city', 'description', 'cluster'], var_name = 'x_feature')
features_df['value'] = np.round(features_df['value'].astype(float), 2)

# features_df['minmax_scaled'] = features_df.groupby('x_feature')['value'].transform(lambda x : scaler.fit_transform(x.values.reshape(-1,1)).flatten())

#hue_order = rfc_table.sort_values(by = 'cluster', ascending = True)['cluster'].unique()

import plotly.express as px
import plotly.graph_objects as go
#display(features_df)

fig = px.scatter(rfc_table, x = subset_features[0], y = subset_features[1], color = 'description',
               color_discrete_map = color_map,
               text = 'city', 
                 
              # points = 'all',
              # box=True,
              
               #hover_data = subset_features, 
              # facet_row_spacing = .01, 
               #facet_col_spacing = .02,
               #facet_col = 'x_feature', 
               #facet_col_wrap = 4,
               height = 1200, 
               width = 1200)

fig.update_layout(hovermode="closest", 
                  margin=dict(l=20, r=20, t=20, b=20),
                  hoverlabel=dict(
                      font_size=14
                  ),
              
              
                 showlegend = False)
fig.update_traces(#width = .5,
                 hoveron = 'points'#,
                 #pointpos=0
)
fig.update_xaxes(matches = None)
#fig.update_yaxes(matches=None, title= None)
fig.for_each_xaxis(lambda xaxis: xaxis.update(showticklabels=True))
fig.show(config = {'displayModeBar':False})

# COMMAND ----------

# joint distribution of features by simple random sample- change to high ranking set of vars from rfc(?)
import random
get_cols = ['city', 'cluster']
feature_cols = [] # manually select these - probably don't do more than 5 
flat_cols = get_cols + feature_cols
features_df_pairplots = rfc_table
features_df_pairplots[feature_cols] = rfc_table[feature_cols].astype(float)
ax_feats = sns.pairplot(data = features_df_pairplots, hue = 'cluster', palette = 'colorblind')
ax_feats.fig.suptitle('Joint distribution of selected features for ' + dbutils.widgets.get('metric') + ' period ' + dbutils.widgets.get('period') + ' recovery clusters', y = 1.01)
plt.show()

# COMMAND ----------

features_df

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

