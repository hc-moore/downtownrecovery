# Databricks notebook source
import pandas as pd
from tslearn.clustering import TimeSeriesKMeans
from tslearn.utils import to_time_series_dataset
import numpy as np
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
from tslearn.datasets import CachedDatasets
from tslearn.preprocessing import TimeSeriesScalerMeanVariance,TimeSeriesScalerMinMax, TimeSeriesResampler
from datetime import datetime, timedelta, date
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats, misc
from scipy.ndimage import gaussian_filter

# COMMAND ----------

def create_and_save_as_table(df, table_name):
    spark_df = spark.createDataFrame(df)
    spark_df.write.format('csv').saveAsTable(table_name)
    
def get_table_as_pandas_df(table_name):
    return sqlContext.sql("select * from " + table_name + " tables").toPandas()

# COMMAND ----------

features_table_name = '_0812_all_model_features_'
# features_table_name = 'model_data_full'
metrics = get_table_as_pandas_df('0714_combined_metrics_df')
features = get_table_as_pandas_df(features_table_name)

# COMMAND ----------

features.display()
features.columns

# COMMAND ----------

## Import Device Count Tables
raw_dc_post_feb = get_table_as_pandas_df('safegraph_patterns_count_agg_postal')

# COMMAND ----------

metrics['metric'].unique()

# COMMAND ----------

import scipy
# idk if this helps
def format_ts_numpy_flexible(metric, scaling, drop_cities, s, rolling=True):
    rq_df = metrics.copy()
    rq_df = metrics[metrics['metric'] == metric][['city', scaling, 'week']]
    rq_df = rq_df.sort_values(by = 'week')[['city', 'week', 'normalized_visits_by_total_visits']]
    rq_df = rq_df.dropna()
    rq_df = rq_df.fillna(0)
    
    # Smoothing
    if rolling:
        rq_df = rq_df.pivot_table(columns = 'city', index = 'week', values = 'normalized_visits_by_total_visits').reset_index()
        rq_df = rq_df.rolling(10, min_periods=1, center=True).mean()
    else:
        rq_df['normalized_visits_by_total_visits'] = rq_df.sort_values(by = 'week').groupby(['city'])['normalized_visits_by_total_visits'].transform(lambda x : scipy.ndimage.gaussian_filter1d(x, sigma = s, order = 0))
        rq_df = rq_df.pivot_table(columns = 'city', index = 'week', values = 'normalized_visits_by_total_visits').reset_index()
        rq_df = rq_df.drop(columns=['week'])
        
    # Drop outliers
    cols_to_drop = drop_cities + ['Hamilton', 'Montreal', 'Quebec']
    rq_df = rq_df.drop(columns=cols_to_drop) 
    
    return rq_df, rq_df.to_numpy()
rq_df, ts_format_dt_rq = format_ts_numpy_flexible('downtown', 'normalized_visits_by_total_visits', ['Dallas', 'Orlando', 'Oklahoma City', 'Mississauga', 'Hamilton', 'Kansas City', 'Salt Lake City'], 3, True)
df = pd.DataFrame(rq_df)
df['New York'].plot()
# df.loc[1:100].plot()
df.columns

# COMMAND ----------

## Converts a pandas dataframe to a numpy 2d array to do time series clustering
def format_ts_numpy(metric, scaling, drop_cities):
    rq_df = metrics.copy()
    rq_df = metrics[metrics['metric'] == metric][['city', scaling, 'week']]
    rq_df = rq_df.dropna()
    rq_df = rq_df.pivot_table(index='city', columns='week', values=scaling, aggfunc='max').T.reset_index().drop(columns=['week'])
    cols_to_drop = drop_cities + ['Hamilton', 'Montreal', 'Quebec']
    rq_df = rq_df.drop(columns=cols_to_drop) 
    rq_df = rq_df.rolling(30, min_periods=1).mean()
    rq_df = rq_df.fillna(0)
    return rq_df, rq_df.to_numpy()

# COMMAND ----------

df[['New York', 'Boston', 'Washington DC', 'San Francisco','Los Angeles']].plot()

# COMMAND ----------

ts_format_dt_rq[0]

# COMMAND ----------

# Set clustering time period and generate clustering from the time period
def gen_cluster(start_week, end_week, clusters, period, metric, rolling=True):
    # Format plots for visualization 
    clusters_per_row = clusters / 2 if clusters % 2 == 0 else (clusters + 1) / 2
    # Define start and end week range for time series clustering
#     rq_df, ts_format_dt_rq = format_ts_numpy(metric, 'normalized_visits_by_total_visits', ['Dallas', 'Orlando', 'Oklahoma City', 'Mississauga', 'Hamilton', 'Kansas City', 'Salt Lake City'], 3, rolling)
    rq_df, ts_format_dt_rq = format_ts_numpy(metric, 'normalized_visits_by_total_visits', ['Dallas', 'Orlando', 'Oklahoma City', 'Mississauga', 'Hamilton'])
    ts_format_period_1_rq = ts_format_dt_rq[start_week:end_week]
    shape = ts_format_period_1_rq.shape
    # Normalize time series data
    print(shape)
    ts_format_dt_rq_scaled = TimeSeriesScalerMeanVariance().fit_transform(ts_format_period_1_rq).reshape(shape[0],shape[1]).T
    ts_format_dt_rq_scaled = to_time_series_dataset(ts_format_dt_rq_scaled)
    print(ts_format_dt_rq_scaled.shape)
    # Set-up plot
    plt.figure(figsize=(12, 8), dpi=80)
    seed = 0
    sz = ts_format_dt_rq_scaled.shape[1]
    # DBA-k-means
    print("DBA k-means")
    dba_km = TimeSeriesKMeans(n_clusters=clusters,
                              n_init=2,
                              metric="softdtw",
                              verbose=True,
                              max_iter_barycenter=10,
                              n_jobs=5,
                              random_state=seed)
    y_pred = dba_km.fit_predict(ts_format_dt_rq_scaled)

    for yi in range(clusters):
        index = (yi + 1)
        plt.subplot(2, int(clusters_per_row), index)
        for xx in ts_format_dt_rq_scaled[y_pred == yi]:
            plt.plot(xx.ravel(), "k-", alpha=.2)
        plt.plot(dba_km.cluster_centers_[yi].ravel(), "r-")
        plt.xlim(0, sz)
        plt.ylim(-4, 4)
        plt.text(0.45, 0.85,'Cluster %d' % (yi),
                 transform=plt.gca().transAxes)
        if yi == 1:
            plt.title("DBA $k$-means " + "Period: " + str(period))

    return pd.DataFrame({
        'city': rq_df.columns.tolist(),
        'cluster': y_pred
    }).sort_values('cluster'), dba_km.cluster_centers_

# COMMAND ----------

per1_clusters, cluster_centers = gen_cluster(0, 30, 11, 1, 'downtown', True)
per1_clusters.display()

# COMMAND ----------

def plot_cluster_centroids(df):
    center_series = pd.DataFrame(df.reshape(df.shape[0], df.shape[1]))
    center_series.T.plot()
    center_series = center_series.T.reset_index()
    return center_series.add_prefix('cluster_')

# COMMAND ----------

cluster_center_series = pd.DataFrame(cluster_centers.reshape(cluster_centers.shape[0], cluster_centers.shape[1]))
cluster_center_series.T.plot()

# COMMAND ----------

plot_cluster_centroids(cluster_centers)

# COMMAND ----------

create_and_save_as_table(plot_cluster_centroids(cluster_centers), 'thirty_week_centroids_2')

# COMMAND ----------

create_and_save_as_table(plot_cluster_centroids(cluster_centers), 'thirty_week_centroids')

# COMMAND ----------

# Create clusters based on slope and integral (creates 2 columns)
def regression_clustering(cluster_series):
    cluster_series_df = pd.DataFrame(cluster_series.reshape(cluster_series.shape[0], cluster_series.shape[1]))
    def lin_regression_res(series):
        slope, intercept, r_value, p_value, std_err = stats.linregress(list(range(0,series.shape[0])), series)
        return [slope, intercept, r_value, p_value, std_err]
    
    # Slope Clustering
    slope_bounds = {
        'great_decline':{
            'upper': -0.009,
            'lower': -0.1
        },
        'slow_decline':{
            'upper': -0.0001,
            'lower': -0.009
        },
        'flat':{
            'upper': 0.0001,
            'lower': -0.0001
        },
        'slow_growth':{
            'upper': 0.009,
            'lower': 0.0001
        },
        'great_growth':{
            'upper': 0.1,
            'lower': 0.009
        },
    }
    cluster_regressions = cluster_series_df.apply(lin_regression_res, axis=1, result_type='expand')
    cluster_regressions.columns = ['slope', 'intercept', 'r_value', 'p_value', 'std_err']
    cluster_regressions.loc[(cluster_regressions['slope'] > slope_bounds['great_growth']['lower']), 'slope_cluster'] = 4 # Great growth over time
    cluster_regressions.loc[(cluster_regressions['slope'] > slope_bounds['slow_growth']['lower']) &
                            (cluster_regressions['slope'] < slope_bounds['slow_growth']['upper'])
                            , 'slope_cluster'] = 3 # Less growth over time
    cluster_regressions.loc[(cluster_regressions['slope'] > slope_bounds['slow_decline']['lower']) &
                            (cluster_regressions['slope'] < slope_bounds['slow_decline']['upper'])
                            , 'slope_cluster'] = 2 # less decline over time
    cluster_regressions.loc[(cluster_regressions['slope'] < slope_bounds['great_decline']['upper']), 'slope_cluster'] = 1 # Sharp decline over time
    cluster_regressions.loc[(cluster_regressions['slope'] > slope_bounds['flat']['lower']) &
                            (cluster_regressions['slope'] < slope_bounds['flat']['upper'])
                            , 'slope_cluster'] = 0 # Flat
    cluster_regressions['slope_cluster'] = cluster_regressions['slope_cluster'].astype(int) 
    # Create Integral Clusters
    ## TODO
    cluster_regressions = cluster_regressions.reset_index()
    return cluster_regressions.rename(columns = {'index': 'cluster'})
# regression_clustering(cluster_centers)

# COMMAND ----------

def map_regression_clusters(raw_clusters, regression_clusters):
    return pd.merge(raw_clusters, regression_clusters[['slope_cluster','cluster']], on='cluster')

# map_regression_clusters(per1_clusters, regression_clustering(cluster_centers))

# COMMAND ----------

## Generate clusters for all time series - LQ CLUSTERS
first_week_index = 14
last_week_index = 123
num_clusters = 5
sigma = 3
metric = 'relative'
rolling = True
#input: gen_cluster(first_week, last_week, no clusters, sigma, metric, rolling =True)
lq_clusters, lq_cluster_centers = gen_cluster(first_week_index, last_week_index, num_clusters, sigma, metric, rolling)
lq_clusters.display()

# COMMAND ----------

create_and_save_as_table(lq_clusters, "lq_clusters_single_period_5")

# COMMAND ----------

create_and_save_as_table(lq_clusters, "lq_clusters_0822")
relative_clusters = pd.concat([pd.DataFrame(data = cluster, columns = [i]) for i, cluster in enumerate(lq_cluster_centers)], axis = 1).reset_index()
relative_clusters['week'] = relative_clusters['index'] + 14
relative_clusters = relative_clusters.set_index('week').drop(columns = 'index')
create_and_save_as_table(relative_clusters, "lq_cluster_centers_0822")

# COMMAND ----------

## Trial Downtown 5
first_week_index = 14
last_week_index = 123
num_clusters = 8
sigma = 3
metric = 'downtown'
rolling = True
#input: gen_cluster(first_week, last_week, no clusters, sigma, metric, rolling =True)
rq_dwtn_clusters, rq_dwtn_cluster_centers = gen_cluster(first_week_index, last_week_index, num_clusters, sigma, metric, rolling)
rq_dwtn_clusters.display()

# COMMAND ----------

create_and_save_as_table(rq_dwtn_clusters, "rq_dwtn_8_clusters_0822")
relative_clusters = pd.concat([pd.DataFrame(data = cluster, columns = [i]) for i, cluster in enumerate(rq_dwtn_cluster_centers)], axis = 1).reset_index()
relative_clusters['week'] = relative_clusters['index'] + 14
relative_clusters = relative_clusters.set_index('week').drop(columns = 'index')
create_and_save_as_table(relative_clusters, "rq_dwtn_8_cluster_centers_0822")

# COMMAND ----------

## Generate clusters for all time series DOWNTOWN PERIODS
first_week_index = 14
last_week_index = 67
num_clusters = 6
sigma = 3
metric = 'downtown'
rolling = True
#input: gen_cluster(first_week, last_week, no clusters, sigma, metric, rolling =True)
rq_dwtn_clusters, rq_dwtn_cluster_centers = gen_cluster(first_week_index, last_week_index, num_clusters, sigma, metric, rolling)
rq_dwtn_clusters.display()

# COMMAND ----------

create_and_save_as_table(rq_dwtn_clusters, "rq_dwtn_clusters_0823_period_1")

# COMMAND ----------

## Generate clusters for all time series DOWNTOWN PERIODS
first_week_index = 14
last_week_index = 119
num_clusters = 9
sigma = 3
metric = 'downtown'
rolling = True
#input: gen_cluster(first_week, last_week, no clusters, sigma, metric, rolling =True)
rq_dwtn_clusters, rq_dwtn_cluster_centers = gen_cluster(first_week_index, last_week_index, num_clusters, sigma, metric, rolling)
rq_dwtn_clusters.display()

# COMMAND ----------

create_and_save_as_table(rq_dwtn_clusters, "rq_dwtn_clusters_0831_all_2")

# COMMAND ----------

# Cluster Centroids Plot for Downtown RQ Clusters
plot_cluster_centroids(rq_dwtn_cluster_centers)

# COMMAND ----------

#old
create_and_save_as_table(rq_dwtn_clusters, "rq_dwtn_clusters_0823_period_1")
relative_clusters = pd.concat([pd.DataFrame(data = cluster, columns = [i]) for i, cluster in enumerate(rq_dwtn_cluster_centers)], axis = 1).reset_index()
relative_clusters['week'] = relative_clusters['index'] + 14
relative_clusters = relative_clusters.set_index('week').drop(columns = 'index')
create_and_save_as_table(relative_clusters, "rq_dwtn_cluster_centers_0823_period_1")

# COMMAND ----------

# LQ CLUSTERS PERIOD 1
## Generate clusters for all time series DOWNTOWN PERIODS
first_week_index = 14
last_week_index = 67
num_clusters = 6
sigma = 3
metric = 'relative'
rolling = True
#input: gen_cluster(first_week, last_week, no clusters, sigma, metric, rolling =True)
lq_clusters, lq_cluster_centers = gen_cluster(first_week_index, last_week_index, num_clusters, sigma, metric, rolling)
lq_clusters.display()

# COMMAND ----------

create_and_save_as_table(lq_clusters, "lq_clusters_0824_period_1_new")

# COMMAND ----------

# LQ CLUSTERS PERIOD 2
## Generate clusters for all time series DOWNTOWN PERIODS
first_week_index = 67
last_week_index = 119
num_clusters = 6
sigma = 3
metric = 'relative'
rolling = True
#input: gen_cluster(first_week, last_week, no clusters, sigma, metric, rolling =True)
lq_clusters, lq_cluster_centers = gen_cluster(first_week_index, last_week_index, num_clusters, sigma, metric, rolling)
lq_clusters.display()

# COMMAND ----------

create_and_save_as_table(lq_clusters, "lq_clusters_0824_period_2_new")

# COMMAND ----------

# LQ CLUSTERS PERIOD ALL
## Generate clusters for all time series DOWNTOWN PERIODS
first_week_index = 14
last_week_index = 119
num_clusters = 7
sigma = 3
metric = 'relative'
rolling = True
#input: gen_cluster(first_week, last_week, no clusters, sigma, metric, rolling =True)
lq_clusters, lq_cluster_centers = gen_cluster(first_week_index, last_week_index, num_clusters, sigma, metric, rolling)
lq_clusters.display()

# COMMAND ----------

create_and_save_as_table(lq_clusters, "lq_clusters_0824_all_time")

# COMMAND ----------

## Generate clusters for all time series
first_week_index = 14
last_week_index = 123
num_clusters = 11
sigma = 3
metric = 'metro'
rolling = True
#input: gen_cluster(first_week, last_week, no clusters, sigma, metric, rolling =True)
rq_city_clusters, rq_city_cluster_centers = gen_cluster(first_week_index, last_week_index, num_clusters, sigma, metric, rolling)
rq_city_clusters.display()

# COMMAND ----------

cluster_labeled_metrics = pd.merge(rq_city_clusters, metrics, on='city')
cluster_labeled_metrics

# COMMAND ----------

# Cluster Centroids Plot for City RQ Clusters
plot_cluster_centroids(rq_city_cluster_centers)

# COMMAND ----------

create_and_save_as_table(rq_city_clusters, "rq_city_clusters_0822")
relative_clusters = pd.concat([pd.DataFrame(data = cluster, columns = [i]) for i, cluster in enumerate(rq_city_cluster_centers)], axis = 1).reset_index()
relative_clusters['week'] = relative_clusters['index'] + 14
relative_clusters = relative_clusters.set_index('week').drop(columns = 'index')
create_and_save_as_table(relative_clusters, "rq_city_cluster_centers_0822")

# COMMAND ----------

#old
all_weeks = 123
per2_clusters, per2_cluster_centers = gen_cluster(1, all_weeks, 10, 3, 'downtown', True)
per2_clusters.display()

# COMMAND ----------

get_table_as_pandas_df('all_time_series_clusters_0821')

# COMMAND ----------

slope_cluster_df = regression_clustering(per2_cluster_centers)
slope_cluster_df.display()
per2_clusters

# COMMAND ----------

per2_all_clusters = map_regression_clusters(per2_clusters, regression_clustering(per2_cluster_centers))
per2_all_clusters.display()

# COMMAND ----------

per2_clusters = gen_cluster(30, 90, 8, 2)
per2_clusters.display()

# COMMAND ----------

per2_clusters = gen_cluster(30, 123, 4, 3)
per2_clusters.display()

# COMMAND ----------

import statsmodels.api as sm
from sklearn.tree import DecisionTreeClassifier, plot_tree
from sklearn.model_selection import cross_val_score
from sklearn import preprocessing

# COMMAND ----------

per2_clusters.display()

# COMMAND ----------

# Feature Engineering
def covid_feat_engineering(raw_cluster_df, cluster_col_name):
    old_metrics_cols_to_drop = ['_c0', 'display_title', 'seasonal_average']
    cols_to_drop = ['display_title']
    enc = preprocessing.LabelEncoder()
    features_clean = features.copy()
    features_clean.display()
    features_clean = features_clean[features['metric'] == 'downtown'].drop(['metric'], axis=1)
    # Create regional dummy variables
    features_clean = pd.concat([pd.get_dummies(features_clean['region']), features_clean], axis=1).drop('region', axis=1)
    ### Identify clusters with more than 5 instances
    cluster_labeled = pd.merge(raw_cluster_df, features_clean, on='city', how='outer')
    cluster_labeled = cluster_labeled.dropna()
    cluster_labeled[cluster_col_name] = cluster_labeled[cluster_col_name].astype('int')
    cluster_labeled = cluster_labeled.groupby('city').mean()
    features_columns = cluster_labeled.columns.tolist()
    features_columns.remove(cluster_col_name)
    city_suffix_list = ['region',
        'total_pop_city',
     'population_density_city',
     'housing_units_city',
     'housing_density_city',
     'pct_renter_city',
     'pct_singlefam_city',
     'pct_multifam_city',
     'median_age_city',
     'bachelor_plus_city',
     'pct_vacant_city',
     'median_rent_city',
     'median_hhinc_city',
     'pct_nhwhite_city',
     'pct_nhblack_city',
     'pct_hisp_city',
     'pct_nhasian_city',
     'pct_commute_auto_city',
     'pct_commute_public_transit_city',
     'pct_commute_bicycle_city',
     'pct_commute_walk_city',
     'average_commute_time_city',]
    features_columns = [ elem for elem in features_columns if elem not in city_suffix_list]
    return cluster_labeled, features_columns

cluster_feat_df, feat_cols = covid_feat_engineering(per2_all_clusters[['city', 'slope_cluster']], 'slope_cluster')
cluster_feat_df

# COMMAND ----------

# dt_important_vars = ['average_commute_time_downtown', 'median_age_downtown', 'pct_hisp_downtown', 'pct_jobs_professional_science_techical', 'pct_commute_auto_downtown', 'pct_commute_public_transit_downtown', 'pct_commute_bicycle_downtown', 'pct_commute_walk_downtown', 'pct_commute_others_downtown', 'pct_jobs_educational_services', 'pct_jobs_finance_insurance', 'pct_jobs_healthcare_social_assistance', 'pct_jobs_accomodation_food_services']
# per_1_feats[dt_important_vars].corr().style.background_gradient(cmap='coolwarm')

# per_1_feats[['average_commute_time_city',  'median_age_city', 'pct_hisp_city', 'pct_jobs_professional_science_techical', 'pct_commute_auto_city',  'pct_commute_public_transit_city', 'pct_commute_walk_city', 'pct_commute_others_city', 'pct_jobs_educational_services', 'pct_jobs_finance_insurance', 'pct_jobs_healthcare_social_assistance', 'pct_jobs_accomodation_food_services']].corr().style.background_gradient(cmap='coolwarm')

per_1_feats.corr().style.background_gradient(cmap='coolwarm')

# COMMAND ----------

cluster_features, feature_col_names = covid_feat_engineering(per2_all_clusters[['city', 'slope_cluster']], 'slope_cluster')
X = cluster_features[feature_col_names]
# X = per_1_feats[dt_important_vars].to_numpy()
scaler = preprocessing.StandardScaler().fit(X)
X_scaled = scaler.transform(X)
y = cluster_features['slope_cluster'].to_numpy()
y

# COMMAND ----------

clf = DecisionTreeClassifier(random_state=0)
model = clf.fit(X_scaled, y)
print(cross_val_score(model, X_scaled, y, cv=10).mean(), ' mean cvs')
feature_imp = list(zip(features_columns, model.feature_importances_))
feat_imp_df = pd.DataFrame.from_records(feature_imp, columns=['variable', 'importance'])
imp_features = feat_imp_df[feat_imp_df['importance'] > 0]['variable'].tolist()
imp_features

# COMMAND ----------

fig = plt.figure(figsize=(25,20))
_ = plot_tree(model, 
           feature_names=feature_col_names,  
           class_names=['Flat', 'Decline', 'Less Growth', 'More Growth'],
           filled=True)

# COMMAND ----------

features_df = cluster_features[imp_features]
all_normalized = (features_df - features_df.mean())/features_df.std()
# all_normalized = all_normalized.astype('float64')
logit_model=sm.MNLogit(cluster_features['slope_cluster'],sm.add_constant(all_normalized))
result=logit_model.fit()
stats1=result.summary()
print(stats1)

# COMMAND ----------

per_1_normalized = (per_1_feats[imp_features] - per_1_feats[imp_features].mean())/per_1_feats[imp_features].std()
logit_model=sm.MNLogit(per_1_feats['manual_cluster'],sm.add_constant(per_1_normalized))
result=logit_model.fit()
stats1=result.summary()
stats2=result.summary2()
print(stats1)
print(stats2)

# COMMAND ----------

per_1_normalized = (per_1_feats[imp_features] - per_1_feats[imp_features].mean())/per_1_feats[imp_features].std()
logit_model=sm.MNLogit(per_1_feats['cluster'],sm.add_constant(per_1_normalized))
result=logit_model.fit()
stats1=result.summary()
stats2=result.summary2()
print(stats1)
# print(stats2)

# COMMAND ----------

per1_clusters.display()

# COMMAND ----------

rq_ts_df = pd.DataFrame(ts_format_dt_rq)
rq_ts_df['dba_kmeans_cluster'] = y_pred
rq_ts_df.sort_values(by='dba_kmeans_cluster')

# COMMAND ----------

cities_clusters = []

for cluster_id in range(0,6):
    cluster_city_names = dt_all_metro_lq_df[dt_all_metro_lq_df['dba_kmeans_cluster'] == cluster_id].index.tolist()
    if 'Hamilton' in cluster_city_names:
        cluster_city_names.remove('Hamilton')
    cities_clusters.append(cluster_city_names)

# COMMAND ----------

len(cities_clusters)

# COMMAND ----------

all_cities_weekly_df['date_range_start_covid'] = pd.to_datetime(all_cities_weekly_df['date_range_start_covid'])
zoomed_in_lqs = all_cities_weekly_df[(all_cities_weekly_df['date_range_start_covid'] > datetime(2021,3,1)) & 
                     (all_cities_weekly_df['date_range_start_covid'] < datetime(2021,4,14))]
zoomed_in_lqs = zoomed_in_lqs.groupby(by=['city','date_range_start_covid'])['nvs_lq'].sum().to_frame()
zoomed_in_lqs

# COMMAND ----------

for cluster in cities_clusters:
    plt.figure(figsize = (12,8))
    sns.set_style("darkgrid")
    in_scope_cities = all_cities_weekly_df[all_cities_weekly_df['city'].isin(cluster)]
    plotting_df = pd.DataFrame()
    sns.lineplot(x = "date_range_start_covid", 
             y = "rolling_nvs_lq",
             hue="city",
             data = in_scope_cities)
    plt.title("LQ Time Series - 9 week Rolling Average")
    plt.xticks(rotation = 25)

# COMMAND ----------

from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.model_selection import StratifiedKFold, KFold
from sklearn.metrics import roc_curve, auc, mean_squared_error
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA

# COMMAND ----------

city_lqs_with_attributes_df = get_table_as_pandas_df('city_lqs_with_attributes_0321')
display(city_lqs_with_attributes_df)

# COMMAND ----------

# Load features df
city_lqs_with_attributes_df = get_table_as_pandas_df('city_lqs_with_attributes_0322')
city_feats_and_targets = city_lqs_with_attributes_df.merge(dt_all_metro_lq_df, on='city', how='outer')
city_feats_and_targets = city_feats_and_targets.set_index('city')
city_feats_and_targets = city_feats_and_targets.drop('display_title', axis=1)
city_feats_and_targets = city_feats_and_targets.drop(['Montréal','Québec'])


# Load lq df
all_cities_weekly_df = get_table_as_pandas_df('us_can_lqs_0317')
rolling_weekly_df = all_cities_weekly_df[['city','nvs_lq']]
dt_all_metro_lq_df = rolling_weekly_df.groupby('city')['nvs_lq'].apply(list).to_frame()
cities = dt_all_metro_lq_df.index.tolist()
dates = all_cities_weekly_df['date_range_start_covid'].unique()
city_weekly_lqs = pd.DataFrame(dt_all_metro_lq_df['nvs_lq'].tolist(), columns=dates)
city_weekly_lqs['city'] = cities
city_weekly_lqs = city_weekly_lqs.replace('Montréal','Montreal')
city_weekly_lqs = city_weekly_lqs.replace('Québec', 'Quebec')
city_feats_and_targets.index

# COMMAND ----------

def create_X_y(target_col, features, targets):
    scaler = StandardScaler()

    # Initialize target and feature columns
    target_cols = ['lq_avg_period1', 'lq_avg_period2', 'lq_avg_period3', 'lq_avg_period4', 'lq_avg_period4a', 'lq_avg_period4b', 'rate_of_recovery', 'downtown_relative_recovery', 'omicron_resilience', 'rolling_nvs_lq', 'dba_kmeans_cluster', 'rolling_nvs_lq', 'nvs_lq']
    feature_cols = features.columns.tolist()

    # Remove target and misc columns from feature column list
    for target in target_cols:
        if target in feature_cols:
            feature_cols.remove(target)
    feature_cols.remove('country')
    
    city_index = features.index.tolist()
    X = scaler.fit_transform(features[feature_cols].to_numpy()[:-2])
    targets = targets.add_suffix('_lq')
    y = targets[target_col].to_numpy()[:-2]
    nan_index = np.where(np.isnan(y))
    y_mean = np.nanmean(y, axis=0)
    y[nan_index] = y_mean
    return X, y

create_X_y('2020-03-09_lq', city_feats_and_targets, city_weekly_lqs)

# COMMAND ----------

# RFR
def find_rfr_n_est(X, y):
    rfr_cross_val = pd.DataFrame()
    for nestimators in range(130, 200):
        model = RandomForestRegressor(
            n_estimators=nestimators,
            min_samples_split=0.01
        )

        kf = KFold(n_splits=5)

        mse_list = np.array([])

        for train_index, test_index in kf.split(X):
            X_train, X_test = X[train_index], X[test_index]
            y_train, y_test = y[train_index], y[test_index]

            model.fit(X_train, y_train)

            model.score(X_test, y_test)

            y_pred = model.predict(X_test)

            mse = mean_squared_error(y_test, y_pred)
            mse_list = np.append(mse_list, mse)
        avg_mse = mse_list.sum() / mse_list.size
        rfr_cross_val = rfr_cross_val.append({
            'estimators': nestimators,
            'avg_mse': avg_mse
        }, ignore_index=True)

    rfr_cross_val = rfr_cross_val.sort_values(by='avg_mse')
    return rfr_cross_val.iloc[0,1]

find_rfr_n_est(X, y)

# COMMAND ----------

time_series_rfr = rfr_results_df.copy()
time_series_rfr = time_series_rfr.rename(columns={
    'lq_avg_period1_feat_imp':'2020-03-01',
    'lq_avg_period2_feat_imp':'2020-08-01',
    'lq_avg_period3_feat_imp':'2021-03-01',
    'lq_avg_period4_feat_imp':'2021-08-01',
})
time_series_rfr = time_series_rfr[['feature','2020-03-01','2020-08-01','2021-03-01','2021-08-01']]
time_series_rfr = time_series_rfr.set_index('feature')

# COMMAND ----------

transposed_time_series_rfr = time_series_rfr.T
transposed_time_series_rfr['date'] = ['2020-03-01','2020-08-01','2021-03-01','2021-08-01']
display(transposed_time_series_rfr)

# COMMAND ----------

# Initialize target and feature columns
target_cols = ['lq_avg_period1', 'lq_avg_period2', 'lq_avg_period3', 'lq_avg_period4', 'lq_avg_period4a', 'lq_avg_period4b', 'rate_of_recovery', 'downtown_relative_recovery', 'omicron_resilience', 'rolling_nvs_lq', 'dba_kmeans_cluster', 'rolling_nvs_lq', 'nvs_lq']
feature_cols = city_feats_and_targets.columns.tolist()

# Remove target and misc columns from feature column list
for target in target_cols:
    if target in feature_cols:
        feature_cols.remove(target)
feature_cols.remove('country')

# COMMAND ----------

rfr_results_df = pd.DataFrame(feature_cols,columns=['feature'])
target_lq_dates = all_cities_weekly_df['date_range_start_covid'].unique()

for target in target_lq_dates:
    X, y = create_X_y(target.strftime('%Y-%m-%d')+'_lq', city_feats_and_targets, city_weekly_lqs)
    model = RandomForestRegressor(
            n_estimators=115,
        )
    model.fit(X, y)

    rfr_results_df[target.strftime('%Y-%m-%d')+'_lq'+"_feat_imp"] =  model.feature_importances_

# COMMAND ----------

rfr_results_df

# COMMAND ----------

week_col = '2022-02-21_lq_feat_imp'
rfr_results_df.sort_values(week_col, ascending=False)[['feature',week_col]]

# COMMAND ----------

rfr_timeseries = rfr_results_df.copy()
rfr_timeseries.columns = rfr_timeseries.columns.str.rstrip('_lq_feat_imp')
rfr_timeseries = rfr_timeseries.rename(columns={'featur':'feature'})
rfr_timeseries = rfr_timeseries.T
rfr_timeseries = rfr_timeseries.reset_index()
rfr_timeseries.columns = rfr_timeseries.iloc[0]
rfr_timeseries = rfr_timeseries[1:]
rfr_timeseries = rfr_timeseries.rename(columns={'feature':'date'})
dates = rfr_timeseries['date'].tolist()
rfr_timeseries = rfr_timeseries.rolling(25, min_periods=3, center=True).mean()
# rfr_timeseries['date'] = dates
display(rfr_timeseries)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

in_scope_rfr = rfr_timeseries.copy()
avg_rfr_score = in_scope_rfr.mean()
top_rfr_feats = avg_rfr_score.sort_values(ascending=False)[:20].to_frame()
top_features = top_rfr_feats.index.tolist()

# rfr_timeseries['date'] = dates
# long_rfr = pd.wide_to_long(rfr_timeseries, ['feat_imp'], i='feature', j='date')
long_rfr = rfr_timeseries.unstack().to_frame()
# long_rfr = long_rfr.reset_index()
long_rfr

# COMMAND ----------

plt.figure(figsize = (12,8))
sns.set_style("darkgrid")
top_rfr_feats = rfr_timeseries[rfr_timeseries.isin(top_features)]
plotting_df = pd.DataFrame()
sns.lineplot(x = "date", 
         y = "rolling_nvs_lq",
         hue="city",
         data = top_rfr_feats)
plt.title("LQ Time Series - 9 week Rolling Average")
plt.xticks(rotation = 25)