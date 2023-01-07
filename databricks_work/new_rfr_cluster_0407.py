# Databricks notebook source
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


def create_and_save_as_table(df, table_name):
    spark_df = spark.createDataFrame(df)
    spark_df.write.format('csv').saveAsTable(table_name)
    
def get_table_as_pandas_df(table_name):
    return sqlContext.sql("select * from " + table_name + " tables").toPandas()

# COMMAND ----------

len(get_table_as_pandas_df("all_us_cities_dc"))

# COMMAND ----------

can_rfr_factors_0626 = get_table_as_pandas_df("can_rfr_features_0626")
us_rfr_factors_0626 = get_table_as_pandas_df("us_rfr_features_0626")
all_rfr_factors_0626 = can_rfr_factors_0626.append(us_rfr_factors_0626)

# COMMAND ----------

all_rfr_factors_0626.iloc[2,0] = "Montréal"
all_rfr_factors_0626.iloc[11,0] = "Québec"
display(all_rfr_factors_0626.reset_index().drop(columns=["index"]))

# COMMAND ----------

create_and_save_as_table(all_rfr_factors_0626,"us_can_features_from_census_0626")

# COMMAND ----------

downtown_rq_df_0713 = get_table_as_pandas_df("0713_downtown_rec_df_totaldevs")

# COMMAND ----------

metrics_df_0714 = get_table_as_pandas_df("0714_combined_metrics_df")

# COMMAND ----------

period_map = {'2022-05-02': 9, '2022-05-09': 9, '2022-05-16': 9, '2022-05-23': 9,
       '2022-05-30': 9, '2022-06-06': 9, '2022-02-28': 8, '2022-03-07': 9,
       '2022-03-14': 9, '2022-03-21': 9, '2022-03-28': 9, '2022-04-04': 9,
       '2022-04-11': 9, '2022-04-18': 9, '2022-04-25': 9, '2022-06-13': 0,
       '2022-01-31': 8, '2022-02-07': 8, '2022-02-14': 8, '2022-02-21': 8,
       '2020-02-24': 0, '2020-03-02': 1, '2020-03-09': 1, '2020-03-16': 1,
       '2020-03-23': 1, '2020-03-30': 1, '2020-04-06': 1, '2020-04-13': 1,
       '2020-04-20': 1, '2020-04-27': 1, '2020-05-04': 1, '2020-05-11': 1,
       '2020-05-18': 1, '2020-05-25': 1, '2020-06-01': 2, '2020-06-08': 2,
       '2020-06-15': 2, '2020-06-22': 2, '2020-06-29': 2, '2020-07-06': 2,
       '2020-07-13': 2, '2020-07-20': 2, '2020-07-27': 2, '2020-08-03': 2,
       '2020-08-10': 2, '2020-08-17': 2, '2020-08-24': 2, '2020-08-31': 2,
       '2020-09-07': 3, '2020-09-14': 3, '2020-09-21': 3, '2020-09-28': 3,
       '2020-10-05': 3, '2020-10-12': 3, '2020-10-19': 3, '2020-10-26': 3,
       '2020-11-02': 3, '2020-11-09': 3, '2020-11-16': 3, '2020-11-23': 3,
       '2020-11-30': 4, '2020-12-07': 4, '2020-12-14': 4, '2020-12-21': 4,
       '2020-12-28': 4, '2021-01-04': 4, '2021-01-11': 4, '2021-01-18': 4,
       '2021-01-25': 4, '2021-02-01': 4, '2021-02-08': 4, '2021-02-15': 4,
       '2021-02-22': 4, '2021-03-01': 5, '2021-03-08': 5, '2021-03-15': 5,
       '2021-03-22': 5, '2021-03-29': 5, '2021-04-05': 5, '2021-04-12': 5,
       '2021-04-19': 5, '2021-04-26': 5, '2021-05-03': 5, '2021-05-10': 5,
       '2021-05-17': 5, '2021-05-24': 5, '2021-05-31': 5, '2021-06-07': 6,
       '2021-06-14': 6, '2021-06-21': 6, '2021-06-28': 6, '2021-07-05': 6,
       '2021-07-12': 6, '2021-07-19': 6, '2021-07-26': 6, '2021-08-02': 6,
       '2021-08-09': 6, '2021-08-16': 6, '2021-08-23': 6, '2021-08-30': 6,
       '2021-09-06': 7, '2021-09-13': 7, '2021-09-20': 7, '2021-09-27': 7,
       '2021-10-04': 7, '2021-10-11': 7, '2021-10-18': 7, '2021-10-25': 7,
       '2021-11-01': 7, '2021-11-08': 7, '2021-11-15': 7, '2021-11-22': 7,
       '2021-11-29': 7, '2021-12-06': 8, '2021-12-13': 8, '2021-12-20': 8,
       '2021-12-27': 8, '2022-01-03': 8, '2022-01-10': 8, '2022-01-17': 8,
       '2022-01-24': 8, '2022-06-20': 0, '2022-06-27': 0}

# COMMAND ----------

metrics_df_0714["season"] = metrics_df_0714["week"].map(period_map)

# COMMAND ----------

metrics_df_0714 = metrics_df_0714[metrics_df_0714["season"]!=0]
downtown_rq_df_0714 = metrics_df_0714[metrics_df_0714["metric"]=="downtown"].groupby(["city","season"])["normalized_visits_by_total_visits"].mean().reset_index().rename(columns={"normalized_visits_by_total_visits": "downtown_rq"})
city_rq_df_0714 = metrics_df_0714[metrics_df_0714["metric"]=="metro"].groupby(["city","season"])["normalized_visits_by_total_visits"].mean().reset_index().rename(columns={"normalized_visits_by_total_visits": "city_rq"})
lq_df_0714 = metrics_df_0714[metrics_df_0714["metric"]=="relative"].groupby(["city","season"])["normalized_visits_by_total_visits"].mean().reset_index().rename(columns={"normalized_visits_by_total_visits": "lq"})

# COMMAND ----------

display(downtown_rq_df_0714[downtown_rq_df_0714["season"]==9])

# COMMAND ----------

metrics_df_0714

# COMMAND ----------

downtown_rq_pivot = metrics_df_0714.drop(columns=["raw_visit_counts","normalized_visits_by_state_scaling"])
downtown_rq_pivot = downtown_rq_pivot[downtown_rq_pivot["metric"]=="downtown"]

# COMMAND ----------

display(pd.pivot_table(downtown_rq_pivot[downtown_rq_pivot["season"]!=0], index="week", columns = "city", values="normalized_visits_by_total_visits").reset_index())

# COMMAND ----------

policy_brief_downtown_rq = downtown_rq_df_0714[downtown_rq_df_0714["season"].isin([8,9])].copy()
policy_brief_downtown_rq = policy_brief_downtown_rq.groupby("city")["downtown_rq"].mean().reset_index().sort_values(by="downtown_rq", ascending=False)
policy_brief_downtown_rq

# COMMAND ----------

policy_brief_city_rq = city_rq_df_0714[city_rq_df_0714["season"].isin([8,9])].copy()
policy_brief_city_rq = policy_brief_city_rq.groupby("city")["city_rq"].mean().reset_index().sort_values(by="city_rq", ascending=False)
policy_brief_city_rq

# COMMAND ----------

display(policy_brief_city_rq[policy_brief_city_rq["city"]!="Hamilton"])

# COMMAND ----------

rfr_downtown = all_rfr_factors_0626[["city","total_pop_city","pct_singlefam_downtown","pct_multifam_downtown","pct_mobile_home_and_others_downtown","pct_renter_downtown",
                                    "median_age_downtown","bachelor_plus_downtown","median_hhinc_downtown","median_rent_downtown","pct_vacant_downtown","pct_nhwhite_city",
                                    "pct_nhblack_city","pct_nhasian_city","pct_hisp_city","pct_commute_auto_city","pct_commute_public_transit_city","pct_commute_bicycle_city",
                                    "pct_commute_walk_city","pct_commute_others_city","housing_units_downtown","average_commute_time_city","pct_jobs_agriculture_forestry_fishing_hunting",
                                    "pct_jobs_mining_quarrying_oil_gas","pct_jobs_utilities","pct_jobs_construction","pct_jobs_manufacturing","pct_jobs_wholesale_trade",
                                    "pct_jobs_retail_trade","pct_jobs_transport_warehouse","pct_jobs_information","pct_jobs_finance_insurance","pct_jobs_real_estate",
                                    "pct_jobs_professional_science_techical","pct_jobs_management_of_companies_enterprises","pct_jobs_administrative_support_waste","pct_jobs_educational_services",
                                    "pct_jobs_healthcare_social_assistance","pct_jobs_arts_entertainment_recreation","pct_jobs_accomodation_food_services","pct_jobs_other","pct_jobs_public_administration",
                                    "employment_entropy","population_density_downtown","employment_density_downtown","housing_density_downtown"]]

# COMMAND ----------

rfr_table = rfr_downtown.merge(policy_brief_downtown_rq, how="left", left_on="city", right_on="city")
rfr_table = rfr_table[rfr_table["city"]!="Hamilton"]

# COMMAND ----------

country_map = {'Vancouver': 1, 'Montréal': 1, 'Calgary': 1, 'Halifax': 1,'London': 1,'Edmonton': 1,'Mississauga': 1,'Ottawa': 1,
 'Winnipeg': 1,'Toronto': 1,'Québec': 1,'Columbus': 0,'Indianapolis': 0,'Charlotte': 0,'San Francisco': 0,'Seattle': 0,'Denver': 0,'Washington DC': 0,
 'Cleveland': 0,'Honolulu': 0,'Cincinnati': 0,'Pittsburgh': 0,'Salt Lake City': 0,'St Louis': 0,'Orlando': 0,'San Antonio': 0,'San Diego': 0,'Dallas': 0,'San Jose': 0,
 'Austin': 0,'Jacksonville': 0,'Fort Worth': 0,'Sacramento': 0,'Kansas City': 0,'Atlanta': 0,'Omaha': 0,'Colorado Springs': 0,'Raleigh': 0,'Miami': 0,'Oakland': 0,
 'Minneapolis': 0,'Tulsa': 0,'Bakersfield': 0,'Wichita': 0,'Tampa': 0,'New Orleans': 0,'Memphis': 0,'Louisville': 0,'Baltimore': 0,'Milwaukee': 0,'Albuquerque': 0,
 'Tucson': 0,'Fresno': 0,'Nashville': 0,'Oklahoma City': 0,'El Paso': 0,'Boston': 0,'Portland': 0,'Las Vegas': 0,'Detroit': 0,'New York': 0,'Los Angeles': 0,
 'Chicago': 0,'Houston': 0,'Phoenix': 0,'Philadelphia': 0}
rfr_table["country"] = rfr_table["city"].map(country_map)

# COMMAND ----------

display(rfr_table)

# COMMAND ----------

# evaluate random forest algorithm for classification
from numpy import mean
from numpy import std
from sklearn.datasets import make_classification
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import RepeatedStratifiedKFold
from sklearn.ensemble import RandomForestRegressor

# Import train_test_split function
from sklearn.model_selection import train_test_split
import pandas as pd
from sklearn.preprocessing import StandardScaler

# COMMAND ----------

rfr_table.columns

# COMMAND ----------

def generate_random_forest(y_label):
    x_dependent_vars_cols = ['total_pop_city', 'pct_singlefam_downtown',
       'pct_multifam_downtown', 'pct_mobile_home_and_others_downtown',
       'pct_renter_downtown', 'median_age_downtown', 'bachelor_plus_downtown',
       'median_hhinc_downtown', 'median_rent_downtown', 'pct_vacant_downtown',
       'pct_nhwhite_city', 'pct_nhblack_city', 'pct_nhasian_city',
       'pct_hisp_city', 'pct_commute_auto_city',
       'pct_commute_public_transit_city', 'pct_commute_bicycle_city',
       'pct_commute_walk_city', 'pct_commute_others_city',
       'housing_units_downtown', 'average_commute_time_city',
       'pct_jobs_agriculture_forestry_fishing_hunting',
       'pct_jobs_mining_quarrying_oil_gas', 'pct_jobs_utilities',
       'pct_jobs_construction', 'pct_jobs_manufacturing',
       'pct_jobs_wholesale_trade', 'pct_jobs_retail_trade',
       'pct_jobs_transport_warehouse', 'pct_jobs_information',
       'pct_jobs_finance_insurance', 'pct_jobs_real_estate',
       'pct_jobs_professional_science_techical',
       'pct_jobs_management_of_companies_enterprises',
       'pct_jobs_administrative_support_waste',
       'pct_jobs_educational_services',
       'pct_jobs_healthcare_social_assistance',
       'pct_jobs_arts_entertainment_recreation',
       'pct_jobs_accomodation_food_services', 'pct_jobs_other',
       'pct_jobs_public_administration', 'employment_entropy',
       'population_density_downtown', 'employment_density_downtown',
       'housing_density_downtown', 'country']
    #x_dependent_vars_cols = list(filter(lambda c: c not in ['LQ_last_25_weeks'], rfr_factors))
    
    X=rfr_table[x_dependent_vars_cols]  # Features
    y=rfr_table[y_label]  # Labels

    # Split dataset into training set and test set
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3)
    
    # define the model
    model = RandomForestRegressor(n_estimators=115)

    RandomForestRegressor(bootstrap=True, ccp_alpha=0.0, criterion='mse',
                          max_depth=None, max_features='auto', max_leaf_nodes=None,
                          max_samples=None, min_impurity_decrease=0.0,
                          min_impurity_split=None, min_samples_leaf=1,
                          min_samples_split=2, min_weight_fraction_leaf=0.0,
                          n_estimators=100, n_jobs=None, oob_score=False,
                          random_state=None, verbose=0, warm_start=False) 
    
    # Feature Scaling
    sc = StandardScaler()
    X_train = sc.fit_transform(X_train)
    X_test = sc.transform(X_test)
    
    # Fit model with data
    model.fit(X_train, y_train)
    
    # R-Square
    score = model.score(X_train, y_train)
    
    # Feature Importance Dataframe
    data = list(zip(x_dependent_vars_cols, model.feature_importances_))
    feature_importances_df = pd.DataFrame(data, columns=['independent var', 'weight'])
    feature_importances_df = feature_importances_df.sort_values(by=['weight'], ascending=False)
    
    #permutation importance
    #perm_importance = permutation_importance(model, X_test, y_test)
    #data2 = list(zip(x_dependent_vars_cols, perm_importance.importances_mean))
    #perm_importances_df = pd.DataFrame(data2, columns=['independent var', 'permutation mean'])
    #perm_importances_df = perm_importances_df.sort_values(by=['permutation mean'], ascending=False)
    
    # Proximity Matrix
    #lq_proximity_matrix = proximityMatrix(model, X, normalize=True)
    
    return {
        'score': score,
        'feature_importances_df': feature_importances_df,
        #'permutation_importances_df': perm_importances_df,
        #'lq_proximity_matrix': lq_proximity_matrix
    }

# COMMAND ----------

generate_random_forest("downtown_rq")

# COMMAND ----------

generate_random_forest("period_2")

# COMMAND ----------

generate_random_forest("period_3")

# COMMAND ----------

generate_random_forest("period_4")

# COMMAND ----------

