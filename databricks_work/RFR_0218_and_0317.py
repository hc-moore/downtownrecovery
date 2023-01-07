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

# MAGIC %md
# MAGIC ##02/18 Work##

# COMMAND ----------

main_df_0218 = get_table_as_pandas_df("0218_main_df")

# COMMAND ----------

main_df_0218 = main_df_0218.set_index("city")

# COMMAND ----------

import matplotlib as plt
fig = plt.figure()
ax = fig.add_axes([0,0,1,1])
ax.bar(main_df_0218.sort_values(by="LQ_last_25_weeks", ascending=False)["city"], main_df_0218.sort_values(by="LQ_last_25_weeks", ascending=False)["LQ_last_25_weeks"])

# COMMAND ----------

main_df_0218.sort_values(by="LQ_last_25_weeks", ascending=False)

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

def generate_random_forest(y_label):
    x_dependent_vars_cols = main_df_0218.columns
    x_dependent_vars_cols = list(filter(lambda c: c not in ['LQ_last_25_weeks'], main_df_0218))
    
    X=main_df_0218[x_dependent_vars_cols]  # Features
    y=main_df_0218[y_label]  # Labels

    # Split dataset into training set and test set
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3)
    
    # define the model
    model = RandomForestRegressor()

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

generate_random_forest("LQ_last_25_weeks")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ##03/17 Work##

# COMMAND ----------

rfr_factors = get_table_as_pandas_df("0320_can_rfr_factors")
rfr_factors = rfr_factors.append(get_table_as_pandas_df("0321_us_rfr_factors_3"))
rfr_factors = rfr_factors.reset_index().iloc[:,1:]
#rfr_factors.iloc[2,0] = "Montreal"
#rfr_factors.iloc[10,0] = "Quebec"

# COMMAND ----------

rfr_factors

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



# COMMAND ----------

create_and_save_as_table(rfr_factors, "city_lqs_with_attributes_0322")

# COMMAND ----------

metrics_df["city"].values

# COMMAND ----------

metrics_df = get_table_as_pandas_df("metrics_df_0320")

# COMMAND ----------

rfr_factors = rfr_factors.merge(metrics_df, how="left", left_on="city", right_on="city")

# COMMAND ----------

rfr_factors = rfr_factors.fillna(0)
display(rfr_factors)

# COMMAND ----------

def generate_random_forest(y_label):
    x_dependent_vars_cols = ['pct_singlefam', 'pct_renter', 'pct_employment_natresource',
       'pct_employment_construction', 'pct_employment_manufacturing',
       'pct_employment_wholesale', 'pct_employment_retail',
       'pct_employment_transpowarehousingutil', 'pct_employment_info',
       'pct_employment_financeinsre', 'pct_employment_profscimgmtadminwaste',
       'pct_employment_eduhealthsocialassist',
       'pct_employment_artentrecaccommfood', 'pct_employment_other',
       'pct_employment_pubadmin', 'median_age', 'bachelor_plus',
       'median_hhinc', 'median_rent', 'pct_vacant', 'pct_nhwhite',
       'pct_nhblack', 'pct_nhasian', 'pct_hisp',
       'pct_biz_accom_food', 'pct_biz_admin_waste',
       'pct_biz_arts_entertainment', 'pct_biz_construction',
       'pct_biz_educational_svcs', 'pct_biz_finance_insurance',
       'pct_biz_healthcare_social_assistance', 'pct_biz_information',
       'pct_biz_mgmt_companies_enterprises', 'pct_biz_manufacturing',
       'pct_biz_other_except_pub_adm', 'pct_biz_professional_sci_tech',
       'pct_biz_public_adm', 'pct_biz_retail_trade',
       'pct_biz_transportation_warehousing', 'pct_biz_utilities',
       'pct_biz_wholesale_trade', 'pct_biz_agriculture', 'pct_biz_mining_gas',
       'pct_biz_real_estate_leasing', 'population_density', 'business_density',
       'housing_density', 'land use entropy', 'country']
    #x_dependent_vars_cols = list(filter(lambda c: c not in ['LQ_last_25_weeks'], rfr_factors))
    
    X=rfr_factors[x_dependent_vars_cols]  # Features
    y=rfr_factors[y_label]  # Labels

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

generate_random_forest("lq_avg_period1")

# COMMAND ----------

generate_random_forest("lq_avg_period2")

# COMMAND ----------

generate_random_forest("lq_avg_period3")

# COMMAND ----------

generate_random_forest("lq_avg_period4")

# COMMAND ----------

generate_random_forest("lq_avg_period4a")

# COMMAND ----------

generate_random_forest("lq_avg_period4b")

# COMMAND ----------

generate_random_forest("rate_of_recovery")

# COMMAND ----------

generate_random_forest("omicron_resilience")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

