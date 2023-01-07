# Databricks notebook source
import pandas as pd
import numpy as np
import statsmodels.api as sm
from statsmodels.multivariate.manova import MANOVA
from statsmodels.stats.anova import anova_lm
import statsmodels.formula.api as smf
import seaborn as sns
import matplotlib.pyplot as plt
import sklearn as sk
from sklearn.datasets import make_classification
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import RepeatedStratifiedKFold
from sklearn.ensemble import RandomForestRegressor

# Import train_test_split function
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

# COMMAND ----------

dbutils.widgets.dropdown(
  name = 'y',
  defaultValue = 'lq_avg_period4',
  choices = ["lq_avg_period1", "lq_avg_period2", "lq_avg_period3", "lq_avg_period4", "lq_avg_period4a", "lq_avg_period4b", "rate_of_recovery", "downtown_relative_recovery", "omicron_resilience"],
  label = 'Dependent Variable'
)



def create_and_save_as_table(df, table_name):
    spark_df = spark.createDataFrame(df)
    spark_df.write.format('csv').saveAsTable(table_name)
    
def get_table_as_pandas_df(table_name):
    return sqlContext.sql("select * from " + table_name + " tables").toPandas()

# COMMAND ----------

metrics_df = get_table_as_pandas_df("metrics_df_0320")
# add population manually, use ACS 2015-2019 data & Canadian 2016 Census data 
model_df = get_table_as_pandas_df("city_lqs_with_attributes")

# COMMAND ----------

display(model_df)

# COMMAND ----------

X = model_df[['city', 'pct_singlefam', 'pct_renter', 'pct_employment_natresource',
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
       'housing_density', 'land use entropy', 'country']]

# COMMAND ----------

rfr_factors = X.merge(metrics_df, how="left", left_on="city", right_on="city")

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
                             #'pct_others',
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

y_labels = ["lq_avg_period1", "lq_avg_period2", "lq_avg_period3", "lq_avg_period4", "rate_of_recovery", "omicron_resilience"]
rf_vars = {}
for y in y_labels:
    rfrs = generate_random_forest(y)
    print(rfrs['score'])
    rf_vars[y] = rfrs['feature_importances_df']
    print(y)
    display(rf_vars[y])

# COMMAND ----------

X = sm.add_constant(X)
dbutils.widgets.dropdown(
  name = 'x',
  defaultValue = rf_vars[dbutils.widgets.get("y")]['independent var'][0],
  choices = rf_vars[dbutils.widgets.get("y")]['independent var'][:10],
  label = 'Independent variable to plot'
)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

y = rfr_factors[dbutils.widgets.get("y")]
subset_cols = rf_vars[dbutils.widgets.get("y")]['independent var'].values.tolist()
subset_cols.append('const')
results = sm.OLS(y, X[subset_cols]).fit()
print(results.summary())
fig = sm.graphics.plot_fit(results, dbutils.widgets.get("x"))
fig.patch.set_facecolor("white")
fig.tight_layout(pad=1.0)

# COMMAND ----------

fit = MANOVA.from_formula("lq_avg_period1 + lq_avg_period2 + lq_avg_period3 + lq_avg_period4 ~ country", data = model_df[["lq_avg_period1", "lq_avg_period2", "lq_avg_period3", "lq_avg_period4", "country"]])
print(fit.mv_test())

# COMMAND ----------

