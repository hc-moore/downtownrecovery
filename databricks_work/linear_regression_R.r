# Databricks notebook source
# Package names
packages <- c("broom",  "ggplot2", "readxl", "dplyr", "tidyr",  "MASS", "nnet", "scales", "stats", "SparkR", "tidyverse")

# Packages loading
invisible(lapply(packages, library, character.only = TRUE))

hiveContext <- sparkR.session()

get_table_as_r_df <- function(table_name) {
  SparkR::as.data.frame(SparkR::sql(paste0("select * from ", table_name, " table"))) 
  }

create_and_save_as_table <- function(df, table_name) {
    spark_df <- SparkR::createDataFrame(df)
    spark_df %>%
      SparkR::saveAsTable(table_name)
  }

# COMMAND ----------

# load in X
# these will be pulled in as strings :')
X <- get_table_as_r_df('all_model_features_0823')
X$city <- str_replace(X$city, "é", "e")
display(X)

# COMMAND ----------



# from stackoverflow to convert several columns from character -> numeric
# https://stackoverflow.com/questions/22772279/converting-multiple-columns-from-character-to-numeric-format-in-r
is_all_numeric <- function(x) {
  !any(is.na(suppressWarnings(as.numeric(na.omit(x))))) & is.character(x)
}

# create the subset of X to model - in this case, it is strictly job variables
X <- X %>%
  dplyr::mutate_if(is_all_numeric, as.numeric)

# get eligible independent variables found in X for the sake of experimenting with which one of them to drop to get information on the most pertinent industries
independent_variables <- colnames(X %>%
                                 dplyr::select(-city))

# COMMAND ----------

# create widgets

# pick which metric to use - downtown, metro (aka city), relative
dbutils.widgets.dropdown("metric", "downtown", as.list(c("downtown", "metro", "relative")))
dbutils.widgets.dropdown('period', '1', list('1', '2'))
# pick which dependent variable to use - the 4 periods, rate of recovery, omicron resilience
#dbutils.widgets.dropdown("dependent_variable", "period_4", as.list(y_labels))

# pick which of the independent variables to drop - they are percentages so they all add to 100 and will create a linear dependency if they are all present
# 
#dbutils.widgets.dropdown("drop_variable", "pct_jobs_other", as.list(independent_variables))

dbutils.widgets.dropdown("vif_threshold", "5", list("5", "10"))

# COMMAND ----------

# filter and load clusters by selected metric and period
if (dbutils.widgets.get('metric') == 'downtown') {
    features = c('total_pop_downtown', 'pct_singlefam_downtown', 'pct_multifam_downtown','pct_renter_downtown','median_age_downtown','bachelor_plus_downtown','median_hhinc_downtown','median_rent_downtown','pct_vacant_downtown','pct_commute_auto_city','pct_commute_public_transit_city','pct_commute_bicycle_city','pct_commute_walk_city','housing_units_downtown','average_commute_time_city','pct_jobs_agriculture_forestry_fishing_hunting','pct_jobs_mining_quarrying_oil_gas', 'pct_jobs_utilities','pct_jobs_construction', 'pct_jobs_manufacturing','pct_jobs_wholesale_trade', 'pct_jobs_retail_trade','pct_jobs_transport_warehouse', 'pct_jobs_information','pct_jobs_finance_insurance','pct_jobs_real_estate','pct_jobs_professional_science_techical','pct_jobs_management_of_companies_enterprises','pct_jobs_administrative_support_waste','pct_jobs_educational_services','pct_jobs_healthcare_social_assistance','pct_jobs_arts_entertainment_recreation','pct_jobs_accomodation_food_services','pct_jobs_public_administration', 'employment_entropy', 'population_density_downtown', 'employment_density_downtown', 'housing_density_downtown','days_school_closing', 'days_workplace_closing','days_cancel_large_events', 'days_cancel_all_events','days_stay_home_requirements', 'days_income_support','days_mask_mandates', 'winter_avg_temp', 'spring_avg_temp', 'summer_avg_temp', 'fall_avg_temp')
    
    if (dbutils.widgets.get('period') == '1'){
        y = get_table_as_r_df("rq_dwtn_clusters_0823_period_1") 
        #metric_clusters['description'] = metric_clusters['cluster'].map({'0':'>=1 sd above',
        #                                                                '1':'=1 sd below',
        #                                                                '2':'average, then decline',
        #                                                                '3': 'average, then uptick',
        #                                                               '4':'<1 sd above',
        #                                                                 '5': '>=1 sd above'})
      }
    else if (dbutils.widgets.get('period') == '2') {
        y = get_table_as_r_df("rq_dwtn_clusters_0823_period_2") 
        #metric_clusters['description'] = metric_clusters['cluster'].map({'0':'=1 sd below',
        #                                                                 '1':'>=1 sd above',
        #                                                                 '2':'<1 sd above',
        #                                                                 '3': '<1 sd below',
        #                                                                 '4':'<1 sd above',
        #                                                                 '5': '>=1 sd above'})
      }
}
else if (dbutils.widgets.get('metric') == 'metro'){
    
    features = c('total_pop_city', 'pct_singlefam_city', 'pct_multifam_city','pct_renter_city','median_age_city','bachelor_plus_city','median_hhinc_city','median_rent_city','pct_vacant_city','pct_commute_auto_city','pct_commute_public_transit_city','pct_commute_bicycle_city','pct_commute_walk_city','housing_units_city','average_commute_time_city','pct_jobs_agriculture_forestry_fishing_hunting','pct_jobs_mining_quarrying_oil_gas', 'pct_jobs_utilities','pct_jobs_construction', 'pct_jobs_manufacturing','pct_jobs_wholesale_trade', 'pct_jobs_retail_trade','pct_jobs_transport_warehouse', 'pct_jobs_information','pct_jobs_finance_insurance','pct_jobs_real_estate','pct_jobs_professional_science_techical','pct_jobs_management_of_companies_enterprises','pct_jobs_administrative_support_waste','pct_jobs_educational_services','pct_jobs_healthcare_social_assistance','pct_jobs_arts_entertainment_recreation','pct_jobs_accomodation_food_services','pct_jobs_public_administration', 'employment_entropy', 'population_density_city', 'employment_density_downtown', 'housing_density_city', 'days_school_closing', 'days_workplace_closing','days_cancel_large_events', 'days_cancel_all_events','days_stay_home_requirements', 'days_income_support','days_mask_mandates', 'winter_avg_temp', 'spring_avg_temp', 'summer_avg_temp', 'fall_avg_temp')
    
    }
    
else if (dbutils.widgets.get('metric') == 'relative'){
    
    features = c('total_pop_downtown', 'total_pop_city','pct_singlefam_downtown', 'pct_singlefam_city', 'pct_multifam_downtown','pct_multifam_city','pct_mobile_home_and_others_city', 'pct_renter_downtown','pct_renter_city', 'median_age_downtown', 'median_age_city','bachelor_plus_downtown', 'bachelor_plus_city', 'median_hhinc_downtown','median_hhinc_city', 'median_rent_downtown', 'median_rent_city','pct_vacant_downtown', 'pct_vacant_city', 'pct_commute_auto_downtown', 'pct_commute_auto_city','pct_commute_public_transit_downtown','pct_commute_public_transit_city', 'pct_commute_bicycle_downtown','pct_commute_bicycle_city', 'pct_commute_walk_downtown','pct_commute_walk_city','pct_commute_others_city', 'housing_units_downtown','housing_units_city', 'average_commute_time_downtown','average_commute_time_city','pct_jobs_agriculture_forestry_fishing_hunting','pct_jobs_mining_quarrying_oil_gas', 'pct_jobs_utilities','pct_jobs_construction', 'pct_jobs_manufacturing','pct_jobs_wholesale_trade', 'pct_jobs_retail_trade','pct_jobs_transport_warehouse', 'pct_jobs_information','pct_jobs_finance_insurance', 'pct_jobs_real_estate','pct_jobs_professional_science_techical','pct_jobs_management_of_companies_enterprises','pct_jobs_administrative_support_waste','pct_jobs_educational_services','pct_jobs_healthcare_social_assistance','pct_jobs_arts_entertainment_recreation','pct_jobs_accomodation_food_services','pct_jobs_public_administration', 'employment_entropy','population_density_downtown', 'population_density_city','employment_density_downtown', 'housing_density_downtown','housing_density_city', 'days_school_closing', 'days_workplace_closing','days_cancel_large_events', 'days_cancel_all_events','days_stay_home_requirements', 'days_income_support','days_mask_mandates', 'winter_avg_temp', 'spring_avg_temp', 'summer_avg_temp', 'fall_avg_temp')
  }

# COMMAND ----------

# create the subset of y to model - in this case, the only relevant columns are city, periods 1-4, omicron resilience, and rate of recovery (each of which is different according to the chosen metric)
y <- y %>%
  dplyr::mutate_if(is_all_numeric, as.numeric)
y$city <- str_replace(y$city, "é", "e")
# now join X and y to create df for modeling
model_df <- inner_join(X, y, by = "city")

# exclude the outlier cities from modeling
outlier_cities <- c("Dallas", "Mississauga", "Orlando", "Oklahoma City", "Hamilton")

model_df <- model_df %>%
  dplyr::filter(!(city %in% outlier_cities))

# COMMAND ----------

# select a baseline cluster- for period 2, this could be 0, 2, 3, 4
model_df$cluster <- relevel(as.factor(model_df$cluster), ref = "0")
test <- multinom(cluster ~ pct_jobs_professional_science_techical +
                 pct_commute_public_transit_city +
                 fall_avg_temp +
                 spring_avg_temp +
                 pct_jobs_public_administration +
                 pct_jobs_educational_services +
                 bachelor_plus_downtown +
                 pct_commute_auto_city +
                 employment_density_downtown, data = model_df)
summary(test)

# COMMAND ----------

z <- summary(test)$coefficients/summary(test)$standard.errors
z

# COMMAND ----------

p <- (1 - pnorm(abs(z), 0, 1)) * 2
p

# COMMAND ----------

exp(coef(test))

# COMMAND ----------

# look at potential multicollinearity
# basically this: http://www.sthda.com/english/wiki/ggplot2-quick-correlation-matrix-heatmap-r-software-and-data-visualization
# Get lower triangle of the correlation matrix
get_lower_tri<-function(cormat){
  cormat[upper.tri(cormat)] <- NA
  return(cormat)
}

# Get upper triangle of the correlation matrix
get_upper_tri <- function(cormat){
  cormat[lower.tri(cormat)]<- NA
  return(cormat)
}

# create pretty variables for display
pretty_variable_names <- data.frame(vars = named_factors, labels = names(named_factors), row.names = NULL)
pretty_variable_names$labels <- str_replace(pretty_variable_names$labels, "Percentage of Jobs in ", "")
pretty_variable_names <- setNames(as.character(pretty_variable_names$vars), as.character(pretty_variable_names$labels))
pretty_variable_names <- pretty_variable_names[str_detect(pretty_variable_names, "pct_jobs")]

# create correlation matrix
cormat <- round(cor(X %>%
  dplyr::select(all_of(independent_variables)) %>%
  dplyr::select(-dbutils.widgets.get("drop_variable")) %>%
  dplyr::rename(pretty_variable_names[pretty_variable_names != dbutils.widgets.get("drop_variable")])), 2)


upper_tri <- get_upper_tri(cormat)
melted_cormat <- reshape2::melt(upper_tri, na.rm = TRUE)

# Heatmap
ggplot(data = melted_cormat, aes(Var2, Var1, fill = value))+
 geom_tile(color = "white")+
 scale_fill_gradient2(low = "blue", high = "red", mid = "white", 
   midpoint = 0, limit = c(-1,1), space = "Lab", 
   name="Pearson\nCorrelation") +
  theme_minimal() + 
labs(title = "Correlation matrix for percentage of jobs in...") +
  theme(axis.text.x = element_text(angle = 45, vjust = 1, size = 12, hjust = 1),
       title = element_text(hjust = .5, size = 12),
       axis.title = element_blank()) +
  coord_fixed()
  


# COMMAND ----------

# to see the adjusted R2, F stat, p-value printed at the bottom
# create formula to solve the b vector in: y = b_0 + b_1 * x_1 + ... + b_n * x_n
model.formula <- as.formula(paste(dbutils.widgets.get("dependent_variable"), "~", paste(colnames(X %>% dplyr::select(-city, -dbutils.widgets.get("drop_variable"))), collapse="+", sep = ""), sep = " "))

# filter model_df according to the widget
model_df_subset <- model_df %>% dplyr::filter(metric == dbutils.widgets.get("metric"))

# model the data
model.ols <- lm(model.formula, model_df_subset)

print(summary(model.ols))

# COMMAND ----------

# create formula to solve the b vector in: y = b_0 + b_1 * x_1 + ... + b_n * x_n
predictor_variables <- colnames(X %>% dplyr::select(-city, -dbutils.widgets.get("drop_variable")))
model.formula <- as.formula(paste(dbutils.widgets.get("dependent_variable"), "~", paste(predictor_variables, collapse="+", sep = ""), sep = " "))

# filter model_df according to the widget
model_df_subset <- model_df %>% dplyr::filter(metric == dbutils.widgets.get("metric"))

# model the data
model.ols <- lm(model.formula, model_df_subset)

# check for multicollinearity- it is very likley multiple job variables will be highly correlated with each other, which can make the solution of the regression model unstable. 
VIFS <- c()
for (x_i in predictor_variables) {
  model.formula.i <- as.formula(paste(x_i, "~", paste(predictor_variables[predictor_variables != x_i], collapse="+", sep = ""), sep = " "))
  
  # model the data
  model.ols.i <- summary(lm(model.formula.i, model_df_subset))
  VIFS[x_i] <- 1 / (1 - model.ols.i$r.squared)
}

# print the results
model.results <- tidy(summary(model.ols))
model.results <- model.results %>%
                    left_join(data.frame(term = pretty_variable_names, label = names(pretty_variable_names)), by = "term") %>%
                    left_join(data.frame(term = names(VIFS), VIF = VIFS, row.names = NULL), by = "term") %>%
                    dplyr::mutate(estimate = round(estimate, 3),
                                  std.error = round(std.error, 3),
                                  statistic = round(statistic, 3),
                                  p.value = round(p.value, 3),
                                  VIF = round(VIF, 3),
                                  label = case_when(is.na(label) ~ term,
                                            !is.na(label) ~ label),
                                 excessive_VIF = VIF > as.numeric(dbutils.widgets.get("vif_threshold")),
                                 signif_codes = case_when(p.value < .001 ~ "***",
                                                          p.value < .01 ~ "**",
                                                          p.value < .05 ~ "*",
                                                          p.value < .1 ~ ".",
                                                          p.value < 1 ~ " ")) %>%
                    dplyr::select(label, estimate, std.error, statistic, p.value, signif_codes, VIF, excessive_VIF)
display(model.results)

# COMMAND ----------

# create formula to solve the b vector in: y = b_0 + b_1 * x_1 + ... + b_n * x_n
model.formula <- as.formula(paste(dbutils.widgets.get("dependent_variable"), "~", paste(colnames(X %>% dplyr::select(-city, -dbutils.widgets.get("drop_variable"))), collapse="+", sep = ""), sep = " "))

# filter model_df according to the widget
model_df_subset <- model_df %>% dplyr::filter(metric == dbutils.widgets.get("metric"))

# model the data
model.ols <- lm(model.formula, model_df_subset)
# diagnostic plots for OLS assumptions- are the residuals normally distributed? is there heteroskedasticity?
par(mfrow = c(2,2))
plot(model.ols)

# COMMAND ----------

# a look at the various metrics
y_eda <- y %>%
  tidyr::pivot_longer(!c("city", "metric"), names_to = "dependent_variable", values_to = "value") %>%
  tidyr::unite("metric_dependent_variable", metric, dependent_variable, remove = FALSE) %>%
  dplyr::mutate(alpha = case_when(metric_dependent_variable != paste(dbutils.widgets.get("metric"), dbutils.widgets.get("dependent_variable"), sep = "_") ~ .95,
                          metric_dependent_variable == paste(dbutils.widgets.get("metric"), dbutils.widgets.get("dependent_variable"), sep = "_") ~ 1))

ggplot(data = y_eda, aes(x = value, group = metric_dependent_variable, color = metric_dependent_variable, alpha = alpha)) +
  geom_line(stat = "density") +
  guides(alpha = FALSE) +
  labs(title = "Distribution of dependent variables by metric")

# COMMAND ----------

