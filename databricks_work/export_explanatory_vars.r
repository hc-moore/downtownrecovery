# Databricks notebook source
# Package names
packages <- c("broom", "ggplot2", "ggrepel", "readxl", "dplyr", "tidyr",  "MASS", "scales", "SparkR", "tidyverse", "zoo")

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

oxford_vars <- get_table_as_r_df("new_oxford_vars_csv")
us_can_census <- get_table_as_r_df("us_can_census_variables_0626")
political_leanings <- get_table_as_r_df("political_leaning_csv")
colnames(political_leanings) <- c("city", "state", "pct_liberal_leaning", "pct_conservative_leaning", "pct_other_leaning")
political_leanings[,c("pct_liberal_leaning", "pct_conservative_leaning", "pct_other_leaning")] <- 100 * political_leanings %>% dplyr::select(starts_with("pct_"))
regions_df <- get_table_as_r_df("regions_csv")
regions_df$city <- str_replace(regions_df$city, "é", "e")

# COMMAND ----------

# ok
display(oxford_vars)

# COMMAND ----------

display(political_leanings)

# COMMAND ----------

# employment variables, like 'pct_jobs ...' and employment_entropy are downtown only
# 2022/07/15: this will need to be modified a bit / may be redundant when we confirm which of the census variables at city AND downtown levels to use. 
# Ideally, limit the number of repeats for user simplicity's sake
downtown_attributes <- us_can_census %>%
  dplyr::select(-ends_with("_city")) %>%
  dplyr::rename_with(~str_remove(., "_downtown")) %>%
  dplyr::mutate(geography = "downtown")

city_attributes <- us_can_census %>%
  dplyr::select(-ends_with("_downtown"), -employment_entropy, -starts_with("pct_jobs")) %>%
  dplyr::rename_with(~str_remove(., "_city")) %>%
  dplyr::mutate(geography = "city")

census_vars <- dplyr::bind_rows(downtown_attributes, city_attributes)

display(census_vars)

# COMMAND ----------

# left join to oxford vars
explanatory_vars <- oxford_vars %>%
  left_join(us_can_census, by = "city") %>%
  left_join(political_leanings %>% dplyr::select(-state), by = "city")
explanatory_vars$city <- str_replace(explanatory_vars$city, "é", "e")
explanatory_vars <- explanatory_vars %>%
left_join(regions_df %>% dplyr::select(city, region, metro_size, display_title), by = "city")

# COMMAND ----------

# save as explanatory_vars.csv for input to shinyapp
display(explanatory_vars)

# COMMAND ----------

# save this to databricks so a single consolidated notebook can visualize all updated data
create_and_save_as_table(explanatory_vars, "shinyapp_export_explanatory_vars")

# COMMAND ----------

# if the results are acceptable, save as explanatory_vars.csv for shiny app to read
# this is the X design matrix
# to be joined with metrics_df (the y matrix) to model y = BX
# political leaning and covid response variables will be duplicated twice, since the vast majority of census variables have a downtown and city version
# so be careful to filter for specific X variables when modeling
# the season column is only relevant for covid response variables
display(explanatory_vars)

# COMMAND ----------

