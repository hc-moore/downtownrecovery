# Databricks notebook source
# Package names
packages <- c("broom", "ggplot2", "readxl", "dplyr", "tidyr",  "MASS", "scales", "SparkR", "tidyverse")

# Packages loading
invisible(lapply(packages, library, character.only = TRUE))

# first: examine overlap weeks
weekly_counts_cuebiq <- read.csv("cuebiq_daily_agg.csv")

# TODO: make sure the vdate column is converted to some usable datetime type of column 
# and name this column 'week'


# if ANY row is != 1, that is a problem
weekly_counts_cuebiq %>%
        dplyr::distinct() %>%
       dplyr::group_by(city, week) %>%
       dplyr::count() %>%
       dplyr::filter(n != 1)

# COMMAND ----------

# Montréal and Québec MUST be processed as Montreal and Quebec
# they can be changed back to the correct letters in the last step before visualization
# but do not save or write any data with é - it corrupts too easily :(
weekly_counts_cuebiq$city <- str_replace(weekly_counts_cuebiq$city, "é", "e")

# has a regions column for colors
regions_df <- read.csv("regions.csv")
regions_df <- regions_df %>%
  dplyr::select(city, region, metro_size, display_title)
regions_df$city <- str_replace(regions_df$city, "é", "e")

# define seasons, then pivot to wide format by season, take average of normalized_visits_by_total_visits
# normalized_visits_by_total_visits will be used to calculate recovery metrics
# this column was provided by safegraph, but for cuebiq we have to create it
# 

all_weekly_metrics[(all_weekly_metrics$week >= base::as.Date("2020-03-02")) & (all_weekly_metrics$week < base::as.Date("2020-06-01")), "Season"] = "Season_1" 
all_weekly_metrics[(all_weekly_metrics$week >= base::as.Date("2020-06-01")) & (all_weekly_metrics$week < base::as.Date("2020-08-31")), "Season"] = "Season_2"
all_weekly_metrics[(all_weekly_metrics$week >= base::as.Date("2020-08-31")) & (all_weekly_metrics$week < base::as.Date("2020-11-30")), "Season"] = "Season_3"
all_weekly_metrics[(all_weekly_metrics$week >= base::as.Date("2020-11-30")) & (all_weekly_metrics$week < base::as.Date("2021-03-01")), "Season"] = "Season_4"
all_weekly_metrics[(all_weekly_metrics$week >= base::as.Date("2021-03-01")) & (all_weekly_metrics$week < base::as.Date("2021-05-31")), "Season"] = "Season_5"
all_weekly_metrics[(all_weekly_metrics$week >= base::as.Date("2021-05-31")) & (all_weekly_metrics$week < base::as.Date("2021-08-30")), "Season"] = "Season_6"
all_weekly_metrics[(all_weekly_metrics$week >= base::as.Date("2021-08-30")) & (all_weekly_metrics$week < base::as.Date("2021-12-06")), "Season"] = "Season_7"
# these are edited to be consistent with policy brief but they do not quite fully represent the months in season 8 and 9- consider changing these for the paper
all_weekly_metrics[(all_weekly_metrics$week >= base::as.Date("2021-12-06")) & (all_weekly_metrics$week < base::as.Date("2022-03-07")), "Season"] = "Season_8"
all_weekly_metrics[(all_weekly_metrics$week >= base::as.Date("2022-03-07")) & (all_weekly_metrics$week < base::as.Date("2022-06-13")), "Season"] = "Season_9"
# TODO: add 3 month averages for seasons 10 and 11

all_seasonal_metrics <- all_weekly_metrics %>%
  dplyr::filter(!is.na(Season)) %>% 
  dplyr::select(city, Season, normalized_visits_by_total_visits) %>% # select only the columns of relevance
  dplyr::distinct() %>% 
  dplyr::filter(!is.na(normalized_visits_by_total_visits)) %>% # the 53rd week of 2020 is NA - drop this so Season_4 doesn't get corrupted
  dplyr::group_by(city, Season) %>% # group by what you want to count as a unique observation
  dplyr::summarise(seasonal_average = mean(normalized_visits_by_total_visits)) %>% # now average the weekly metrics over the season
  dplyr::ungroup() %>% # ungroup because R 
  dplyr::select(city, Season, seasonal_average) %>% # select relevant columns
  left_join(regions_df, by = "city") # left join to regions_df to get geographic / display cols

# COMMAND ----------

# save this as all_seasonal_metrics.csv to push to website. will be input to rankings tab and y vals of explanatory vars tab
display(all_seasonal_metrics)

# COMMAND ----------

colnames(all_weekly_metrics)

# COMMAND ----------

# COMMAND ----------

all_weekly_metrics_disp <- all_weekly_metrics %>%
  dplyr::filter(!is.na(Season)) %>%
  dplyr::select(-Season) %>% # omit columns that have no use in weekly metrics
  dplyr::distinct() %>%
  dplyr::filter(!is.na(normalized_visits_by_total_visits)) %>% # drop the lone NA week - the 53rd week of 2020
  dplyr::ungroup() %>% # because R
  left_join(regions_df, by = "city") # geographic and display options

# COMMAND ----------
glimpse(all_weekly_metrics_disp)

# COMMAND ----------
glimpse(all_weekly_metrics_disp %>%
       dplyr::group_by(week, metric) %>%
       dplyr::count())