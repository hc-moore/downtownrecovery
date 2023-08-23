library(ggplot2)
library(readxl)
library(stringr)

library(zoo)
library(tidyverse)
library(broom)
library(dplyr)
library(scales)
library(sf)
library(sp)
library(spdep)
library(geojsonio)

existing_data <- read.csv("~/data/downtownrecovery/curated_data/one_week_metrics_canada_usa_revised_cuebiq_update_hll_region_health_crime_april2023.csv") %>%
                  select(-week, -normalized_visits_by_total_visits)

us_update <- read.csv("~/data/downtownrecovery/curated_data/all_model_features_20230714.csv") %>% select(-X, -X.1)


all_data <- existing_data %>% select(-pct_hisp_city, pct_hisp_downtown) %>%
              left_join(us_update %>% select(city, pct_hisp_city, pct_hisp_downtown))

write.csv(all_data, "~/data/downtownrecovery/curated_data/variables_data_20230717.csv")

oxford_vars <- read.csv("input_data/oxford_explanatory_vars.csv")

all_attributes <- read.csv("input_data/us_can_census_variables_0626.csv")

voter_results <- read.csv("input_data/political_leaning.csv")

oxford_vars$city <- str_replace(oxford_vars$city, "é", "e")
all_attributes$city <- str_replace(all_attributes$city, "é", "e")
voter_results$city <- str_replace(voter_results$city, "é", "e")



explanatory_vars <- oxford_vars %>% inner_join(all_attributes, by = "city") %>% inner_join(voter_results %>% select(-state), by = "city")

write.csv(explanatory_vars, "input_data/explanatory_vars.csv")
