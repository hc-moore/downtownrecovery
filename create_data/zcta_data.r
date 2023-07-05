library(lubridate)
library(tidyverse)
library(dplyr)

explanatory_vars_2023 <- read.csv('~/git/downtownrecovery/shinyapp/input_data/all_model_features_1015_weather.csv')

fixed_us_density <- read.csv('~/data/downtownrecovery/us_city_index_20230702.csv')

fixed_us_density %>% glimpse()

with_fixed_density <- explanatory_vars_2023 %>% left_join(fixed_us_density %>%
                                      select(city, land_area_downtown_m2:housing_density_city_km2), by = 'city')

with_fixed_density %>%
  select(city, contains("density")) %>%
  filter(!is.na(housing_density_downtown_m2)) %>%
  select(city,
         contains("population_density_downtown"),
         contains("population_density_city"),
         contains("housing_density_downtown"),
         contains("housing_density_city"),
         contains("employment_density_downtown"))

fixed_density_df <- with_fixed_density %>%
  select(city, contains("density")) %>%
  filter(!is.na(housing_density_downtown_m2)) %>%
  select(city,
         contains("population_density_downtown"),
         contains("population_density_city"),
         contains("housing_density_downtown"),
         contains("housing_density_city"),
         contains("employment_density_downtown"))

fixed_density_df %>%
  filter(population_density_downtown != population_density_downtown_m2)


identical(fixed_density_df$population_density_downtown, fixed_density_df$population_density_downtown_m2)
identical(fixed_density_df$housing_density_downtown, fixed_density_df$housing_density_downtown_m2)
identical(fixed_density_df$housing_density_downtown, fixed_density_df$housing_density_downtown_m2)

write.csv(fixed_density_df, "~/data/downtownrecovery/census_data/fixed_us_density.csv")

