source("load_data.R")
source("dtra_functions.R")

original_var_data <- read.csv('~/git/downtown-recovery/static/variables_data.csv')


explanatory_vars <- read.csv('~/data/downtownrecovery/curated_data/one_week_metrics_canada_usa_revised_cuebiq_update_hll_region_health_crime_april2023.csv')

new_var_data <- explanatory_vars %>% select(city, contains('density'))

original_var_data %>%
  select(-contains('density')) %>%
  left_join(new_var_data)

write.csv(plot_data, "../docs/model_data_full_cuebiq_update.csv")

plot_data %>%
  dplyr::select(-city) %>%
  pivot_wider(names_from = "metric", values_from = "seasonal_average") %>%
  inner_join(regions_df %>% dplyr::select(region, color), by = "region") %>%
  distinct() %>%
  arrange(region, display_title) 

write.csv(plot_data %>%
            dplyr::select(-city) %>%
            pivot_wider(names_from = "metric", values_from = "seasonal_average") %>%
            inner_join(regions_df %>% dplyr::select(region, color), by = "region") %>%
            distinct() %>%
            arrange(region, display_title), "../docs/model_data_metrics_cuebiq_update.csv")
