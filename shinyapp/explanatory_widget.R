source("load_data.R")
source("dtra_functions.R")

plot_x_vars <- named_factors
plot_data <- create_model_df(unname(plot_x_vars))

plot_data %>% glimpse()


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
