source("load_data.R")
source("dtra_functions.R")



plot_cities <- c("Washington DC", "Salt Lake City, UT", "New York, NY","San Francisco, CA",
                 "El Paso, TX", "Los Angeles, CA", "San Diego, CA", "Portland, OR",
                 "Boston, MA", "Chicago, IL", "Vancouver, BC", "Toronto, ON")

outlier_cities <- c("Dallas", "Mississauga", "Orlando", "Oklahoma City")

plot_data <- recovery_patterns_df_long(11) %>%
  inner_join(regions_df %>% dplyr::select(city, display_title, region, color), by = "display_title")

plot_data$week <- as.Date(plot_data$week)

plot_data <- plot_data %>%
  arrange(week, region, display_title)

recovery_patterns_plot(na.omit(plot_data %>%
                         dplyr::filter((metric == "downtown") &
                                         (city %in% outlier_cities))), "downtown", 11)

plot_data %>% glimpse()

plot_data %>% group_by(city) %>% count()

summary(plot_data)


write.csv(plot_data, "../docs/all_weekly_metrics_plot_cuebiq_update.csv")
