source("load_data.R")
source("dtra_functions.R")







setwd("E:\\git/downtownrecovery/shinyapp")
plot_data <- recovery_rankings_df_widget() %>%
  inner_join(regions_df %>% dplyr::select(region, color), by = "region") %>%
  distinct() %>%
  arrange(region, display_title)

plot_data %>% glimpse()


write.csv(plot_data, "../docs/ranking_data_cuebiq_update.csv")

