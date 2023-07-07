source("load_data.R")
source("dtra_functions.R")






outlier_cities <- c("Dallas", "Orlando", "Oklahoma City")
setwd("E:\\git/downtownrecovery/shinyapp")
plot_data_new <- recovery_rankings_df_widget() %>%
  distinct() %>%
  arrange(region, display_title) %>%
  filter((Season == "Season_13") & !(city %in% outlier_cities))

plot_data_new %>% glimpse()

plot_data_new %>% 
  filter(metric == "downtown") %>%
  group_by(region) %>%
  count()

plot_data <- read.csv("~/git/downtown-recovery/static/ranking_data.csv")
plot_data_new %>% group_by(Season) %>% count()
plot_data_full <- rbind(plot_data, plot_data_new %>% select(-lq_rank))


write.csv(plot_data_full, "~/git/downtown-recovery/static/ranking_data.csv")

