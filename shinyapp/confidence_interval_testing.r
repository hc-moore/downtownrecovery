library(ggplot2)
library(gsignal)
library(readxl)
library(lmtest)
library(stringr)
library(forecast)
library(ggrepel)
library(arrow)
library(lubridate)
library(tidyverse)
library(broom)
library(dplyr)
library(scales)
library(plotly)
library(astsa)
library(zoo)

rm(list=ls())
gc()

# 2023-01: cuebiq data and color update
region_colors <- c("Canada" = "#DC4633",
                   "Midwest" = "#6FC7EA",
                   "Northeast" = "#8DBF2E",
                   "Pacific" = "#00A189",
                   "Southeast" = "#AB1368",
                   "Southwest" = "#F1C500")

# updated cuebiq query
weekly_rq <- read.csv("~/git/downtown-recovery/shinyapp/input_data/all_weekly_metrics_cuebiq_update_hll.csv") %>%
  select(-X) %>%
  mutate(week = as.Date(week),
         year = as.factor(year(week))) %>%
  arrange(week) %>%
  filter(week >= "2020-03-15") %>%
  filter(metric == "downtown")

weekly_rq %>% glimpse()


weekly_ntv <- read.csv("~/data/downtownrecovery/full_ntv.csv") %>%
  select(-X) %>%
  mutate(week = as.Date(date_range_start)) %>%
  arrange(week) %>%
  select(-date_range_start) %>%
  mutate(year = as.factor(year(week))) %>%
  filter(((week <= "2020-01-01") | (week >= "2020-03-15")) & (year != 2023))
  

weekly_ntv %>% glimpse()

# Custom function to return mean, sd, 95% conf interval
custom_stat_fun_2 <- function(x) {
  # x     = numeric vector
  # na.rm = boolean, whether or not to remove NA's
  
  m  <- mean(x, na.rm = TRUE)
  s  <- sd(x, na.rm = TRUE)
  hi <- m + 2*s
  lo <- m - 2*s
  
  ret <- c(mean = m, stdev = s, hi.95 = hi, lo.95 = lo) 
  return(ret)
}



weekly_ntv_rolling <- weekly_ntv %>%
  group_by(city) %>%
  arrange(week) %>%
  mutate(rolling_avg = rollmean(
    normalized_visits_by_total_visits,
    k = 11,
    na.pad = TRUE,
    align = "right"
  ),
  
  rolling_sd = rollapply(
    rolling_avg,
    width = 11,
    FUN = sd,
    na.pad = TRUE,
    align = "right"
  ),
  rolling_z = (rolling_avg - normalized_visits_by_total_visits) / (rolling_sd / sqrt(11))
  ) %>%
  filter(!is.na(rolling_avg) & !is.na(rolling_sd)) %>%
  mutate(hi = rolling_avg + 2*rolling_sd,
         lo = rolling_avg - 2*rolling_sd) %>%
  ungroup()

weekly_rq_rolling <- weekly_rq %>%
  group_by(city) %>%
  arrange(week) %>%
  mutate(rolling_avg = rollmean(
    normalized_visits_by_total_visits,
    k = 30,
    na.pad = TRUE,
    align = "right"
  ),
  rolling_sd = rollapply(
    rolling_avg,
    width = 30,
    FUN = sd,
    na.pad = TRUE,
    align = "right"
  ),
  rolling_z = (rolling_avg - normalized_visits_by_total_visits) / (rolling_sd / sqrt(30))
  ) %>%
  filter(!is.na(rolling_avg) & !is.na(rolling_sd)) %>%
  mutate(hi = rolling_avg + 2*rolling_sd,
         lo = rolling_avg - 2*rolling_sd) %>%
  ungroup()

weekly_rq_rolling %>% glimpse()

plot_cities <- sample(unique(weekly_rq_rolling$city), 6)

plot_patterns_df <- weekly_rq_rolling %>%
  filter(city %in% plot_cities)

starting_lqs <- plot_patterns_df %>%
  dplyr::filter(week == min(week)) %>%
  dplyr::select(city, region, week, rolling_avg) %>%
  dplyr::arrange(desc(rolling_avg))

ending_lqs <- plot_patterns_df %>%
  group_by(city) %>%
  mutate(latest_data = max(week)) %>%
  ungroup() %>%
  dplyr::filter(week == latest_data) %>%
  dplyr::select(city, region, week, rolling_avg) %>%
  dplyr::arrange(desc(rolling_avg))

total_cities <- length(unique(plot_patterns_df$city))
total_weeks <- length(unique(plot_patterns_df$week))


plot_patterns_df %>%
  ggplot(aes(x = week,y = rolling_avg, color = region, label = city, group = city)) +
  # Data
  geom_ribbon(aes(ymin = lo, ymax = hi), alpha = 0.4) +
  geom_line(size = 1, alpha = 0.9) +
  geom_label_repel(
    data = starting_lqs,
    size = 3,
    direction = "y",
    hjust = "right",
    force = 1,
    na.rm  = TRUE,
    min.segment.length = 0,
    segment.curvature = 1e-20,
    segment.angle = 20,
    # this was determined to be a decent offset based on the commented out line below
    # leaving it in as future reference 
    nudge_x = rep(-35, times = total_cities),
    show.legend = FALSE
    #nudge_x = rep(-total_weeks / as.numeric(input$rolling_window[1]), times = total_cities),
  ) +
  geom_label_repel(
    data = ending_lqs,
    size = 3,
    direction = "y",
    hjust = "left",
    force = 1,
    na.rm = TRUE,
    min.segment.length = 0,
    segment.curvature =  1e-20,
    segment.angle = 20,
    # this was determined to be a decent offset based on the commented out line below
    # leaving it in as future reference 
    nudge_x = rep(35, times = total_cities),
    show.legend = FALSE
    #nudge_x = rep(total_weeks / as.numeric(input$rolling_window[1]), times = total_cities),
  ) +
  # Aesthetics
  labs(title = "Downtown Recovery", x = "",
       subtitle = "30-Week Rolling Average with +/-2 Standard Deviations") +
  theme(
    axis.text.x = element_text(size = 10, angle = 45, vjust = 1, hjust = 1),
    axis.title = element_text(size = 10, hjust = .5),
    plot.title = element_text(size = 12, hjust = .5),
    plot.subtitle = element_text(size = 10, hjust = .5)
  ) +
  scale_x_date(
    breaks = "4 weeks",
    date_labels = "%Y.%m",
    expand = expansion(mult = .15)
  ) +
  scale_y_continuous("Metric", labels = scales::percent) +
  # 2023/01 update: changed to school of cities colors
  scale_color_manual(values = region_colors)

