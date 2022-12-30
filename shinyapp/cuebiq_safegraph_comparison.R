## includes code adapted from the following sources:
# https://github.com/rstudio/shiny-examples/blob/master/087-crandash/
# https://github.com/eparker12/nCoV_tracker
# https://rviews.rstudio.com/2019/10/09/building-interactive-world-maps-in-shiny/
# https://github.com/rstudio/shiny-examples/tree/master/063-superzip-example

# shinyapps.io compatible package loading
# cannot use anything other than explicitly listing library(package)
library(ggplot2)
library(readxl)
library(glue)
library(stringr)
library(crosstalk)
library(tidyverse)
library(lubridate)
library(broom)
library(dplyr)
library(scales)
library(plotly)
library(geojsonio)

git_path <- "E:\\git/downtownrecovery/shinyapp/"

raw_safegraph_data <- read.csv(paste0(git_path, "safegraph_dt_recovery.csv")) 
raw_safegraph_data %>% glimpse()

raw_safegraph_data <- raw_safegraph_data %>%
  dplyr::select(-X, -postal_code, -is_downtown) %>%
  rename(country_code = country) %>%
  mutate(
         city = str_replace(city, "é", "e"))

cuebiq_data <- read.csv("E:\\data/downtownrecovery/counts/cuebiq_daily_agg.csv") %>%
  mutate(source = "cuebiq",
         vdate = as.Date(as.character(vdate), format = "%Y%m%d"),
         date_range_start = lubridate::floor_date(vdate, unit = "week",
                                                  week_start = getOption("lubridate.week.start", 1))) %>%
  dplyr::select(-X) %>%
  rename(state = geography_name, raw_visit_counts = n_devices,
         state_visit_counts = userbase)
cuebiq_data %>% glimpse()

cuebiq_data %>%
  group_by(date_range_start) %>%
  count() %>%
  arrange(date_range_start)

all_weekly_metrics <- read.csv(paste0(git_path,"input_data/all_weekly_metrics.csv"))
all_weekly_metrics <- all_weekly_metrics %>%
                        dplyr::filter(metric == "downtown")

all_seasonal_metrics <- read.csv(paste0(git_path,"input_data/all_seasonal_metrics.csv"))

all_seasonal_metrics <- all_seasonal_metrics %>%
  dplyr::select(
    city,
    display_title,
    metric,
    starts_with("Season"),
    region
  ) %>%
  dplyr::filter(metric == "downtown")

all_city_coords <- read.csv(paste0(git_path,"input_data/all_city_coords.csv"))

raw_safegraph_data_agg <- raw_safegraph_data %>%
                            group_by(city, date_range_start, country_code, source) %>%
                            summarise(raw_visit_counts = sum(raw_visit_counts),
                                      normalized_visits_by_total_visits = sum(normalized_visits_by_total_visits),
                                      normalized_visits_by_state_scaling = sum(normalized_visits_by_state_scaling)
                            ) %>%
                            mutate(date_range_start = as.Date(date_range_start),
                                   ) %>%
                            left_join(cuebiq_data %>% select(city, state) %>% distinct())

raw_safegraph_data_agg %>% glimpse()

cuebiq_data_userbase <- cuebiq_data %>% 
                          dplyr::select(state_visit_counts, country_code:date_range_start) %>%
                          distinct() %>%
                          pivot_wider(id_cols = c('country_code', 'state', 'vdate', 'source', 'date_range_start'),
                                      names_from = 'provider_id', names_prefix = 'provider_', values_from = 'state_visit_counts') %>%
                          mutate(provider_190199 = replace_na(provider_190199, 0),
                                 provider_700199 = replace_na(provider_700199, 0),
                                 provider_ratio = provider_190199 / (provider_190199 + provider_700199))

cuebiq_data_userbase %>% glimpse()


cuebiq_data_agg <- cuebiq_data %>%
                    dplyr::select(raw_visit_counts, city:state, vdate:date_range_start) %>%
                    distinct() %>%
                    left_join(cuebiq_data_userbase) %>%
                    group_by(city, country_code, state, source, date_range_start) %>%
                    summarise(
                      raw_visit_counts_unscaled = sum(raw_visit_counts),
                      state_visit_counts = sum(provider_190199),
                      raw_visit_counts = sum(raw_visit_counts * provider_ratio),
                      normalized_visits_by_total_visits = sum(raw_visit_counts / state_visit_counts)
                              ) %>%
                    ungroup() %>%
                    filter(date_range_start >= min(raw_safegraph_data_agg$date_range_start)) %>%
                    mutate(country_code = case_when(country_code == "CA" ~ "CAN",
                                                    country_code == "US" ~ "USA"))
                    



cuebiq_data_agg %>%
  group_by(date_range_start) %>%
  count() %>%
  arrange(-n)

all_counts <- bind_rows(raw_safegraph_data_agg, cuebiq_data_agg) %>%
                filter(city != "Hamilton")

all_counts %>% glimpse()

summary(all_counts)

all_counts %>%
  arrange(-raw_visit_counts)



p <- ggplot(all_counts %>% filter(date_range_start >= "2021-05-17"),
            aes(x = date_range_start,
                y = normalized_visits_by_total_visits,
                color = source)) +
      geom_line() +
      facet_wrap(city~., nrow = 11, scales = 'free') +
      theme(axis.text = element_blank())

fig <- ggplotly(p)

fig




