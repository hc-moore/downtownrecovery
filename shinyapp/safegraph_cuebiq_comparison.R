library(ggplot2)
library(readxl)
library(stringr)
library(ggrepel)
library(ggpmisc)
library(lubridate)
library(arrow)
library(tidyverse)
library(broom)
library(dplyr)
library(htmlwidgets)
library(scales)
library(sf)
library(sp)
library(spdep)
library(plotly)
library(geojsonio)

input_path <- "C:\\Users/hannah/git/downtownrecovery/shinyapp/"

cuebiq_data <- read.csv(paste0(input_path, "input_data/cuebiq_daily_agg.csv")) %>%
  filter(vdate >= 20210524) %>%
  mutate(country_code = case_when(country_code == "US" ~ "USA",
                                  TRUE ~ "CAN"),
         as_datetime = as.Date(as.character(vdate), format = "%Y%m%d"),
         source = "cuebiq"
  ) %>%
  rename(raw_visit_counts = n_devices,
         total_visits = userbase,
         state = geography_name) %>%
  select(-X) %>%
  pivot_wider(names_from = 'provider_id', values_from = 'total_visits', names_prefix = 'provider_')

safegraph_data <- read_parquet(paste0(input_path, "input_data/safegraph_dt_recovery.pq")) %>%
  mutate(city = str_replace(city, "é", "e")) %>%
  select(-country, -postal_code, -is_downtown) %>%
  inner_join(cuebiq_data %>% select(city, country_code, state) %>% distinct()) %>%
  group_by(date_range_start, city, country_code, state) %>%
  summarise(raw_visit_counts = sum(raw_visit_counts),
            normalized_visits_by_total_visits = sum(normalized_visits_by_total_visits)) %>%
  mutate(source = "safegraph")
  
  

safegraph_data %>%
  group_by(country_code) %>%
  count()

safegraph_data %>% glimpse()

cuebiq_data %>% glimpse()

cuebiq_data_agg <- cuebiq_data %>%
  group_by(as_datetime, state, city, country_code, source) %>%
  summarise(provider_190199_ratio = provider_190199 / sum(provider_190199, provider_700199),
            raw_visit_counts = raw_visit_counts,
            total_visits = provider_190199,
            date_range_start = lubridate::floor_date(as_datetime,
                                                     unit = "week",
                                                     week_start = getOption("lubridate.week.start", 1))
            ) %>%
  ungroup() %>%
  # the date starting the first full week for at which point the provider_id in CAN switched
  
  group_by(date_range_start, city, state, country_code, source) %>%
  summarise(
    normalized_visits_by_total_visits =  sum(raw_visit_counts*provider_190199_ratio / total_visits),
    raw_visit_counts = sum(raw_visit_counts*provider_190199_ratio),
            total_visits = sum(total_visits)
           
            
            )





cuebiq_data_agg %>% glimpse()

safegraph_data %>% glimpse()

all_counts <- bind_rows(cuebiq_data_agg, safegraph_data)

all_counts %>% glimpse()

#' raw_visit_counts close to safegraph over the selected range
#' normalized_visits_by_total_visits is NOT
#' safegraph has a much higher device count than cuebiq, so the statewide denominator from cuebiq will not properly
#' normalized to match safegraph's normalized value
#' the downtowns are already a large % of a state's population, so an equally large numerator / smaller than expected denominator == larger normalized_by_total_visits
#' 'solve for' what the denominator should be
#' the visit to each POI was scaled by total visits in the week 
#' raw_visit_counts / all_visits_in_state_by_week = normalized_visits_by_total_visits
#' therefore... all_state_visits = raw_visit_counts/ normalized_visits_by_total_visits


p <- all_counts %>%
  group_by(date_range_start, source, state) %>%
  summarise(state_visit_est = mean(raw_visit_counts / normalized_visits_by_total_visits)) %>%
  ggplot(aes(x = date_range_start, y = state_visit_est, color = source)) +
  geom_line() +
  facet_wrap(.~state, scales = "free") +
  theme(axis.text = element_blank())

ggplotly(p)

# on average, how much greater is safegraph than cuebiq?
# generally, it's about 2x
p2 <- all_counts %>%
  filter(date_range_start >= "2021-05-24") %>%
  ungroup() %>%
  select(date_range_start, city, state, source:normalized_visits_by_total_visits) %>%
  pivot_wider(id_cols = c('date_range_start', 'city', 'state'), names_from = 'source', values_from = 'normalized_visits_by_total_visits') %>%
  group_by(date_range_start, state) %>%
  summarise(safegraph_cuebiq_ratio = mean(safegraph / cuebiq)) %>%
  filter(!is.na(safegraph_cuebiq_ratio)) %>%
  ggplot(aes(x = date_range_start, y = safegraph_cuebiq_ratio, color = state)) +
  geom_line() +
  theme(axis.text = element_blank())

ggplotly(p2)
