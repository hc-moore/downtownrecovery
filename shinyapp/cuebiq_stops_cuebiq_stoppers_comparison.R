library(ggplot2)
library(readxl)
library(stringr)
library(lubridate)
library(arrow)
library(tidyverse)
library(broom)
library(dplyr)
library(scales)
library(plotly)



# orignal cuebiq query reading in cuebiq device counts by downtowns & by statewide visits
cuebiq_data <- read.csv("~/data/downtownrecovery/counts/cuebiq_daily_agg_20230104.csv") %>%
  mutate(
    as_datetime = as.Date(as.character(vdate), format = "%Y%m%d"),
    source = "stops_uplevelled",
    userbase = as.numeric(userbase),
    provider_id = as.character(provider_id))%>%
  select(-X)

cuebiq_data %>% glimpse()

cuebiq_data %>%
  filter((provider_id == "190199") & (as_datetime >= "2022-06-01")) %>%
  distinct(as_datetime, geography_name, .keep_all = TRUE) %>%
  group_by(as_datetime) %>%
  summarise(total_users = sum(userbase)) %>%
  glimpse()

15925683 / 67109259

15943200 / 68329624

16007060 / 72885584

cuebiq_data_stoppers_20230413 <- read.csv("~/data/downtownrecovery/update_2023/stoppers_query_20230413.csv") %>%
  mutate(
    event_date = str_extract(event_date, "\\d+$"),
    as_datetime = as.Date(as.character(event_date), format = "%Y%m%d"),
    source = "cuebiq_20230413",
    userbase = as.numeric(userbase))%>%
  select(-X) %>%
  left_join(cuebiq_data %>% select(city, geography_name, country_code) %>% distinct()) %>%
  distinct()

summary(cuebiq_data_stoppers_20230413)
cuebiq_data_stoppers_20230413 %>% glimpse()


# read in safegraph raw device counts
safegraph_data <- read_parquet("~/data/downtownrecovery/counts/safegraph_dt_recovery.pq") %>%
  mutate(city = str_replace(city, "é", "e")) %>%
  select(-country, -postal_code, -is_downtown) %>%
  inner_join(cuebiq_data %>% select(city, country_code, geography_name) %>% distinct()) %>%
  group_by(date_range_start, city, country_code, geography_name) %>%
  summarise(raw_visit_counts = sum(raw_visit_counts),
            normalized_visits_by_total_visits = sum(normalized_visits_by_total_visits)) %>%
  mutate(source = "safegraph")

safegraph_data %>% glimpse()

cuebiq_data_agg <- cuebiq_data %>%
  # turn longform userbase column into wide with 2 columns for each provider
  pivot_wider(names_from = 'provider_id', values_from = 'userbase', names_prefix = 'provider_') %>%
  # day, state, city, country, and source are the columns to index by
  group_by(as_datetime, geography_name, city, country_code, source) %>%
  #' the provider information was not collected at the downtown level, but was statewide.
  #' this ratio is computed under the assumption that the volume of devices by provider should be
  #' evenly distributed across the state
  summarise(provider_190199_ratio = provider_190199 / sum(provider_190199, provider_700199),
            raw_visit_counts = n_devices,
            total_visits = provider_190199,
            date_range_start = lubridate::floor_date(as_datetime,
                                                     unit = "week",
                                                     week_start = getOption("lubridate.week.start", 1))
  ) %>%
  ungroup() %>%
  group_by(as_datetime,  geography_name,city, country_code, source) %>%
  summarise(
    normalized_visits_by_total_visits = (raw_visit_counts*provider_190199_ratio) / (total_visits),
    raw_visit_counts = (raw_visit_counts*provider_190199_ratio),
    total_visits = (total_visits)
  ) %>%
  mutate(table = "stops_uplevelled") %>%
  ungroup() 

cuebiq_data_agg %>% glimpse()

# stoppers just has one provider by design
cuebiq_stoppers_agg <- cuebiq_data_stoppers_20230413 %>%
  # day, state, city, country, and source are the columns to index by
  group_by(as_datetime, geography_name, city, source) %>%
  #' the provider information was not collected at the downtown level, but was statewide.
  #' this ratio is computed under the assumption that the volume of devices by provider should be
  #' evenly distributed across the state
  summarise(raw_visit_counts = n_devices,
            total_visits = userbase,
            date_range_start = lubridate::floor_date(as_datetime,
                                                     unit = "week",
                                                     week_start = getOption("lubridate.week.start", 1))
  ) %>%
  ungroup() %>%
  # the date starting the first full week for at which point the provider_id in CAN switched
  
  group_by(date_range_start, geography_name, city, source) %>%
  summarise(
    normalized_visits_by_total_visits = sum(raw_visit_counts) / sum(total_visits),
    raw_visit_counts = sum(raw_visit_counts),
    total_visits = sum(total_visits)
  ) %>%
  mutate(table = "stoppers_hll_by_geohash") %>%
  ungroup()

cuebiq_data_agg %>% glimpse()

safegraph_data %>% glimpse()

sg_cuebiq <- bind_rows(cuebiq_stoppers_agg,
                         safegraph_data) %>%
             group_by(city, source) %>%
             mutate(normalized_minmax_scaled = (normalized_visits_by_total_visits - min(normalized_visits_by_total_visits))/ 
                                                  (max(normalized_visits_by_total_visits) - min(normalized_visits_by_total_visits))
             ) %>%
  ungroup()

sg_cuebiq %>% glimpse()

#' raw_visit_counts close to safegraph over the selected range
#' normalized_visits_by_total_visits is NOT
#' safegraph has a much higher device count than cuebiq, so the statewide denominator from cuebiq will not properly
#' normalized to match safegraph's normalized value
#' the downtowns are already a large % of a state's population, so an equally large numerator / smaller than expected denominator == larger normalized_by_total_visits
#' 'solve for' what the denominator should be
#' the visit to each POI was scaled by total visits in the week 
#' raw_visit_counts / all_visits_in_state_by_week = normalized_visits_by_total_visits
#' therefore... all_state_visits = raw_visit_counts/ normalized_visits_by_total_visits

# weekly downtown activity normalized by weekly statewide activity

p <- sg_cuebiq %>%
  filter((date_range_start >= "2021-05-21") &
           # (city %in% c("Portland", "Cleveland", "Edmonton", "Toronto",
           #              "Milwaukee", "Los Angeles", "St Louis", "San Jose",
           #              "Fresno", "Minneapolis", "Tucson", "Tampa", "Raleigh",
           #              "Louisville")) &
           (date_range_start <= "2022-06-01")) %>%
  distinct(date_range_start, source, city, .keep_all = TRUE) %>%
  ggplot(aes(x = date_range_start, y = normalized_minmax_scaled, color = source)) +
  geom_line() +
  facet_wrap(~city, ncol = 6, scales = 'free') +
  theme(axis.text = element_blank())



ggplotly(p)

comparison_window <- sg_cuebiq %>%
  filter((date_range_start >= "2021-05-21") &
         (date_range_start <= "2022-05-02")
  )

comparison_df <- comparison_window %>%
  pivot_longer(cols = c("normalized_visits_by_total_visits")) %>%
  pivot_wider(id_cols = c("city", "date_range_start", "name"), names_from = "source", values_from = "value") %>%
  mutate(value_change = cuebiq_20230413 / safegraph) %>%
  arrange(date_range_start, city) %>%
  ungroup()

comparison_df %>% glimpse()

last_safegraph_date <- sg_cuebiq %>%
  filter((source == 'safegraph') & !is.na(normalized_visits_by_total_visits)) %>%
  group_by(city) %>%
  summarise(last_valid_date = max(date_range_start)) %>%
  pull(last_valid_date) %>%
  unique() %>%
  min()

last_safegraph_date



# normalized_visits_by_total_visits with <= 2022-06-06 as safegraph; > 2022-06-06 as cuebiq
p2 <- sg_cuebiq %>%
  filter((date_range_start < "2022-12-05") & (date_range_start >= "2019-01-01")) %>%
  pivot_wider(id_cols = c('date_range_start', 'city'), names_from = 'source', values_from = 'normalized_visits_by_total_visits') %>%
  mutate(value = case_when(date_range_start <= last_safegraph_date ~ safegraph,
                           TRUE ~ cuebiq_20230413)
  ) %>%
  left_join(comparison_df %>%
              # give this the entirety of season 9 to 'adjust' to safegraph counts
              filter((date_range_start >= "2022-01-01") & (date_range_start <= last_safegraph_date)) %>%
              group_by(city) %>%
              summarise(
                #' on average, what would cuebiq's normalized visits have to be multiplied by to
                #' get safegraph's normalized visits for the last 3 months? 
                ratio = mean(safegraph / cuebiq_20230413)) %>% 
              filter(!is.na(ratio) ) %>%
              distinct()) %>%
  group_by(date_range_start, city) %>%
  mutate(value = case_when(date_range_start > last_safegraph_date ~ ratio * value,
                           TRUE ~ value)) %>%
  ggplot(aes(x = date_range_start, y = value)) +
  geom_line() +
  facet_wrap(~city, ncol = 6, scales = 'free') +
  theme(axis.text = element_blank())

ggplotly(p2)

cuebiq_data_agg %>% glimpse()

normalized_cuebiq <- cuebiq_data_agg %>%
  rename(date_range_start = as_datetime) %>%
  filter(date_range_start > last_safegraph_date) %>%
  left_join(comparison_df %>%
              # give this  ___ to 'adata:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABIAAAASCAYAAABWzo5XAAAAWElEQVR42mNgGPTAxsZmJsVqQApgmGw1yApwKcQiT7phRBuCzzCSDSHGMKINIeDNmWQlA2IigKJwIssQkHdINgxfmBBtGDEBS3KCxBc7pMQgMYE5c/AXPwAwSX4lV3pTWwAAAABJRU5ErkJggg==djust' to safegraph counts
              filter((date_range_start >= "2022-01-01") & (date_range_start <= last_safegraph_date)) %>%
              group_by(city) %>%
              summarise(
                #' on average, what would cuebiq's normalized visits have to be multiplied by to
                #' get safegraph's normalized visits for the last ___? 
                ratio = mean(safegraph / cuebiq_20230413)) %>% 
              filter(!is.na(ratio) ) %>%
              distinct()) %>%
  group_by(date_range_start, city) %>%
  summarise(normalized_visits_by_total_visits = ratio * normalized_visits_by_total_visits) %>%
  ungroup()

normalized_cuebiq %>% glimpse()

ggplotly(normalized_cuebiq %>% 
           ggplot(aes(x = date_range_start, y = normalized_visits_by_total_visits)) +
           geom_line() +
           facet_wrap(.~city, nrow = 6, scales = 'free'))


# TODO: append new downtown rqs to old and find 11, 13, 15, etc week rolling average to be 
# recovery patterns plot

# TODO: append new seasonal rqs to old

# TODO: update the csvs used in javascripts to create plotly plots 

# TODO: write up methodology changes since addition of cuebiq data


downtown_rq_cuebiq <- rbind(safegraph_data %>%
                              ungroup() %>%
                              filter(date_range_start <= "2020-01-01") %>%
                              select(date_range_start, city, normalized_visits_by_total_visits),
                            normalized_cuebiq) %>%
  mutate(week_num = lubridate::isoweek(date_range_start),
         year = lubridate::year(date_range_start)) %>%
  select(-date_range_start) %>%
  pivot_wider(id_cols = c('city', 'week_num'), names_from = 'year', names_prefix = 'ntv_', values_from = 'normalized_visits_by_total_visits') %>%
  # ignore 
  filter(!is.na(ntv_2022)) %>%
  mutate(rec_2022 = ntv_2022 / ntv_2019) %>%
  pivot_longer(cols = 'rec_2022', names_to = "year", values_to = "normalized_visits_by_total_visits") %>%
  mutate(year = substr(year, 5, 9),
         week = as.Date(paste(year, week_num, 1, sep = "_"), format = "%Y_%W_%w"),
         metric = "downtown")

downtown_rq_cuebiq %>% glimpse()

downtown_rq_safegraph <- read.csv("git/downtownrecovery/shinyapp/input_data/all_weekly_metrics.csv") %>%
  filter(city != "Hamilton")   


downtown_rq_safegraph %>% glimpse()

downtown_rq <- rbind(downtown_rq_cuebiq %>%
                       left_join(downtown_rq_safegraph %>%
                                   select(city, region, metro_size, display_title) %>% distinct()) %>%
                       select(-ntv_2019, -ntv_2022, -week_num, -year),
                     downtown_rq_safegraph)


ggplotly(downtown_rq %>%
           filter((week >= "2020-03-16") & (metric == "downtown")) %>%
           ggplot(aes(x = week, y = normalized_visits_by_total_visits)) +
           geom_line() +
           facet_wrap(.~city, nrow = 6))

# this will become all_weekly_metrics.csv, minus the Season column

write.csv(downtown_rq, "git/downtownrecovery/shinyapp/input_data/all_weekly_metrics_cuebiq_update.csv")



downtown_rq[(downtown_rq$week >= base::as.Date("2020-03-02")) & (downtown_rq$week < base::as.Date("2020-06-01")), "Season"] = "Season_1" 
downtown_rq[(downtown_rq$week >= base::as.Date("2020-06-01")) & (downtown_rq$week < base::as.Date("2020-08-31")), "Season"] = "Season_2"
downtown_rq[(downtown_rq$week >= base::as.Date("2020-08-31")) & (downtown_rq$week < base::as.Date("2020-11-30")), "Season"] = "Season_3"
downtown_rq[(downtown_rq$week >= base::as.Date("2020-11-30")) & (downtown_rq$week < base::as.Date("2021-03-01")), "Season"] = "Season_4"
downtown_rq[(downtown_rq$week >= base::as.Date("2021-03-01")) & (downtown_rq$week < base::as.Date("2021-05-31")), "Season"] = "Season_5"
downtown_rq[(downtown_rq$week >= base::as.Date("2021-05-31")) & (downtown_rq$week < base::as.Date("2021-08-30")), "Season"] = "Season_6"
downtown_rq[(downtown_rq$week >= base::as.Date("2021-08-30")) & (downtown_rq$week < base::as.Date("2021-12-06")), "Season"] = "Season_7"
# these are edited to be consistent with policy brief but they do not quite fully represent the months in season 8 and 9- consider changing these for the paper
downtown_rq[(downtown_rq$week >= base::as.Date("2021-12-06")) & (downtown_rq$week < base::as.Date("2022-03-07")), "Season"] = "Season_8"
downtown_rq[(downtown_rq$week >= base::as.Date("2022-03-07")) & (downtown_rq$week < base::as.Date("2022-06-13")), "Season"] = "Season_9"
downtown_rq[(downtown_rq$week >= base::as.Date("2022-06-13")) & (downtown_rq$week < base::as.Date("2022-08-29")), "Season"] = "Season_10"
downtown_rq[(downtown_rq$week >= base::as.Date("2022-08-29")) & (downtown_rq$week < base::as.Date("2022-12-05")), "Season"] = "Season_11"



ranking_df_safegraph <- read.csv("git/downtownrecovery/shinyapp/input_data/all_seasonal_metrics.csv") %>%
  filter(city != "Hamilton")

ranking_df_safegraph %>% glimpse()

seasonal_rq <- downtown_rq %>%
  filter(Season %in% c("Season_10", "Season_11")) %>%
  group_by(city, Season) %>%
  mutate(seasonal_average = mean(normalized_visits_by_total_visits, na.rm = TRUE)) %>%
  select(-week, -normalized_visits_by_total_visits) %>%
  distinct()

seasonal_rq %>% glimpse()

cuebiq_denom <- read.csv("data/downtownrecovery/seasonal_rq_US_cuebiq.csv") %>%
  mutate(source = "cuebiq") %>%
  select(-Column1) %>%
  pivot_longer(cols = Season_1:Season_9, names_to = 'Season', values_to = 'seasonal_average')%>%
  group_by(Season, source) %>%
  dplyr::arrange(-seasonal_average) %>%
  mutate(lq_rank = rank(-seasonal_average,
                        ties.method = "first"),
         Season = case_when(str_detect(Season, "Season_\\d{1}$") ~ str_replace(Season, "Season_", "Season_0"),
                            TRUE ~ Season)
  ) %>%
  ungroup() 

cuebiq_denom %>% glimpse()

ranking_df <- rbind(seasonal_rq,
                    ranking_df_safegraph) %>%
  group_by(Season) %>%
  dplyr::arrange(-seasonal_average) %>%
  mutate(lq_rank = rank(-seasonal_average,
                        ties.method = "first")) %>%
  ungroup() 


ranking_df %>% glimpse()

write.csv(ranking_df, "git/downtownrecovery/shinyapp/input_data/all_seasonal_metrics_cuebiq_update.csv")

g1 <-
  ggplot(ranking_df %>%
           filter(Season %in% c("Season_10", "Season_11"))) +
  aes(x = lq_rank,
      group = display_title,
      fill = region
  ) +
  geom_tile(
    aes(y = seasonal_average / 2,
        height = seasonal_average,
        width = 1), 
    alpha = .8,
    color = "white") +
  
  geom_text(
    aes(y = 0, label = paste("", city,  ":", percent(round(seasonal_average, 2), 1))),
    color = "white",
    hjust = "inward",
    size = 4
  ) +
  coord_flip(clip = "off", expand = FALSE) +
  labs(title = paste("Downtown Recovery Rankings"),
       fill = "Region") +
  scale_y_continuous("", labels = scales::percent) +
  scale_x_reverse("") +
  theme(panel.grid = element_blank(),
        axis.text.y = element_blank(),
        axis.title = element_blank(),
        axis.title.y = element_blank(),
        plot.title = element_text(size = 12, hjust = .5),
        plot.subtitle = element_text(size = 10, hjust = .5),
        plot.margin = unit(c(1, 1, 1, 3), "cm")
  )+
  scale_fill_manual(values = c("Canada" = "#e41a1c",
                               "Midwest" = "#377eb8",
                               "Northeast" = "#4daf4a",
                               "Pacific" = "#984ea3",
                               "Southeast" = "#ff7f00",
                               "Southwest" = "#e6ab02")
  ) +
  facet_wrap(.~Season, nrow = 1)
g1
