#===============================================================================
# Calculate recovery rates for Portland, Minneapolis & SF
#    - March 1 - June 18, 2023 vs 2019
#    - new HDBSCAN downtown definitions
#    - standardize using MSA
#===============================================================================

# Load packages
#=====================================

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'sf', 'lubridate', 'leaflet', 'plotly', 'htmlwidgets',
       'tidyr'))

# Load MSA data
#=====================================

s_filepath <- 'C:/Users/jpg23/data/downtownrecovery/spectus_exports/sensitivity_analysis/'

msa <-
  list.files(path = paste0(s_filepath, 'MSA')) %>% 
  map_df(~read_delim(
    paste0(s_filepath, 'MSA/', .),
    delim = '\001',
    col_names = c('msa_name', 'provider_id', 'approx_distinct_devices_count', 
                  'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date) %>%
  rename(msa_count = approx_distinct_devices_count) %>%
  filter(msa_name %in% c('Minneapolis-St. Paul-Bloomington, MN-WI',
                         'San Francisco-Oakland-Berkeley, CA',
                         'Portland-Vancouver-Hillsboro, OR-WA'))

head(msa)

unique(msa$provider_id)

plot_ly() %>%
  add_lines(data = msa %>% filter(provider_id == '190199'),
            x = ~date, y = ~msa_count,
            split = ~msa_name,
            name = ~msa_name,
            hoverinfo = 'text',
            opacity = .6,
            line = list(shape = "linear", color = '#e0091e')) %>%
  add_lines(data = msa %>% filter(provider_id == '700199'),
            x = ~date, y = ~msa_count,
            split = ~msa_name,
            name = ~msa_name,
            opacity = .6,
            line = list(shape = "linear", color = 'orange')) %>%
  add_lines(data = msa %>% filter(provider_id == '230599'),
            x = ~date, y = ~msa_count,
            split = ~msa_name,
            name = ~msa_name,
            opacity = .6,
            line = list(shape = "linear", color = 'purple')) %>%
  layout(title = "Counts by MSA and provider",
         xaxis = list(title = "Date", zerolinecolor = "#ffff",
                      tickformat = "%b %Y"),
         yaxis = list(title = "MSA counts", zerolinecolor = "#ffff",
                      ticksuffix = "  "),
         shapes = list(list(y0 = 0, y1 = 1, yref = "paper",
                            x0 = as.Date('2021-05-17'), x1 = as.Date('2021-05-17'),
                            line = list(color = 'black', dash = 'dash'))))

msa_190 <- msa %>% filter(provider_id == '190199') %>% select(-provider_id)

msa_names <- msa %>% 
  select(msa_name) %>%
  distinct() %>%
  mutate(msa_no_state = str_remove_all(msa_name, ',.*$')) %>%
  separate(msa_no_state, remove = FALSE, sep = '-',
           into = c('name1', 'name2', 'name3', 'name4', 'name5', 'name6')) %>%
  pivot_longer(
    cols = starts_with('name'),
    names_to = 'city',
    values_to = 'city_value'
  ) %>%
  select(msa_name, city = city_value) %>%
  filter(!is.na(city) & !city %in% c('', ' ')) %>%
  mutate(city = str_remove_all(city, '\\.')) %>%
  filter(city %in% c('Portland', 'Minneapolis', 'San Francisco'))

msa_names

# Load downtown data
#=====================================

dt_19 <-
  list.files(path = paste0(s_filepath, 'hdbscan_3cities_2019')) %>%
  map_df(~read_delim(
    paste0(s_filepath, 'hdbscan_3cities_2019/', .),
    delim = '\001',
    col_names = c('city', 'approx_distinct_devices_count', 'event_date'),
    col_types = c('cii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date) 

dt_23 <-
  list.files(path = paste0(s_filepath, 'hdbscan_3cities_2023')) %>%
  map_df(~read_delim(
    paste0(s_filepath, 'hdbscan_3cities_2023/', .),
    delim = '\001',
    col_names = c('city', 'approx_distinct_devices_count', 'event_date'),
    col_types = c('cii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date)

# Combine them

range(dt_19$date)
range(dt_23$date)

dt <- rbind(dt_19, dt_23)

head(dt)

# Join downtowns with MSA
#=====================================

head(dt)
head(msa_190)
head(msa_names)

dt1 <- dt %>%
  mutate(city = str_remove(city, '\\s\\w{2}$')) %>%
  left_join(msa_names)

head(dt1)

final_df <- 
  dt1 %>% 
  left_join(msa_190, by = c('msa_name', 'date')) %>%
  rename(downtown_devices = approx_distinct_devices_count) %>%
  mutate(normalized = downtown_devices/msa_count)

head(final_df)

rec_rate_cr <-
  final_df %>%
  # Determine week and year # for each date
  mutate(
    date_range_start = floor_date(
      date,
      unit = "week",
      week_start = getOption("lubridate.week.start", 1)),
    week_num = isoweek(date_range_start),
    year = year(date_range_start)) %>%
  # Calculate # of devices by big_area, week and year
  dplyr::group_by(city, year, week_num) %>%
  dplyr::summarize(downtown_devices = sum(downtown_devices, na.rm = T),
                   msa_count = sum(msa_count, na.rm = T)) %>%
  dplyr::ungroup() 

head(rec_rate_cr)

# do i take the normalized count for each entire period and then divide
# 2023 by 2019? or do i calculate recovery rates by week and then take the
# avg recovery rate?

# option 1:

final_rq <-
  rec_rate_cr %>%
  dplyr::group_by(city, year) %>%
  dplyr::summarize(downtown_devices = sum(downtown_devices, na.rm = T),
                   msa_count = sum(msa_count, na.rm = T)) %>%
  mutate(normalized = downtown_devices/msa_count) %>%
  select(-c(downtown_devices, msa_count)) %>%
  pivot_wider(
    names_from = year,
    names_prefix = 'normalized_',
    values_from = normalized
  ) %>%
  mutate(rate = normalized_2023/normalized_2019) %>%
  data.frame() %>%
  select(-starts_with('normalized'))

final_rq

# option 2:

final_rq2 <-
  rec_rate_cr %>%
  dplyr::group_by(year, week_num, city) %>%
  dplyr::summarize(downtown_devices = sum(downtown_devices, na.rm = T),
                   msa_count = sum(msa_count, na.rm = T)) %>%
  dplyr::ungroup() %>%
  mutate(normalized = downtown_devices/msa_count) %>%
  pivot_wider(
    id_cols = c('week_num', 'city'),
    names_from = 'year',
    names_prefix = 'ntv',
    values_from = 'normalized') %>%
  mutate(rec2023 = ntv2023/ntv2019) %>%
  select(-starts_with('ntv')) %>%
  pivot_longer(
    cols = rec2023,
    names_to = 'year',
    values_to = 'rq') %>%
  group_by(city) %>%
  summarize(rq_avg = mean(rq, na.rm = T)) %>%
  ungroup() %>%
  data.frame()

final_rq2




for_plot <-
  rec_rate_cr %>%
  dplyr::group_by(year, week_num, city) %>%
  dplyr::summarize(downtown_devices = sum(downtown_devices, na.rm = T),
                   msa_count = sum(msa_count, na.rm = T)) %>%
  dplyr::ungroup() %>%
  mutate(normalized = downtown_devices/msa_count,
         week = as.Date(paste(year, week_num, 1, sep = '_'),
                        format = '%Y_%W_%w'))

head(for_plot)

# MAKE THIS BY WEEK:

trend2019 <-
  plot_ly() %>%
  add_lines(data = for_plot %>% filter(year == 2019),
            x = ~week, y = ~normalized,
            split = ~city,
            name = ~city,
            opacity = .6,
            line = list(shape = "linear", color = '#e0091e')) %>%
  layout(title = "Normalized HDBSCAN downtown counts by week",
         xaxis = list(title = "week", zerolinecolor = "#ffff",
                      tickformat = "%Y-%m"),
         yaxis = list(title = "normalized", zerolinecolor = "#ffff",
                      ticksuffix = "  "))

trend2019

saveWidget(
  trend2019,
  'C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/hdbscan_wsj_2019.html')

trend2023 <-
  plot_ly() %>%
  add_lines(data = for_plot %>% filter(year == 2023),
            x = ~week, y = ~normalized,
            split = ~city,
            name = ~city,
            opacity = .6,
            line = list(shape = "linear", color = '#e0091e')) %>%
  layout(title = "Normalized HDBSCAN downtown counts by week",
         xaxis = list(title = "week", zerolinecolor = "#ffff",
                      tickformat = "%Y-%m"),
         yaxis = list(title = "normalized", zerolinecolor = "#ffff",
                      ticksuffix = "  "))

trend2023

saveWidget(
  trend2023,
  'C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/hdbscan_wsj_2023.html')
