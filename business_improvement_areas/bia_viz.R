#===============================================================================
# Create plots and maps showing recovery patterns for Business Improvement Areas
# for Toronto City Council presentation.
#
# Author: Julia Greenberg
# Date created: 5/22/23
#===============================================================================

#-----------------------------------------
# Load packages
#-----------------------------------------

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'ggplot2', 'sf', 'lubridate', 'plotly', 'zoo'))

#-----------------------------------------
# Load data
#-----------------------------------------

## Shapefile of BIAs

bia_sf <- read_sf("C:/Users/jpg23/data/downtownrecovery/business_improvement_areas/business_improvement_areas_simplified.geojson") %>%
  select(bia = AREA_NAME)

head(bia_sf)
class(bia_sf)
n_distinct(bia_sf$bia) # there are 85 BIAs

## 1/1/2019 - 4/25/2023 (userbase + BIAs)

both <- read_delim(
  "C:/Users/jpg23/data/downtownrecovery/spectus_exports/BIAs/20230522_190855_00007_3yd5w_80fe0af5-8ae8-468e-ad40-4d1714548545.gz",
  delim = '\001',
  col_names = c('bia', 'provider', 'n_devices', 'userbase', 'date'),
  col_types = c('cciii')
) %>%
  mutate(date = as.Date(as.character(date), format = "%Y%m%d")) %>%
  data.frame() %>%
  arrange(date)

head(both)
glimpse(both)

## 4/25/2023 - 5/19/2023 (userbase)

userbase <- read_delim(
  ?,
  delim = '\001',
  col_names = c('province', 'userbase', 'provider', 'date'),
  col_types = c('cici')
) %>%
  data.frame()

head(userbase)
glimpse(userbase)

## 5/15/2023 - 5/19/2023 (BIAs)

bia <- read_delim(
  ?,
  delim = '\001',
  col_names = c('bia', 'provider', 'n_devices', 'date'),
  col_types = c('ccii')
) %>%
  data.frame()

head(bia)
glimpse(bia)

#-----------------------------------------
# Plots (exploratory)
#-----------------------------------------

# First look at trends over time, by provider, for all BIAs.

all_bia <-
  both %>%
  group_by(bia, provider, date) %>%
  summarize(n_devices = sum(n_devices, na.rm = T),
            userbase = sum(userbase, na.rm = T)) %>%
  data.frame() %>%
  mutate(normalized = n_devices/userbase)

head(all_bia)

trend_all <- 
  all_bia %>%
  ggplot(aes(x = date, y = n_devices, group = provider, color = provider,
             alpha = .8)) +
  geom_line() +
  theme_bw()

ggplotly(trend_all)

normalized_all <- 
  all_bia %>%
  ggplot(aes(x = date, y = normalized, group = provider, color = provider,
             alpha = .8)) +
  geom_line() +
  theme_bw()

ggplotly(normalized_all)

one_provider <- 
  all_bia %>%
  # Starting 5/17/21, switch providers
  filter((provider == '700199' & date < as.Date('2021-05-17')) | 
           (provider == '190199' & date >= as.Date('2021-05-17')))

head(one_provider)

one_prov_plot <- 
  one_provider %>%
  ggplot(aes(x = date, y = normalized, alpha = .8)) +
  geom_line() +
  theme_bw()

ggplotly(one_prov_plot)

#-----------------------------------------
# Trend plot: recovery rate (all BIAs)
#-----------------------------------------

# 11-week rolling average of weekly normalized counts

# 1. Group by week and year number and sum device counts in both BIA and in userbase
# 2. Calculate normalized device counts (for each BIA) by dividing BIA count by userbase count
# 3. Calculate recovery quotient for each week in year 202X by dividing normalized device count by normalized device count in equivalent week in 2019 (for each BIA)
# 4. Calculate 11-week rolling average of recovery quotient (for each BIA)

# Make sure to omit BIAs with less than 10 devices in the weekly aggregates.

rec_rate <-
  both %>%
  # Starting 5/17/21, switch providers
  filter((provider == '700199' & date < as.Date('2021-05-17')) | 
           (provider == '190199' & date >= as.Date('2021-05-17'))) %>%
  # Determine week and year # for each date
  mutate(
    date_range_start = floor_date(
      date,
      unit = "week",
      week_start = getOption("lubridate.week.start", 1)),
    week_num = isoweek(date_range_start),
    year = year(date_range_start)) %>%
  # Calculate # of devices by BIA, week and year
  group_by(bia, year, week_num) %>%
  summarize(n_devices = sum(n_devices, na.rm = T),
            userbase = sum(userbase, na.rm = T)) %>%
  ungroup() # %>%
# Calculate normalized device count
# mutate(normalized = n_devices/userbase)

# Are there 11-week rolling averages that are <10? Filter these out
# filter(n_devices >= 10) %>%

head(rec_rate)
summary(rec_rate$week_num)
rec_rate %>% filter(week_num == 53) %>% head()

# all_bia_for_plot <-
#   rec_rate %>%
#   filter(year > 2018) %>%
#   group_by(year, week_num) %>%
#   summarize(n_devices = sum(n_devices, na.rm = T),
#             userbase = sum(userbase, na.rm = T)) %>%
#   ungroup() %>%
#   mutate(normalized = n_devices/userbase) %>%
#   pivot_wider(
#     id_cols = c('week_num'), 
#     names_from = 'year', 
#     names_prefix = 'ntv', 
#     values_from = 'normalized') %>%
#   mutate(rec2020 = ntv2020/ntv2019,
#          rec2021 = ntv2021/ntv2019,
#          rec2022 = ntv2022/ntv2019,
#          rec2023 = ntv2023/ntv2019) %>%
#   pivot_longer(
#     cols = rec2020:rec2023, 
#     names_to = 'year', 
#     values_to = 'normalized') %>%
#   filter(week_num < 53) %>%
#   mutate(year = substr(year, 4, 7),
#          week = as.Date(paste(year, week_num, 1, sep = '_'), 
#                         format = '%Y_%W_%w')) %>%
#   filter(!is.na(normalized))
  
  
  arrange(year, week_num) %>%
  mutate(rq_rolling = zoo::rollmean(rq, k = 11, fill = 'extend')) %>% # fill = NA
    ungroup()

head(all_bia_for_plot)

# Any 11-week periods where n_devices_avg < 10?
all_bia_for_plot %>% filter(n_devices_avg < 10) # nope!

#-----------------------------------------
# Trend plot: recovery rate (Downtown Yonge)
#-----------------------------------------

# 11-week rolling average of weekly normalized counts

#-----------------------------------------
# Trend plot: recovery rate (Financial District)
#-----------------------------------------

# 11-week rolling average of weekly normalized counts

#-----------------------------------------
# Maps
#-----------------------------------------

# Compare overall NORMALIZED device count for period of March 1, 2023 through 
# latest date available to same time period in 2019.


# 1. Choropleth map of "recovery rate" for all BIAs
#-----------------------------------------------------------------



# Join spatial data with device count data.

nrow(bia_sf)
nrow(?)

bia_final <- left_join(bia_sf, ?)

nrow(bia_final)

# 2. Map zoomed in on downtown yonge and financial district BIAs
#-----------------------------------------------------------------