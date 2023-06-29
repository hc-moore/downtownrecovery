################################################################################
# Consolidate employment zone & userbase counts files exported from Spectus,
# for Laura's work on recovery rates in employment zones.
# 
# Author: Julia Greenberg
# Date: 6/28/23
################################################################################

#-----------------------------------------
# Load packages
#-----------------------------------------

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'lubridate'))

#-----------------------------------------
# Load data
#-----------------------------------------

# Userbase: 1/1/2019 - 4/25/2023

filepath_bia <- 'C:/Users/jpg23/data/downtownrecovery/spectus_exports/BIAs/'

userbase1 <- read_delim(
  paste0(filepath_bia, "bia_userbase/20230522_190855_00007_3yd5w_80fe0af5-8ae8-468e-ad40-4d1714548545.gz"),
  delim = '\001',
  col_names = c('bia', 'provider', 'n_devices', 'userbase', 'date'),
  col_types = c('cciii')
) %>%
  mutate(date = as.Date(as.character(date), format = "%Y%m%d")) %>%
  data.frame() %>%
  arrange(date) %>%
  select(-c(bia, n_devices)) %>%
  distinct()

filepath <- 'C:/Users/jpg23/data/downtownrecovery/spectus_exports/employment_zones/'

# Userbase: 4/10/23 - 6/18/23
userbase2 <-
  list.files(path = paste0(filepath, 'userbase')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'userbase/', .),
    delim = '\001',
    col_names = c('geography_name', 'userbase', 'event_date'),
    col_types = c('cii')
  )) %>%
  data.frame() %>%
  filter(geography_name == 'Ontario') %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d"),
         provider = '190199') %>%
  arrange(date) %>%
  select(-event_date)

# Region-wide: 1/1/19 - 7/7/21
region1 <-
  list.files(path = paste0(filepath, 'region_20190101_20210707')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'region_20190101_20210707/', .),
    delim = '\001',
    col_names = c('ez', 'provider_id', 'approx_distinct_devices_count', 
                  'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date)

# Region-wide: 7/7/21 - 6/27/23
region2 <-
  list.files(path = paste0(filepath, 'region_20210707_20230627')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'region_20210707_20230627/', .),
    delim = '\001',
    col_names = c('ez', 'provider_id', 'approx_distinct_devices_count', 
                  'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date)

# City-wide: 1/1/19 - 8/12/22
city1 <-
  list.files(path = paste0(filepath, 'city_20190101_20220812')) %>%
  map_df(~read_delim(
    paste0(filepath, 'city_20190101_20220812/', .),
    delim = '\001',
    col_names = c('small_area', 'big_area', 'provider_id', 
                  'approx_distinct_devices_count', 'event_date'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-c(event_date, geography_name))

# City-wide: 8/12/22 - ?
city2 <-
  list.files(path = paste0(filepath, 'city_20220812_??')) %>%
  map_df(~read_delim(
    paste0(filepath, 'city_20220812_??/', .),
    delim = '\001',
    col_names = c('small_area', 'big_area', 'provider_id', 
                  'approx_distinct_devices_count', 'event_date'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-c(event_date, geography_name))

#-----------------------------------------
# Combine userbase data
#-----------------------------------------

head(userbase1)
head(userbase2)

range(userbase1$date)
range(userbase2$date)

userbase <- 
  userbase1 %>% 
  filter(date < as.Date('2023-04-10')) %>%
  rbind(userbase2)

range(userbase$date)

#-----------------------------------------
# Combine region-wide data
#-----------------------------------------

head(region1)
head(region2)

range(region1$date)
range(region2$date)

region <-
  region1 %>%
  filter(date < as.Date('2021-07-07')) %>%
  rbind(region2) %>%
  filter(date <= as.Date('2023-06-18')) # stop at 6/18/23 (when userbase ends)

range(region$date)

#-----------------------------------------
# Combine city-wide data
#-----------------------------------------

head(city1)
head(city2)

range(city1$date)
range(city2$date)

city <- 
  city1 %>% 
  filter(date < as.Date('2022-08-12')) %>%
  rbind(city2)

range(city$date)

#-----------------------------------------
# Combine city & region-wide data
#-----------------------------------------
