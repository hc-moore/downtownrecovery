################################################################################
# Consolidate downtown counts & userbase counts files exported from Spectus,
# for July 2023 website update.
# 
# Author: Julia Greenberg
# Date: 6/26/23
################################################################################

#-----------------------------------------
# Load packages
#-----------------------------------------

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'lubridate'))

#-----------------------------------------
# Load data
#-----------------------------------------

filepath <- 'C:/Users/jpg23/data/downtownrecovery/spectus_exports/website_update_july2023/'

userbase <-
  list.files(path = paste0(filepath, 'userbase')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'userbase/', .),
    delim = '\001',
    col_names = c('geography_name', 'userbase', 'event_date'),
    col_types = c('cii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date)

downtowns <-
  list.files(path = paste0(filepath, 'downtowns')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'downtowns/', .),
    delim = '\001',
    col_names = c('city', 'approx_distinct_devices_count', 'event_date'),
    col_types = c('cii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date)

head(userbase)
head(downtowns)

range(userbase$date)
range(downtowns$date)

city_to_state <- data.frame(
  Albuquerque=c('New Mexico'),
  Atlanta=c('Georgia'),
  Austin=c('Texas'),
  Bakersfield=c('California'),
  Baltimore=c('Maryland'),
  Boston=c('Massachusetts'),
  Calgary=c('Alberta'),
  Charlotte=c('North Carolina'),
  Chicago=c('Illinois'),
  Cincinnati=c('Ohio'),
  Cleveland=c('Ohio'),
  `Colorado Springs`=c('Colorado'),
  Columbus=c('Ohio'),
  Dallas=c('Texas'),
  Denver=c('Colorado'),
  Detroit=c('Michigan'),
  Edmonton=c('Alberta'),
  `El Paso`=c('Texas'),
  `Fort Worth`=c('Texas'),
  Fresno=c('California'),
  Halifax=c('Nova Scotia'),
  Honolulu=c('Hawaii'),
  Houston=c('Texas'),
  Indianapolis=c('Indiana'),
  Jacksonville=c('Florida'),
  `Kansas City`=c('Missouri'),
  `Las Vegas`=c('Nevada'),
  `London`=c('Ontario'),
  `Los Angeles`=c('California'),
  Louisville=c('Kentucky'),
  Memphis=c('Tennessee'),
  Miami=c('Florida'),
  Milwaukee=c('Wisconsin'),
  Minneapolis=c('Minnesota'),
  Mississauga=c('Ontario'),
  Montreal=c('Quebec'),
  Nashville=c('Tennessee'),
  `New Orleans`=c('Louisiana'),
  `New York`=c('New York'),
  Oakland=c('California'),
  `Oklahoma City`=c('Oklahoma'),
  Omaha=c('Nebraska'),
  Orlando=c('Florida'),
  Ottawa=c('Ontario'),
  Philadelphia=c('Pennsylvania'),
  Phoenix=c('Arizona'),
  Pittsburgh=c('Pennsylvania'),
  Portland=c('Oregon'),
  Quebec=c('Quebec'),
  Raleigh=c('North Carolina'),
  Sacramento=c('California'),
  `Salt Lake City`=c('Utah'),
  `San Antonio`=c('Texas'),
  `San Diego`=c('California'),
  `San Francisco`=c('California'), 
  `San Jose`=c('California'),
  Seattle=c('Washington'),
  `St Louis`=c('Missouri'),
  Tampa=c('Florida'),
  Toronto=c('Ontario'),
  Tucson=c('Arizona'),
  Tulsa=c('Oklahoma'),
  Vancouver=c('British Columbia'),
  `Washington DC`=c('District of Columbia'),
  Wichita=c('Kansas'),
  Winnipeg=c('Manitoba')) %>%
  pivot_longer(
    cols = everything(),
    names_to = 'city',
    values_to = 'state'
  ) %>%
  mutate(city = str_replace_all(city, '\\.', ' '))

#-----------------------------------------
# Consolidate data
#-----------------------------------------

#date, city name, downtown devices, and userbase devices

downtowns1 <- downtowns %>%
  left_join(city_to_state)

downtowns1 %>% filter(is.na(state))

range(downtowns1$date)
range(userbase$date)

head(downtowns1)
head(userbase)

final_df <- 
  downtowns1 %>% 
  left_join(userbase, by = c('state' = 'geography_name', 'date')) %>%
  rename(downtown_devices = approx_distinct_devices_count)

head(final_df)

nrow(downtowns)
nrow(final_df)

final_df %>% filter(is.na(state))

summary(final_df)

#-----------------------------------------
# Export data
#-----------------------------------------

write.csv(final_df, 
          paste0(filepath, 'downtown_userbase_20230410_20230618.csv'), 
          row.names = F)

# write.csv(userbase, paste0(filepath, 'userbase_20230410_20230618.csv'), row.names = F)
# write.csv(userbase, paste0(filepath, 'downtowns_20230410_20230618.csv'), row.names = F)
