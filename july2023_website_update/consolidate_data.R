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

# 'ggplot2', 'sf', 'lubridate', 'plotly', 'zoo', 
#        'htmlwidgets', 'BAMMtools', 'leaflet'))

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

#-----------------------------------------
# Export data
#-----------------------------------------

write.csv(userbase, paste0(filepath, 'userbase_20230410_20230618.csv'), row.names = F)
write.csv(userbase, paste0(filepath, 'downtowns_20230410_20230618.csv'), row.names = F)
