#===============================================================================
# Compare March - June 2023 data from provider 230399 from stop_uplevelled table 
# vs stoppers_hll_by_geohash for all 60+ HDBSCAN downtowns
#===============================================================================

# Load packages
#=====================================

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'sf', 'lubridate', 'leaflet', 'plotly', 'htmlwidgets'))

# Load downtown & MSA data
#=====================================

filepath <- 'C:/Users/jpg23/data/downtownrecovery/spectus_exports/stop_uplevelled_230399_2023/'

dt <-
  list.files(path = paste0(filepath, 'downtown_march_june/')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'downtown_march_june/', .),
    delim = '\001',
    col_names = c('city', 'zone_date', 'provider_id', 'n_stops', 'n_distinct_devices'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-c(zone_date, provider_id))

msa <-
  list.files(path = paste0(filepath, 'msa_march_june/')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'msa_march_june/', .),
    delim = '\001',
    col_names = c('msa_name', 'zone_date', 'provider_id', 'n_stops', 'n_distinct_devices'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-c(zone_date, provider_id)) %>%
  rename(n_stops_msa = n_stops, n_distinct_devices_msa = n_distinct_devices)

glimpse(dt)
glimpse(msa)
