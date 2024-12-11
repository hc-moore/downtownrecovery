#===============================================================================
# Create dataset of top 300 metros
#===============================================================================

# Load packages
#=====================================

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'sf', 'lubridate', 'leaflet', 'plotly', 'htmlwidgets', 
       'broom', 'forecast', 'tidycensus', 'cancensus', 'geohashTools'))

set_cancensus_api_key('CensusMapper_bcd591107b93e609a0bb5415f58cb31b')

# Load data
#=====================================

f0 <- '/Users/jpg23/data/downtownrecovery/spectus_exports/' 
# f1 <- paste0(f0, 'top_300_1month/')
# f1 <- paste0(f0, 'top_300_20230301_20230331/')
f2 <- paste0(f0, 'top_300_20230401_20230531/')
f3 <- paste0(f0, 'top_300_20230601_20231231/')
# f4 <- paste0(f0, 'top_300_20240101_20240931/')
f4 <- paste0(f0, 'top_300_20240101_20240331/')

# # March 1 - 31, 2023
# d1 <-
#   list.files(path = f1) %>% 
#   map_df(~read_delim(
#     paste0(f1, .),
#     delim = '\001',
#     col_names = c('geohash6', 'n_stops'),
#     col_types = c('ci')
#   )) %>%
#   data.frame()
# 
# head(d1)
# nrow(d1)
# n_distinct(d1$geohash6)

# # March 1 - April 1, 2023
# d1 <-
#   list.files(path = f1) %>%
#   map_df(~read_delim(
#     paste0(f1, .),
#     delim = '\001',
#     col_names = c('geohash6', 'n_stops', 'n_distinct_devices'),
#     col_types = c('cii')
#   )) %>%
#   data.frame() %>%
#   select(-n_distinct_devices)
# 
# head(d1)
# nrow(d1)
# n_distinct(d1$geohash6)

# st_write(d1, 
#          '/Users/jpg23/data/downtownrecovery/top_300_metros/top300_1month.csv',
#          row.names = F)

# April 1 - May 31, 2023
d2 <-
  list.files(path = f2) %>% 
  map_df(~read_delim(
    paste0(f2, .),
    delim = '\001',
    col_names = c('geohash6', 'n_stops'),
    col_types = c('ci')
  )) %>%
  data.frame()

head(d2)
nrow(d2)
n_distinct(d2$geohash6)

# June 1 - December 31, 2023
d3 <-
  list.files(path = f3) %>% 
  map_df(~read_delim(
    paste0(f3, .),
    delim = '\001',
    col_names = c('geohash6', 'n_stops'),
    col_types = c('ci')
  )) %>%
  data.frame()

head(d3)
nrow(d3)
n_distinct(d3$geohash6)

# January 1 - March 31, 2024
d4 <-
  list.files(path = f4) %>% 
  map_df(~read_delim(
    paste0(f4, .),
    delim = '\001',
    col_names = c('geohash6', 'n_stops'),
    col_types = c('ci')
  )) %>%
  data.frame()

# # January 1 - September 31, 2024
# d4 <-
#   list.files(path = f4) %>% 
#   map_df(~read_delim(
#     paste0(f4, .),
#     delim = '\001',
#     col_names = c('geohash6', 'n_stops'),
#     col_types = c('ci')
#   )) %>%
#   data.frame()

head(d4)
nrow(d4)
n_distinct(d4$geohash6)

# Aggregate total stops by geohash
#=====================================

d <- 
  rbind(d2, d3, d4)  %>%
  # rbind(d1, d2, d3, d4)  %>%
  group_by(geohash6) %>%
  summarize(total_stops = sum(n_stops, na.rm = T)) %>%
  data.frame()

head(d)
n_distinct(d$geohash6)
nrow(d)

st_write(d,
         '/Users/jpg23/data/downtownrecovery/top_300_metros/top300_2023-04-01_2024-03-31.csv',
         row.names = F)

# st_write(d,
#          '/Users/jpg23/data/downtownrecovery/top_300_metros/top300_2023-03-01_2024-09-31.csv',
#          row.names = F)
