#===============================================================================
# Create dataset of top 300 metros - 1 month - for Jeff to explore
#===============================================================================

# Load packages
#=====================================

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'sf', 'lubridate', 'leaflet', 'plotly', 'htmlwidgets', 
       'broom', 'forecast', 'tidycensus', 'cancensus', 'geohashTools'))

set_cancensus_api_key('CensusMapper_bcd591107b93e609a0bb5415f58cb31b')

# Load data
#=====================================

filepath <- '/Users/jpg23/data/downtownrecovery/spectus_exports/top_300_1month/'

d <-
  list.files(path = filepath) %>% 
  map_df(~read_delim(
    paste0(filepath, .),
    delim = '\001',
    col_names = c('geohash6', 'n_stops', 'n_distinct_devices'),
    col_types = c('cii')
  )) %>%
  data.frame()

head(d)
nrow(d)
n_distinct(d$geohash6)

st_write(d, 
         '/Users/jpg23/data/downtownrecovery/top_300_metros/top300_1month.csv',
         row.names = F)
