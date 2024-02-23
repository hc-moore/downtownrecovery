# Load packages
#=====================================

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'sf', 'lubridate', 'leaflet', 'plotly', 'htmlwidgets', 
       'broom', 'forecast'))

# Load downtown & MSA data
#=====================================

filepath <- '/Users/jpg23/data/downtownrecovery/spectus_exports/albuquerque/albuquerque_5digits/'

# First load June onwards
geo5 <-
  list.files(path = filepath) %>% 
  map_df(~read_delim(
    paste0(filepath, .),
    delim = '\001',
    col_names = c('geohash5', 'block_group_id', 'processing_date', 'visitors', 
                  'stops'),
    col_types = c('cciii')
  )) %>%
  data.frame()

head(geo5)

write.csv(geo5,
          '/Users/jpg23/data/downtownrecovery/albuquerque/albuquerque_geo5.csv',
          row.names = F)