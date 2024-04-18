#===============================================================================
# Create Spectus dataset for blocks surrounding Albuquerque rail trail
#===============================================================================

# Load packages
#-----------------------------------------

source('~/git/timathomas/functions/functions.r')

ipak(c('tidyverse', 'lubridate', 'ggplot2', 'plotly', 
       'sf', 'leaflet', 'BAMMtools', 'gtools', 'htmlwidgets'))

# Load rail-trail blocks data
#-----------------------------------------

filepath <- '/Users/jpg23/data/downtownrecovery/spectus_exports/albuquerque/albuquerque_rail_trail_blocks/'

rt <-
  list.files(path = filepath) %>%
  map_df(~read_delim(
    paste0(filepath, .),
    delim = '\001',
    col_names = c('visit_block', 'home_bg', 'processing_date', 
                  'visitors', 'stops'),
    col_types = c('cciii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(processing_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-processing_date)

head(rt)

write.csv(rt,
          '/Users/jpg23/data/urban-displacement/eddit/albuquerque/spectus_rail_trail_blocks.csv',
          row.names = F)
