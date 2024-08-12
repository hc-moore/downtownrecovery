#===============================================================================
# Explore 
#===============================================================================

# Load packages
#=====================================

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'sf', 'lubridate', 'leaflet', 'plotly', 'htmlwidgets', 
       'broom', 'forecast', 'tidycensus', 'cancensus', 'geohashTools'))

set_cancensus_api_key('CensusMapper_bcd591107b93e609a0bb5415f58cb31b')

# Load data
#=====================================

filepath <- '/Users/jpg23/data/downtownrecovery/spectus_exports/top_300_metros/'

d <-
  list.files(path = filepath) %>% 
  map_df(~read_delim(
    paste0(filepath, .),
    delim = '\001',
    col_names = c('geohash6', 'zone_date', 'provider_id', 'n_stops', 'n_distinct_devices'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-c(zone_date, provider_id))

n_distinct(d$geohash6)

# Get polygons for geohashes
#=====================================

# Example list of geohashes
geohashes <- unique(d$geohash)
geohashes

geohashes_sf <- gh_to_sf(geohashes)
geohashes_sf$geohash <- rownames(geohashes_sf)
geohashes_sf <- geohashes_sf %>% remove_rownames() %>% select(-ID)

head(geohashes_sf)

# Map all the geohashes
leaflet(data = geohashes_sf) %>%
  addTiles() %>%
  addPolygons(label = ~geohash,
              color = 'black',
              opacity = 1)
