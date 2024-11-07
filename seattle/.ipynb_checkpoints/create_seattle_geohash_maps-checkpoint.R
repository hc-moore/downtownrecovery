#===============================================================================
# Create maps showing standardized activity in Seattle 6-digit geohashes,
# March 1 - April 1, 2019 and March 1 - April 1, 2024
#===============================================================================

# Load packages
#=====================================

source('~/git/timathomas/functions/functions.r')
ipak(c('sf', 'geohashTools', 'tigris', 'tidyverse', 'leaflet'))

# Load data
#=====================================

base_path <- '/Users/jpg23/data/downtownrecovery/spectus_exports/seattle/'
path19 <- paste0(base_path, 'seattle_2019/')
path24 <- paste0(base_path, 'seattle_2024/')

s19 <-
  list.files(path = path19) %>% 
  map_df(~read_delim(
    paste0(path19, .),
    delim = '\001',
    col_names = c('geohash_id', 'event_date', 'unique_devices'),
    col_types = c('cii')
  )) %>%
  data.frame()

head(s19)
nrow(s19)
hist(s19$unique_devices)
range(s19$unique_devices)

s24 <-
  list.files(path = path24) %>% 
  map_df(~read_delim(
    paste0(path24, .),
    delim = '\001',
    col_names = c('geohash_id', 'event_date', 'unique_devices'),
    col_types = c('cii')
  )) %>%
  data.frame()

head(s24)
nrow(s24)
hist(s24$unique_devices)
range(s24$unique_devices)

# Scale unique_devices columns
#=====================================

s19_scaled <- s19 %>% mutate_at(c('unique_devices'), ~(scale(.) %>% as.vector))
head(s19_scaled)
range(s19_scaled$unique_devices)

s24_scaled <- s24 %>% mutate_at(c('unique_devices'), ~(scale(.) %>% as.vector))
head(s24_scaled)
range(s24_scaled$unique_devices)

# Get polygons for geohashes
#=====================================

# Full list of geohashes
geos <- read.csv('/Users/jpg23/data/downtownrecovery/seattle/seattle_6digit_geohashes.csv')

geohash_list <- geos %>% pull(geohashid)
geohash_list %>% head()

geo_polygons <- lapply(geohash_list, function(gh) gh_decode(gh, return="polygon"))
geohash_sf <- st_sfc(lapply(geo_polygons, function(p) st_polygon(list(p))))