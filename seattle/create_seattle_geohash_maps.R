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

# Sum unique devices by geohash
#=====================================

s19_sum <- s19 %>%
  group_by(geohash_id) %>%
  summarize(sum_unique_devices = sum(unique_devices, na.rm = T)) %>%
  data.frame() %>% 
  filter(geohash_id %in% s24$geohash_id)

head(s19_sum)

s24_sum <- s24 %>%
  group_by(geohash_id) %>%
  summarize(sum_unique_devices = sum(unique_devices, na.rm = T)) %>%
  data.frame()

head(s24_sum)

# Scale unique_devices columns
#=====================================

s19_scaled <- s19_sum %>% 
  mutate_at(c('sum_unique_devices'), ~(scale(.) %>% as.vector))

head(s19_scaled)
range(s19_scaled$sum_unique_devices)

s24_scaled <- s24_sum %>% 
  mutate_at(c('sum_unique_devices'), ~(scale(.) %>% as.vector))

head(s24_scaled)
range(s24_scaled$sum_unique_devices)

# Load geohash polygons & merge
#=====================================

geo_sf <- st_read('/Users/jpg23/data/downtownrecovery/seattle/seattle_6digit_geohashes.geojson')
head(geo_sf)

geo_sf19 <- geo_sf %>%
  left_join(s19_scaled)

head(geo_sf19)
nrow(geo_sf19) == nrow(geo_sf)

geo_sf24 <- geo_sf %>%
  left_join(s24_scaled)

head(geo_sf24)

# Map both 2019 & 2024
#=====================================

st_write(geo_sf19,
         '/Users/jpg23/data/downtownrecovery/seattle/seattle_unique_devices_2019.geojson')

st_write(geo_sf24,
         '/Users/jpg23/data/downtownrecovery/seattle/seattle_unique_devices_2024.geojson')
