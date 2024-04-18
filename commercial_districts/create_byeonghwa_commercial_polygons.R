#===============================================================================
# Subset Byeonghwa's commercial districts to Portland, SF, NYC, Chicago and 
# Toronto - so I can query them and compare with other commercial polygons
#===============================================================================

# Load packages
#-----------------------------------------

source('~/git/timathomas/functions/functions.r')

ipak(c('tidyverse', 'sf', 'tigris', 'cancensus'))

# Load commercial district data
#-----------------------------------------

c <- st_read('/Users/jpg23/UDP/downtown_recovery/commercial_districts/commercial_boundaries_within_bound.gpkg')

head(c)
unique(c$MSA_NAME)

comm <- c %>%
  filter(MSA_NAME %in% c('Portland-Vancouver-Hillsboro',
                         'San Francisco-Oakland-Berkeley',
                         'New York-Newark-Jersey City',
                         'Chicago-Naperville-Elgin',
                         'Toronto'))

portland <- comm %>% filter(MSA_NAME == 'Portland-Vancouver-Hillsboro')
plot(portland$geom)

sf <- comm %>% filter(MSA_NAME == 'San Francisco-Oakland-Berkeley')
plot(sf$geom)

# Download city boundaries
#-----------------------------------------

# US
p <- places(state = c('OR', 'CA', 'NY', 'IL')) %>%
  filter(NAME %in% c('Portland', 'San Francisco', 'New York', 'Chicago')) %>%
  select(geometry)

head(p)

# get Toronto too!
t <- get_census(dataset = 'CA21', regions = list(CMA="35535"), 
                vectors = c("v_CA16_408"), level = 'CSD', geo_format = "sf") %>%
  filter(`Region Name` == 'Toronto (C)') %>%
  select(geometry)

head(t)
plot(t)

st_crs(p)
st_crs(t)
st_crs(comm)

t1 <- t %>% st_transform(crs = st_crs(p))

st_crs(t1) == st_crs(p)

cities <- rbind(t1, p)
cities

# Crop commercial districts to city
# boundaries
#-----------------------------------------

comm1 <- comm %>% st_transform(crs = st_crs(p))

cropped <- st_intersection(comm1, cities) %>%
  mutate(city = case_when(
    MSA_NAME == 'San Francisco-Oakland-Berkeley' ~ 'San Francisco',
    MSA_NAME == 'Portland-Vancouver-Hillsboro' ~ 'Portland',
    MSA_NAME == 'Toronto' ~ 'Toronto',
    MSA_NAME == 'New York-Newark-Jersey City' ~ 'New York',
    MSA_NAME == 'Chicago-Naperville-Elgin' ~ 'Chicago'
  )) %>%
  select(city, district = CB_ID)
  
head(cropped)

sf_crop <- cropped %>% filter(MSA_NAME == 'San Francisco-Oakland-Berkeley')
plot(sf_crop$geom)

p_crop <- cropped %>% filter(MSA_NAME == 'Portland-Vancouver-Hillsboro')
plot(p_crop$geom)

t_crop <- cropped %>% filter(MSA_NAME == 'Toronto')
plot(t_crop$geom)

n_crop <- cropped %>% filter(MSA_NAME == 'New York-Newark-Jersey City')
plot(n_crop$geom)

c_crop <- cropped %>% filter(MSA_NAME == 'Chicago-Naperville-Elgin')
plot(c_crop$geom)

# Export final shapefile
#-----------------------------------------

st_write(
  cropped %>% select(city, district), 
  '/Users/jpg23/UDP/downtown_recovery/commercial_districts/byeonghwa_commercial_5cities.geojson')
