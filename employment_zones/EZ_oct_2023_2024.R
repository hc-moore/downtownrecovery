#===============================================================================
# Create shapefile of recovery rates for employment zones in Toronto (Oct 2023
# vs Oct 2024 from stop_uplevelled table)
#===============================================================================

# Load packages
#=====================================

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'sf', 'lubridate')) #, 'broom', 'forecast'))

# Load data
#=====================================

filepath <- '/Users/jpg23/data/downtownrecovery/spectus_exports/employment_zones/'
fp23 <- paste0(filepath, 'stopuplevelled_oct2023/')
fp24 <- paste0(filepath, 'stopuplevelled_oct2024/')

# Oct 2023
o23 <-
  list.files(path = fp23) %>% 
  map_df(~read_delim(
    paste0(fp23, .),
    delim = '\001',
    col_names = c('employment_zone', 'n_devices23'),
    col_types = 'ci'
  )) %>%
  data.frame()

# Oct 2024
o24 <-
  list.files(path = fp24) %>% 
  map_df(~read_delim(
    paste0(fp24, .),
    delim = '\001',
    col_names = c('employment_zone', 'n_devices24'),
    col_types = 'ci'
  )) %>%
  data.frame()

head(o23)
head(o24)

n_distinct(o23$employment_zone) == n_distinct(o24$employment_zone) # true

# Calculate recovery rate
o <- o23 %>% 
  left_join(o24) %>% 
  mutate(rq = n_devices24/n_devices23) %>% 
  select(employment_zone, rq)

nrow(o) # 81 - good, same as before
head(o)

summary(o$rq)

# Load spatial data
#=====================================

fp <- '/Users/jpg23/UDP/downtown_recovery/employment_zones/'
ez <- st_read(paste0(fp, 'final_sf.geojson'))
head(ez)

ez_sf <- ez %>% select(employment_zone = new_ez) %>% left_join(o)
nrow(ez_sf)
head(ez_sf)

st_write(ez_sf,
         paste0(fp, 'ez_oct_2023_2024_rq.geojson'))
