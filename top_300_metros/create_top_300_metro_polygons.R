#===============================================================================
# Create polygon for boundaries of major cities within 300 largest cities in
# US & Canada (by population)
#===============================================================================

# Load packages
#=====================================

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'sf', 'tidycensus', 'cancensus'))

# Pull US data
#=====================================

us <- get_acs(geography = "cbsa", 
              variables = "B01003_001", 
              # state = "VT", 
              survey = "acs5",
              year = 2022,
              geometry = TRUE)

head(us)

us1 <- us %>% 
  arrange(desc(estimate)) %>%
  select(name = NAME, population = estimate)

head(us1)

# Pull Canadian data
#=====================================

canada <- get_census(
  dataset = 'CA21', 
  regions = list(C = '01'), 
  vectors = c('population' = 'v_CA21_1'), 
  level = 'CMA',
  geo_format = 'sf') %>%
  mutate(year = 2021) %>%
  select(name, population) %>%
  arrange(desc(population))

head(canada)

# Combine US & Canada, pick top 300
#=====================================

all_metros <- rbind(us1, canada %>% st_transform(st_crs(us1))) %>% 
  arrange(desc(population)) %>%
  head(300)

head(all_metros)
st_crs(all_metros)
unique(all_metros$name)

# Export shapefile
#=====================================

st_write(all_metros,
         '/Users/jpg23/data/downtownrecovery/top_300_metros/top_300_metros.geojson')
