#===============================================================================
# Create shapefile of all the CBSAs in the US & Canada (for normalization)
#===============================================================================

# Load packages
#=====================================

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'tigris', 'sf', 'stringr'))

# Load US shapefile
#=====================================

# cbsa_raw <- combined_statistical_areas(cb = TRUE, year = 2021)
cbsa_raw <- core_based_statistical_areas(cb = TRUE, year = 2021)

head(cbsa_raw)

US_city_list <- 
    paste0('\\b(Albuquerque|Atlanta|Austin|Bakersfield|Baltimore|Boston|Charlotte|',
    'Chicago|Cincinnati|Cleveland|Colorado Springs|Columbus|Dallas|Denver|',
    'Detroit|El Paso|Fort Worth|Fresno|Honolulu|Houston|Indianapolis|',
    'Jacksonville|Kansas City|Las Vegas|Los Angeles|Louisville|Memphis|Miami|', 
    'Milwaukee|Minneapolis|Nashville|New Orleans|New York|Oakland|Oklahoma City|',
    'Omaha|Orlando|Philadelphia|Phoenix|Pittsburgh|Portland|Raleigh|Sacramento|',
    'Salt Lake City|San Antonio|San Diego|San Francisco|San Jose|Seattle|', 
    'St\\. Louis|Tampa|Tucson|Tulsa|Washington|Wichita)\\b')

cbsa <- cbsa_raw %>% 
  mutate(cbsa_name = gsub('-.*$', '', NAME)) %>%
  filter(str_detect(cbsa_name, US_city_list) &
           !NAME %in% c('Austin, MN', 'Cleveland, MS', 'Cleveland, TN',
                        'Columbus, GA-AL', 'Columbus, IN', 'Columbus, MS', 
                        'Columbus, NE', 'Jacksonville, IL', 'Jacksonville, NC',
                        'Jacksonville, TX', 'Las Vegas, NM', 'Miami, OK',
                        'New Philadelphia-Dover, OH', 
                        'Portland-South Portland, ME',
                        'Washington Court House, OH', 'Washington, IN', 
                        'Washington, NC', 'Wichita Falls, TX')) %>%
  select(msa_name = NAME) %>%
  arrange(msa_name)

head(cbsa)
unique(cbsa$msa_name)
n_distinct(cbsa$msa_name)
    
# Load Canada shapefile
#=====================================

canada_city_list <- 
  paste0('\\b(Calgary|Edmonton|Halifax|London|Mississauga|Montreal|Ottawa|',
  'Quebec|Toronto|Vancouver|Winnipeg)\\b')

# CMA (census metropolitan area)
cma_raw <- st_read('C:/Users/jpg23/data/downtownrecovery/sensitivity_analysis/CMAs/canada_CMAs/lcma000b21a_e.shp')

head(cma_raw)

cma <- cma_raw %>%
  mutate(msa_name0 = word(CMANAME, 1),
         msa_name = str_replace_all(msa_name0, 'é', 'e')) %>%
  filter(str_detect(msa_name, canada_city_list)) %>%
  select(msa_name) %>%
  arrange(msa_name) %>%
  group_by(msa_name) %>% 
  summarize(geometry = st_union(geometry)) %>%
  ungroup()

head(cma)
unique(cma$msa_name)

# Combine US & Canada
#=====================================

head(cbsa)
head(cma)

st_crs(cbsa)
st_crs(cma)

all_msa <- rbind(cbsa, cma %>% st_transform(st_crs(cbsa)))
st_crs(all_msa)

unique(all_msa$msa_name)

st_write(
  all_msa, 
  'C:/Users/jpg23/data/downtownrecovery/sensitivity_analysis/CMAs/all_MSAs.geojson')
