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

filepath <- '/Users/jpg23/data/downtownrecovery/spectus_exports/albuquerque/'

rt <-
  list.files(path = paste0(filepath, 'albuquerque_rail_trail_blocks/')) %>%
  map_df(~read_delim(
    paste0(filepath, 'albuquerque_rail_trail_blocks/', .),
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

# write.csv(rt,
#           '/Users/jpg23/data/urban-displacement/eddit/albuquerque/spectus_rail_trail_blocks.csv',
#           row.names = F)

# Load device recurring area table data
#-----------------------------------------

# daily sample size for provider 230399
dra <-
  list.files(path = paste0(filepath, 'albuquerque_device_recurring_area/')) %>%
  map_df(~read_delim(
    paste0(filepath, 'albuquerque_device_recurring_area/', .),
    delim = '\001',
    col_names = c('block_group_id', 'date', 'unique_visitors'),
    col_types = c('cii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(date), format = "%Y%m%d")) %>%
  arrange(date)

head(dra)

# Get block group population for 2022
#-----------------------------------------

blgr_acs <- get_acs(
  geography = "block group",
  variables = 'B01003_001',
  year = 2019,    
  survey = "acs5",       
  state = "NM",     
  # county = "Bernalillo",
  geometry = FALSE
) %>%
  mutate(home_bg = paste0('US.NM.',
                          substr(GEOID, 3, 5),
                          '.',
                          substr(GEOID, 6, 11),
                          '.',
                          substr(GEOID, 12, 12))) %>%
  data.frame()

head(blgr_acs)
  
blgr_acs1 <- blgr_acs %>% select(home_bg, estimate)

head(blgr_acs1)

# Calculate weighted stops
#-----------------------------------------

# To estimate total numbers (sample weighted by block group population) --
# copied from 'downtownrecovery_uot/greensboro/greensboro_food_explore.ipynb':
 
#  1. join 'rt' with census data (by block group) for 2022 
#     (blgr_acs1) -- so each block group would have the same 
#     census population # across all dates = 'estimate'

with_pop <- rt %>% left_join(blgr_acs1, by = 'home_bg')

nrow(with_pop) == nrow(rt) # should be true

head(with_pop)

#  2. join 'dra' with 'with_pop' on block group ID and date, so I have a new 
#     column in the table that indicates the sample size for that home block 
#     group ('unique_visitors')

with_samp <- with_pop %>% 
  left_join(dra, by = c('home_bg' = 'block_group_id', 'date'))

nrow(with_samp) == nrow(with_pop) # should be true
head(with_samp)

#  3. create block group specific 'weight' var which represents the share of the 
#     block group population that's accounted for in the sample, which = 
#     'unique_visitors'/'estimate'

with_samp1 <- with_samp %>%
  mutate(weight = unique_visitors/estimate) %>%
  select(-c('unique_visitors', 'estimate'))

head(with_samp1)

#  4. multiply daily total stops # by reciprocal of weight (so 1/weight) to get 
#     estimate of how many total stops there were BY DAY. The weights vary by 
#     day so can't aggregate first, need to do this by day. Then aggregate this 
#     new 'estimated stat' for the whole year. Can get sum for whole year OR 
#     average daily visits (total/365 days)

with_samp2 <- with_samp1 %>%
  mutate(wt_stops = stops/weight)

head(with_samp2)
summary(with_samp2$wt_stops)
hist(with_samp2$wt_stops)

# Aggregate weighted and non-weighted
# stops over the entire year
#-----------------------------------------

# group by 'home_bg' & 'visit_block' and sum 'stops' and 'wt_stops'
yr_agg <- with_samp2 %>%
  group_by(home_bg, visit_block) %>%
  summarize(tot_stops = sum(stops, na.rm = T),
            tot_wt_stops = sum(wt_stops, na.rm = T)) %>%
  data.frame() %>%
  left_join(blgr_acs %>% select(GEOID, home_bg)) %>%
  rename(home_block_group = GEOID) %>%
  select(-home_bg) %>%
  relocate(visit_block, home_block_group)

head(yr_agg)

write.csv(yr_agg,
          '/Users/jpg23/data/urban-displacement/eddit/albuquerque/rail_trail_blocks_visits_yearly_agg.csv',
          row.names = F)
