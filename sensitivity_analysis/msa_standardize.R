#===============================================================================
# Standardize downtown counts using MSA, not state/province
#===============================================================================

# Load packages
#=====================================

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'sf', 'lubridate', 'leaflet', 'plotly', 'htmlwidgets',
       'tidyr'))

# Load MSA data
#=====================================

s_filepath <- 'C:/Users/jpg23/data/downtownrecovery/spectus_exports/sensitivity_analysis/'

msa <-
  list.files(path = paste0(s_filepath, 'MSA')) %>% 
  map_df(~read_delim(
    paste0(s_filepath, 'MSA/', .),
    delim = '\001',
    col_names = c('msa_name', 'provider_id', 'approx_distinct_devices_count', 
                  'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date) %>%
  filter((provider_id == '700199' & date < as.Date('2021-05-17')) | 
           (provider_id == '190199' & date >= as.Date('2021-05-17'))) %>%
  select(-provider_id)

msa_names <- msa %>% 
  mutate(msa_no_state = str_remove_all(msa_name, ',.*$')) %>%
  separate(msa_no_state, remove = FALSE, sep = '-',
           into = c('name1', 'name2', 'name3', 'name4', 'name5', 'name6')) %>%
  pivot_longer(
    cols = starts_with('name'),
    names_to = 'city',
    values_to = 'city_value'
  ) %>%
  select(msa_name, city = city_value) %>%
  filter(!is.na(city) & !city %in% c('', ' ')) %>%
  mutate(city = str_remove_all(city, '\\.'))

head(msa_names %>% data.frame(), 15)
tail(msa_names %>% data.frame(), 15)

head(msa)
glimpse(msa)
range(msa$date)
unique(msa$msa_name)

# Load downtown data
#=====================================

downtown_filepath <- 'C:/Users/jpg23/data/downtownrecovery/spectus_exports/sensitivity_analysis/'
newprov_filepath <- 'C:/Users/jpg23/data/downtownrecovery/spectus_exports/new_provider_230599/'

# Original polygons (spectus only) - pre-2023
orig_spec1 <-
  list.files(path = paste0(downtown_filepath, 'original_downtowns_pre2023')) %>% 
  map_df(~read_delim(
    paste0(downtown_filepath, 'original_downtowns_pre2023/', .),
    delim = '\001',
    col_names = c('city', 'provider_id', 'approx_distinct_devices_count', 
                  'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date) %>%
  mutate(cat = 'original')

# Original polygons (spectus only) - 2023
orig_spec2 <-
  list.files(path = paste0(newprov_filepath, 'downtown_20190101_20230721')) %>% 
  map_df(~read_delim(
    paste0(newprov_filepath, 'downtown_20190101_20230721/', .),
    delim = '\001',
    col_names = c('city', 'provider_id', 'approx_distinct_devices_count', 
                  'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date) %>%
  filter(date <= as.Date('2023-06-18')) %>%
  mutate(cat = 'original')

head(orig_spec1)
head(orig_spec2)

# Combine pre-2023 and 2023 original downtowns
orig_spec <- rbind(orig_spec1, orig_spec2) %>%
  filter(city %in% c('Cleveland', 'Halifax', 'Portland', 'Salt Lake City',
                     'San Diego', 'San Francisco', 'St Louis', 'Toronto',
                     'Nashville', 'Calgary', 'Edmonton', 'Montreal', 'Ottawa',
                     'Vancouver'))

# LEHD clusters (Cleveland + SLC + SF) & Will Hollingsworth's Portland
lehd_portland <-
  list.files(path = paste0(downtown_filepath, 'lehd_and_portland')) %>% 
  map_df(~read_delim(
    paste0(downtown_filepath, 'lehd_and_portland/', .),
    delim = '\001',
    col_names = c('city', 'provider_id', 'approx_distinct_devices_count', 
                  'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date) %>%
  mutate(cat = 'lehd_portland')

# Cleveland Downtown Alliance (Sean McDonnell) & LEHD clusters (St Louis & San Diego)
c_stl_sd <-
  list.files(path = paste0(downtown_filepath, 'cleveland_stlouis_sandiego')) %>% 
  map_df(~read_delim(
    paste0(downtown_filepath, 'cleveland_stlouis_sandiego/', .),
    delim = '\001',
    col_names = c('city', 'provider_id', 'approx_distinct_devices_count', 
                  'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date) %>%
  mutate(cat = 'c_stl_sd')

# City-defined (Toronto, St Louis, Salt Lake City, San Francisco, Portland,
# Halifax, Cleveland, San Diego)
city_defined <-
  list.files(path = paste0(downtown_filepath, 'city_defined')) %>%
  map_df(~read_delim(
    paste0(downtown_filepath, 'city_defined/', .),
    delim = '\001',
    col_names = c('city', 'provider_id', 'approx_distinct_devices_count',
                  'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date) %>%
  mutate(cat = 'city_defined',
         city = case_when(
           city == 'stl' ~ 'St Louis',
           city == 'slc' ~ 'Salt Lake City',
           city == 'sf' ~ 'San Francisco',
           city == 'sandiego' ~ 'San Diego',
           TRUE ~ str_to_title(city)
         ))

# Byeonghwa - San Francisco, Cleveland, Portland, Salt Lake City and Toronto
# (method 1: including retail)
byeonghwa_retail_1 <-
  list.files(path = paste0(downtown_filepath, 'byeonghwa_group1')) %>%
  map_df(~read_delim(
    paste0(downtown_filepath, 'byeonghwa_group1/', .),
    delim = '\001',
    col_names = c('city', 'provider_id', 'approx_distinct_devices_count',
                  'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date) %>%
  mutate(cat = 'byeonghwa_retail_1')

# Byeonghwa - office-only (SF, Cleveland, Portland, SLC, Toronto, Calgary, 
# Edmonton, Montreal, Ottawa, and Vancouver)
byeonghwa_office <-
  list.files(path = paste0(downtown_filepath, 'byeonghwa_office')) %>%
  map_df(~read_delim(
    paste0(downtown_filepath, 'byeonghwa_office/', .),
    delim = '\001',
    col_names = c('city', 'provider_id', 'approx_distinct_devices_count',
                  'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date) %>%
  mutate(cat = 'byeonghwa_office')

# City-defined Nashville (based on Planning Department polygon sent by Crissy Cassetty)
nash <-
  list.files(path = paste0(downtown_filepath, 'nashville_citydefined')) %>%
  map_df(~read_delim(
    paste0(downtown_filepath, 'nashville_citydefined/', .),
    delim = '\001',
    col_names = c('city', 'provider_id', 'approx_distinct_devices_count',
                  'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date) %>%
  mutate(cat = 'nashville')

# Combine them all
downtown <- rbind(orig_spec, lehd_portland, c_stl_sd, city_defined, 
                  byeonghwa_retail_1, byeonghwa_office, nash) %>%
  filter(date <= as.Date('2023-06-18') & # last date for provider 190199
           # change providers at 5/17/21
           ((provider_id == '700199' & date < as.Date('2021-05-17')) | 
              (provider_id == '190199' & date >= as.Date('2021-05-17')))) %>%
  mutate(full_name = case_when(
    city == 'Portland' & cat == 'lehd_portland' ~ 'Portland (Will Hollingsworth)',
    city == 'Cleveland' & cat == 'lehd_portland' ~ 'Cleveland (LEHD)',
    city == 'Salt Lake City' & cat == 'lehd_portland' ~ 'Salt Lake City (LEHD)',
    city == 'San Francisco' & cat == 'lehd_portland' ~ 'San Francisco (LEHD)',
    city == 'Cleveland' & cat == 'c_stl_sd' ~ 'Cleveland (Downtown Alliance)',
    city == 'St Louis' & cat == 'c_stl_sd' ~ 'St Louis (LEHD)',
    city == 'San Diego' & cat == 'c_stl_sd' ~ 'San Diego (LEHD)',
    cat == 'city_defined' ~ paste0(city, ' (city-defined)'),
    cat == 'original' ~ paste0(city, ' (original - spectus only)'),
    cat == 'byeonghwa_retail_1' ~ paste0(city, ' (Byeonghwa - retail/office)'),
    cat == 'byeonghwa_office' ~ paste0(city, ' (Byeonghwa - office only)'),
    cat == 'nashville' ~ 'Nashville (Planning Dept)',
    TRUE ~ NA_character_
  )) %>%
  select(-c(provider_id, cat))

head(downtown)
glimpse(downtown)
range(downtown$date)
unique(downtown$city)
unique(downtown$full_name)

# Join downtowns with MSA
#=====================================

head(downtown)
head(msa)
head(msa_names)

downtown1 <- downtown %>%
  left_join(msa_names)

head(downtown1)

downtown1 %>% filter(is.na(msa_name))
