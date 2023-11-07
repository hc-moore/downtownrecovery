#===============================================================================
# Combine recovery rates for the following geographic definitions, for the
# sensitivity analysis:
#
# 1. Byeonghwa's commercial boundary
# 2. Byeonghwa's office boundary
# 3. our original zip code boundary
# 4. our new HDBSCAN boundary
# 5. city-defined downtowns
#
# Created 11/1/23
#===============================================================================

# Load packages
#-------------------------------------------------------------------------------

source('~/git/timathomas/functions/functions.r')

ipak(c('tidyverse', 'lubridate'))

# ipak(c('tidyverse', 'lubridate', 'ggplot2', 'plotly', 
#        'sf', 'leaflet', 'BAMMtools', 'gtools', 'htmlwidgets'))

# Load recovery rates data
#-------------------------------------------------------------------------------

### 1. Byeonghwa's commercial boundary
#-------------------------------------------------------

comm_filepath <- 'C:/Users/jpg23/data/downtownrecovery/sensitivity_analysis/byeonghwa_commercial/'

# Load US data
#=====================================

comm_us <- read.csv(paste0(comm_filepath, 'core_cb_us_count.csv')) %>%
  filter(provider_id == '190199') %>%
  select(city, date_range_start, downtown_devices = devices_core_cb, 
         msa_count = device_msa)

head(comm_us)
unique(comm_us$city)

comm_rq_us <-
  comm_us %>%
  filter((date_range_start >= as.Date('2019-03-04') &
            date_range_start <= as.Date('2019-06-10')) |
           (date_range_start >= as.Date('2023-02-27'))) %>%
  mutate(year = year(date_range_start)) %>%
  group_by(city, year) %>%
  summarize(dt = sum(downtown_devices, na.rm = T),
            msa = sum(msa_count, na.rm = T)) %>%
  ungroup() %>%
  mutate(norm = dt/msa) %>%
  pivot_wider(
    id_cols = c('city'),
    names_from = 'year',
    names_prefix = 'ntv',
    values_from = 'norm'
  ) %>%
  mutate(rq_comm = ntv2023/ntv2019) %>%
  data.frame() %>%
  select(city, rq_comm) %>%
  arrange(desc(rq_comm))

comm_rq_us

# Load Canada data
#=====================================

comm_ca <- read.csv(paste0(comm_filepath, 'imputation_Canada_msa_SAITS_core_cb_ca.csv')) %>%
  select(-X)

head(comm_ca)

comm_rq_ca <-
  comm_ca %>%
  filter(provider_id == '190199') %>%
  filter((date_range_start >= as.Date('2019-03-04') & 
            date_range_start <= as.Date('2019-06-10')) | 
           (date_range_start >= as.Date('2023-02-27'))) %>%
  mutate(week_num = isoweek(date_range_start),
         year = year(date_range_start)) %>%
  select(-date_range_start) %>%
  pivot_wider(
    id_cols = c('city', 'week_num'),
    names_from = 'year',
    names_prefix = 'ntv',
    values_from = 'normalized'
  ) %>%
  mutate(rec2023 = ntv2023/ntv2019) %>%
  data.frame() %>%
  group_by(city) %>%
  summarize(rq_comm = mean(rec2023, na.rm = T)) %>%
  ungroup() %>%
  data.frame() %>%
  arrange(desc(rq_comm))

comm_rq_ca

# Combine US & Canada commercial data
#=====================================

commercial <- rbind(comm_rq_us, comm_rq_ca) %>%
  arrange(desc(rq_comm))

commercial
  
### 2. Byeonghwa's office boundary
#-------------------------------------------------------

# ???

office <- ???

### 3. our original zip code boundary
#-------------------------------------------------------

# Load imputed MSA data for Canada
#=====================================

canada_dt <- c('Calgary', 'Edmonton', 'Halifax', 'Mississauga', 'Montreal',
               'Ottawa', 'Quebec', 'Toronto', 'Vancouver', 'Winnipeg', 'London')

imputed_zip <- read.csv('C:/Users/jpg23/data/downtownrecovery/imputed_canada_190199/imputation_Canada_msa_Byeonghwa_us_ca_SAITS.csv') %>%
  filter(city %in% canada_dt) %>%
  select(date_range_start, city, provider_id, normalized = normalized_msa)

head(imputed_zip)

zip_rq_ca <-
  imputed_zip %>%
  filter(provider_id == '190199') %>%
  filter((date_range_start >= as.Date('2019-03-04') & 
            date_range_start <= as.Date('2019-06-10')) | 
           (date_range_start >= as.Date('2023-02-27'))) %>%
  mutate(week_num = isoweek(date_range_start),
         year = year(date_range_start)) %>%
  select(-date_range_start) %>%
  pivot_wider(
    id_cols = c('city', 'week_num'),
    names_from = 'year',
    names_prefix = 'ntv',
    values_from = 'normalized'
  ) %>%
  mutate(rec2023 = ntv2023/ntv2019) %>%
  data.frame() %>%
  group_by(city) %>%
  summarize(rq = mean(rec2023, na.rm = T)) %>%
  ungroup() %>%
  data.frame() %>%
  arrange(desc(rq))

zip_rq_ca

# Load downtown data for US
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
  select(-event_date)

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
  filter(date <= as.Date('2023-06-18'))

head(orig_spec1)
head(orig_spec2)

# Combine pre-2023 and 2023 original downtowns

downtown_zip <- rbind(orig_spec1, orig_spec2) %>%
  filter(!(city %in% canada_dt))

# Load MSA data
#=====================================

msa <-
  list.files(path = paste0(downtown_filepath, 'MSA')) %>%
  map_df(~read_delim(
    paste0(downtown_filepath, 'MSA/', .),
    delim = '\001',
    col_names = c('msa_name', 'provider_id', 'approx_distinct_devices_count',
                  'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date) %>%
  rename(msa_count = approx_distinct_devices_count) %>%
  filter(provider_id != '230599')

msa_names <- read.csv('C:/Users/jpg23/data/downtownrecovery/sensitivity_analysis/msa_names.csv')

dt1_zip <- downtown_zip %>% left_join(msa_names)

final_dt_zip <-
  dt1_zip %>%
  left_join(msa, by = c('msa_name', 'provider_id', 'date'))

head(final_dt_zip)

not_imputed_us <- final_dt_zip %>%
  mutate(date_range_start = 
           floor_date(date, unit = "week", 
                      week_start = getOption("lubridate.week.start", 1))) %>%
  filter(provider_id == '190199') %>%
  group_by(city, date_range_start) %>%
  summarize(downtown_devices = sum(approx_distinct_devices_count, na.rm = T),
            msa_count = sum(msa_count, na.rm = T)) %>%
  ungroup() %>%
  data.frame()

head(not_imputed_us)

zip_rq_us <-
  not_imputed_us %>%
  filter((date_range_start >= as.Date('2019-03-04') &
            date_range_start <= as.Date('2019-06-10')) |
           (date_range_start >= as.Date('2023-02-27'))) %>%
  mutate(year = year(date_range_start)) %>%
  group_by(city, year) %>%
  summarize(dt = sum(downtown_devices, na.rm = T),
            msa = sum(msa_count, na.rm = T)) %>%
  ungroup() %>%
  mutate(norm = dt/msa) %>%
  pivot_wider(
    id_cols = c('city'),
    names_from = 'year',
    names_prefix = 'ntv',
    values_from = 'norm'
  ) %>%
  mutate(rq = ntv2023/ntv2019) %>%
  data.frame() %>%
  select(city, rq) %>%
  arrange(desc(rq))

zip_rq_us

old_zip <- rbind(zip_rq_ca, zip_rq_us) %>%
  arrange(desc(rq)) %>%
  rename(rq_zip = rq)

old_zip


### 4. our new HDBSCAN boundary
#-------------------------------------------------------

hdbscan <- read.csv("C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/final_hdbscan_for_website.csv")

head(hdbscan)


### 5. city-defined downtowns
#-------------------------------------------------------

city_defined <- ???


# Combine into one dataset
#-------------------------------------------------------------------------------

nrow(commercial)
# nrow(office)
nrow(old_zip)
nrow(hdbscan)
# nrow(city_defined)

all_rq <-
  commercial %>%
  # left_join(office) %>%
  left_join(old_zip) %>%
  left_join(hdbscan %>% rename(rq_hdbscan = seasonal_average)) # %>%
  # left_join(city_defined)

all_rq

write.csv(
  all_rq,
  'C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/compared_RQs_sensitivity.csv',
  row.names = F
)
  