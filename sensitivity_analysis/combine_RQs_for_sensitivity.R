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

ipak(c('tidyverse', 'lubridate', 'plotly'))

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

# Load Honolulu data
#=====================================

downtown_filepath <- 'C:/Users/jpg23/data/downtownrecovery/spectus_exports/sensitivity_analysis/'

honolulu_comm <-
  list.files(path = paste0(downtown_filepath, 'honolulu_commercial')) %>% 
  map_df(~read_delim(
    paste0(downtown_filepath, 'honolulu_commercial/', .),
    delim = '\001',
    col_names = c('city', 'provider_id', 'approx_distinct_devices_count', 
                  'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date)

# Join with MSA data
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

head(honolulu_comm)
msa %>% filter(str_detect(msa_name, 'Honolulu')) %>% head()

hono_msa <-
  honolulu_comm %>%
  filter(provider_id == '190199') %>%
  select(-provider_id) %>%
  left_join(
    msa %>% 
      filter(msa_name == 'Urban Honolulu, HI' & provider_id == '190199') %>%
      select(msa_count, date)
  )

head(hono_msa)

hono_weekly <- hono_msa %>%
  mutate(date_range_start =
           floor_date(date, unit = "week",
                      week_start = getOption("lubridate.week.start", 1))) %>%
  group_by(city, date_range_start) %>%
  summarize(downtown_devices = sum(approx_distinct_devices_count, na.rm = T),
            msa_count = sum(msa_count, na.rm = T)) %>%
  ungroup() %>%
  data.frame()

head(hono_weekly)

hono_rq <-
  hono_weekly %>%
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

hono_rq

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

commercial <- rbind(comm_rq_us, comm_rq_ca, hono_rq) %>%
  arrange(desc(rq_comm))

commercial
  
### 2. Byeonghwa's office boundary
#-------------------------------------------------------

# ???

# office <- ???

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

# Join with MSA
#=====================================

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

# 1/1/2019 - 12/26/2021
city_defined1 <- 
  list.files(path = paste0(downtown_filepath, 'sarah_citydefined_1')) %>% 
  map_df(~read_delim(
    paste0(downtown_filepath, 'sarah_citydefined_1/', .),
    delim = '\001',
    col_names = c('city', 'provider_id', 'approx_distinct_devices_count', 
                  'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date) %>%
  filter(date < as.Date('2021-12-27') & !is.na(city))

# 12/27/2021 - 5/7/2023
city_defined2 <- 
  list.files(path = paste0(downtown_filepath, 'sarah_citydefined_2')) %>% 
  map_df(~read_delim(
    paste0(downtown_filepath, 'sarah_citydefined_2/', .),
    delim = '\001',
    col_names = c('city', 'provider_id', 'approx_distinct_devices_count', 
                  'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date) %>%
  filter(date < as.Date('2023-05-07') & !is.na(city))

# 5/7/2023 - 6/18/2023
city_defined3 <- 
  list.files(path = paste0(downtown_filepath, 'sarah_citydefined_3')) %>% 
  map_df(~read_delim(
    paste0(downtown_filepath, 'sarah_citydefined_3/', .),
    delim = '\001',
    col_names = c('city', 'provider_id', 'approx_distinct_devices_count', 
                  'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date) %>%
  filter(!is.na(city) & str_detect(city, '\\s\\w{2}$'))

# Dallas, Atlanta, Portland, Wichita
city_defined4 <- 
  list.files(path = paste0(downtown_filepath, 'sarah_citydefined_4')) %>% 
  map_df(~read_delim(
    paste0(downtown_filepath, 'sarah_citydefined_4/', .),
    delim = '\001',
    col_names = c('city', 'provider_id', 'approx_distinct_devices_count', 
                  'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date) %>%
  filter(!is.na(city))

city_defined_both <- 
  rbind(city_defined1, city_defined2, city_defined3, city_defined4) %>%
  mutate(city = case_when(
    city == 'Washington DC' ~ 'Washington DC',
    city == 'Quebec City QC' ~ 'Quebec',
    city == 'New York City NY' ~ 'New York',
    city == 'St. Louis MO' ~ 'St Louis',
    city == 'Colorado Springs, CO' ~ 'Colorado Springs',
    city == 'Tuscon AZ' ~ 'Tucson',
    city == 'Philidelphia PA ' ~ 'Philadelphia',
    TRUE ~ str_remove(city, '\\s\\w{2}\\s*$'))) %>%
  filter(!is.na(city))

head(city_defined_both)
unique(city_defined_both$city)

# Make sure the data look right
check_citydefined <-
  plot_ly() %>%
  add_lines(data = city_defined_both %>% filter(provider_id == '700199'),
            x = ~date, y = ~approx_distinct_devices_count,
            split = ~city,
            name = ~paste0(city, ' - ', provider_id),
            opacity = .6,
            line = list(shape = "linear", color = 'purple')) %>%
  add_lines(data = city_defined_both %>% filter(provider_id == '190199'),
            x = ~date, y = ~approx_distinct_devices_count,
            split = ~city,
            name = ~paste0(city, ' - ', provider_id),
            opacity = .6,
            line = list(shape = "linear", color = 'orange')) %>%
  layout(title = "Unique devices by provider and date (city-defined)",
         xaxis = list(title = "Date", zerolinecolor = "#ffff",
                      tickformat = "%b %Y"),
         yaxis = list(title = "# of unique devices", zerolinecolor = "#ffff",
                      ticksuffix = "  "))

check_citydefined

city_cd <- unique(city_defined_both$city)
city_msa <- unique(msa_names$city)

setdiff(city_cd, city_msa)
setdiff(city_msa, city_cd)

n_distinct(city_defined_both$city)
n_distinct(msa_names$city)

city_defined_msa <- 
  city_defined_both %>% 
  left_join(msa_names) %>%
  left_join(msa, by = c('msa_name', 'provider_id', 'date')) %>%
  mutate(date_range_start =
           floor_date(date, unit = "week",
                      week_start = getOption("lubridate.week.start", 1))) %>%
  group_by(city, provider_id, date_range_start) %>%
  summarize(downtown_devices = sum(approx_distinct_devices_count, na.rm = T),
            msa_count = sum(msa_count, na.rm = T)) %>%
  ungroup() %>%
  data.frame() %>%
  filter(!is.na(provider_id) & provider_id != '230599') %>%
  mutate(normalized = downtown_devices/msa_count)

head(city_defined_msa)

# write.csv(
#   city_defined_msa,
#   'C:/Users/jpg23/data/downtownrecovery/sensitivity_analysis/citydefined_for_imputation.csv',
#   row.names = F
# )

# Calculate RQs for US
#=====================================

head(city_defined_msa)

citydefined_rq_us <-
  city_defined_msa %>%
  filter(!city %in% canada_dt & provider_id == '190199') %>%
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
  mutate(rq_citydefined = ntv2023/ntv2019) %>%
  data.frame() %>%
  select(city, rq_citydefined) %>%
  arrange(desc(rq_citydefined))

citydefined_rq_us
  
# Calculate RQs for Canada
#=====================================
  
imputed_citydefined_nohalmis <- read.csv('C:/Users/jpg23/data/downtownrecovery/sensitivity_analysis/imputation_citydefined_SAITS.csv')
  
imputed_hal_mis <- read.csv('C:/Users/jpg23/data/downtownrecovery/sensitivity_analysis/imputation_citydefined_SAITS(halifex, mississauga).csv') %>%
  separate(date_range_start, into = c("day", "month", "year"), sep = "/") %>%
  mutate(date_range_start = paste(year, month, day, sep = "-")) %>%
  select(-c(day, month, year))

imputed_citydefined0 <- 
  imputed_citydefined_nohalmis %>%
  select(-matches('Halifax|Mississauga')) %>%
  full_join(imputed_hal_mis)

colnames(imputed_citydefined0) <- 
  gsub("normalized.", "", colnames(imputed_citydefined0)) 

colnames(imputed_citydefined0) <- 
  gsub(".7", "_7", colnames(imputed_citydefined0))

colnames(imputed_citydefined0) <- 
  gsub("\\.19", "_19", colnames(imputed_citydefined0))

names(imputed_citydefined0)

imputed_citydefined <- imputed_citydefined0 %>%
  pivot_longer(
    cols = contains('199'),
    names_to = c('city', 'provider_id'),
    names_pattern = '(.*)_(.*)',
    values_to = 'normalized'
  ) %>%
  mutate(city = str_replace_all(city, '\\.', ' '))

head(imputed_citydefined)

good_cities <- unique(city_defined_msa$city)
imputed_cities <- unique(imputed_citydefined$city)

# Make sure the cities are spelled consistently
setdiff(good_cities, imputed_cities)
setdiff(imputed_cities, good_cities)

# Plot normalized counts - does imputation look correct?
check_citydefined_imp <-
  plot_ly() %>%
  add_lines(data = imputed_citydefined %>% filter(provider_id == '700199'),
            x = ~date_range_start, y = ~normalized,
            split = ~city,
            name = ~paste0(city, ' - ', provider_id),
            opacity = .6,
            line = list(shape = "linear", color = 'purple')) %>%
  add_lines(data = imputed_citydefined %>% filter(provider_id == '190199'),
            x = ~date_range_start, y = ~normalized,
            split = ~city,
            name = ~paste0(city, ' - ', provider_id),
            opacity = .6,
            line = list(shape = "linear", color = 'orange')) %>%
  layout(title = "IMPUTED FOR CANADA: Normalized unique devices by provider and week (city-defined)",
         xaxis = list(title = "Date", zerolinecolor = "#ffff",
                      tickformat = "%b %Y"),
         yaxis = list(title = "Normalized # of unique devices", zerolinecolor = "#ffff",
                      ticksuffix = "  "))

check_citydefined_imp

citydefined_rq_ca <- 
  imputed_citydefined %>%
  filter(city %in% canada_dt & provider_id == '190199' &
           ((date_range_start >= as.Date('2019-03-04') & 
            date_range_start <= as.Date('2019-06-10')) |
           (date_range_start >= as.Date('2023-02-27')))) %>%
  mutate(week_num = isoweek(date_range_start),
         year = year(date_range_start)) %>%
  select(-c(date_range_start, provider_id)) %>%
  pivot_wider(
    id_cols = c('city', 'week_num'),
    names_from = 'year',
    names_prefix = 'ntv',
    values_from = 'normalized'
  ) %>%
  mutate(rec2023 = ntv2023/ntv2019) %>%
  data.frame() %>%
  group_by(city) %>%
  summarize(rq_citydefined = mean(rec2023, na.rm = T)) %>%
  ungroup() %>%
  data.frame() %>%
  arrange(desc(rq_citydefined))
  
# Combine Canada & US RQs
#=====================================

citydefined_rq_us
citydefined_rq_ca

city_defined <- rbind(citydefined_rq_us, citydefined_rq_ca) %>%
  arrange(desc(rq_citydefined))

city_defined


# Combine into one dataset
#-------------------------------------------------------------------------------

nrow(commercial)
# nrow(office)
nrow(old_zip)
nrow(hdbscan)
nrow(city_defined)

all_rq <-
  commercial %>%
  # left_join(office) %>%
  full_join(old_zip) %>%
  full_join(hdbscan %>% rename(rq_hdbscan = seasonal_average)) %>%
  full_join(city_defined)

all_rq

write.csv(
  all_rq,
  'C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/compared_RQs_sensitivity.csv',
  row.names = F
)
  