#===============================================================================
# Compare downtowns, but use provider 190199 (imputed for Canada), 
# and standardize by MSA
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
  # change providers at 5/17/21 for Canada ONLY
  mutate(
    to_keep = case_when(
      str_detect(msa_name, '\\s') & provider_id == '190199' ~ 'yes',
      !(str_detect(msa_name, '\\s')) & 
        ((provider_id == '700199' & date < as.Date('2021-05-17')) | 
           (provider_id == '190199' & date >= as.Date('2021-05-17'))) ~ 'yes',
      TRUE ~ 'no'
    )) %>%
  filter(to_keep == 'yes') %>%
  select(-c(provider_id, to_keep)) %>%
  rename(msa_count = approx_distinct_devices_count)

msa_names <- msa %>% 
  select(msa_name) %>%
  distinct() %>%
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
  mutate(city = str_remove_all(city, '\\.'),
         city = case_when(
           city == 'Washington' ~ 'Washington DC',
           city == 'Urban Honolulu' ~ 'Honolulu',
           city == 'Louisville/Jefferson County' ~ 'Louisville',
           TRUE ~ city
         )) %>%
  add_row(city = 'Mississauga', msa_name = 'Toronto') %>%
  filter(!(msa_name == 'Portland-Vancouver-Hillsboro, OR-WA' & 
             city == 'Vancouver') & city != 'Arlington')

head(msa_names %>% data.frame(), 15)
tail(msa_names %>% data.frame(), 15)

head(msa)
glimpse(msa)
range(msa$date)
unique(msa$msa_name)


# Load downtown data
#=====================================

# Original downtown polygons, state/province
imputed <- read.csv("C:/Users/jpg23/data/downtownrecovery/imputed_canada_190199/imputation_canada_final.csv")

head(imputed)
glimpse(imputed)

# Original downtown polygons, MSA

