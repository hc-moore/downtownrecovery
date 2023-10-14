#===============================================================================
# Calculate recovery rates (and show trends) for all cities' downtowns
#   - definitions using HDBSCAN
#   - standardized by MSA
#   - 190199 only - Canadian data imputed pre-5/17/21
#===============================================================================

# Load packages
#-----------------------------------------

source('~/git/timathomas/functions/functions.r')

ipak(c('tidyverse', 'lubridate', 'ggplot2', 'plotly', 
       'sf', 'leaflet', 'BAMMtools', 'gtools', 'htmlwidgets'))

# Load downtown data
#-----------------------------------------

filepath <- 'C:/Users/jpg23/data/downtownrecovery/spectus_exports/sensitivity_analysis/'

not_t_m <-
  list.files(path = paste0(filepath, 'hdbscan_all')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'hdbscan_all/', .),
    delim = '\001',
    col_names = c('city', 'provider_id', 'approx_distinct_devices_count', 
                  'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date) %>%
  filter(city != 'Toronto' & provider_id != '230599')

unique(not_t_m$city)
unique(not_t_m$provider_id)

t_m <-
  list.files(path = paste0(filepath, 'hdbscan_t_m')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'hdbscan_t_m/', .),
    delim = '\001',
    col_names = c('city', 'provider_id', 'approx_distinct_devices_count', 
                  'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date) %>%
  filter(provider_id != '230599')

unique(t_m$city)
unique(t_m$provider_id)

# Stack them
dt <- 
  rbind(not_t_m, t_m) %>%
  mutate(city = str_remove(city, '\\s\\w{2}$'),
         city = case_when(
           city == 'Indianapolis city (balance)' ~ 'Indianapolis',
           city == 'Nashville-Davidson metropolitan government (balance)' ~ 
             'Nashville',
           city == "Ottawa - Gatineau (Ontario part / partie de l'Ontario)" ~
             'Ottawa',
           city == 'Urban Honolulu' ~ 'Honolulu',
           city == 'Montréal' ~ 'Montreal',
           city == 'St. Louis' ~ 'St Louis',
           city == 'Washington' ~ 'Washington DC',
           city == 'Québec' ~ 'Quebec',
           TRUE ~ city
           )) %>%
  rename(downtown_devices = approx_distinct_devices_count)

head(dt)
n_distinct(dt$city)
unique(dt$city)

# Load MSA data
#-----------------------------------------

msa <-
  list.files(path = paste0(filepath, 'MSA')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'MSA/', .),
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

head(msa)
unique(msa$provider_id)

head(msa_names)

# Join downtown & MSA data
#-----------------------------------------

msa_cities <- unique(msa_names$city)
dt_cities <- unique(dt$city)

setdiff(dt_cities, msa_cities)

dt1 <- dt %>% left_join(msa_names)

head(dt1)
head(msa)

dt1 %>% filter(is.na(msa_name)) # should be no rows

final_df <- 
  dt1 %>% 
  left_join(msa, by = c('msa_name', 'provider_id', 'date'))

head(final_df)

# Export normalized counts for imputation
#-----------------------------------------

for_imputation <-
  final_df %>%
  mutate(
    date_range_start = floor_date(
      date,
      unit = "week",
      week_start = getOption("lubridate.week.start", 1))) %>%
  group_by(date_range_start, city, provider_id) %>%
  summarize(downtown_devices = sum(downtown_devices, na.rm = T),
            msa_count = sum(msa_count, na.rm = T)) %>%
  ungroup() %>%
  data.frame() %>%
  mutate(normalized = downtown_devices/msa_count)

head(for_imputation)
range(for_imputation$date_range_start)

write.csv(for_imputation,
          'C:/Users/jpg23/data/downtownrecovery/sensitivity_analysis/hdbscan_dt_for_imputation.csv',
          row.names = F)

# Load imputed data
#-----------------------------------------

# COMMENT OUT ALL THE ABOVE AND JUST RUN BELOW ONCE I HAVE IMPUTED DATA!!!!

imputed <- ???

# Plot
trend_by_prov <- plot_ly() %>%
  add_lines(data = imputed,
            x = ~date_range_start, y = ~normalized,
            split = ~city,
            color = ~provider_id,
            colors = c("#ffa600", "#bc5090"),
            name = ~paste0(provider_id, ' - ', city),
            text = ~paste0(provider_id, ' - ', city),
            opacity = .7,
            line = list(shape = "linear")) %>%
  layout(title = "Weekly counts normalized by MSA (HDBSCAN downtowns)",
         xaxis = list(title = "Week", zerolinecolor = "#ffff",
                      tickformat = "%b %Y"),
         yaxis = list(title = "Normalized", zerolinecolor = "#ffff",
                      ticksuffix = "  "))

trend_by_prov





# to calculate rankings:

# 1. sum downtown unique devices and MSA unique devices by city and year
# 2. calculate normalized count for overall time period by dividing downtown/MSA for each year (2019 and 2023)
# 3. divide normalized 2023 by normalized 2019

# probably need to adapt this:

rq <-
  imputed %>%
  mutate(week_num = isoweek(date_range_start), 
         year = year(date_range_start)) %>%
  select(-date_range_start) %>%
  pivot_wider(
    id_cols = c('week_num', 'city'),
    names_from = 'year',
    names_prefix = 'ntv',
    values_from = 'normalized') %>%
  mutate(rec2023 = ntv2023/ntv2019) %>%
  select(-starts_with('ntv')) %>%
  pivot_longer(
    cols = rec2023,
    names_to = 'year',
    values_to = 'rq')

# ?????