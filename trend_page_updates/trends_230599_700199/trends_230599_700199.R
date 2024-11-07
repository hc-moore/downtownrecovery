#===============================================================================
# Explore trend data for a couple months, a couple cities, from
# providers 230599 vs 700199 vs 230399
#===============================================================================

# Load packages
#=====================================

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'sf', 'lubridate', 'leaflet', 'plotly', 'htmlwidgets', 
       'broom', 'forecast'))

# Load downtown data
#=====================================

# 230399 (March 1 - April 30, 2024)
dt_230399_1 <- read.csv('/Users/jpg23/data/downtownrecovery/stop_uplevelled_2023_2024/downtown_raw_march_april_2024.csv')

# 230399 (May 1 - Oct 13, 2024)
dt_230399_2 <- read.csv('/Users/jpg23/data/downtownrecovery/stop_uplevelled_2023_2024/downtown_raw_may-oct2024.csv')

filepath <- '/Users/jpg23/data/downtownrecovery/spectus_exports/trend_page_updates/230599_700199/'

# 230599 + 700199 (Feb 1 - Aug 31, 2024)
dt <-
  list.files(path = filepath) %>% 
  map_df(~read_delim(
    paste0(filepath, .),
    delim = '\001',
    col_names = c('city', 'zone_date', 'provider_id', 'n_stops', 'n_distinct_devices'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-zone_date) %>%
  filter(date >= as.Date('2024-03-01'))

head(dt_230399_1)
head(dt_230399_2)
head(dt)

range(dt_230399_1$date)
range(dt_230399_2$date)
range(dt$date)

dt_230399 <- rbind(dt_230399_1, dt_230399_2) %>%
  filter(date <= as.Date('2024-08-31') & city %in% unique(dt$city)) %>%
  mutate(provider_id = '230399')

range(dt_230399$date)
range(dt$date)

head(dt_230399)
head(dt)

all_dt <- rbind(dt, dt_230399)
table(all_dt$provider_id)
table(all_dt$city)

# Plot all providers, all cities
#=====================================

all_cities <- plot_ly() %>%
  add_lines(data = all_dt %>% filter(provider_id == '700199'),
            x = ~date, y = ~n_distinct_devices,
            name = ~paste0(provider_id, ': ', city),
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ': ', n_distinct_devices),
            line = list(shape = "linear", color = '#002642')) %>%  
  add_lines(data = all_dt %>% filter(provider_id == '230399'),
            x = ~date, y = ~n_distinct_devices,
            name = ~paste0(provider_id, ': ', city),
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ': ', n_distinct_devices),
            line = list(shape = "linear", color = '#840032')) %>%
  add_lines(data = all_dt %>% filter(provider_id == '230599'),
            x = ~date, y = ~n_distinct_devices,
            name = ~paste0(provider_id, ': ', city),
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ': ', n_distinct_devices),
            line = list(shape = "linear", color = '#e59500'))  

all_cities

saveWidget(
  all_cities,
  paste0('/Users/jpg23/UDP/downtown_recovery/trend_updates/compare_3_providers/unique_devices_3providers_stopuplevelled.html'))
