#===============================================================================
# Explore 2023 data from stop_uplevelled table for St Louis & Toronto
#===============================================================================

# Load packages
#=====================================

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'sf', 'lubridate', 'leaflet', 'plotly', 'htmlwidgets'))

# Load data
#=====================================

filepath <- 'C:/Users/jpg23/data/downtownrecovery/spectus_exports/stop_uplevelled_toronto_stlouis/'

su <-
  list.files(path = filepath) %>% 
  map_df(~read_delim(
    paste0(filepath, .),
    delim = '\001',
    col_names = c('city', 'date', 'provider_id', 'unique_users'),
    col_types = c('c?ci')
  )) %>%
  data.frame() %>%
  arrange(date)

head(su)
range(su$date)
unique(su$city)
unique(su$provider_id)

su1 <- su %>%
  mutate(
    date_range_start = floor_date(
      date,
      unit = "week",
      week_start = getOption("lubridate.week.start", 1))) %>%
  # Calculate # of devices by week and provider
  group_by(city, date_range_start, provider_id) %>%
  summarize(n_devices = sum(unique_users, na.rm = T)) %>%
  ungroup() %>%
  mutate(mytext = paste0('<br>Week of ', date_range_start, ': ', n_devices)) %>%
  filter(date_range_start < as.Date('2023-08-07') &
           date_range_start >= as.Date('2023-01-01'))

head(su1)

su_plot <- plot_ly() %>%
  add_lines(data = su1 %>% filter(provider_id == '700199'),
            x = ~date_range_start, y = ~n_devices,
            name = ~paste0("700199 - ", city),
            linetype = ~city,
            opacity = .9,
            text = ~mytext,
            line = list(shape = "linear", color = 'purple')) %>%
  add_lines(data = su1 %>% filter(provider_id == '190199'),
            x = ~date_range_start, y = ~n_devices,
            name = ~paste0("190199 - ", city),
            linetype = ~city,
            opacity = .8,
            text = ~mytext,
            line = list(shape = "linear", color = 'darkgreen')) %>%
  add_lines(data = su1 %>% filter(provider_id == '230599'),
            x = ~date_range_start, y = ~n_devices,
            name = ~paste0("230599 - ", city),
            linetype = ~city,
            opacity = .9,
            text = ~mytext,
            line = list(shape = "linear", color = 'orange')) %>%
  add_lines(data = su1 %>% filter(provider_id == '230399'),
            x = ~date_range_start, y = ~n_devices,
            name = ~paste0("230399 - ", city),
            linetype = ~city,
            opacity = .9,
            text = ~mytext,
            line = list(shape = "linear", color = 'red'))

su_plot

saveWidget(
  su_plot,
  'C:/Users/jpg23/UDP/downtown_recovery/new_provider_230599/stop_uplevelled_2023.html')
