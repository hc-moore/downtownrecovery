#===============================================================================
# Explore 3 providers (700199, 230399, 230599) from stop_uplevelled table
#===============================================================================

# Load packages
#=====================================

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'lubridate', 'plotly', 'htmlwidgets'))

# Load data
#=====================================

filepath <- '/Users/jpg23/data/downtownrecovery/stop_uplevelled_3providers/'

d <-
  list.files(path = filepath) %>% 
  map_df(~read_delim(
    paste0(filepath, .),
    delim = '\001',
    col_names = c('country_code', 'zone_date', 'provider_id', 'n_stops', 
                  'n_distinct_devices'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-zone_date)

head(d)
range(d$date)

# Plot data
#=====================================

us <- d %>% filter(country_code == 'US')
ca <- d %>% filter(country_code == 'CA')

us_plot <- plot_ly() %>%
  add_lines(data = us %>% filter(provider_id == '230399'),
            x = ~date, y = ~n_distinct_devices,
            name = '230399',
            opacity = .7,
            text = ~paste0('230399: ', n_distinct_devices),
            line = list(shape = "linear", color = '#f46036')) %>%
  add_lines(data = us %>% filter(provider_id == '230599'),
            x = ~date, y = ~n_distinct_devices,
            name = '230599',
            opacity = .7,
            text = ~paste0('230599: ', n_distinct_devices),
            line = list(shape = "linear", color = '#2e294e')) %>%  
  add_lines(data = us %>% filter(provider_id == '700199'),
            x = ~date, y = ~n_distinct_devices,
            name = '700199',
            opacity = .7,
            text = ~paste0('700199: ', n_distinct_devices),
            line = list(shape = "linear", color = '#1b998b')) %>%  
  layout(title = "Unique devices by provider in the US (stop_uplevelled)",
         xaxis = list(title = "Day", zerolinecolor = "#ffff",
                      tickformat = "%Y-%m-%d"),
         yaxis = list(title = "Unique devices", zerolinecolor = "#ffff",
                      ticksuffix = "  ")) 

us_plot

saveWidget(
  us_plot,
  '/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/stopuplevelled_US_3providers_april_june_2024.html')

ca_plot <- plot_ly() %>%
  add_lines(data = ca %>% filter(provider_id == '230399'),
            x = ~date, y = ~n_distinct_devices,
            name = '230399',
            opacity = .7,
            text = ~paste0('230399: ', n_distinct_devices),
            line = list(shape = "linear", color = '#f46036')) %>%
  add_lines(data = ca %>% filter(provider_id == '230599'),
            x = ~date, y = ~n_distinct_devices,
            name = '230599',
            opacity = .7,
            text = ~paste0('230599: ', n_distinct_devices),
            line = list(shape = "linear", color = '#2e294e')) %>%  
  add_lines(data = ca %>% filter(provider_id == '700199'),
            x = ~date, y = ~n_distinct_devices,
            name = '700199',
            opacity = .7,
            text = ~paste0('700199: ', n_distinct_devices),
            line = list(shape = "linear", color = '#1b998b')) %>%  
  layout(title = "Unique devices by provider in Canada (stop_uplevelled)",
         xaxis = list(title = "Day", zerolinecolor = "#ffff",
                      tickformat = "%Y-%m-%d"),
         yaxis = list(title = "Unique devices", zerolinecolor = "#ffff",
                      ticksuffix = "  ")) 

ca_plot

saveWidget(
  ca_plot,
  '/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/stopuplevelled_Canada_3providers_april_june_2024.html')
