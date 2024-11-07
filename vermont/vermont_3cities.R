#===============================================================================
# Create plots showing sample sizes / trends for Burlington, Montpelier, and
# Stowe, VT to explore whether we could use the data to study migration trends 
# in the state
#
# Date created: 12/6/23
#===============================================================================

# Load packages
#-----------------------------------------

source('~/git/timathomas/functions/functions.r')

ipak(c('tidyverse', 'lubridate', 'ggplot2', 'plotly', 
       'sf', 'leaflet', 'BAMMtools', 'gtools', 'htmlwidgets'))

# Load data
#-----------------------------------------

filepath <- 'C:/Users/jpg23/data/downtownrecovery/spectus_exports/vermont/vermont_3cities/'

vt <-
  list.files(path = filepath) %>%
  map_df(~read_delim(
    paste0(filepath, .),
    delim = '\001',
    col_names = c('city', 'provider_id', 'approx_distinct_devices_count',
                  'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date)

unique(vt$city)
unique(vt$provider_id)

vt3_plot <-
  plot_ly() %>%
  add_lines(data = vt %>% filter(provider_id == '190199'),
            x = ~date, y = ~approx_distinct_devices_count,
            name = ~paste0(city, ', 190199'),
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ', 190199: ', approx_distinct_devices_count),
            line = list(shape = "linear", color = '#d6ad09')) %>%
  add_lines(data = vt %>% filter(provider_id == '700199'),
            x = ~date, y = ~approx_distinct_devices_count,
            name = ~paste0(city, ', 700199'),
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ', 700199: ', approx_distinct_devices_count),
            line = list(shape = "linear", color = '#50437a')) %>%
  add_lines(data = vt %>% filter(provider_id == '230599'),
            x = ~date, y = ~approx_distinct_devices_count,
            name = ~paste0(city, ', 230599'),
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ', 230599: ', approx_distinct_devices_count),
            line = list(shape = "linear", color = '#97a8e8')) %>%  
  layout(title = "Daily unique device counts by provider in 3 Vermont cities",
         xaxis = list(title = "Date", zerolinecolor = "#ffff",
                      tickformat = "%b %Y"),
         yaxis = list(title = "Number of unique devices", zerolinecolor = "#ffff",
                      ticksuffix = "  "),
         shapes = list(list(y0 = 0, y1 = 1, yref = "paper",
                            x0 = as.Date('2021-05-17'), x1 = as.Date('2021-05-17'),
                            line = list(color = 'gray', dash = 'dash'))))

vt3_plot

saveWidget(
  vt3_plot,
  'C:/Users/jpg23/UDP/downtown_recovery/vermont/vt_3cities.html')
