#===============================================================================
# Explore June - Dec 2023 data from provider 230399 from stop_uplevelled table 
# for all 60+ HDBSCAN downtowns
#===============================================================================

# Load packages
#=====================================

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'sf', 'lubridate', 'leaflet', 'plotly', 'htmlwidgets'))

# Load downtown & MSA data
#=====================================

filepath <- 'C:/Users/jpg23/data/downtownrecovery/spectus_exports/stop_uplevelled_230399_2023/'

dt <-
  list.files(path = paste0(filepath, 'downtown/')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'downtown/', .),
    delim = '\001',
    col_names = c('city', 'zone_date', 'provider_id', 'n_stops', 'n_distinct_devices'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-c(zone_date, provider_id))

msa <-
  list.files(path = paste0(filepath, 'msa/')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'msa/', .),
    delim = '\001',
    col_names = c('msa_name', 'zone_date', 'provider_id', 'n_stops', 'n_distinct_devices'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-c(zone_date, provider_id)) %>%
  rename(n_stops_msa = n_stops, n_distinct_devices_msa = n_distinct_devices)

glimpse(dt)
glimpse(msa)

# Join them
#=====================================

msa_names <- read.csv('C:/Users/jpg23/data/downtownrecovery/sensitivity_analysis/msa_names.csv')

msa_cities <- unique(msa_names$city)
dt_cities <- unique(dt$city)

setdiff(dt_cities, msa_cities)
setdiff(msa_cities, dt_cities)

dt1 <- dt %>% left_join(msa_names)

head(dt1)
head(msa)

dt1 %>% filter(is.na(msa_name)) # should be no rows

final_df <-
  dt1 %>%
  left_join(msa, by = c('msa_name', 'date')) %>%
  mutate(normalized_distinct = n_distinct_devices/n_distinct_devices_msa,
         normalized_stops = n_stops/n_stops_msa)

head(final_df)

# Plot daily unique devices
#=====================================

# Raw numbers
unique_raw_plot <-
  plot_ly() %>%
  add_lines(data = final_df,
            x = ~date, y = ~n_distinct_devices,
            name = ~paste0(city, ': downtown'),
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ' downtown: ', round(n_distinct_devices, 3)),
            line = list(shape = "linear", color = '#445e3d')) %>%
  add_lines(data = final_df,
            x = ~date, y = ~n_distinct_devices_msa,
            name = ~paste0(city, ': MSA'),
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ' MSA: ', round(n_distinct_devices_msa, 3)),
            line = list(shape = "linear", color = '#b0c4ab')) %>%  
  layout(title = "Daily unique devices from provider 230399 (stop_uplevelled table), HDBSCAN downtown and MSA",
         xaxis = list(title = "Day", zerolinecolor = "#ffff",
                      tickformat = "%Y-%m-%d"),
         yaxis = list(title = "Unique devices", zerolinecolor = "#ffff",
                      ticksuffix = "  "))

unique_raw_plot

saveWidget(
  unique_raw_plot,
  'C:/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/unique_plot.html')

# Normalized
unique_norm_plot <-
  plot_ly() %>%
  add_lines(data = final_df,
            x = ~date, y = ~normalized_distinct,
            name = ~city,
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ': ', round(normalized_distinct, 3)),
            line = list(shape = "linear", color = '#6665a8')) %>%
  layout(title = "Daily unique devices from provider 230399 (stop_uplevelled table), normalized by MSA",
         xaxis = list(title = "Day", zerolinecolor = "#ffff",
                      tickformat = "%Y-%m-%d"),
         yaxis = list(title = "Normalized unique devices", zerolinecolor = "#ffff",
                      ticksuffix = "  "))

unique_norm_plot

saveWidget(
  unique_norm_plot,
  'C:/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/unique_plot_norm.html')

# Plot daily normalized stops
#=====================================

# Raw numbers
stops_raw_plot <-
  plot_ly() %>%
  add_lines(data = final_df,
            x = ~date, y = ~n_stops,
            name = ~paste0(city, ': downtown'),
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ' downtown: ', round(n_stops, 3)),
            line = list(shape = "linear", color = '#c2580e')) %>%
  add_lines(data = final_df,
            x = ~date, y = ~n_stops_msa,
            name = ~paste0(city, ': MSA'),
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ' MSA: ', round(n_stops_msa, 3)),
            line = list(shape = "linear", color = '#f09a5d')) %>%  
  layout(title = "Daily stops from provider 230399 (stop_uplevelled table), HDBSCAN downtown and MSA",
         xaxis = list(title = "Day", zerolinecolor = "#ffff",
                      tickformat = "%Y-%m-%d"),
         yaxis = list(title = "Stops", zerolinecolor = "#ffff",
                      ticksuffix = "  "))

stops_raw_plot

saveWidget(
  stops_raw_plot,
  'C:/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/stops_plot.html')

# Normalized
stops_norm_plot <-
  plot_ly() %>%
  add_lines(data = final_df,
            x = ~date, y = ~normalized_stops,
            name = ~city,
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ': ', round(normalized_stops, 3)),
            line = list(shape = "linear", color = '#6e3849')) %>%
  layout(title = "Daily stops from provider 230399 (stop_uplevelled table), normalized by MSA",
         xaxis = list(title = "Day", zerolinecolor = "#ffff",
                      tickformat = "%Y-%m-%d"),
         yaxis = list(title = "Normalized stops", zerolinecolor = "#ffff",
                      ticksuffix = "  "))

stops_norm_plot

saveWidget(
  stops_norm_plot,
  'C:/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/stops_plot_norm.html')
