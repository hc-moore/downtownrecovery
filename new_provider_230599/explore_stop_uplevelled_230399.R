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


# Plot weekly % change in normalized
# stops
#=====================================

weekly_change <-
  final_df %>%
  mutate(
    date_range_start = floor_date(
      date,
      unit = "week",
      week_start = getOption("lubridate.week.start", 1))) %>%
  group_by(city, date_range_start) %>%
  summarize(n_stops = sum(n_stops, na.rm = T),
            n_stops_msa = sum(n_stops_msa, na.rm = T)) %>%
  ungroup() %>%
  mutate(normalized_stops = n_stops/n_stops_msa) %>%
  data.frame() %>%
  # Add lag variable (previous week's normalized_stops)
  arrange(city, date_range_start) %>%
  # Filter out weeks that contain 8/23 & 12/15
  filter(date_range_start != as.Date('2023-08-21') &
           date_range_start != as.Date('2023-08-14') &
           date_range_start != as.Date('2023-12-11')) %>%
  group_by(city) %>%
  mutate(prev_norm_stops = lag(normalized_stops, n = 1),
         perc_change = (normalized_stops - prev_norm_stops)/prev_norm_stops) %>%
  ungroup() %>%
  data.frame() %>%
  select(city, date_range_start, perc_change)

head(weekly_change)

weekly_change_plot <-
  plot_ly() %>%
  add_lines(data = weekly_change,
            x = ~date_range_start, y = ~perc_change,
            name = ~city,
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ': ', round(perc_change, 3)),
            line = list(shape = "linear", color = '#e89b1e')) %>%
  layout(title = "Weekly percent change in total stops from provider 230399 (stop_uplevelled table), normalized by MSA",
         xaxis = list(title = "Week", zerolinecolor = "#ffff",
                      tickformat = "%Y-%m-%d"),
         yaxis = list(title = "Percent change from previous week", zerolinecolor = "#ffff",
                      ticksuffix = "  "))

weekly_change_plot

saveWidget(
  weekly_change_plot,
  'C:/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/weekly_change_plot.html')

# Plot monthly % change in normalized
# stops
#=====================================

monthly_change <-
  final_df %>%
  # Filter out problematic dates
  filter(date != as.Date('2023-08-22') &
           date != as.Date('2023-08-23') &
           date != as.Date('2023-12-15')) %>%
  mutate(
    date_range_start = floor_date(
      date,
      unit = "month")) %>%
  group_by(city, date_range_start) %>%
  summarize(n_stops = sum(n_stops, na.rm = T),
            n_stops_msa = sum(n_stops_msa, na.rm = T)) %>%
  ungroup() %>%
  mutate(normalized_stops = n_stops/n_stops_msa) %>%
  data.frame() %>%
  # Add lag variable (previous week's normalized_stops)
  arrange(city, date_range_start) %>%
  group_by(city) %>%
  mutate(prev_norm_stops = lag(normalized_stops, n = 1),
         perc_change = (normalized_stops - prev_norm_stops)/prev_norm_stops) %>%
  ungroup() %>%
  data.frame() %>%
  select(city, date_range_start, perc_change)

head(monthly_change)

monthly_change_plot <-
  plot_ly() %>%
  add_lines(data = monthly_change,
            x = ~date_range_start, y = ~perc_change,
            name = ~city,
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ': ', round(perc_change, 3)),
            line = list(shape = "linear", color = '#e89b1e')) %>%
  layout(title = "Monthly percent change in total stops from provider 230399 (stop_uplevelled table), normalized by MSA",
         xaxis = list(title = "Month", zerolinecolor = "#ffff",
                      tickformat = "%b %Y"),
         yaxis = list(title = "Percent change from previous month", zerolinecolor = "#ffff",
                      ticksuffix = "  "))

monthly_change_plot

saveWidget(
  monthly_change_plot,
  'C:/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/monthly_change_plot.html')


# Plot avg. weekly % change for
# June-July-August and Sept-Oct-Nov
#=====================================

head(weekly_change)

seasons <-
  weekly_change %>%
  mutate(
    period = case_when(
      month(date_range_start) %in% c(6, 7, 8) ~ 'summer',
      month(date_range_start) %in% c(9, 10, 11) ~ 'fall',
      TRUE ~ NA_character_
    )
  ) %>%
  filter(!is.na(period)) %>%
  group_by(period, city) %>%
  summarize(avg_weekly_perc_change = mean(perc_change, na.rm = T)) %>%
  data.frame()

head(seasons)

summer_ranking <-
  seasons %>%
  filter(period == 'summer') %>%
  arrange(desc(avg_weekly_perc_change)) %>%
  select(-period)

summer_ranking

write.csv(summer_ranking, 
          'C:/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/summer_ranking.csv',
          row.names = F)

fall_ranking <-
  seasons %>%
  filter(period == 'fall') %>%
  arrange(desc(avg_weekly_perc_change)) %>%
  select(-period)

fall_ranking

write.csv(fall_ranking, 
          'C:/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/fall_ranking.csv',
          row.names = F)
