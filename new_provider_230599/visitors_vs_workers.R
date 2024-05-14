#===============================================================================
# Separately analyze stops for "workers" (8am-6pm Mon-Fri) vs "visitors" (other)
# from stop_uplevelled, provider 230399
#===============================================================================

# Load packages
#=====================================

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'sf', 'lubridate', 'leaflet', 'plotly', 'htmlwidgets'))

# Load downtown & MSA data
#=====================================

filepath <- '/Users/jpg23/data/downtownrecovery/spectus_exports/stop_uplevelled_230399_2023/'
filepath1 <- '/Users/jpg23/data/downtownrecovery/spectus_exports/trends_work_nonwork/'


#### NOTE: dt1, msa1, dt3, and msa3 all need to be re-exported!


# 3/1/23 - 5/27/23
dt1 <-
  list.files(path = paste0(filepath1, 'downtown_20230301_to_20230601/')) %>% 
  map_df(~read_delim(
    paste0(filepath1, 'downtown_20230301_to_20230601/', .),
    delim = '\001',
    col_names = c('work_hrs', 'city', 'zone_date', 'n_stops', 'n_distinct_devices'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-zone_date)

# plot_ly() %>%
#   add_lines(data = dt1,
#             x = ~date, y = ~n_stops,
#             name = ~city,
#             opacity = .7,
#             split = ~city,
#             line = list(shape = "linear", color = '#4287f5'))

# 3/1/23 - 5/30/23
msa1 <-
  list.files(path = paste0(filepath1, 'msa_20230301_to_20230601/')) %>% 
  map_df(~read_delim(
    paste0(filepath1, 'msa_20230301_to_20230601/', .),
    delim = '\001',
    col_names = c('work_hrs', 'msa_name', 'zone_date', 'n_stops', 'n_distinct_devices'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-zone_date) %>%
  rename(n_stops_msa = n_stops, n_distinct_devices_msa = n_distinct_devices)

# plot_ly() %>%
#   add_lines(data = msa1,
#             x = ~date, y = ~n_stops_msa,
#             name = ~msa_name,
#             opacity = .7,
#             split = ~msa_name,
#             line = list(shape = "linear", color = '#4287f5'))

# 6/1/23 - 1/3/24
dt2 <-
  list.files(path = paste0(filepath, 'hourly_downtown/')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'hourly_downtown/', .),
    delim = '\001',
    col_names = c('work_hrs', 'city', 'zone_date', 'n_stops', 'n_distinct_devices'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-zone_date) %>%
  filter(date <= as.Date('2024-01-01'))

# 6/1/23 - 1/2/24
msa2 <-
  list.files(path = paste0(filepath, 'hourly_msa/')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'hourly_msa/', .),
    delim = '\001',
    col_names = c('work_hrs', 'msa_name', 'zone_date', 'n_stops', 'n_distinct_devices'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-zone_date) %>%
  rename(n_stops_msa = n_stops, n_distinct_devices_msa = n_distinct_devices) %>%
  filter(date <= as.Date('2024-01-01'))

# 1/2/24 - 5/4/24
dt3 <-
  list.files(path = paste0(filepath1, 'downtown_20240102_to_20240507/')) %>% 
  map_df(~read_delim(
    paste0(filepath1, 'downtown_20240102_to_20240507/', .),
    delim = '\001',
    col_names = c('work_hrs', 'city', 'zone_date', 'n_stops', 'n_distinct_devices'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-zone_date)

# 1/2/24 - 4/18/24
msa3 <-
  list.files(path = paste0(filepath1, 'msa_20240102_to_20240507/')) %>% 
  map_df(~read_delim(
    paste0(filepath1, 'msa_20240102_to_20240507/', .),
    delim = '\001',
    col_names = c('work_hrs', 'msa_name', 'zone_date', 'n_stops', 'n_distinct_devices'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-zone_date) %>%
  rename(n_stops_msa = n_stops, n_distinct_devices_msa = n_distinct_devices)

dt <- rbind(dt1, dt2, dt3)
msa <- rbind(msa1, msa2, msa3)

head(dt)
head(msa)

range(dt$date)
range(msa$date)


# Join them
#=====================================

msa_names <- read.csv('/Users/jpg23/data/downtownrecovery/sensitivity_analysis/msa_names.csv')

msa_cities <- unique(msa_names$city)
dt_cities <- unique(dt$city)

setdiff(dt_cities, msa_cities)
setdiff(msa_cities, dt_cities)

dt_m <- dt %>% left_join(msa_names)

head(dt_m)
head(msa)

dt_m %>% filter(is.na(msa_name)) # should be no rows

final_df <-
  dt_m %>%
  left_join(msa, by = c('msa_name', 'date', 'work_hrs')) %>%
  mutate(
    day_of_week = wday(date, label = T),
    work_hrs = case_when(
      day_of_week %in% c('Sat', 'Sun') ~ 'no',
      TRUE ~ work_hrs
    ),
    normalized_distinct = n_distinct_devices/n_distinct_devices_msa,
    normalized_stops = n_stops/n_stops_msa,
    date_range_start = floor_date(
      date,
      unit = "week",
      week_start = getOption("lubridate.week.start", 1))
  )

head(final_df)


# Plot weekly normalized total stops, 
# by work hours vs. not work hours
#=====================================

work_not <-
  final_df %>%
  group_by(city, date_range_start, work_hrs) %>%
  summarize(tot_stops_dt = sum(n_stops, na.rm = T),
            tot_stops_msa = sum(n_stops_msa, na.rm = T)) %>%
  ungroup() %>%
  mutate(norm_tot = tot_stops_dt/tot_stops_msa) %>%
  data.frame() %>%
  filter(date_range_start != as.Date('2024-01-01'))

head(work_not)

work_plot <-
  plot_ly() %>%
  add_lines(data = work_not %>% filter(work_hrs == 'yes'),
            x = ~date_range_start, y = ~norm_tot,
            name = ~paste0(city, ' - work hours'),
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ' - work hours: ', round(norm_tot, 3)),
            line = list(shape = "linear", color = '#4287f5')) %>%f
  add_lines(data = work_not %>% filter(work_hrs == 'no'),
            x = ~date_range_start, y = ~norm_tot,
            name = ~paste0(city, ' - non-work hours'),
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ' - non-work hours: ', round(norm_tot, 3)),
            line = list(shape = "linear", color = '#3a1570')) %>%  
  layout(title = "Weekly normalized total stops, working hours (8am-6pm Mon-Fri) vs.<br>non-working hours, from provider 230399 (stop_uplevelled table), normalized by MSA",
         xaxis = list(title = "Week", zerolinecolor = "#ffff",
                      tickformat = "%Y-%m-%d"),
         yaxis = list(title = "Normalized total stops", zerolinecolor = "#ffff",
                      ticksuffix = "  "))

work_plot

saveWidget(
  work_plot,
  '/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/weekly_wrk_hrs_plot.html')
