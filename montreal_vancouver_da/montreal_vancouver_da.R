################################################################################
# Consolidate Montreal & Vancouver DA data (with userbase) for Amir.
#
# Author: Julia Greenberg
# Date: 7.21.23
################################################################################

#-----------------------------------------
# Load packages
#-----------------------------------------

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'arrow', 'readbulk', 'ggplot2', 'plotly', 'lubridate', 'qs'))

#-----------------------------------------
# Load userbase data
#-----------------------------------------

filepath_u1 <- 'C:/Users/jpg23/data/downtownrecovery/spectus_exports/DAs/montreal_vancouver/canada_userbase_6.21.23_7.18.23/'

# userbase 6.21.23 - 7.15.23
userbase1 <-
  list.files(path = filepath_u1) %>%
  map_df(~read_delim(
    paste0(filepath_u1, .),
    delim = '\001',
    col_names = c('geography_name', 'userbase', 'provider_id', 'event_date'),
    col_types = c('ciii')
  )) %>%
  data.frame() %>%
  filter(geography_name %in% c('Quebec', 'British Columbia')) %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date)

# userbase 1.1.19 - 6.20.23
filepath_u2 <- 'C:/Users/jpg23/data/downtownrecovery/spectus_exports/DAs/montreal_vancouver/quebec_bc_userbase_1.1.19_6.20.23/'

userbase2 <-
  list.files(path = filepath_u2) %>%
  map_df(~read_delim(
    paste0(filepath_u2, .),
    delim = '\001',
    col_names = c('geography_name', 'userbase', 'provider_id', 'event_date'),
    col_types = c('ciii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date)

range(userbase1$date)
glimpse(userbase1)
unique(userbase1$provider_id)

range(userbase2$date)
glimpse(userbase2)
unique(userbase2$provider_id)

userbase <- rbind(userbase1, userbase2)

#-----------------------------------------
# Load DA data
#-----------------------------------------

filepath_da <- 'C:/Users/jpg23/data/downtownrecovery/spectus_exports/DAs/montreal_vancouver/montreal_vancouver_DAs'

da <- read_bulk(directory = filepath_da, 
                fun = read_parquet, 
                subdirectories = TRUE) %>%
  mutate(date = as.Date(paste0(substr(Subdirectory, 12, 15), '-', 
                               substr(Subdirectory, 16, 17), '-',
                               substr(Subdirectory, 18, 19)))) %>%
  select(-c(Subdirectory, File))

head(da)
range(da$date)

# When is each provider available?

da70 <- da %>% filter(provider_id == '700199')
range(da70$date)

da19 <- da %>% filter(provider_id == '190199')
range(da19$date)

da23 <- da %>% filter(provider_id == '230599')
range(da23$date)

#-----------------------------------------
# Which provider to use?
#-----------------------------------------

# Aggregate each dataset to week, not date, to make it possible to plot

userbase_new <- userbase %>%
  mutate(
  date_range_start = floor_date(
    date,
    unit = "week",
    week_start = getOption("lubridate.week.start", 1)),
  week_num = isoweek(date_range_start),
  year = year(date_range_start)) %>%
  # Calculate # of devices by province, week and year
  group_by(geography_name, provider_id, year, week_num) %>%
  summarize(userbase = sum(userbase, na.rm = T)) %>%
  ungroup() %>%
  mutate(week = as.Date(paste(year, week_num, 1, sep = '_'),
                        format = '%Y_%W_%w'),
         mytext = paste0('<br>Week of ', week, ': ', userbase)) # Monday of week

da_new <- da %>%
  mutate(
    date_range_start = floor_date(
      date,
      unit = "week",
      week_start = getOption("lubridate.week.start", 1)),
    week_num = isoweek(date_range_start),
    year = year(date_range_start)) %>%
  # Calculate # of devices by province, week and year
  group_by(city, provider_id, year, week_num) %>%
  summarize(n_devices = sum(approx_distinct_devices_count, na.rm = T)) %>%
  ungroup() %>%
  mutate(week = as.Date(paste(year, week_num, 1, sep = '_'),
                        format = '%Y_%W_%w'),
         mytext = paste0('<br>Week of ', week, ': ', n_devices)) %>% # Monday of week
  filter(year >= 2019)

vancouver_plotly <- 
  plot_ly() %>%
  add_lines(data = userbase_new %>% filter(geography_name == 'British Columbia' & 
                                             provider_id == '700199'),
            x = ~week, y = ~userbase,
            name = "British Columbia - 700199",
            opacity = .9,
            text = ~mytext,
            line = list(shape = "linear", color = 'purple')) %>%
  add_lines(data = userbase_new %>% filter(geography_name == 'British Columbia' & 
                                             provider_id == '190199'),
            x = ~week, y = ~userbase,
            name = "British Columbia - 190199",
            opacity = .8,
            text = ~mytext,
            line = list(shape = "linear", color = 'darkgreen')) %>%
  add_lines(data = userbase_new %>% filter(geography_name == 'British Columbia' & 
                                             provider_id == '230599'),
            x = ~week, y = ~userbase,
            name = "British Columbia - 230599",
            opacity = .9,
            text = ~mytext,
            line = list(shape = "linear", color = 'green')) %>%
  add_lines(data = da_new %>% filter(city == 'vancouver' & provider_id == '700199'),
            x = ~week, y = ~n_devices,
            name = "Vancouver DAs - 700199",
            opacity = .5,
            text = ~mytext,
            line = list(shape = "linear", color = 'red')) %>%
  add_lines(data = da_new %>% filter(city == 'vancouver' & provider_id == '190199'),
            x = ~week, y = ~n_devices,
            name = "Vancouver DAs - 190199",
            opacity = .8,
            text = ~mytext,
            line = list(shape = "linear", color = 'lightblue')) %>%
  add_lines(data = da_new %>% filter(city == 'vancouver' & provider_id == '230599'),
            x = ~week, y = ~n_devices,
            name = "Vancouver DAs - 230599",
            opacity = .8,
            text = ~mytext,
            line = list(shape = "linear", color = 'orange')) %>%
  layout(title = "Vancouver / British Columbia",
         xaxis = list(title = "Date", zerolinecolor = "#ffff", 
                      tickformat = "%b %Y"),
         yaxis = list(title = "Devices", zerolinecolor = "#ffff",
                      ticksuffix = "  "))

vancouver_plotly

montreal_plotly <- 
  plot_ly() %>%
  add_lines(data = userbase_new %>% filter(geography_name == 'Quebec' & 
                                             provider_id == '700199'),
            x = ~week, y = ~userbase,
            name = "Quebec - 700199",
            opacity = .9,
            text = ~mytext,
            line = list(shape = "linear", color = 'purple')) %>%
  add_lines(data = userbase_new %>% filter(geography_name == 'Quebec' & 
                                             provider_id == '190199'),
            x = ~week, y = ~userbase,
            name = "Quebec - 190199",
            opacity = .8,
            text = ~mytext,
            line = list(shape = "linear", color = 'darkgreen')) %>%
  add_lines(data = userbase_new %>% filter(geography_name == 'Quebec' & 
                                             provider_id == '230599'),
            x = ~week, y = ~userbase,
            name = "Quebec - 230599",
            opacity = .9,
            text = ~mytext,
            line = list(shape = "linear", color = 'green')) %>%
  add_lines(data = da_new %>% filter(city == 'montreal' & 
                                       provider_id == '700199'),
            x = ~week, y = ~n_devices,
            name = "Montreal DAs - 700199",
            opacity = .5,
            text = ~mytext,
            line = list(shape = "linear", color = 'red')) %>%
  add_lines(data = da_new %>% filter(city == 'montreal' & 
                                       provider_id == '190199'),
            x = ~week, y = ~n_devices,
            name = "Montreal DAs - 190199",
            opacity = .8,
            text = ~mytext,
            line = list(shape = "linear", color = 'lightblue')) %>%
  add_lines(data = da_new %>% filter(city == 'montreal' & 
                                       provider_id == '230599'),
            x = ~week, y = ~n_devices,
            name = "Montreal DAs - 230599",
            opacity = .8,
            text = ~mytext,
            line = list(shape = "linear", color = 'orange')) %>%
  layout(title = "Montreal / Quebec",
         xaxis = list(title = "Date", zerolinecolor = "#ffff", 
                      tickformat = "%b %Y"),
         yaxis = list(title = "Devices", zerolinecolor = "#ffff",
                      ticksuffix = "  "))

montreal_plotly

#-----------------------------------------
# Join DA & userbase
#-----------------------------------------

head(da)
head(userbase)

da_geo <- da %>% 
  mutate(geography_name = case_when(
    city == 'vancouver' ~ 'British Columbia',
    TRUE ~ 'Quebec'
  ))

head(da_geo)

d <-
  da_geo %>%
  filter(date >= as.Date('2019-01-01')) %>%
  left_join(userbase %>% mutate(provider_id = as.character(provider_id)), 
            by = c('geography_name', 'provider_id', 'date')) %>%
  mutate(normalized = approx_distinct_devices_count/userbase) %>%
  filter(
    (provider_id == '700199' & date < as.Date('2021-05-17')) | 
      (provider_id == '190199' & date >= as.Date('2021-05-17')) |
      (provider_id == '230599' & date > as.Date('2023-06-18')))

# Export data for Amir
qsave(d, 'C:/Users/jpg23/data/downtownrecovery/montreal_vancouver_da/montreal_vancouver_da_userbase.qs')

head(d)
summary(d$normalized)

# Aggregate by week
d_agg <-
  d %>%
  mutate(
    date_range_start = floor_date(
      date,
      unit = "week",
      week_start = getOption("lubridate.week.start", 1))
  ) %>%
  group_by(city, provider_id, date_range_start) %>%
  summarize(avg_norm = mean(normalized, na.rm = T)) %>%
  data.frame() %>%
  mutate(mytext = paste0('<br>Week of ', date_range_start, ': ', 
                         round(avg_norm, 7)))

head(d_agg)

norm_plotly <- 
  plot_ly() %>%
  add_lines(data = d_agg %>% filter(city == 'montreal' & 
                                      provider_id == '700199'),
            x = ~date_range_start, y = ~avg_norm,
            name = "Montreal - 700199",
            opacity = .9,
            text = ~mytext,
            line = list(shape = "linear", color = 'blue')) %>%
  add_lines(data = d_agg %>% filter(city == 'montreal' &
                                      provider_id == '190199'),
            x = ~date_range_start, y = ~avg_norm,
            name = "Montreal - 190199",
            opacity = .9,
            text = ~mytext,
            line = list(shape = "linear", color = 'red')) %>%
  add_lines(data = d_agg %>% filter(city == 'montreal' &
                                      provider_id == '230599'),
            x = ~date_range_start, y = ~avg_norm,
            name = "Montreal - 230599",
            opacity = .9,
            text = ~mytext,
            line = list(shape = "linear", color = 'green')) %>%
  add_lines(data = d_agg %>% filter(city == 'vancouver' &
                                      provider_id == '700199'),
            x = ~date_range_start, y = ~avg_norm,
            name = "Vancouver - 700199",
            opacity = .9,
            text = ~mytext,
            line = list(shape = "linear", color = 'orange')) %>%
  add_lines(data = d_agg %>% filter(city == 'vancouver' &
                                      provider_id == '190199'),
            x = ~date_range_start, y = ~avg_norm,
            name = "Vancouver - 190199",
            opacity = .9,
            text = ~mytext,
            line = list(shape = "linear", color = 'purple')) %>%
  add_lines(data = d_agg %>% filter(city == 'vancouver' &
                                      provider_id == '230599'),
            x = ~date_range_start, y = ~avg_norm,
            name = "Vancouver - 230599",
            opacity = .9,
            text = ~mytext,
            line = list(shape = "linear", color = 'pink')) %>%
  layout(title = "Average normalized value by week",
         xaxis = list(title = "Date", zerolinecolor = "#ffff", 
                      tickformat = "%b %Y"),
         yaxis = list(title = "Devices", zerolinecolor = "#ffff",
                      ticksuffix = "  ")) 

norm_plotly
