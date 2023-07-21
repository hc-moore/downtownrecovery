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
ipak(c('tidyverse', 'arrow', 'readbulk', 'ggplot2', 'plotly', 'lubridate'))

#-----------------------------------------
# Load userbase data
#-----------------------------------------

# THEY ADDED A NEW PROVIDER - NEED TO REQUERY DATA FOR QUEBEC & BRITISH COLUMBIA

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
                        format = '%Y_%W_%w')) # Monday of week

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
                        format = '%Y_%W_%w')) %>% # Monday of week
  filter(year >= 2019)

vancouver_plotly <- 
  plot_ly() %>%
  add_lines(data = userbase_new %>% filter(geography_name == 'British Columbia' & 
                                             provider_id == '700199'),
            x = ~week, y = ~userbase,
            name = "British Columbia - 700199",
            opacity = .9,
            line = list(shape = "linear", color = 'purple')) %>%
  add_lines(data = userbase_new %>% filter(geography_name == 'British Columbia' & 
                                             provider_id == '230599'),
            x = ~week, y = ~userbase,
            name = "British Columbia - 230599",
            opacity = .9,
            line = list(shape = "linear", color = 'green')) %>%
  add_lines(data = da_new %>% filter(city == 'vancouver' & provider_id == '700199'),
            x = ~week, y = ~n_devices,
            name = "Vancouver DAs - 700199",
            opacity = .5,
            line = list(shape = "linear", color = 'red')) %>%
  add_lines(data = da_new %>% filter(city == 'vancouver' & provider_id == '190199'),
            x = ~week, y = ~n_devices,
            name = "Vancouver DAs - 190199",
            opacity = .8,
            line = list(shape = "linear", color = 'lightblue')) %>%
  add_lines(data = da_new %>% filter(city == 'vancouver' & provider_id == '230599'),
            x = ~week, y = ~n_devices,
            name = "Vancouver DAs - 230599",
            opacity = .8,
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
            line = list(shape = "linear", color = 'purple')) %>%
  add_lines(data = userbase_new %>% filter(geography_name == 'Quebec' & 
                                             provider_id == '230599'),
            x = ~week, y = ~userbase,
            name = "Quebec - 230599",
            opacity = .9,
            line = list(shape = "linear", color = 'green')) %>%
  add_lines(data = da_new %>% filter(city == 'montreal' & provider_id == '700199'),
            x = ~week, y = ~n_devices,
            name = "Montreal DAs - 700199",
            opacity = .5,
            line = list(shape = "linear", color = 'red')) %>%
  add_lines(data = da_new %>% filter(city == 'montreal' & provider_id == '190199'),
            x = ~week, y = ~n_devices,
            name = "Montreal DAs - 190199",
            opacity = .8,
            line = list(shape = "linear", color = 'lightblue')) %>%
  add_lines(data = da_new %>% filter(city == 'montreal' & provider_id == '230599'),
            x = ~week, y = ~n_devices,
            name = "Montreal DAs - 230599",
            opacity = .8,
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


