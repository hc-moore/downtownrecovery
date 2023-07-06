################################################################################
# Consolidate employment zone & userbase counts files exported from Spectus,
# for Laura's work on recovery rates in employment zones.
# 
# Author: Julia Greenberg
# Date: 6/28/23
################################################################################

#-----------------------------------------
# Load packages
#-----------------------------------------

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'lubridate', 'ggplot2', 'plotly'))

#-----------------------------------------
# Load data
#-----------------------------------------

# Userbase: 1/1/2019 - 4/25/2023

filepath_bia <- 'C:/Users/jpg23/data/downtownrecovery/spectus_exports/BIAs/'

userbase1 <- read_delim(
  paste0(filepath_bia, "bia_userbase/20230522_190855_00007_3yd5w_80fe0af5-8ae8-468e-ad40-4d1714548545.gz"),
  delim = '\001',
  col_names = c('bia', 'provider', 'n_devices', 'userbase', 'date'),
  col_types = c('cciii')
) %>%
  mutate(date = as.Date(as.character(date), format = "%Y%m%d")) %>%
  data.frame() %>%
  arrange(date) %>%
  select(-c(bia, n_devices)) %>%
  distinct()

filepath <- 'C:/Users/jpg23/data/downtownrecovery/spectus_exports/employment_zones/'

# Userbase: 4/10/23 - 6/18/23
userbase2 <-
  list.files(path = paste0(filepath, 'userbase')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'userbase/', .),
    delim = '\001',
    col_names = c('geography_name', 'userbase', 'event_date'),
    col_types = c('cii')
  )) %>%
  data.frame() %>%
  filter(geography_name == 'Ontario') %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d"),
         provider = '190199') %>%
  arrange(date) %>%
  select(-c(event_date, geography_name))

# Region-wide: 1/1/19 - 7/7/21
region1 <-
  list.files(path = paste0(filepath, 'region_20190101_20210707')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'region_20190101_20210707/', .),
    delim = '\001',
    col_names = c('ez', 'provider_id', 'approx_distinct_devices_count', 
                  'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date)

# Region-wide: 7/7/21 - 6/27/23
region2 <-
  list.files(path = paste0(filepath, 'region_20210707_20230627')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'region_20210707_20230627/', .),
    delim = '\001',
    col_names = c('ez', 'provider_id', 'approx_distinct_devices_count', 
                  'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date)

# City-wide: 1/1/19 - 8/12/22
city1 <-
  list.files(path = paste0(filepath, 'city_20190101_20220812')) %>%
  map_df(~read_delim(
    paste0(filepath, 'city_20190101_20220812/', .),
    delim = '\001',
    col_names = c('small_area', 'big_area', 'provider_id', 
                  'approx_distinct_devices_count', 'event_date'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date)

# City-wide: 8/12/22 - 6/29/23
city2 <-
  list.files(path = paste0(filepath, 'city_20220812_20230629')) %>%
  map_df(~read_delim(
    paste0(filepath, 'city_20220812_20230629/', .),
    delim = '\001',
    col_names = c('small_area', 'big_area', 'provider_id', 
                  'approx_distinct_devices_count', 'event_date'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date)

#-----------------------------------------
# Combine userbase data
#-----------------------------------------

head(userbase1)
head(userbase2)

range(userbase1$date)
range(userbase2$date)

userbase <- 
  userbase1 %>% 
  filter(date < as.Date('2023-04-10')) %>%
  rbind(userbase2) %>%
  filter(date < as.Date('2023-06-01') & # through end of May
           # change providers at 5/17/21
           ((provider == '700199' & date < as.Date('2021-05-17')) | 
           (provider == '190199' & date >= as.Date('2021-05-17')))) %>%
  select(-provider)

head(userbase)
range(userbase$date)

#-----------------------------------------
# Combine region-wide data
#-----------------------------------------

head(region1)
head(region2)

range(region1$date)
range(region2$date)

region <-
  region1 %>%
  filter(date < as.Date('2021-07-07')) %>%
  rbind(region2) %>%
  filter(date < as.Date('2023-06-01') & # through end of May
           # change providers at 5/17/21
           ((provider_id == '700199' & date < as.Date('2021-05-17')) | 
              (provider_id == '190199' & date >= as.Date('2021-05-17')))) %>%
  mutate(ez = as.character(round(as.integer(ez), 0))) %>%
  select(-provider_id) %>%
  rename(n_devices = approx_distinct_devices_count) %>%
  left_join(userbase) # add userbase

head(region)
range(region$date)

#-----------------------------------------
# Combine city-wide data
#-----------------------------------------

head(city1)
head(city2)

range(city1$date)
range(city2$date)

city <- 
  city1 %>% 
  filter(date < as.Date('2022-08-12')) %>%
  rbind(city2) %>%
  filter(date < as.Date('2023-06-01') & # through end of May
           # change providers at 5/17/21
           (provider_id == '700199' & date < as.Date('2021-05-17')) | 
           (provider_id == '190199' & date >= as.Date('2021-05-17'))) %>%
  select(-provider_id) %>%
  # Aggregate city data to 'big_area'
  dplyr::group_by(big_area, date) %>%
  dplyr::summarize(n_devices = sum(approx_distinct_devices_count, na.rm = T)) %>%
  data.frame() %>%
  left_join(userbase) # add userbase

head(city)
range(city$date)

#-----------------------------------------
# Look at all 3 datasets
#-----------------------------------------

all_plotly <- 
  plot_ly() %>%
  add_lines(data = userbase, x = ~date, y = ~userbase, 
            name = "Userbase",
            opacity = .9,
            line = list(shape = "linear", color = '#d6ad09')) %>%
  add_lines(data = city, x = ~date, y = ~n_devices,
            name = "City",
            opacity = .3,
            line = list(shape = "linear", color = '#8c0a03')) %>%
  add_lines(data = region, x = ~date, y = ~n_devices,
            name = "Region", 
            opacity = .3,
            line = list(shape = "linear", color = '#6bc0c2')) %>%
  layout(title = "Trends by area",
         xaxis = list(title = "Date", zerolinecolor = "#ffff", 
                      tickformat = "%b %Y"),
         yaxis = list(title = "Devices", zerolinecolor = "#ffff",
                      ticksuffix = "  "))

all_plotly

#-----------------------------------------
# Calculate city recovery rates
#-----------------------------------------

rec_rate_city <-
  city %>%
  # Determine week and year # for each date
  mutate(
    date_range_start = floor_date(
      date,
      unit = "week",
      week_start = getOption("lubridate.week.start", 1)),
    week_num = isoweek(date_range_start),
    year = year(date_range_start)) %>%
  # Calculate # of devices by big_area, week and year
  dplyr::group_by(big_area, year, week_num) %>%
  dplyr::summarize(n_devices = sum(n_devices, na.rm = T),
            userbase = sum(userbase, na.rm = T)) %>%
  dplyr::ungroup() 

head(rec_rate_city)

all_city_for_plot <-
  rec_rate_city %>%
  filter(year > 2018) %>%
  dplyr::group_by(year, week_num) %>%
  dplyr::summarize(n_devices = sum(n_devices, na.rm = T),
            userbase = sum(userbase, na.rm = T)) %>%
  dplyr::ungroup() %>%
  mutate(normalized = n_devices/userbase) %>%
  pivot_wider(
    id_cols = c('week_num'),
    names_from = 'year',
    names_prefix = 'ntv',
    values_from = 'normalized') %>%
  mutate(rec2020 = ntv2020/ntv2019,
         rec2021 = ntv2021/ntv2019,
         rec2022 = ntv2022/ntv2019,
         rec2023 = ntv2023/ntv2019) %>%
  select(-starts_with('ntv')) %>%
  pivot_longer(
    cols = rec2020:rec2023,
    names_to = 'year',
    values_to = 'rq') %>%
  filter(week_num < 53) %>%
  mutate(year = substr(year, 4, 7),
         week = as.Date(paste(year, week_num, 1, sep = '_'),
                        format = '%Y_%W_%w')) %>% # Monday of week
  filter(!(year == 2023 & week_num > 22)) %>%
  arrange(year, week_num) %>%
  mutate(rq_rolling = zoo::rollmean(rq, k = 11, fill = NA, align = 'right')) %>%
  ungroup() %>%
  data.frame() %>%
  filter(!(year == 2020 & week_num < 12))

head(all_city_for_plot)
tail(all_city_for_plot)

all_city_plot <-
  all_city_for_plot %>%
  ggplot(aes(x = week, y = rq_rolling)) +
  geom_line(size = .8) +
  ggtitle('Recovery rate for all Employment Zones in City of Toronto (11 week rolling average)') +
  scale_x_date(date_breaks = "4 month", date_labels = "%b %Y") +
  scale_y_continuous(labels = scales::percent_format(accuracy = 1),
                     limits = c(0.5, 1.3),
                     breaks = seq(.5, 1.3, .2)) +
  xlab('Month') +
  ylab('Recovery rate') +
  theme(
    # axis.text.x = element_text(angle = 90),
    panel.grid.major = element_line(color = 'light gray',
                                    linewidth = .5,
                                    linetype = 1),
    panel.grid.minor.x = element_blank(),
    panel.background = element_blank(),
    plot.title = element_text(hjust = .5),
    axis.ticks = element_blank(),
    axis.title.y = element_text(margin = margin(r = 15)),
    axis.title.x = element_text(margin = margin(t = 15))
  )

all_city_plot

# Now by employment zone
each_city_for_plot <-
  rec_rate_city %>%
  filter(year > 2018) %>%
  dplyr::group_by(year, week_num, big_area) %>%
  dplyr::summarize(n_devices = sum(n_devices, na.rm = T),
            userbase = sum(userbase, na.rm = T)) %>%
  dplyr::ungroup() %>%
  mutate(normalized = n_devices/userbase) %>%
  pivot_wider(
    id_cols = c('week_num', 'big_area'),
    names_from = 'year',
    names_prefix = 'ntv',
    values_from = 'normalized') %>%
  mutate(rec2020 = ntv2020/ntv2019,
         rec2021 = ntv2021/ntv2019,
         rec2022 = ntv2022/ntv2019,
         rec2023 = ntv2023/ntv2019) %>%
  select(-starts_with('ntv')) %>%
  pivot_longer(
    cols = rec2020:rec2023,
    names_to = 'year',
    values_to = 'rq') %>%
  filter(week_num < 53) %>%
  mutate(year = substr(year, 4, 7),
         week = as.Date(paste(year, week_num, 1, sep = '_'),
                        format = '%Y_%W_%w')) %>% # Monday of week
  filter(!(year == 2023 & week_num > 22)) %>%
  arrange(big_area, year, week_num) %>%
  dplyr::group_by(big_area) %>%
  mutate(rq_rolling = zoo::rollmean(rq, k = 11, fill = NA, align = 'right')) %>%
  dplyr::ungroup() %>%
  data.frame() %>%
  filter(!(year == 2020 & week_num < 12)) %>%
  filter(!is.na(big_area)) %>%
  mutate(
    mytext = paste0(big_area, '<br>Week of ', week, ': ',
                    scales::percent(rq_rolling, accuracy = 2)))

head(each_city_for_plot)

# Plotly
each_city_plotly <- 
  plot_ly() %>%
  add_lines(data = each_city_for_plot, 
            x = ~week, y = ~rq_rolling, 
            split = ~big_area,
            name = ~big_area, 
            text = ~mytext,
            hoverinfo = 'text',
            opacity = .3,
            line = list(shape = "linear", color = '#8c0a03')) %>%
  layout(title = "Recovery rate for all Employment Zones in City of Toronto (11 week rolling average)",
         xaxis = list(title = "Week", zerolinecolor = "#ffff", 
                      tickformat = "%b %Y"),
         yaxis = list(title = "Recovery rate", zerolinecolor = "#ffff",
                      tickformat = ".0%", ticksuffix = "  "))

each_city_plotly




#-----------------------------------------
# Make city data spatial
#-----------------------------------------

# Load city shapefile
city_sf <- st_read('C:/Users/jpg23/data/downtownrecovery/shapefiles/employment_lands/final_city_sf.shp')

head(city_sf)

n_distinct(city$big_area)
n_distinct(city_sf$big_area) # 44 big areas in both

n_distinct(city$small_area)
n_distinct(city_sf$small_area) # 919 small areas in shapefile, only 913 in data



# city1 <- city_sf %>% left_join(city)
# 
# nrow(city)
# nrow(city_sf)
# nrow(city1)
# 
# head(city1)

#-----------------------------------------
# Calculate region recovery rates
#-----------------------------------------

#-----------------------------------------
# Make region data spatial
#-----------------------------------------

#-----------------------------------------
# Combine city & region data
#-----------------------------------------
