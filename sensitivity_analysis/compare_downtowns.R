################################################################################
# Sensitivity analysis - compare recovery rates for different definitions of 
# downtown. Includes:
#  - LEHD clusters (Cleveland, Portland, San Diego, San Francisco, Salt Lake
#    City, St Louis)
#  - Portland (definition based on Will Hollingsworth's suggestion)
#  - Cleveland (definition based on Downtown Cleveland Alliance - suggestion
#    from Sean McDonnell)
#  - city-defined boundaries that Sarah or I found online (Toronto, St Louis, 
#    Salt Lake City, San Francisco, Portland, Halifax, Cleveland, San Diego)
#
# Author: Julia Greenberg
# Date created: 8.11.2023
################################################################################

# Load packages
#=====================================

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'sf', 'lubridate', 'leaflet', 'plotly', 'htmlwidgets'))


# Load userbase data
#=====================================

u_filepath <- 'C:/Users/jpg23/data/downtownrecovery/spectus_exports/new_provider_230599/'

userbase1 <-
  list.files(path = paste0(u_filepath, 'userbase_20190101_20230721')) %>% 
  map_df(~read_delim(
    paste0(u_filepath, 'userbase_20190101_20230721/', .),
    delim = '\001',
    col_names = c('geography_name', 'provider_id', 'userbase', 'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date)

userbase2 <-
  list.files(path = paste0(u_filepath, 'userbase_20230722_20230804')) %>% 
  map_df(~read_delim(
    paste0(u_filepath, 'userbase_20230722_20230804/', .),
    delim = '\001',
    col_names = c('geography_name', 'provider_id', 'userbase', 'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date)

userbase <- rbind(userbase1, userbase2) %>% 
  filter(date <= as.Date('2023-06-18') & # last date for provider 190199
         # change providers at 5/17/21
         ((provider_id == '700199' & date < as.Date('2021-05-17')) | 
            (provider_id == '190199' & date >= as.Date('2021-05-17')))) %>%
  select(-provider_id)

head(userbase)
glimpse(userbase)
range(userbase$date)
unique(userbase$provider_id)
unique(userbase$geography_name)


# Load downtown data
#=====================================

downtown_filepath <- 'C:/Users/jpg23/data/downtownrecovery/spectus_exports/sensitivity_analysis/'
newprov_filepath <- 'C:/Users/jpg23/data/downtownrecovery/spectus_exports/new_provider_230599/'

# Original polygons (spectus only) - pre-2023
orig_spec1 <-
  list.files(path = paste0(downtown_filepath, 'original_downtowns_pre2023')) %>% 
  map_df(~read_delim(
    paste0(downtown_filepath, 'original_downtowns_pre2023/', .),
    delim = '\001',
    col_names = c('city', 'provider_id', 'approx_distinct_devices_count', 
                  'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date) %>%
  mutate(cat = 'original')

# Original polygons (spectus only) - 2023
orig_spec2 <-
  list.files(path = paste0(newprov_filepath, 'downtown_20190101_20230721')) %>% 
  map_df(~read_delim(
    paste0(newprov_filepath, 'downtown_20190101_20230721/', .),
    delim = '\001',
    col_names = c('city', 'provider_id', 'approx_distinct_devices_count', 
                  'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date) %>%
  filter(date <= as.Date('2023-06-18')) %>%
  mutate(cat = 'original')

head(orig_spec1)
head(orig_spec2)

# Combine pre-2023 and 2023 original downtowns
orig_spec <- rbind(orig_spec1, orig_spec2) %>%
  filter(city %in% c('Cleveland', 'Halifax', 'Portland', 'Salt Lake City',
                     'San Diego', 'San Francisco', 'St Louis', 'Toronto',
                     'Nashville'))

# LEHD clusters (Cleveland + SLC + SF) & Will Hollingsworth's Portland
lehd_portland <-
  list.files(path = paste0(downtown_filepath, 'lehd_and_portland')) %>% 
  map_df(~read_delim(
    paste0(downtown_filepath, 'lehd_and_portland/', .),
    delim = '\001',
    col_names = c('city', 'provider_id', 'approx_distinct_devices_count', 
                  'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date) %>%
  mutate(cat = 'lehd_portland')

# Cleveland Downtown Alliance (Sean McDonnell) & LEHD clusters (St Louis & San Diego)
c_stl_sd <-
  list.files(path = paste0(downtown_filepath, 'cleveland_stlouis_sandiego')) %>% 
  map_df(~read_delim(
    paste0(downtown_filepath, 'cleveland_stlouis_sandiego/', .),
    delim = '\001',
    col_names = c('city', 'provider_id', 'approx_distinct_devices_count', 
                  'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date) %>%
  mutate(cat = 'c_stl_sd')

# City-defined (Toronto, St Louis, Salt Lake City, San Francisco, Portland,
# Halifax, Cleveland, San Diego)
city_defined <-
  list.files(path = paste0(downtown_filepath, 'city_defined')) %>%
  map_df(~read_delim(
    paste0(downtown_filepath, 'city_defined/', .),
    delim = '\001',
    col_names = c('city', 'provider_id', 'approx_distinct_devices_count',
                  'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date) %>%
  mutate(cat = 'city_defined',
         city = case_when(
           city == 'stl' ~ 'St Louis',
           city == 'slc' ~ 'Salt Lake City',
           city == 'sf' ~ 'San Francisco',
           city == 'sandiego' ~ 'San Diego',
           TRUE ~ str_to_title(city)
         ))

# Byeonghwa - San Francisco, Cleveland, Portland, Salt Lake City and Toronto
# (method 1: including retail)
byeonghwa_retail_1 <-
  list.files(path = paste0(downtown_filepath, 'byeonghwa_group1')) %>%
  map_df(~read_delim(
    paste0(downtown_filepath, 'byeonghwa_group1/', .),
    delim = '\001',
    col_names = c('city', 'provider_id', 'approx_distinct_devices_count',
                  'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date) %>%
  mutate(cat = 'byeonghwa_retail_1')

# City-defined Nashville (based on Planning Department polygon sent by Crissy Cassetty)
nash <-
  list.files(path = paste0(downtown_filepath, 'nashville_citydefined')) %>%
  map_df(~read_delim(
    paste0(downtown_filepath, 'nashville_citydefined/', .),
    delim = '\001',
    col_names = c('city', 'provider_id', 'approx_distinct_devices_count',
                  'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date) %>%
  mutate(cat = 'nashville')

# Combine them all
downtown <- rbind(orig_spec, lehd_portland, c_stl_sd, city_defined, 
                  byeonghwa_retail_1, nash) %>%
  filter(date <= as.Date('2023-06-18') & # last date for provider 190199
           # change providers at 5/17/21
           ((provider_id == '700199' & date < as.Date('2021-05-17')) | 
              (provider_id == '190199' & date >= as.Date('2021-05-17')))) %>%
  mutate(full_name = case_when(
    city == 'Portland' & cat == 'lehd_portland' ~ 'Portland (Will Hollingsworth)',
    city == 'Cleveland' & cat == 'lehd_portland' ~ 'Cleveland (LEHD)',
    city == 'Salt Lake City' & cat == 'lehd_portland' ~ 'Salt Lake City (LEHD)',
    city == 'San Francisco' & cat == 'lehd_portland' ~ 'San Francisco (LEHD)',
    city == 'Cleveland' & cat == 'c_stl_sd' ~ 'Cleveland (Downtown Alliance)',
    city == 'St Louis' & cat == 'c_stl_sd' ~ 'St Louis (LEHD)',
    city == 'San Diego' & cat == 'c_stl_sd' ~ 'San Diego (LEHD)',
    cat == 'city_defined' ~ paste0(city, ' (city-defined)'),
    cat == 'original' ~ paste0(city, ' (original - spectus only)'),
    cat == 'byeonghwa_retail_1' ~ paste0(city, ' (Byeonghwa - retail/office)'),
    cat == 'nashville' ~ 'Nashville (Planning Dept)',
    TRUE ~ NA_character_
  )) %>%
  select(-c(provider_id, cat))

head(downtown)
glimpse(downtown)
range(downtown$date)
unique(downtown$city)
unique(downtown$full_name)

# Join downtowns with userbase
#=====================================

city_to_state <- data.frame(
  Albuquerque=c('New Mexico'),
  Atlanta=c('Georgia'),
  Austin=c('Texas'),
  Bakersfield=c('California'),
  Baltimore=c('Maryland'),
  Boston=c('Massachusetts'),
  Calgary=c('Alberta'),
  Charlotte=c('North Carolina'),
  Chicago=c('Illinois'),
  Cincinnati=c('Ohio'),
  Cleveland=c('Ohio'),
  `Colorado Springs`=c('Colorado'),
  Columbus=c('Ohio'),
  Dallas=c('Texas'),
  Denver=c('Colorado'),
  Detroit=c('Michigan'),
  Edmonton=c('Alberta'),
  `El Paso`=c('Texas'),
  `Fort Worth`=c('Texas'),
  Fresno=c('California'),
  Halifax=c('Nova Scotia'),
  Honolulu=c('Hawaii'),
  Houston=c('Texas'),
  Indianapolis=c('Indiana'),
  Jacksonville=c('Florida'),
  `Kansas City`=c('Missouri'),
  `Las Vegas`=c('Nevada'),
  `London`=c('Ontario'),
  `Los Angeles`=c('California'),
  Louisville=c('Kentucky'),
  Memphis=c('Tennessee'),
  Miami=c('Florida'),
  Milwaukee=c('Wisconsin'),
  Minneapolis=c('Minnesota'),
  Mississauga=c('Ontario'),
  Montreal=c('Quebec'),
  Nashville=c('Tennessee'),
  `New Orleans`=c('Louisiana'),
  `New York`=c('New York'),
  Oakland=c('California'),
  `Oklahoma City`=c('Oklahoma'),
  Omaha=c('Nebraska'),
  Orlando=c('Florida'),
  Ottawa=c('Ontario'),
  Philadelphia=c('Pennsylvania'),
  Phoenix=c('Arizona'),
  Pittsburgh=c('Pennsylvania'),
  Portland=c('Oregon'),
  Quebec=c('Quebec'),
  Raleigh=c('North Carolina'),
  Sacramento=c('California'),
  `Salt Lake City`=c('Utah'),
  `San Antonio`=c('Texas'),
  `San Diego`=c('California'),
  `San Francisco`=c('California'), 
  `San Jose`=c('California'),
  Seattle=c('Washington'),
  `St Louis`=c('Missouri'),
  Tampa=c('Florida'),
  Toronto=c('Ontario'),
  Tucson=c('Arizona'),
  Tulsa=c('Oklahoma'),
  Vancouver=c('British Columbia'),
  `Washington DC`=c('District of Columbia'),
  Wichita=c('Kansas'),
  Winnipeg=c('Manitoba')) %>%
  pivot_longer(
    cols = everything(),
    names_to = 'city',
    values_to = 'state'
  ) %>%
  mutate(city = str_replace_all(city, '\\.', ' '))

downtown1 <- downtown %>%
  left_join(city_to_state)

head(downtown1)
head(userbase)

final_df <- 
  downtown1 %>% 
  left_join(userbase, by = c('state' = 'geography_name', 'date')) %>%
  rename(downtown_devices = approx_distinct_devices_count) %>%
  mutate(normalized = downtown_devices/userbase)

head(final_df)

rec_rate_cr <-
  final_df %>%
  # Determine week and year # for each date
  mutate(
    date_range_start = floor_date(
      date,
      unit = "week",
      week_start = getOption("lubridate.week.start", 1)),
    week_num = isoweek(date_range_start),
    year = year(date_range_start)) %>%
  # Calculate # of devices by big_area, week and year
  dplyr::group_by(full_name, year, week_num) %>%
  dplyr::summarize(downtown_devices = sum(downtown_devices, na.rm = T),
                   userbase = sum(userbase, na.rm = T)) %>%
  dplyr::ungroup() 

head(rec_rate_cr)

each_cr_for_plot <-
  rec_rate_cr %>%
  filter(year > 2018) %>%
  dplyr::group_by(year, week_num, full_name) %>%
  dplyr::summarize(downtown_devices = sum(downtown_devices, na.rm = T),
                   userbase = sum(userbase, na.rm = T)) %>%
  dplyr::ungroup() %>%
  mutate(normalized = downtown_devices/userbase) %>%
  pivot_wider(
    id_cols = c('week_num', 'full_name'),
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
  filter(!(year == 2023 & week_num > 24)) %>%
  arrange(full_name, year, week_num) %>%
  dplyr::group_by(full_name) %>%
  mutate(rq_rolling = zoo::rollmean(rq, k = 11, fill = NA, align = 'right')) %>%
  dplyr::ungroup() %>%
  data.frame() %>%
  filter(!(year == 2020 & week_num < 12)) %>%
  filter(!is.na(full_name))

head(each_cr_for_plot)
tail(each_cr_for_plot)

unique(each_cr_for_plot$full_name)

# Now add data from website to compare (https://downtownrecovery.com/charts/patterns)

original <- read.csv("C:/Users/jpg23/data/downtownrecovery/sensitivity_analysis/data_from_website.csv") %>%
  filter(city %in% c('Cleveland', 'Halifax', 'Portland', 'Salt Lake City',
                     'San Diego', 'San Francisco', 'St Louis', 'Toronto',
                     'Nashville') &
           metric == 'downtown') %>%
  mutate(week = as.Date(week),
         full_name = paste0(city, ' (original - safegraph + spectus)')) %>%
  select(week, full_name, rq_rolling = rolling_avg)

head(original)
unique(original$full_name)
range(original$week)
range(each_cr_for_plot$week)

for_plot_final <- each_cr_for_plot %>%
  select(week, full_name, rq_rolling) %>%
  rbind(original) %>%
  mutate(
    mytext = paste0(full_name, '<br>Week of ', week, ': ',
                    scales::percent(rq_rolling, accuracy = 2)))

# Color differently based on type of polygon
city_forplot <- for_plot_final %>% filter(str_detect(full_name, 'city-defined'))
lehd_forplot <- for_plot_final %>% filter(str_detect(full_name, 'LEHD'))
will_forplot <- for_plot_final %>% filter(str_detect(full_name, 'Will Hollingsworth'))
alliance_forplot <- for_plot_final %>% filter(str_detect(full_name, 'Downtown Alliance'))
original_safegraph_forplot <- for_plot_final %>% filter(str_detect(full_name, 'original - safegraph'))
original_spectus_forplot <- for_plot_final %>% filter(str_detect(full_name, 'original - spectus'))
byeonghwa_retail1_forplot <- for_plot_final %>% filter(str_detect(full_name, 'Byeonghwa - retail/office'))
nashville_forplot <- for_plot_final %>% filter(str_detect(full_name, 'Nashville \\(Planning'))

# Plotly
each_cr_plotly <-
  plot_ly() %>%
  add_lines(data = city_forplot,
            x = ~week, y = ~rq_rolling,
            split = ~full_name,
            name = ~full_name,
            text = ~mytext,
            hoverinfo = 'text',
            opacity = .6,
            line = list(shape = "linear", color = '#e0091e')) %>%
  add_lines(data = lehd_forplot,
            x = ~week, y = ~rq_rolling,
            split = ~full_name,
            name = ~full_name,
            text = ~mytext,
            hoverinfo = 'text',
            opacity = .6,
            line = list(shape = "linear", color = '#2183a3')) %>%
  add_lines(data = will_forplot,
            x = ~week, y = ~rq_rolling,
            split = ~full_name,
            name = ~full_name,
            text = ~mytext,
            hoverinfo = 'text',
            opacity = .6,
            line = list(shape = "linear", color = 'orange')) %>%
  add_lines(data = alliance_forplot,
            x = ~week, y = ~rq_rolling,
            split = ~full_name,
            name = ~full_name,
            text = ~mytext,
            hoverinfo = 'text',
            opacity = .6,
            line = list(shape = "linear", color = '#289e2d')) %>%
  add_lines(data = original_safegraph_forplot,
            x = ~week, y = ~rq_rolling,
            split = ~full_name,
            name = ~full_name,
            text = ~mytext,
            hoverinfo = 'text',
            opacity = .9,
            line = list(shape = "linear", color = '#5d1aba')) %>%
  add_lines(data = original_spectus_forplot,
            x = ~week, y = ~rq_rolling,
            split = ~full_name,
            name = ~full_name,
            text = ~mytext,
            hoverinfo = 'text',
            opacity = .9,
            line = list(shape = "linear", color = '#e83cdf')) %>%
  add_lines(data = byeonghwa_retail1_forplot,
            x = ~week, y = ~rq_rolling,
            split = ~full_name,
            name = ~full_name,
            text = ~mytext,
            hoverinfo = 'text',
            opacity = .9,
            line = list(shape = "linear", color = '#547885')) %>% 
  add_lines(data = nashville_forplot,
            x = ~week, y = ~rq_rolling,
            split = ~full_name,
            name = ~full_name,
            text = ~mytext,
            hoverinfo = 'text',
            opacity = .9,
            line = list(shape = "linear", color = '#bad6b6')) %>% 
  layout(title = "Recovery rate for alternative downtowns (11 week rolling average)",
         xaxis = list(title = "Week", zerolinecolor = "#ffff",
                      tickformat = "%b %Y"),
         yaxis = list(title = "Recovery rate", zerolinecolor = "#ffff",
                      tickformat = ".0%", ticksuffix = "  "))

each_cr_plotly

saveWidget(
  each_cr_plotly,
  'C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/compare_downtown_definitions.html')
