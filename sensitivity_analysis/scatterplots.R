#===============================================================================
# Create scatterplots where one axis shows March-May 2023 rankings by city, 
# standardized by state/province and using 190199 only (for US cities), and the
# other axis has:
#   1. Normalized by MSA
#   2. No geographic normalization (but create 6-month chunks and compare to the
#      same 6-month chunk in 2019).
#   3. Total stops (rather than unique devices) from Spectus
#===============================================================================

# Setup and Libraries
# ========================================================

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'lubridate', 'leaflet', 'plotly', 'htmlwidgets'))


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

canada <- c('Manitoba', 'British Columbia', 'Alberta', 'Ontario', 'Quebec',
            'Nova Scotia')

userbase <- rbind(userbase1, userbase2) %>% 
  # change providers at 5/17/21 for Canada ONLY
  mutate(to_keep = case_when(
    !(geography_name %in% canada) & provider_id == '190199' ~ 'yes',
    (geography_name %in% canada) & 
      ((provider_id == '700199' & date < as.Date('2021-05-17')) | 
         (provider_id == '190199' & date >= as.Date('2021-05-17'))) ~ 'yes',
    TRUE ~ 'no'
  )) %>%
  filter(to_keep == 'yes' & date <= as.Date('2023-06-18')) %>%
  select(-c(provider_id, to_keep))

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
  select(-event_date)

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
  filter(date <= as.Date('2023-06-18'))

head(orig_spec1)
head(orig_spec2)

# Combine pre-2023 and 2023 original downtowns

canada_dt <- c('Calgary', 'Edmonton', 'Halifax', 'Mississauga', 'Montreal',
               'Ottawa', 'Quebec', 'Toronto', 'Vancouver', 'Winnipeg', 'London')

downtown <- rbind(orig_spec1, orig_spec2) %>%
  # change providers at 5/17/21 for Canada ONLY
  mutate(to_keep = case_when(
    !(city %in% canada_dt) & provider_id == '190199' ~ 'yes',
    (city %in% canada_dt) & 
      ((provider_id == '700199' & date < as.Date('2021-05-17')) | 
         (provider_id == '190199' & date >= as.Date('2021-05-17'))) ~ 'yes',
    TRUE ~ 'no'
  )) %>%
  filter(to_keep == 'yes' & date <= as.Date('2023-06-18')) %>%
  select(-c(provider_id, to_keep))

head(downtown)


# Load MSA data
#=====================================

s_filepath <- 'C:/Users/jpg23/data/downtownrecovery/spectus_exports/sensitivity_analysis/'

msa <-
  list.files(path = paste0(s_filepath, 'MSA')) %>% 
  map_df(~read_delim(
    paste0(s_filepath, 'MSA/', .),
    delim = '\001',
    col_names = c('msa_name', 'provider_id', 'approx_distinct_devices_count', 
                  'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date) %>%
  # change providers at 5/17/21 for Canada ONLY
  mutate(
    to_keep = case_when(
      str_detect(msa_name, '\\s') & provider_id == '190199' ~ 'yes',
      !(str_detect(msa_name, '\\s')) & 
        ((provider_id == '700199' & date < as.Date('2021-05-17')) | 
           (provider_id == '190199' & date >= as.Date('2021-05-17'))) ~ 'yes',
      TRUE ~ 'no'
    )) %>%
  filter(to_keep == 'yes') %>%
  select(-c(provider_id, to_keep)) %>%
  rename(msa_count = approx_distinct_devices_count)

msa_names <- msa %>% 
  select(msa_name) %>%
  distinct() %>%
  mutate(msa_no_state = str_remove_all(msa_name, ',.*$')) %>%
  separate(msa_no_state, remove = FALSE, sep = '-',
           into = c('name1', 'name2', 'name3', 'name4', 'name5', 'name6')) %>%
  pivot_longer(
    cols = starts_with('name'),
    names_to = 'city',
    values_to = 'city_value'
  ) %>%
  select(msa_name, city = city_value) %>%
  filter(!is.na(city) & !city %in% c('', ' ')) %>%
  mutate(city = str_remove_all(city, '\\.'),
         city = case_when(
           city == 'Washington' ~ 'Washington DC',
           city == 'Urban Honolulu' ~ 'Honolulu',
           city == 'Louisville/Jefferson County' ~ 'Louisville',
           TRUE ~ city
         )) %>%
  add_row(city = 'Mississauga', msa_name = 'Toronto') %>%
  filter(!(msa_name == 'Portland-Vancouver-Hillsboro, OR-WA' & 
             city == 'Vancouver') & city != 'Arlington')

head(msa_names %>% data.frame(), 15)
tail(msa_names %>% data.frame(), 15)

head(msa)
glimpse(msa)
range(msa$date)
unique(msa$msa_name)


# Create RQs without normalizing
#=====================================

head(downtown)

rankings_no_norm <- downtown %>%
  # Only keep March-May, 2019 and 2023
  filter((date >= as.Date('2019-03-01') & date < as.Date('2019-06-01')) |
           (date >= as.Date('2023-03-01') & date < as.Date('2023-06-01'))) %>%
  mutate(which_year = year(date)) %>%
  group_by(city, which_year) %>%
  summarise(total_devices = sum(approx_distinct_devices_count, na.rm = T)) %>%
  ungroup() %>%
  pivot_wider(
    names_from = which_year,
    names_prefix = 'yr_',
    values_from = total_devices
  ) %>%
  mutate(rq_no_norm = yr_2023/yr_2019) %>%
  select(city, rq_no_norm) %>%
  arrange(desc(rq_no_norm))

head(rankings_no_norm)


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
  dplyr::group_by(city, year, week_num) %>%
  dplyr::summarize(downtown_devices = sum(downtown_devices, na.rm = T),
                   userbase = sum(userbase, na.rm = T)) %>%
  dplyr::ungroup() 

head(rec_rate_cr)

each_cr_for_plot <-
  rec_rate_cr %>%
  filter(year > 2018) %>%
  dplyr::group_by(year, week_num, city) %>%
  dplyr::summarize(downtown_devices = sum(downtown_devices, na.rm = T),
                   userbase = sum(userbase, na.rm = T)) %>%
  dplyr::ungroup() %>%
  mutate(normalized = downtown_devices/userbase) %>%
  pivot_wider(
    id_cols = c('week_num', 'city'),
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
  arrange(city, year, week_num) %>%
  dplyr::group_by(city) %>%
  mutate(rq_rolling = zoo::rollmean(rq, k = 11, fill = NA, align = 'right')) %>%
  dplyr::ungroup() %>%
  data.frame() %>%
  filter(!(year == 2020 & week_num < 12) & !is.na(city) & 
           week >= as.Date('2020-05-11')) %>%
  select(week, city, rq_rolling)


# Join downtowns with MSA
#=====================================

downtown_msa <- downtown %>%
  left_join(msa_names)

head(downtown_msa)

final_df_msa <- 
  downtown_msa %>% 
  left_join(msa, by = c('msa_name', 'date')) %>%
  rename(downtown_devices = approx_distinct_devices_count) %>%
  mutate(normalized = downtown_devices/msa_count)

head(final_df_msa)

rec_rate_cr_msa <-
  final_df_msa %>%
  # Determine week and year # for each date
  mutate(
    date_range_start = floor_date(
      date,
      unit = "week",
      week_start = getOption("lubridate.week.start", 1)),
    week_num = isoweek(date_range_start),
    year = year(date_range_start)) %>%
  # Calculate # of devices by big_area, week and year
  dplyr::group_by(city, year, week_num) %>%
  dplyr::summarize(downtown_devices = sum(downtown_devices, na.rm = T),
                   msa_count = sum(msa_count, na.rm = T)) %>%
  dplyr::ungroup() 

head(rec_rate_cr_msa)

each_cr_for_plot_msa <-
  rec_rate_cr_msa %>%
  filter(year > 2018) %>%
  dplyr::group_by(year, week_num, city) %>%
  dplyr::summarize(downtown_devices = sum(downtown_devices, na.rm = T),
                   msa_count = sum(msa_count, na.rm = T)) %>%
  dplyr::ungroup() %>%
  mutate(normalized = downtown_devices/msa_count) %>%
  pivot_wider(
    id_cols = c('week_num', 'city'),
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
  arrange(city, year, week_num) %>%
  dplyr::group_by(city) %>%
  mutate(rq_rolling = zoo::rollmean(rq, k = 11, fill = NA, align = 'right')) %>%
  dplyr::ungroup() %>%
  data.frame() %>%
  filter(!(year == 2020 & week_num < 12)) %>%
  filter(!is.na(city))

head(each_cr_for_plot_msa)
tail(each_cr_for_plot_msa)


# Original rankings (March-May 2023)
#=====================================

unique((each_cr_for_plot %>% filter(week >= as.Date('2023-02-15')))$week)

rankings_orig <- each_cr_for_plot %>%
  filter(week >= as.Date('2023-02-27') & week <= as.Date('2023-05-22')) %>%
  group_by(city) %>%
  summarize(avg_rq_orig = mean(rq_rolling, na.rm = TRUE)) %>%
  arrange(desc(avg_rq_orig)) # %>%
  # mutate(rank_orig = row_number())

head(rankings_orig)


# MSA rankings (March-May 2023)
#=====================================

unique((each_cr_for_plot_msa %>% filter(week >= as.Date('2023-02-15')))$week)

rankings_msa <- each_cr_for_plot_msa %>%
  filter(week >= as.Date('2023-02-27') & week <= as.Date('2023-05-22')) %>%
  group_by(city) %>%
  summarize(avg_rq_msa = mean(rq_rolling, na.rm = TRUE)) %>%
  arrange(desc(avg_rq_msa)) # %>%
  # mutate(rank_msa = row_number())

head(rankings_msa)


# Original vs. no normalization scatterplot
#=====================================

n_distinct(rankings_orig$city)
n_distinct(rankings_no_norm$city)

orig_no_norm <- rankings_orig %>% 
  left_join(rankings_no_norm) %>%
  mutate(mytext = paste0(city, ': <br>- RQ (state/province) = ', 
                         round(avg_rq_orig, 2), '<br>- RQ (no normalization) = ', 
                         round(rq_no_norm, 2)))

head(orig_no_norm)

orig_no_norm_scatter <- 
  plot_ly(data = orig_no_norm,
          text = ~mytext,
          hoverinfo = 'text',
          x = ~avg_rq_orig, 
          y = ~rq_no_norm) %>%
  layout(title = 'Recovery quotient: normalized by state/province vs. no normalization<br>(provider 190199 only in U.S.), March - May 2023', 
         xaxis = list(title = 'RQ (standardized by state/province)'), 
         yaxis = list(title = 'RQ (no normalization)'),
         shapes = list(
           type = "line", 
           x0 = 0, 
           x1 = 2, 
           xref = "x",
           y0 = 0, 
           y1 = ~max(avg_rq_orig, rq_no_norm),
           yref = "y",
           line = list(color = "black", dash = "dot", width = .5)
         ))

orig_no_norm_scatter

saveWidget(
  orig_no_norm_scatter,
  'C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/scatter_state_v_no_norm.html')


# Original vs. MSA scatterplot
#=====================================

n_distinct(rankings_orig$city)
n_distinct(rankings_msa$city)

orig_msa <- rankings_orig %>% 
  left_join(rankings_msa) %>%
  mutate(mytext = paste0(city, ': <br>- RQ (state/province) = ', 
                         round(avg_rq_orig, 2), '<br>- RQ (MSA) = ', 
                         round(avg_rq_msa, 2)))

head(orig_msa)

orig_msa_scatter <- 
  plot_ly(data = orig_msa,
          text = ~mytext,
          hoverinfo = 'text',
          x = ~avg_rq_orig, 
          y = ~avg_rq_msa) %>%
  layout(title = 'Recovery quotient: normalized by state/province vs. MSA<br>(provider 190199 only in U.S.), March - May 2023', 
         xaxis = list(title = 'RQ (standardized by state/province)'), 
         yaxis = list(title = 'RQ (standardized by MSA)'),
         shapes = list(
           type = "line", 
           x0 = 0, 
           x1 = ~max(avg_rq_orig, avg_rq_msa), 
           xref = "x",
           y0 = 0, 
           y1 = ~max(avg_rq_orig, avg_rq_msa),
           yref = "y",
           line = list(color = "black", dash = "dot", width = .5)
         ))

orig_msa_scatter

saveWidget(
  orig_msa_scatter,
  'C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/scatter_state_v_MSA.html')


# Unique devices vs. total stops scatterplot
#=====================================

# (how to get total stops?)