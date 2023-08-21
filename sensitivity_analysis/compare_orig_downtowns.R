#===============================================================================
# Create CSV with average distance between recovery rates for original
# downtowns - [safegraph + spectus] vs. [spectus only]
#===============================================================================

# Load packages
#=====================================

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'sf', 'lubridate', 'leaflet', 'plotly', 'htmlwidgets',
       'arrow'))

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
downtown <- rbind(orig_spec1, orig_spec2) %>%
  filter((provider_id == '700199' & date < as.Date('2021-05-17')) | 
            (provider_id == '190199' & date >= as.Date('2021-05-17'))) %>%
  select(-c(provider_id, cat))


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

unique(final_df$city)

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
  filter(!(year == 2020 & week_num < 12) & !is.na(city) & week >= as.Date('2020-05-11')) %>%
  select(week, city, rq_rolling)

# Now add data from website to compare
#=====================================

# https://downtownrecovery.com/charts/patterns
original <- read.csv("C:/Users/jpg23/data/downtownrecovery/sensitivity_analysis/data_from_website.csv") %>%
  filter(metric == 'downtown') %>%
  mutate(week = as.Date(week)) %>%
  select(week, city, rq_rolling_safegraph = rolling_avg)

head(each_cr_for_plot)
head(original)

unique(each_cr_for_plot$city)
unique(original$city)

range(each_cr_for_plot$week)
range(original$week)

# Compare for entire time period
#=====================================

entire_pd <- original %>%
  left_join(each_cr_for_plot, by = c('week', 'city')) %>%
  mutate(diff = rq_rolling - rq_rolling_safegraph) %>%
  group_by(city) %>%
  summarize(avg_diff = mean(diff, na.rm = T)) %>%
  data.frame() %>%
  arrange(desc(avg_diff))

head(entire_pd)
unique(entire_pd$city)
summary(entire_pd$avg_diff)

write.csv(
  entire_pd,
  "C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/all_weeks_avg_diff_orig_downtowns.csv",
  row.names = F)

all_weeks <- ggplot(entire_pd, aes(x = reorder(city, desc(avg_diff)), 
                                  y = avg_diff)) +
  geom_bar(stat="identity") +
  coord_flip() +
  ggtitle("Average difference, entire time period: [spectus only] - [safegraph + spectus]") +
  scale_y_continuous(labels = scales::percent) +
  theme_bw() +
  theme(axis.title.x = element_blank(),
        axis.title.y = element_blank()) +
  annotate("text", x = 44, y = .3, 
           label = "[spectus only] > [safegraph + spectus]") +
  annotate("text", x = 44, y = -.1, 
           label = "[safegraph + spectus] >\n[spectus only]")

all_weeks

# Compare for June 2023 only
#=====================================

june_df <- original %>%
  filter(week >= as.Date('2023-06-01')) %>%
  left_join(each_cr_for_plot, by = c('week', 'city')) %>%
  mutate(diff = rq_rolling - rq_rolling_safegraph) %>%
  group_by(city) %>%
  summarize(avg_diff = mean(diff, na.rm = T)) %>%
  data.frame() %>%
  arrange(desc(avg_diff))

head(june_df)
unique(june_df$city)
summary(june_df$avg_diff)

write.csv(
  june_df,
  "C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/june2023_avg_diff_orig_downtowns.csv",
  row.names = F)

june_only <- ggplot(june_df, aes(x = reorder(city, desc(avg_diff)), 
                                   y = avg_diff)) +
  geom_bar(stat="identity") +
  coord_flip() +
  ggtitle("Average difference, June 2023: [spectus only] - [safegraph + spectus]") +
  scale_y_continuous(labels = scales::percent) +
  theme_bw() +
  theme(axis.title.x = element_blank(),
        axis.title.y = element_blank()) +
  annotate("text", x = 44, y = .45, 
           label = "[spectus only] > [safegraph + spectus]") +
  annotate("text", x = 44, y = -.2, 
           label = "[safegraph + spectus] >\n[spectus only]")

june_only

# Rankings for March - June 2023
#=====================================

rankings <- each_cr_for_plot %>%
  filter(week >= as.Date('2023-03-07') & week < as.Date('2023-07-03')) %>%
  group_by(city) %>%
  summarize(avg_rq = mean(rq_rolling, na.rm = TRUE)) %>%
  arrange(desc(avg_rq))

head(rankings)
unique(rankings$city)

rank_plot <- ggplot(rankings, aes(x = reorder(city, avg_rq), 
                                  y = avg_rq)) +
  geom_bar(stat="identity") +
  coord_flip() +
  ggtitle("Recovery quotient rankings for March - June 2023 (spectus only)") +
  scale_y_continuous(labels = scales::percent) +
  theme_bw() +
  theme(axis.title.x = element_blank(),
        axis.title.y = element_blank())

rank_plot

# Compare Safegraph & Spectus 2019
#=====================================

safe0 <- read_parquet("C:/Users/jpg23/Downloads/safegraph_dt_recovery.pq")

# How many zips per city?
safe0 %>% 
  group_by(city) %>% 
  summarize(n_zip = n_distinct(postal_code)) %>% 
  arrange(desc(n_zip)) %>% 
  head()

safe <- safe0 %>%
  mutate(city = str_replace(city, "é", "e")) %>%
  select(-postal_code, -is_downtown, -normalized_visits_by_state_scaling) %>%
  group_by(date_range_start, city) %>%
  summarise(counts_safegraph = sum(raw_visit_counts),
            normalized_safegraph = sum(normalized_visits_by_total_visits))

head(rec_rate_cr)
head(safe)

compare_2019 <- safe %>%
  left_join(
    rec_rate_cr %>%
      mutate(normalized_spectus = downtown_devices/userbase,
             date_range_start = as.Date(paste(year, week_num, 1, sep = '_'),
                                        format = '%Y_%W_%w')) %>%
      select(city, date_range_start, counts_spectus = downtown_devices,
             normalized_spectus)
  ) %>%
  filter(date_range_start >= as.Date('2019-01-01') & 
           date_range_start < as.Date('2019-12-30')) %>%
  data.frame() %>%
  pivot_longer(
    cols = -c(date_range_start, city), 
    names_to = c('type', 'provider'), 
    names_pattern = '(counts|normalized)_(safegraph|spectus)'
  )

head(compare_2019)
range(compare_2019$date_range_start)

# Counts
#-------------------

compare_counts <- plot_ly() %>%
  add_lines(data = compare_2019 %>% filter(type == 'counts'),
            x = ~date_range_start, y = ~value,
            split = ~city,
            color = ~provider,
            colors = c("#ffa600", "#bc5090"),
            name = ~paste0(provider, ' - ', city),
            text = ~paste0(provider, ' - ', city),
            opacity = .7,
            line = list(shape = "linear")) %>%
  layout(title = "Downtown counts - Safegraph vs Spectus (2019)",
         xaxis = list(title = "Week", zerolinecolor = "#ffff",
                      tickformat = "%b %Y"),
         yaxis = list(title = "Downtown raw counts", zerolinecolor = "#ffff",
                      ticksuffix = "  "))

compare_counts

saveWidget(
  compare_counts,
  'C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/safegraph_spectus_2019_counts.html')

# Normalized
#-------------------

compare_norm <- plot_ly() %>%
  add_lines(data = compare_2019 %>% filter(type == 'normalized'),
            x = ~date_range_start, y = ~value,
            split = ~city,
            color = ~provider,
            colors = c("#ffa600", "#bc5090"),
            name = ~paste0(provider, ' - ', city),
            text = ~paste0(provider, ' - ', city),
            opacity = .7,
            line = list(shape = "linear")) %>%
  layout(title = "Downtown normalized counts - Safegraph vs Spectus (2019)",
         xaxis = list(title = "Week", zerolinecolor = "#ffff",
                      tickformat = "%b %Y"),
         yaxis = list(title = "Downtown normalized counts", zerolinecolor = "#ffff",
                      ticksuffix = "  "))

compare_norm

saveWidget(
  compare_norm,
  'C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/safegraph_spectus_2019_norm.html')
