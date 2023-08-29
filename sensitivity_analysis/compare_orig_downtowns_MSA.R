#===============================================================================
# Create CSV with average distance between recovery rates for original
# downtowns - [safegraph + spectus] vs. [spectus only]. Use MSA instead of
# state/province to standardize.
#===============================================================================

# Load packages
#=====================================

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'sf', 'lubridate', 'leaflet', 'plotly', 'htmlwidgets',
       'arrow'))

# Load MSA data
#=====================================

s_filepath <- 'C:/Users/jpg23/data/downtownrecovery/spectus_exports/sensitivity_analysis/'

msa_2prov <-
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
  select(-event_date)

unique(msa_2prov$provider_id)

msa <- msa_2prov %>%
  filter((provider_id == '700199' & date < as.Date('2021-05-17')) | 
           (provider_id == '190199' & date >= as.Date('2021-05-17'))) %>%
  select(-provider_id)

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

downtown_2prov <- rbind(orig_spec1, orig_spec2) %>% 
  filter(date <= as.Date('2023-06-18'))

head(downtown_2prov)

# Combine pre-2023 and 2023 original downtowns
downtown <- rbind(orig_spec1, orig_spec2) %>%
  filter((provider_id == '700199' & date < as.Date('2021-05-17')) | 
            (provider_id == '190199' & date >= as.Date('2021-05-17'))) %>%
  select(-c(provider_id, cat))

head(downtown)

# Plot shift in providers (spectus)
#=====================================

msa_2prov_forplot <- msa_2prov %>%
  filter(provider_id != '230599') %>%
  mutate(
    date_range_start = floor_date(
      date,
      unit = "week",
      week_start = getOption("lubridate.week.start", 1))) %>%
    # Calculate # of devices by big_area, week and year
    dplyr::group_by(msa_name, provider_id, date_range_start) %>%
    dplyr::summarize(msa_count = sum(msa_count, na.rm = T)) %>%
    dplyr::ungroup() 

head(msa_2prov_forplot)

dt_2prov_forplot <- downtown_2prov %>%
  filter(provider_id != '230599') %>%
  mutate(
    date_range_start = floor_date(
      date,
      unit = "week",
      week_start = getOption("lubridate.week.start", 1))) %>%
  # Calculate # of devices by big_area, week and year
  dplyr::group_by(city, provider_id, date_range_start) %>%
  dplyr::summarize(dt_devices = sum(approx_distinct_devices_count, na.rm = T)) %>%
  dplyr::ungroup() 

head(dt_2prov_forplot)

prov_shift <- plot_ly() %>%
  add_lines(data = dt_2prov_forplot,
            x = ~date_range_start, y = ~dt_devices,
            split = ~city,
            color = ~provider_id,
            colors = c("#592d6b", "#f09329"),
            name = ~paste0('Downtown ', provider_id, ' - ', city),
            text = ~paste0('Downtown ', provider_id, ' - ', city),
            opacity = .7,
            line = list(shape = "linear")) %>%
  add_lines(data = msa_2prov_forplot,
            x = ~date_range_start, y = ~msa_count,
            split = ~msa_name,
            color = ~provider_id,
            colors = c("#592d6b", "#f09329"),
            name = ~paste0('MSA ', provider_id, ' - ', msa_name),
            text = ~paste0('MSA ', provider_id, ' - ', msa_name),
            opacity = .7,
            line = list(shape = "linear")) %>%
  layout(title = "Spectus provider shift (userbase & MSAs)",
         xaxis = list(title = "Week", zerolinecolor = "#ffff",
                      tickformat = "%b %Y"),
         yaxis = list(title = "Raw counts", zerolinecolor = "#ffff",
                      ticksuffix = "  "),
         shapes = list(list(y0 = 0, y1 = 1, yref = "paper",
                            x0 = as.Date('2021-05-17'), x1 = as.Date('2021-05-17'),
                            line = list(color = 'gray', dash = 'dot'))))

prov_shift

saveWidget(
  prov_shift,
  'C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/prov_shift_dt_MSA.html')

# Plot provider shift (normalized)
#=====================================

# Join downtowns with MSA
#=====================================

norm_prov2 <- dt_2prov_forplot %>%
  left_join(msa_names) %>%
  left_join(msa_2prov_forplot, by = c('msa_name', 'date_range_start', 
                                      'provider_id')) %>%
  mutate(normalized = dt_devices/msa_count)

head(norm_prov2)

norm_prov2_plot <- plot_ly() %>%
  add_lines(data = norm_prov2,
            x = ~date_range_start, y = ~normalized,
            split = ~city,
            color = ~provider_id,
            colors = c("#592d6b", "#f09329"),
            name = ~paste0('Normalized ', provider_id, ' - ', city),
            text = ~paste0('Normalized ', provider_id, ' - ', city),
            opacity = .7,
            line = list(shape = "linear")) %>%
  layout(title = "Spectus provider shift (normalized, standardized using MSA)",
         xaxis = list(title = "Week", zerolinecolor = "#ffff",
                      tickformat = "%b %Y"),
         yaxis = list(title = "Normalized counts", zerolinecolor = "#ffff",
                      ticksuffix = "  "),
         shapes = list(list(y0 = 0, y1 = 1, yref = "paper",
                            x0 = as.Date('2021-05-17'), x1 = as.Date('2021-05-17'),
                            line = list(color = 'gray', dash = 'dot'))))

norm_prov2_plot

saveWidget(
  norm_prov2_plot,
  'C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/prov_shift_norm_MSA.html')

# Manipulate data for plotting
#=====================================

downtown1 <- downtown %>%
  left_join(msa_names)

head(downtown1)
head(msa)

final_df <- 
  downtown1 %>% 
  left_join(msa, by = c('msa_name', 'date')) %>%
  rename(downtown_devices = approx_distinct_devices_count) %>%
  mutate(normalized = downtown_devices/msa_count)

head(final_df)

final_2019 <- final_df %>% filter(date < as.Date('2020-01-01'))
range(final_2019$date)
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
                   msa_count = sum(msa_count, na.rm = T)) %>%
  dplyr::ungroup() 

head(rec_rate_cr)

each_cr_for_plot <-
  rec_rate_cr %>%
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
  filter(!(year == 2020 & week_num < 12) & !is.na(city) & 
           week >= as.Date('2020-05-11')) %>%
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

all_weeks <- ggplot(entire_pd, aes(x = reorder(city, desc(avg_diff)), 
                                  y = avg_diff)) +
  geom_bar(stat="identity") +
  coord_flip() +
  ggtitle("Average difference, entire time period: [spectus only, standardized using MSA] - [safegraph + spectus]") +
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

june_only <- ggplot(june_df, aes(x = reorder(city, desc(avg_diff)), 
                                   y = avg_diff)) +
  geom_bar(stat="identity") +
  coord_flip() +
  ggtitle("Average difference, June 2023: [spectus only, standardized using MSA] - [safegraph + spectus]") +
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
  ggtitle("Recovery quotient rankings for March - June 2023 (spectus only, standardized using MSA)") +
  scale_y_continuous(labels = scales::percent) +
  theme_bw() +
  theme(axis.title.x = element_blank(),
        axis.title.y = element_blank())

rank_plot

# Compare Safegraph & Spectus
# (provider 700199) in 2019
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
      mutate(normalized_spectus = downtown_devices/msa_count,
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
  layout(title = "Downtown counts - Safegraph vs Spectus (2019, standardized by MSA), provider 700199",
         xaxis = list(title = "Week", zerolinecolor = "#ffff",
                      tickformat = "%b %Y"),
         yaxis = list(title = "Downtown raw counts", zerolinecolor = "#ffff",
                      ticksuffix = "  "))

compare_counts

saveWidget(
  compare_counts,
  'C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/safegraph_spectus_2019_counts_700199_MSA.html')

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
  layout(title = "Downtown normalized counts - Safegraph vs Spectus (2019, standardized by MSA), provider 700199",
         xaxis = list(title = "Week", zerolinecolor = "#ffff",
                      tickformat = "%b %Y"),
         yaxis = list(title = "Downtown normalized counts", zerolinecolor = "#ffff",
                      ticksuffix = "  "))

compare_norm

saveWidget(
  compare_norm,
  'C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/safegraph_spectus_2019_norm_700199_MSA.html')


# Compare Safegraph & Spectus
# (provider 190199) in 2019
#=====================================

msa_190199 <- msa_2prov %>% 
  filter(date >= as.Date('2019-01-01') & 
           date <= as.Date('2019-12-31') & provider_id == '190199') %>%
  select(-provider_id)

downtown_190199 <- rbind(orig_spec1, orig_spec2) %>%
  filter(date >= as.Date('2019-01-01') & 
           date <= as.Date('2019-12-31') & provider_id == '190199') %>%
  select(-c(provider_id, cat))

head(msa_190199)
head(downtown_190199)

final_df_190199 <- 
  downtown_190199 %>% 
  left_join(msa_names) %>%
  left_join(msa_190199, by = c('msa_name', 'date')) %>%
  rename(downtown_devices = approx_distinct_devices_count) %>%
  mutate(normalized = downtown_devices/msa_count)

rec_rate_cr_190199 <-
  final_df_190199 %>%
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

compare_2019_190199 <- safe %>%
  left_join(
    rec_rate_cr_190199 %>%
      mutate(normalized_spectus = downtown_devices/msa_count,
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

head(compare_2019_190199)
range(compare_2019_190199$date_range_start)

# Counts
#-------------------

compare_counts_190199 <- plot_ly() %>%
  add_lines(data = compare_2019_190199 %>% filter(type == 'counts'),
            x = ~date_range_start, y = ~value,
            split = ~city,
            color = ~provider,
            colors = c("#ffa600", "#bc5090"),
            name = ~paste0(provider, ' - ', city),
            text = ~paste0(provider, ' - ', city),
            opacity = .7,
            line = list(shape = "linear")) %>%
  layout(title = "Downtown counts - Safegraph vs Spectus (2019, standardized by MSA), provider 190199",
         xaxis = list(title = "Week", zerolinecolor = "#ffff",
                      tickformat = "%b %Y"),
         yaxis = list(title = "Downtown raw counts", zerolinecolor = "#ffff",
                      ticksuffix = "  "))

compare_counts_190199

saveWidget(
  compare_counts_190199,
  'C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/safegraph_spectus_2019_counts_190199_MSA.html')

# Normalized
#-------------------

compare_norm_190199 <- plot_ly() %>%
  add_lines(data = compare_2019_190199 %>% filter(type == 'normalized'),
            x = ~date_range_start, y = ~value,
            split = ~city,
            color = ~provider,
            colors = c("#ffa600", "#bc5090"),
            name = ~paste0(provider, ' - ', city),
            text = ~paste0(provider, ' - ', city),
            opacity = .7,
            line = list(shape = "linear")) %>%
  layout(title = "Downtown normalized counts - Safegraph vs Spectus (2019, standardized by MSA), provider 190199",
         xaxis = list(title = "Week", zerolinecolor = "#ffff",
                      tickformat = "%b %Y"),
         yaxis = list(title = "Downtown normalized counts", zerolinecolor = "#ffff",
                      ticksuffix = "  "))

compare_norm_190199

saveWidget(
  compare_norm_190199,
  'C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/safegraph_spectus_2019_norm_190199_MSA.html')
