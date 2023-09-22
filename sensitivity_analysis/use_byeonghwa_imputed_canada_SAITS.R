#===============================================================================
# Compare downtowns, but use provider 190199 (imputed for Canada by Byeonghwa
# using SAITS: Self-Attention-based Imputation for Time Series), and standardized 
# by MSA
#===============================================================================

# Load packages
#=====================================

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'sf', 'lubridate', 'leaflet', 'plotly', 'htmlwidgets',
       'tidyr', 'ggplot2'))

# Load imputed MSA data for Canada
#=====================================

imputed <- read.csv('C:/Users/jpg23/data/downtownrecovery/imputed_canada_190199/imputation_Canada_msa_Byeonghwa_us_ca_SAITS.csv') %>%
  filter(city %in% canada_dt) %>%
  mutate(mytext = case_when(
    date_range_start < as.Date('2021-05-17') & provider_id == '190199' ~ 
             paste('IMPUTED:', city, paste0('Provider ', provider_id), 
                   normalized_msa, sep = '<br>'),
           TRUE ~ paste(city, paste0('Provider ', provider_id), normalized_msa, 
                        sep = '<br>')))

head(imputed)
table(imputed$city)
range(imputed$date_range_start)

# Explore imputed MSA data for Canada
#=====================================

imputed_plot <-
  plot_ly() %>%
  add_lines(data = imputed %>% filter(provider_id == '700199'),
            x = ~date_range_start, y = ~normalized_msa,
            split = ~city,
            name = ~city,
            text = ~mytext,
            hoverinfo = 'text',
            opacity = .6,
            line = list(shape = "linear", color = '#214e36')) %>%
  add_lines(data = imputed %>% filter(provider_id == '190199'),
            x = ~date_range_start, y = ~normalized_msa,
            split = ~city,
            name = ~city,
            text = ~mytext,
            hoverinfo = 'text',
            opacity = .6,
            line = list(shape = "linear", color = '#40a7de')) %>%
  layout(title = "Provider 190199 imputed for Canada pre-5/17/21 (using SAITS)",
         xaxis = list(title = "Week", zerolinecolor = "#ffff",
                      tickformat = "%b %Y"),
         yaxis = list(title = "Normalized by MSA", zerolinecolor = "#ffff",
                      ticksuffix = "  "),
         shapes = list(list(y0 = 0, y1 = 1, yref = "paper",
                            x0 = as.Date('2021-05-17'), x1 = as.Date('2021-05-17'),
                            line = list(color = 'black', dash = 'dash'))))

imputed_plot

saveWidget(
  imputed_plot,
  'C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/byeonghwa_imputed_Canada_by_MSA_190199_SAITS.html')

# Load MSA data
#=====================================

s_filepath <- 'C:/Users/jpg23/data/downtownrecovery/spectus_exports/sensitivity_analysis/'

msa0 <-
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

head(msa0)

msa <- msa0 %>%
  # keep provider 190199 only: US cities (all dates) AND Canadian cities after 5/17/21
  filter(provider_id == '190199' & 
           (str_detect(msa_name, '\\s') | 
              (!str_detect(msa_name, '\\s') & date >= as.Date('2021-05-17')))) %>%
  rename(msa_count = approx_distinct_devices_count) %>%
  select(-provider_id)

head(msa)
unique(msa$msa_name)

msa_us <- msa %>% filter(str_detect(msa_name, '\\s'))
range(msa_us$date) # good - US has all dates

msa_ca <- msa %>% filter(!str_detect(msa_name, '\\s'))
range(msa_ca$date) # good - Canada has only 5/17/21 and later

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
unique(msa_names$msa_name)

head(msa)
glimpse(msa)
range(msa$date)
unique(msa$msa_name)

# Load downtown data for US
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
  # keep provider 190199 only: US cities (all dates) AND Canadian cities after 5/17/21
  filter(provider_id == '190199' & 
           (!(city %in% canada_dt) | 
              (city %in% canada_dt & date >= as.Date('2021-05-17')))) %>%
  select(-provider_id)

dt_us <- downtown %>% filter(!city %in% canada_dt)
range(dt_us$date) # good - US has all dates

dt_ca <- downtown %>% filter(city %in% canada_dt)
range(dt_ca$date) # good - Canada has only 5/17/21 and later

head(downtown)
unique(downtown$city)

# Join downtown & MSAs (in US only)
#=====================================

downtown1 <- downtown %>%
  left_join(msa_names)

head(downtown1)
head(msa)

final_df <- 
  downtown1 %>% 
  left_join(msa, by = c('msa_name', 'date')) %>%
  rename(downtown_devices = approx_distinct_devices_count) %>%
  select(-msa_name)

head(final_df)
unique(final_df$city)

# Join US & imputed Canadian data
#=====================================

missing_imputed <- final_df %>%
  mutate(date_range_start = 
           floor_date(date, unit = "week", 
                      week_start = getOption("lubridate.week.start", 1))) %>%
  group_by(city, date_range_start) %>%
  summarize(downtown_devices = sum(downtown_devices, na.rm = T),
            msa_count = sum(msa_count, na.rm = T)) %>%
  ungroup() %>%
  data.frame() %>%
  mutate(normalized_msa = downtown_devices/msa_count) %>%
  select(city, date_range_start, normalized_msa)

head(missing_imputed)

head(imputed)

imputed_new <- imputed %>% 
  mutate(date_range_start = as.Date(date_range_start)) %>%
  filter(provider_id == '190199' & date_range_start < as.Date('2021-05-17') &
           city %in% canada_dt) %>%
  select(city, date_range_start, normalized_msa)

head(imputed_new)
glimpse(imputed_new)

range(missing_imputed$date_range_start)
range(imputed_new$date_range_start)

rq <-
  bind_rows(missing_imputed, imputed_new) %>%
  mutate(week_num = isoweek(date_range_start), year = year(date_range_start)) %>%
  select(-date_range_start) %>%
  pivot_wider(
    id_cols = c('week_num', 'city'),
    names_from = 'year',
    names_prefix = 'ntv',
    values_from = 'normalized_msa') %>%
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

head(rq)

write.csv(rq,
          'C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/byeonghwa_RQ_imputed_canada_byMSA_SAITS.csv',
          row.names = F)

# Rankings for March - June 2023
#=====================================

rankings <- rq %>%
  filter(week >= as.Date('2023-03-07') & week < as.Date('2023-07-03')) %>%
  group_by(city) %>%
  summarize(avg_rq = mean(rq_rolling, na.rm = TRUE)) %>%
  arrange(desc(avg_rq)) %>%
  data.frame()

head(rankings)
unique(rankings$city)

write.csv(rankings,
          'C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/byeonghwa_rankings_imputed_canada_normalized_by_MSA_SAITS.csv',
          row.names = F)

rank_plot <- ggplot2::ggplot(rankings, aes(x = reorder(city, avg_rq), 
                                  y = avg_rq)) +
  geom_bar(stat="identity") +
  coord_flip() +
  ggtitle("Recovery quotient rankings for March - June 2023\n(spectus only, standardized using MSA, provider 190199 only -\nimputed pre-5/17/21 for Canada by Byeonghwa using SAITS method)") +
  scale_y_continuous(labels = scales::percent) +
  theme_bw() +
  theme(axis.title.x = element_blank(),
        axis.title.y = element_blank())

rank_plot

# Create RQ plots
#=====================================

rq_plot <- plot_ly() %>%
  add_lines(data = rq,
            x = ~week, y = ~rq_rolling,
            split = ~city,
            name = ~city,
            text = ~paste0(city, ': ', round(rq_rolling, 3)),
            opacity = .7,
            line = list(shape = "linear")) %>%
  layout(title = "Recovery rates: Spectus, standardized by MSA, provider 190199 (Canada imputed pre-5/17/21 by Byeonghwa using SAITS method)",
         xaxis = list(title = "Week", zerolinecolor = "#ffff",
                      tickformat = "%b %Y"),
         yaxis = list(title = "Recovery rate", zerolinecolor = "#ffff",
                      ticksuffix = "  "),
         shapes = list(list(y0 = 0, y1 = 1, yref = "paper",
                            x0 = as.Date('2021-05-17'), x1 = as.Date('2021-05-17'),
                            line = list(color = 'black', dash = 'dash'))))

rq_plot

saveWidget(
  rq_plot,
  'C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/byeonghwa_RQ_trends_imputed_Canada_by_MSA_190199_SAITS.html')

