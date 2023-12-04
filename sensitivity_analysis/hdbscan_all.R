#===============================================================================
# Calculate recovery rates (and show trends) for all cities' downtowns
#   - definitions using HDBSCAN
#   - standardized by MSA
#   - 190199 only - Canadian data imputed pre-5/17/21
#===============================================================================

# Load packages
#-----------------------------------------

source('~/git/timathomas/functions/functions.r')

ipak(c('tidyverse', 'lubridate', 'ggplot2', 'plotly', 
       'sf', 'leaflet', 'BAMMtools', 'gtools', 'htmlwidgets'))

# # Load downtown data
# #-----------------------------------------
# 
# filepath <- 'C:/Users/jpg23/data/downtownrecovery/spectus_exports/sensitivity_analysis/'
# 
# not_t_m <-
#   list.files(path = paste0(filepath, 'hdbscan_all')) %>%
#   map_df(~read_delim(
#     paste0(filepath, 'hdbscan_all/', .),
#     delim = '\001',
#     col_names = c('city', 'provider_id', 'approx_distinct_devices_count',
#                   'event_date'),
#     col_types = c('ccii')
#   )) %>%
#   data.frame() %>%
#   mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
#   arrange(date) %>%
#   select(-event_date) %>%
#   filter(city != 'Toronto' & provider_id != '230599')
# 
# unique(not_t_m$city)
# unique(not_t_m$provider_id)
# 
# t_m <-
#   list.files(path = paste0(filepath, 'hdbscan_t_m')) %>%
#   map_df(~read_delim(
#     paste0(filepath, 'hdbscan_t_m/', .),
#     delim = '\001',
#     col_names = c('city', 'provider_id', 'approx_distinct_devices_count',
#                   'event_date'),
#     col_types = c('ccii')
#   )) %>%
#   data.frame() %>%
#   mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
#   arrange(date) %>%
#   select(-event_date) %>%
#   filter(provider_id != '230599')
# 
# unique(t_m$city)
# unique(t_m$provider_id)
# 
# # Stack them
# dt <-
#   rbind(not_t_m, t_m) %>%
#   mutate(city = str_remove(city, '\\s\\w{2}$'),
#          city = case_when(
#            city == 'Indianapolis city (balance)' ~ 'Indianapolis',
#            city == 'Nashville-Davidson metropolitan government (balance)' ~
#              'Nashville',
#            city == "Ottawa - Gatineau (Ontario part / partie de l'Ontario)" ~
#              'Ottawa',
#            city == 'Urban Honolulu' ~ 'Honolulu',
#            city == 'Montréal' ~ 'Montreal',
#            city == 'St. Louis' ~ 'St Louis',
#            city == 'Washington' ~ 'Washington DC',
#            city == 'Québec' ~ 'Quebec',
#            TRUE ~ city
#            )) %>%
#   rename(downtown_devices = approx_distinct_devices_count)
# 
# head(dt)
# n_distinct(dt$city)
# unique(dt$city)
# 
# # Load MSA data
# #-----------------------------------------
# 
# msa <-
#   list.files(path = paste0(filepath, 'MSA')) %>%
#   map_df(~read_delim(
#     paste0(filepath, 'MSA/', .),
#     delim = '\001',
#     col_names = c('msa_name', 'provider_id', 'approx_distinct_devices_count',
#                   'event_date'),
#     col_types = c('ccii')
#   )) %>%
#   data.frame() %>%
#   mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
#   arrange(date) %>%
#   select(-event_date) %>%
#   rename(msa_count = approx_distinct_devices_count) %>%
#   filter(provider_id != '230599')
# 
# msa_names <- read.csv('C:/Users/jpg23/data/downtownrecovery/sensitivity_analysis/msa_names.csv')
# 
# head(msa)
# unique(msa$provider_id)
# 
# head(msa_names)
# 
# # Join downtown & MSA data
# #-----------------------------------------
# 
# msa_cities <- unique(msa_names$city)
# dt_cities <- unique(dt$city)
# 
# setdiff(dt_cities, msa_cities)
# 
# dt1 <- dt %>% left_join(msa_names)
# 
# head(dt1)
# head(msa)
# 
# dt1 %>% filter(is.na(msa_name)) # should be no rows
# 
# final_df <-
#   dt1 %>%
#   left_join(msa, by = c('msa_name', 'provider_id', 'date'))
# 
# head(final_df)
# 
# canada_cities <- c('Calgary', 'Edmonton', 'Halifax', 'London', 'Mississauga',
#                    'Montreal', 'Ottawa', 'Quebec', 'Toronto', 'Vancouver',
#                    'Winnipeg')
# 
# unique(final_df$provider_id)
# 
# # Export for Amir's weekend vs weekday analysis
# for_amir <-
#   final_df %>%
#   filter(# provider_id == '190199' &
#            !(city %in% canada_cities & date < as.Date('2021-05-17'))) %>%
#   mutate(normalized = downtown_devices/msa_count)
# 
# head(for_amir)
# 
# amir_plot_190 <-
#   plot_ly() %>%
#   add_lines(data = for_amir %>% filter(!city %in% canada_cities &
#                                          provider_id == '190199'),
#             x = ~date, y = ~downtown_devices,
#             name = ~city,
#             opacity = .7,
#             split = ~city,
#             text = ~paste0(city, ': ', downtown_devices),
#             line = list(shape = "linear", color = '#d6ad09')) %>%
#   add_lines(data = for_amir %>% filter(city %in% canada_cities &
#                                          provider_id == '190199'),
#             x = ~date, y = ~downtown_devices,
#             name = ~city,
#             opacity = .7,
#             split = ~city,
#             text = ~paste0(city, ': ', downtown_devices),
#             line = list(shape = "linear", color = 'purple')) %>%
#   layout(title = "190199")
# 
# amir_plot_190
# 
# amir_plot_700 <-
#   plot_ly() %>%
#   add_lines(data = for_amir %>% filter(!city %in% canada_cities &
#                                          provider_id == '700199'),
#             x = ~date, y = ~downtown_devices,
#             name = ~city,
#             opacity = .7,
#             split = ~city,
#             text = ~paste0(city, ': ', downtown_devices),
#             line = list(shape = "linear", color = '#d6ad09')) %>%
#   add_lines(data = for_amir %>% filter(city %in% canada_cities &
#                                          provider_id == '700199'),
#             x = ~date, y = ~downtown_devices,
#             name = ~city,
#             opacity = .7,
#             split = ~city,
#             text = ~paste0(city, ': ', downtown_devices),
#             line = list(shape = "linear", color = 'purple')) %>%
#   layout(title = "700199")  
# 
# amir_plot_700
# 
# write.csv(for_amir,
#           'C:/Users/jpg23/data/downtownrecovery/sensitivity_analysis/for_amir_weekend_weekday.csv',
#           row.names = F)
# 
# # Export normalized counts for imputation
# #-----------------------------------------
# 
# for_imputation <-
#   final_df %>%
#   mutate(
#     date_range_start = floor_date(
#       date,
#       unit = "week",
#       week_start = getOption("lubridate.week.start", 1))) %>%
#   group_by(date_range_start, city, provider_id) %>%
#   summarize(downtown_devices = sum(downtown_devices, na.rm = T),
#             msa_count = sum(msa_count, na.rm = T)) %>%
#   ungroup() %>%
#   data.frame() %>%
#   mutate(normalized = downtown_devices/msa_count)
# 
# head(for_imputation)
# range(for_imputation$date_range_start)
# 
# write.csv(for_imputation,
#           'C:/Users/jpg23/data/downtownrecovery/sensitivity_analysis/hdbscan_dt_for_imputation.csv',
#           row.names = F)

# Calculate RQs for US
#-----------------------------------------

canada_cities <- c('Calgary', 'Edmonton', 'Halifax', 'London', 'Mississauga',
                   'Montreal', 'Ottawa', 'Quebec', 'Toronto', 'Vancouver',
                   'Winnipeg')

not_imputed_us <- read.csv('C:/Users/jpg23/data/downtownrecovery/sensitivity_analysis/hdbscan_dt_for_imputation.csv') %>%
  filter(!city %in% canada_cities)

head(not_imputed_us)

# rq (for US):

# 1. filter to only March - June 18, 2019 and 2023
# 2. sum downtown unique devices and MSA unique devices by city and year
# 3. calculate normalized count by dividing downtown/MSA for each year
#    (for overall time period)
# 4. divide normalized 2023 by normalized 2019

rq_us <-
  not_imputed_us %>%
  filter(provider_id == '190199') %>%
  filter((date_range_start >= as.Date('2019-03-04') &
            date_range_start <= as.Date('2019-06-10')) |
           (date_range_start >= as.Date('2023-02-27'))) %>%
  mutate(year = year(date_range_start)) %>%
  group_by(city, year) %>%
  summarize(dt = sum(downtown_devices, na.rm = T),
            msa = sum(msa_count, na.rm = T)) %>%
  ungroup() %>%
  mutate(norm = dt/msa) %>%
  pivot_wider(
    id_cols = c('city'),
    names_from = 'year',
    names_prefix = 'ntv',
    values_from = 'norm'
  ) %>%
  mutate(rq = ntv2023/ntv2019) %>%
  data.frame() %>%
  select(city, rq) %>%
  arrange(desc(rq))

rq_us

# Calculate RQs for Canada
#-----------------------------------------

imputed_canada <- read.csv('C:/Users/jpg23/data/downtownrecovery/sensitivity_analysis/imputation_Canada_msa_SAITS_hdbscan_fin.csv') %>%
  filter(city %in% canada_cities)

head(imputed_canada)

# rq2 (for Canada):

# 1. sum downtown unique devices and MSA unique devices by city, year and week
# 2. calculate normalized count by week
# 3. divide normalized count 2023 by normalized count 2019 to get weekly RQ
# 4. take average RQ by city

rq_ca <-
  imputed_canada %>%
  filter(provider_id == '190199') %>%
  filter((date_range_start >= as.Date('2019-03-04') & 
            date_range_start <= as.Date('2019-06-10')) | 
           (date_range_start >= as.Date('2023-02-27'))) %>%
  mutate(week_num = isoweek(date_range_start),
         year = year(date_range_start)) %>%
  select(-date_range_start) %>%
  pivot_wider(
    id_cols = c('city', 'week_num'),
    names_from = 'year',
    names_prefix = 'ntv',
    values_from = 'normalized'
  ) %>%
  mutate(rec2023 = ntv2023/ntv2019) %>%
  data.frame() %>%
  group_by(city) %>%
  summarize(rq = mean(rec2023, na.rm = T)) %>%
  ungroup() %>%
  data.frame() %>%
  arrange(desc(rq))

rq_ca


# # Plot
# imputed_plot <-
#   plot_ly() %>%
#   add_lines(data = imputed %>% filter(provider_id == '190199'),
#             x = ~date_range_start, y = ~normalized,
#             name = ~paste0(city, ":  provider 190199"),
#             opacity = .7,
#             split = ~city,
#             text = ~paste0(city, ': ', round(normalized, 3)),
#             line = list(shape = "linear", color = '#d6ad09')) %>%
#   add_lines(data = imputed %>% filter(provider_id == '700199'),
#             x = ~date_range_start, y = ~normalized,
#             name = ~paste0(city, ": provider 700199"),
#             opacity = .7,
#             split = ~city,
#             text = ~paste0(city, ': ', round(normalized, 3)),
#             line = list(shape = "linear", color = '#8c0a03')) %>%
#   layout(title = "Weekly counts normalized by MSA (imputed for Canada, HDBSCAN downtowns)",
#          xaxis = list(title = "Week", zerolinecolor = "#ffff",
#                       tickformat = "%b %Y"),
#          yaxis = list(title = "Normalized", zerolinecolor = "#ffff",
#                       ticksuffix = "  "),
#          shapes = list(list(y0 = 0, y1 = 1, yref = "paper",
#                             x0 = as.Date('2021-05-17'), x1 = as.Date('2021-05-17'),
#                             line = list(color = 'black', dash = 'dash'))))
# 
# imputed_plot
# 
# saveWidget(
#   imputed_plot,
#   'C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/hdbscan_downtowns_by_provider.html')

# Export weekly RQs for Amir
#-----------------------------------------

rq_amir <-
  imputed_canada %>%
  rbind(not_imputed_us %>% select(-c(downtown_devices, msa_count))) %>%
  filter(provider_id == '190199') %>%
  filter(date_range_start >= as.Date('2023-01-02') |
           (date_range_start >= as.Date('2018-12-31') & 
              date_range_start <= as.Date('2019-06-10'))) %>%
  mutate(week_num = isoweek(date_range_start),
         year = year(date_range_start)) %>%
  select(-date_range_start) %>%
  pivot_wider(
    id_cols = c('city', 'week_num'),
    names_from = 'year',
    names_prefix = 'ntv',
    values_from = 'normalized'
  ) %>%
  mutate(
    rq23 = case_when(
      !is.na(ntv2019) ~ ntv2023/ntv2019,
      TRUE ~ ntv2023/ntv2018),
    date_range_start = as.Date(paste(2023, week_num, 1, sep="-"), "%Y-%U-%u")
    ) %>%
  data.frame() %>%
  select(city, date_range_start, rq23)

head(rq_amir)
tail(rq_amir)
range(rq_amir$date_range_start)

write.csv(rq_amir,
          'C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/hdbscan_weekly_rq_for_amir.csv',
          row.names = F)

# Combine Canada & US RQs
#-----------------------------------------

head(rq_us)
head(rq_ca)

rq <- rbind(rq_us, rq_ca) %>%
  arrange(desc(rq))

rq

write.csv(rq,
          'C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/hdbscan_rankings.csv',
          row.names = F)

rank_plot <- 
  ggplot(rq, aes(x = reorder(city, rq), y = rq)) +
  geom_bar(stat="identity") +
  coord_flip() +
  ggtitle("Recovery quotient rankings, March - mid-June 2023\n(HDBSCAN downtowns, spectus only, standardized using MSA, provider 190199,\n imputed for Canada using SAITS, rq for US and rq2 for Canada)") +
  scale_y_continuous(labels = scales::percent) +
  theme_bw() +
  theme(axis.title.x = element_blank(),
        axis.title.y = element_blank())

rank_plot

# Compare to old rankings on website
#-----------------------------------------

site_rank <- read.csv("C:/Users/jpg23/data/downtownrecovery/sensitivity_analysis/ranking_data_from_website.csv")

head(rq)
head(site_rank)

site_cities <- unique(site_rank$city)
rq_cities <- unique(rq$city)

setdiff(site_cities, rq_cities)
setdiff(rq_cities, site_cities)

# How did the rankings change?

rank_comparison <-
  rq %>% mutate(rank = row_number()) %>% select(city, rank) %>%
  inner_join(site_rank %>% filter(Season == 'Season_13') %>% 
               arrange(desc(seasonal_average)) %>%
               mutate(rank_site = row_number()) %>%
               select(city, rank_site)) %>%
  mutate(rank_diff = rank - rank_site) %>%
  arrange(rank_diff) %>%
  select(city, rank_diff)

rank_comparison

# How did the RQs change?

comparison <-
  rq %>%
  inner_join(site_rank %>% filter(Season == 'Season_13') %>%
               select(city, rq_site = seasonal_average)) %>%
  mutate(diff = rq - rq_site) %>%
  arrange(desc(diff))

head(comparison)

write.csv(comparison,
          'C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/hdbscan_site_rank_comparison.csv',
          row.names = F)

comparison_plot <-
  ggplot(comparison,
         aes(x = reorder(city, diff), y = diff)) +
  geom_bar(stat="identity") +
  coord_flip() +
  ggtitle("Change from old RQ on website to updated HDBSCAN RQ\n(negative = new RQ is lower than old RQ)") +
  scale_y_continuous(labels = scales::percent) +
  theme_bw() +
  theme(axis.title.x = element_blank(),
        axis.title.y = element_blank())

comparison_plot # export to "C:\Users\jpg23\UDP\downtown_recovery\sensitivity_analysis\hdbscan_compare_to_old_rankings.png"

# Export
#-----------------------------------------

# Format for Jeff

final_export <-
  rq %>%
  # get display_title and region
  left_join(site_rank %>% filter(Season == 'Season_13') %>%
              select(display_title, city, region)) %>%
  mutate(
    region = case_when(
      city == 'Oklahoma City' ~ 'Southwest',
      city == 'Orlando' ~ 'Southeast',
      city == 'Cleveland' ~ 'Midwest',
      city == 'Dallas' ~ 'Southwest',
      TRUE ~ region      
    ),
    display_title = case_when(
      city == 'Oklahoma City' ~ 'Oklahoma City, OK',
      city == 'Orlando' ~ 'Orlando, FL',
      city == 'Cleveland' ~ 'Cleveland, OH',
      city == 'Dallas' ~ 'Dallas, TX',
      TRUE ~ display_title
    )) %>%
  rename(seasonal_average = rq)

# check
final_export %>% filter(city %in% c('Oklahoma City', 'Orlando', 'Cleveland', 'Dallas'))

final_export

write.csv(final_export,
          'C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/final_hdbscan_for_website.csv',
          row.names = F)
