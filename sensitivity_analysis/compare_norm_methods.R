# Load packages
#-----------------------------------------

source('~/git/timathomas/functions/functions.r')

ipak(c('tidyverse', 'lubridate', 'ggplot2', 'plotly', 
       'sf', 'leaflet', 'BAMMtools', 'gtools', 'htmlwidgets'))

# Load data (weekly normalized imputed)
#-----------------------------------------

canada_cities <- c('Calgary', 'Edmonton', 'Halifax', 'London', 'Mississauga',
                   'Montreal', 'Ottawa', 'Quebec', 'Toronto', 'Vancouver',
                   'Winnipeg')

imputed_norm <- read.csv('C:/Users/jpg23/data/downtownrecovery/sensitivity_analysis/imputation_Canada_msa_SAITS_hdbscan_fin.csv') %>%
  filter(!city %in% canada_cities)

# Calculate recovery rates
#-----------------------------------------

rq2 <-
  imputed_norm %>%
  filter(provider_id == '190199') %>%
  filter((date_range_start >= as.Date('2019-03-04') & 
            date_range_start <= as.Date('2019-06-10')) | 
           (date_range_start >= as.Date('2023-02-27'))) %>%
  mutate(week_num = isoweek(date_range_start),
         year = year(date_range_start)) %>%
  pivot_wider(
    id_cols = c('city', 'week_num'),
    names_from = 'year',
    names_prefix = 'ntv',
    values_from = 'normalized'
  ) %>%
  mutate(rec2023 = ntv2023/ntv2019) %>%
  data.frame() %>%
  select(city, rec2023) %>%
  group_by(city) %>%
  summarize(rq2 = mean(rec2023, na.rm = T)) %>%
  ungroup() %>%
  data.frame() %>%
  arrange(desc(rq2)) %>%
  mutate(rank_rq2 = row_number())

head(rq2)

rq_on_site <- read.csv('C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/hdbscan_rankings.csv') %>%
  filter(!city %in% canada_cities) %>%
  mutate(city = case_when(
    city == 'St Louis' ~ 'St. Louis',
    TRUE ~ city
  )) %>%
  rename(rq = rec2023) %>%
  arrange(desc(rq)) %>%
  mutate(rank_rq = row_number())

head(rq_on_site)

rq_compare <- rq2 %>% left_join(rq_on_site) %>% 
  mutate(diff = rq2 - rq) %>%
  relocate(city, rq, rq2, rank_rq, rank_rq2, diff)

rq_compare

write.csv(rq_compare,
          'C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/compare_rq_rq2_US.csv',
          row.names = F)
