#===============================================================================
# Compare Oct 2023 and Oct 2024 for Canadian cities
#===============================================================================

# Load packages
#=====================================

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse'))

# Load data
#=====================================

# Sept - Nov 2024
d2 <- read.csv('/Users/jpg23/UDP/downtown_recovery/trend_updates/canada/sept_nov_2024/stopuplevelled_canada_sept_nov_2024_outliers_removed.csv')
head(d2)
range(d2$date)

d2_sub <- d2 %>%
  filter(date >= as.Date('2024-10-01') & date < as.Date('2024-11-01')) %>%
  select(c(city, date, normalized_distinct_clean, normalized_stops_clean))

range(d2_sub$date)
unique(d2_sub$city)
head(d2_sub)

# May 2023 - May 2024
d1 <- read.csv('/Users/jpg23/data/downtownrecovery/stop_uplevelled_2023_2024/trends_may2023_may2024.csv')
head(d1)
range(d1$date)

d1_sub <- d1 %>%
  filter(city %in% d2_sub$city & date >= as.Date('2023-10-01') &
           date < as.Date('2023-11-01'))

range(d1_sub$date)
unique(d1_sub$city)

# Compare avg normalized_distinct_clean
#=====================================

d1_avg <- d1_sub %>%
  group_by(city) %>%
  summarize(avg_23 = mean(normalized_distinct_clean)) %>%
  data.frame()

d2_avg <- d2_sub %>%
  group_by(city) %>%
  summarize(avg_24 = mean(normalized_distinct_clean)) %>%
  data.frame()

d1_avg
d2_avg

d <- d1_avg %>% 
  left_join(d2_avg) %>% 
  mutate(pct_change = ((avg_24 - avg_23) / avg_23)*100) %>%
  arrange(desc(pct_change))

d

write.csv(
  d, 
  '/Users/jpg23/UDP/downtown_recovery/trend_updates/canada/oct_2023_2024.csv',
  row.names = F
)
