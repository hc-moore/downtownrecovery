#===============================================================================
# Create new trends dataset for Canadian cities only, Oct 2023 - Oct 2024
#===============================================================================

# Load packages
#=====================================

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse'))

# Load data
#=====================================

# May - November, 2024 -> filter to May 1, 2024 - October 31, 2024
d2 <- read.csv('/Users/jpg23/UDP/downtown_recovery/trend_updates/canada/sept_nov_2024/stopuplevelled_canada_sept_nov_2024_outliers_removed.csv') %>%
  filter(date < as.Date('2024-11-01')) %>%
  select(c(city, date, normalized_distinct_clean, normalized_stops_clean))

range(d2$date)
head(d2)

# May 2023 - May 2024 -> filter to October 1, 2023 - April 30, 2024
d1 <- read.csv('/Users/jpg23/data/downtownrecovery/stop_uplevelled_2023_2024/trends_may2023_may2024.csv') %>%
  filter(date >= as.Date('2023-10-01') & date < as.Date('2024-05-01') &
           city %in% d2$city)

range(d1$date)
head(d1)

d <- rbind(d1, d2)
head(d)
range(d$date)

write.csv(d,
          '/Users/jpg23/UDP/downtown_recovery/trend_updates/canada/trend_canada_oct23_oct24.csv',
          row.names = F)
