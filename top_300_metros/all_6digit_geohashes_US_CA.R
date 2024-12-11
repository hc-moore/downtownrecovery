#===============================================================================
# Create dataset of all 6-digit geohashes in the US and Canada
#===============================================================================

# Load packages
#=====================================

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'lubridate'))

# Load data
#=====================================

top300 <- read.csv('/Users/jpg23/data/downtownrecovery/top_300_metros/top300_2023-04-01_2024-03-31.csv')

fp <- '/Users/jpg23/data/downtownrecovery/spectus_exports/not_top300_6digit_geohashes_US_CA/'

other <-
  list.files(path = fp) %>%
  map_df(~read_delim(
    paste0(fp, .),
    delim = '\001',
    col_names = c('geohash6', 'n_stops'),
    col_types = c('ci')
  )) %>%
  data.frame()

head(top300)
n_distinct(top300$geohash6)

head(other)
n_distinct(other$geohash6)

# Is there any overlap? There shouldn't be
top300 %>% filter(geohash6 %in% other$geohash6)
other %>% filter(geohash6 %in% top300$geohash6)

# Combine
both <- rbind(top300 %>% rename(n_stops = total_stops), other) %>% arrange(desc(n_stops))
head(both)
n_distinct(both$geohash6)

write.csv(
  both,
  '/Users/jpg23/data/downtownrecovery/top_300_metros/all_6digit_geohashes_US_CA_2023-04-01_2024-03-31.csv',
  row.names = F
)
