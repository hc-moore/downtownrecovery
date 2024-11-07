#===============================================================================
# Combine Honolulu & non-Honolulu commercial downtown boundaries from Byeonghwa
#===============================================================================

# Load packages
#-------------------------------------------------------------------------------

source('~/git/timathomas/functions/functions.r')

ipak(c('tidyr', 'tidyverse', 'sf'))

# Load data
#-------------------------------------------------------------------------------

setwd('C:/Users/jpg23/data/downtownrecovery/sensitivity_analysis/new_downtowns/')

nh <- st_read('commercial_not_honolulu.geojson')
h <- st_read('honolulu_commercial.geojson')

head(nh)
head(h)

# Combine data & rename cities & select relevant columns
#-------------------------------------------------------------------------------

# Check other file for correct names
hdbscan <- st_read('HDBSCAN_downtowns_with_province.geojson') %>%
  rename(city_long = city) %>%
  mutate(city = str_remove(city_long, '\\s\\w{2}$')) %>%
  st_drop_geometry()

head(hdbscan)  
unique(hdbscan$city)
n_distinct(hdbscan$city)

h1 <- h %>% mutate(city = 'Honolulu HI') %>% select(city)
head(h1)

nh1 <- nh %>%
  mutate(NAME = case_when(
    NAME == 'Dallas-Fort-Worth-Arlington' & Retail == 169 ~ 'Dallas',
    NAME == 'Dallas-Fort-Worth-Arlington' & Retail == 75 ~ 'Fort-Worth',
    NAME == 'San-Francisco-Oakland-Berkeley' & Retail == 961 ~ 'Oakland',
    NAME == 'San-Francisco-Oakland-Berkeley' & Retail == 5004 ~ 'San-Francisco',
    NAME == 'Toronto' & Retail == 725 ~ 'Mississauga',
    TRUE ~ NAME
  )) %>%
  separate(NAME, sep = "-", into = c("c1", "c2", "c3")) %>%
  mutate(city = case_when(
    c1 %in% c('Oklahoma', 'Kansas', 'New', 'San', 'Las', 'St', 'Colorado',
              'El', 'Los') ~ paste0(c1, ' ', c2),
    c1 == 'Salt' ~ 'Salt Lake City',
    TRUE ~ c1
  )) %>%
  mutate(city = case_when(
    city == 'St Louis' ~ 'St. Louis',
    city == 'Montreal' ~ 'Montréal',
    city == 'Quebec' ~ 'Québec',
    TRUE ~ city)) %>%
  add_row(city = 'Fort Worth') %>%
  select(city) %>%
  full_join(hdbscan) %>%
  select(city = city_long) %>%
  filter(city != 'Honolulu HI')

head(nh1)
unique(nh1$city)
n_distinct(nh1$city)

setdiff(unique(nh1$city), unique(hdbscan$city_long))
setdiff(unique(hdbscan$city_long), unique(nh1$city))

st_crs(h1)
st_crs(nh1)

all <- rbind(h1 %>% st_transform(st_crs(nh1)), nh1)

n_distinct(all$city)
unique(all$city)

st_write(all, 'all_commercial.geojson')
