#===============================================================================
# Compare Spectus data with Environics data for Business Improvement Areas.
#
# Author: Julia Greenberg
# Date created: 5/24/23
#===============================================================================

#-----------------------------------------
# Load packages
#-----------------------------------------

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'ggplot2', 'zonator', 'rlist', 'lubridate'))

#-----------------------------------------
# Load data
#-----------------------------------------

filepath <- 'C:/Users/jpg23/data/downtownrecovery/environics/'

setwd(filepath)

file_list <- list.files(filepath)

file_list %>%
  map(function(filename){ # iterate through each file name
    assign(x = file_path_sans_ext(basename(filename)), # Remove file extension and path directories
           value = read_csv(paste0(filepath, filename)) %>%
             select(name = GeofenceName, Unique_Visitors, Daily_Visits,
                    lat = CEL_LATITUDE, long = CEL_LONGITUDE, Sunday:December) %>%
             # Add year variable, pulled from file name
             mutate(year = as.integer(paste0('20', substr(file_path_sans_ext(basename(filename)), 12, 13)))),
           envir = .GlobalEnv)
  })

e <- rbind(SpadinaChtn19, SpadinaChtn20, SpadinaChtn21, SpadinaChtn22) %>%
  data.frame()

#-----------------------------------------
# Calculate unique visitors and total
# visits by place type & month
#-----------------------------------------

e %>% group_by(year) %>% count()

View(e)
head(e)

e1 <- e %>%
  pivot_longer(
    cols = January:December,
    names_to = 'month',
    values_to = 'monthly_count'
  ) %>%
  filter(monthly_count != 0) %>%
  mutate(same_count = case_when(
    Daily_Visits == monthly_count ~ 'yes',
    TRUE ~ 'no'
  )) %>%
  data.frame()

e1 %>% group_by(same_count) %>% count()
e1 %>% filter(Daily_Visits != monthly_count) %>% head()
e1 %>% filter(Daily_Visits == monthly_count) %>% head()

e %>% filter(long == -75.51918)
e1 %>% filter(long == -75.51918)

e1 %>% group_by(name) %>% count() %>% data.frame()

nrow(e1)
nrow(e1 %>% distinct(name, month, lat, long))

e1 %>% 
  group_by(name, month, lat, long, year) %>% 
  count() %>% 
  arrange(desc(n)) %>% 
  data.frame() %>% 
  head(5)

e1 %>% 
  filter(name == 'Spadina_Chinatown_BIA' & lat == 43.65005 & long == -79.39150 & 
           month == 'November' & year == 2021) %>%
  relocate(name, Daily_Visits, monthly_count, month)

e %>% 
  filter(name == 'Spadina_Chinatown_BIA' & lat == 43.65005 & long == -79.39150 & 
           year == 2021 & November > 0) %>%
  mutate(yearly_visits = select(., January:December) %>% rowSums(na.rm = TRUE),
         same_count = ifelse(yearly_visits == Daily_Visits, 'yes', 'no')) %>%
  select(name, Daily_Visits, yearly_visits, same_count, November, January:December)

head(e1)

# Sum total visits and unique visitors by month
e2 <-
  e1 %>%
  group_by(name, year, month) %>%
  summarize(
    unique_visitors = sum(Unique_Visitors, na.rm = T),
    total_visits = sum(monthly_count, na.rm = T)
  ) %>%
  data.frame()

head(e2)
e2 %>% filter(unique_visitors > total_visits) # none - good
e2 %>% group_by(year) %>% count()

#-----------------------------------------
# Compare with Spectus data
#-----------------------------------------

# Read in Chinatown data
spectus_raw <- read.csv('C:/Users/jpg23/data/downtownrecovery/chinatown.csv')
head(spectus_raw)
glimpse(spectus_raw)
spectus_raw %>% group_by(bia) %>% count()

spectus <- 
  spectus_raw %>%
  mutate(month = month(ymd(date), label = TRUE, abbr = FALSE)) %>%
  group_by(year, month) %>%
  summarize(n_devices = sum(n_devices, na.rm = T),
            userbase = sum(userbase, na.rm = T)) %>%
  mutate(normalized = n_devices/userbase) %>%
  data.frame() %>%
  mutate(month_date = as.Date(paste(as.character(year), month, '01', sep = '_'),
                              format = '%Y_%B_%d'))

head(spectus)

e3 <- e2 %>%
  mutate(month_date = as.Date(paste(as.character(year), month, '01', sep = '_'),
                              format = '%Y_%B_%d'))

head(e3)

e3_bia <- e3 %>% filter(name == 'Spadina_Chinatown_BIA') %>% 
  mutate(scaled = scale(unique_visitors))

e3_not_bia <- e3 %>% filter(name != 'SpadinaChntwn')

range(spectus$month_date)
range(e3_bia$month_date)
range(e3_not_bia$month_date)

s_for_plot <- spectus %>% 
  filter(month_date <= as.Date('2021-12-01') & 
           month_date >= as.Date('2019-01-01')) %>%
  mutate(scaled = scale(normalized))

head(s_for_plot)

compare_plot <- 
  plot_ly() %>%
  add_lines(data = s_for_plot, x = ~month_date, y = ~scaled,
            name = "Spectus",
            opacity = .9,
            line = list(shape = "linear", color = '#8c0a03')) %>%
  add_lines(data = e3_bia, x = ~month_date, y = ~scaled,
            name = "Environics", 
            opacity = .3,
            line = list(shape = "linear", color = "blue")) %>%
  layout(title = "Scaled activity levels of unique visitors in Chinatown BIA",
         xaxis = list(title = "Month", zerolinecolor = "#ffff", 
                      tickformat = "%b %Y"),
         yaxis = list(title = "Scaled activity levels", zerolinecolor = "#ffff",
                      ticksuffix = "  "))

compare_plot
