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

# Chinatown
#----------------------

filepath_chinatown <- 'C:/Users/jpg23/data/downtownrecovery/environics/Chinatown/'

setwd(filepath_chinatown)

file_list_chinatown <- list.files(filepath_chinatown)

file_list_chinatown %>%
  map(function(filename){ # iterate through each file name
    assign(x = file_path_sans_ext(basename(filename)), # Remove file extension and path directories
           value = read_csv(paste0(filepath_chinatown, filename)) %>%
             select(name = GeofenceName, Unique_Visitors, Daily_Visits,
                    lat = CEL_LATITUDE, long = CEL_LONGITUDE, Sunday:December) %>%
             # Add year variable, pulled from file name
             mutate(year = as.integer(paste0('20', substr(file_path_sans_ext(basename(filename)), 12, 13)))),
           envir = .GlobalEnv)
  })

e_chinatown <- 
  rbind(SpadinaChtn19, SpadinaChtn20, SpadinaChtn21, SpadinaChtn22) %>%
  data.frame()

head(e_chinatown)

# Downtown Yonge
#----------------------

filepath_yonge <- 'C:/Users/jpg23/data/downtownrecovery/environics/DowntownYonge/'

setwd(filepath_yonge)

file_list_yonge <- list.files(filepath_yonge)

file_list_yonge %>%
  map(function(filename){ # iterate through each file name
    assign(x = file_path_sans_ext(basename(filename)), # Remove file extension and path directories
           value = read_csv(paste0(filepath_yonge, filename)) %>%
             select(name = GeofenceName, Unique_Visitors, Daily_Visits,
                    lat = CEL_LATITUDE, long = CEL_LONGITUDE, Sunday:December) %>%
             # Add year variable, pulled from file name
             mutate(year = as.integer(paste0('20', substr(file_path_sans_ext(basename(filename)), 4, 5)))),
           envir = .GlobalEnv)
  })

e_yonge <- 
  rbind(DTY19, DTY20, DTY21, DTY22) %>%
  data.frame()

head(e_yonge)

# Kennedy Road
#----------------------

filepath_ken <- 'C:/Users/jpg23/data/downtownrecovery/environics/KennedyRd/'

setwd(filepath_ken)

file_list_ken <- list.files(filepath_ken)

file_list_ken %>%
  map(function(filename){ # iterate through each file name
    assign(x = file_path_sans_ext(basename(filename)), # Remove file extension and path directories
           value = read_csv(paste0(filepath_ken, filename)) %>%
             select(name = GeofenceName, Unique_Visitors, Daily_Visits,
                    lat = CEL_LATITUDE, long = CEL_LONGITUDE, Sunday:December) %>%
             # Add year variable, pulled from file name
             mutate(year = as.integer(substr(file_path_sans_ext(basename(filename)), 7, 10))),
           envir = .GlobalEnv)
  })

e_ken <- 
  rbind(KenRd_2019_2022, KenRd_2020_2022, KenRd_2021_2022, KenRd_2022_2023) %>%
  data.frame()

head(e_ken)

# Liberty Village
#----------------------

filepath_lib <- 'C:/Users/jpg23/data/downtownrecovery/environics/LibertyVillage/'

setwd(filepath_lib)

file_list_lib <- list.files(filepath_lib)

file_list_lib %>%
  map(function(filename){ # iterate through each file name
    assign(x = file_path_sans_ext(basename(filename)), # Remove file extension and path directories
           value = read_csv(paste0(filepath_lib, filename)) %>%
             select(name = GeofenceName, Unique_Visitors, Daily_Visits,
                    lat = CEL_LATITUDE, long = CEL_LONGITUDE, Sunday:December) %>%
             # Add year variable, pulled from file name
             mutate(year = as.integer(paste0('20', substr(file_path_sans_ext(basename(filename)), 8, 9)))),
           envir = .GlobalEnv)
  })

e_lib <- 
  rbind(LibVill19, LibVill20, LibVill21, LibVill22) %>%
  data.frame()

head(e_lib)

# Combine!
#----------------------

e <- rbind(e_chinatown, e_yonge, e_ken, e_lib)

View(e)
head(e)
nrow(e)
e %>% group_by(year) %>% count()
e %>% group_by(name) %>% count()

#-----------------------------------------
# Calculate unique visitors and total
# visits by place type & month
#-----------------------------------------

e1 <- e %>%
  pivot_longer(
    cols = January:December,
    names_to = 'month',
    values_to = 'monthly_count'
  ) %>%
  filter(monthly_count != 0) %>%
  # mutate(same_count = case_when(
  #   Daily_Visits == monthly_count ~ 'yes',
  #   TRUE ~ 'no'
  # )) %>%
  data.frame()

# e1 %>% group_by(same_count) %>% count()
# e1 %>% filter(Daily_Visits != monthly_count) %>% head()
# e1 %>% filter(Daily_Visits == monthly_count) %>% head()
# 
# e %>% filter(long == -75.51918)
# e1 %>% filter(long == -75.51918)
# 
# e1 %>% group_by(name) %>% count() %>% data.frame()
# 
# nrow(e1)
# nrow(e1 %>% distinct(name, month, lat, long))
# 
# e1 %>% 
#   group_by(name, month, lat, long, year) %>% 
#   count() %>% 
#   arrange(desc(n)) %>% 
#   data.frame() %>% 
#   head(5)
# 
# e1 %>% 
#   filter(name == 'Spadina_Chinatown_BIA' & lat == 43.65005 & long == -79.39150 & 
#            month == 'November' & year == 2021) %>%
#   relocate(name, Daily_Visits, monthly_count, month)
# 
# e %>% 
#   filter(name == 'Spadina_Chinatown_BIA' & lat == 43.65005 & long == -79.39150 & 
#            year == 2021 & November > 0) %>%
#   mutate(yearly_visits = select(., January:December) %>% rowSums(na.rm = TRUE),
#          same_count = ifelse(yearly_visits == Daily_Visits, 'yes', 'no')) %>%
#   select(name, Daily_Visits, yearly_visits, same_count, November, January:December)
# 
# head(e1)

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
e2 %>% group_by(name) %>% count()

#-----------------------------------------
# Compare with Spectus data
#-----------------------------------------

# Read in Chinatown data
spectus_raw <- read.csv('C:/Users/jpg23/data/downtownrecovery/compare_environics.csv')
head(spectus_raw)
glimpse(spectus_raw)
spectus_raw %>% group_by(bia) %>% count()

spectus <- 
  spectus_raw %>%
  mutate(month = month(ymd(date), label = TRUE, abbr = FALSE)) %>%
  group_by(bia, year, month) %>%
  summarize(n_devices = sum(n_devices, na.rm = T),
            userbase = sum(userbase, na.rm = T)) %>%
  mutate(normalized = n_devices/userbase) %>%
  data.frame() %>%
  mutate(month_date = as.Date(paste(as.character(year), month, '01', sep = '_'),
                              format = '%Y_%B_%d')) %>% 
  filter(month_date <= as.Date('2021-12-01') & 
           month_date >= as.Date('2019-01-01')) %>%
  mutate(bia = case_when(
           bia == 'Chinatown' ~ 'Spectus (Chinatown)',
           bia == 'Downtown Yonge' ~ 'Spectus (Downtown Yonge)',
           bia == 'Kennedy Road' ~ 'Spectus (Kennedy Road)',
           bia == 'Liberty Village' ~ 'Spectus (Liberty Village)'
         )) %>%
  group_by(bia) %>%
  mutate(scaled = scale(normalized)) %>%
  ungroup()

head(spectus)

e3 <- e2 %>%
  mutate(month_date = as.Date(paste(as.character(year), month, '01', sep = '_'),
                              format = '%Y_%B_%d'))

head(e3)

# Filter to BIAs only
e3_bia <- e3 %>% filter(str_detect(name, '_BIA')) %>% 
  mutate(name = case_when(
           name == 'Downtown_Yonge_BIA' ~ 'Environics (Downtown Yonge)',
           name == 'KennedyRoad_BIA' ~ 'Environics (Kennedy Road)',
           name == 'LibertyVillage_BIA' ~ 'Environics (Liberty Village)',
           name == 'Spadina_Chinatown_BIA' ~ 'Environics (Chinatown)'
         )) %>% 
  filter(month_date <= as.Date('2021-12-01') & 
           month_date >= as.Date('2019-01-01')) %>%
  group_by(name) %>%
  mutate(scaled = scale(unique_visitors)) %>%
  ungroup()

e3_bia %>% group_by(name) %>% count()

head(e3_bia)

range(spectus$month_date)
range(e3_bia$month_date)
range(e3_not_bia$month_date)

compare_plot <- 
  plot_ly(colors = c("#f0b002", "#ee684b", "#0bb09d", "#853973",
                     "#f0b002", "#ee684b", "#0bb09d", "#853973")) %>%
  add_lines(data = spectus, x = ~month_date, y = ~scaled, color = ~bia,
            name = ~bia,
            opacity = .9,
            line = list(shape = "linear")) %>%
  add_lines(data = e3_bia, x = ~month_date, y = ~scaled, color = ~name,
            name = ~name, 
            opacity = .25,
            line = list(shape = "linear")) %>% # dash = 'dash'
  layout(title = "Scaled activity levels of unique visitors in Toronto Business Improvement Areas",
         xaxis = list(title = "Month", zerolinecolor = "#ffff", 
                      tickformat = "%b %Y"),
         yaxis = list(title = "Scaled activity levels", zerolinecolor = "#ffff",
                      ticksuffix = "  "))

compare_plot
