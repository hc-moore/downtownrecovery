#===============================================================================
# Compare March - June 2023 data from provider 230399 from stop_uplevelled table 
# vs stoppers_hll_by_geohash for all 60+ HDBSCAN downtowns
#===============================================================================

# Load packages
#=====================================

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'sf', 'lubridate', 'leaflet', 'plotly', 'htmlwidgets',
       'ggpubr'))

# Load downtown & MSA data
# from stop_uplevelled
#=====================================

filepath <- 'C:/Users/jpg23/data/downtownrecovery/spectus_exports/stop_uplevelled_230399_2023/'

dt <-
  list.files(path = paste0(filepath, 'downtown_march_june/')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'downtown_march_june/', .),
    delim = '\001',
    col_names = c('city', 'zone_date', 'provider_id', 'n_stops', 'n_distinct_devices'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-c(zone_date, provider_id))

msa <-
  list.files(path = paste0(filepath, 'msa_march_june/')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'msa_march_june/', .),
    delim = '\001',
    col_names = c('msa_name', 'zone_date', 'provider_id', 'n_stops', 'n_distinct_devices'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-c(zone_date, provider_id)) %>%
  rename(n_stops_msa = n_stops, n_distinct_devices_msa = n_distinct_devices)

glimpse(dt)
glimpse(msa)


# Join them
#=====================================

msa_names <- read.csv('C:/Users/jpg23/data/downtownrecovery/sensitivity_analysis/msa_names.csv')

msa_cities <- unique(msa_names$city)
dt_cities <- unique(dt$city)

setdiff(dt_cities, msa_cities)
setdiff(msa_cities, dt_cities)

dt1 <- dt %>% left_join(msa_names)

head(dt1)
head(msa)

dt1 %>% filter(is.na(msa_name)) # should be no rows

stop_uplevelled <-
  dt1 %>%
  left_join(msa, by = c('msa_name', 'date')) %>%
  # mutate(normalized_distinct = n_distinct_devices/n_distinct_devices_msa,
  #        normalized_stops = n_stops/n_stops_msa) %>%
  mutate(
    date_range_start = floor_date(
      date,
      unit = "week",
      week_start = getOption("lubridate.week.start", 1))) %>%
  group_by(date_range_start, city) %>%
  summarize(downtown_devices = sum(n_distinct_devices, na.rm = T),
            msa_count = sum(n_distinct_devices_msa, na.rm = T)) %>%
  ungroup() %>%
  data.frame() %>%
  mutate(normalized = downtown_devices/msa_count)

head(stop_uplevelled)
range(stop_uplevelled$date_range_start)


# Load stoppers_hll_by_geohash data
#=====================================

canada_cities <- c('Calgary', 'Edmonton', 'Halifax', 'London', 'Mississauga',
                   'Montreal', 'Ottawa', 'Quebec', 'Toronto', 'Vancouver',
                   'Winnipeg')

not_imputed_us <- read.csv('C:/Users/jpg23/data/downtownrecovery/sensitivity_analysis/hdbscan_dt_for_imputation.csv') %>%
  filter(!city %in% canada_cities & provider_id == '190199') %>%
  select(date_range_start, city, normalized)

imputed_canada <- read.csv('C:/Users/jpg23/data/downtownrecovery/sensitivity_analysis/imputation_Canada_msa_SAITS_hdbscan_fin.csv') %>%
  filter(city %in% canada_cities & provider_id == '190199') %>%
  select(-provider_id)

head(not_imputed_us)
head(imputed_canada)

stoppers_hll <- 
  rbind(not_imputed_us, imputed_canada) %>%
  filter(date_range_start >= as.Date('2023-02-27') & 
           date_range_start <= as.Date('2023-05-29'))

head(stoppers_hll)
range(stoppers_hll$date_range_start)


# Plot trends to compare
#=====================================

compare_plot <-
  plot_ly() %>%
  add_lines(data = stop_uplevelled,
            x = ~date_range_start, y = ~normalized,
            name = ~paste0(city, ":  stop_uplevelled"),
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ', week of ', date_range_start, ': ', round(normalized, 3)),
            line = list(shape = "linear", color = '#d6ad09')) %>%
  add_lines(data = stoppers_hll,
            x = ~date_range_start, y = ~normalized,
            name = ~paste0(city, ": stoppers_hll"),
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ', week of ', date_range_start, ': ', round(normalized, 3)),
            line = list(shape = "linear", color = '#8c0a03')) %>%
  layout(title = "Weekly unique device counts for HDBSCAN downtowns, normalized by MSA:<br>stop_uplevelled (provider 230399) vs. stoppers_hll_by_geohash",
         xaxis = list(title = "Week", zerolinecolor = "#ffff",
                      tickformat = "%b %Y"),
         yaxis = list(title = "Normalized", zerolinecolor = "#ffff",
                      ticksuffix = "  ")) # ,
         # shapes = list(list(y0 = 0, y1 = 1, yref = "paper",
         #                    x0 = as.Date('2021-05-17'), x1 = as.Date('2021-05-17'),
         #                    line = list(color = 'black', dash = 'dash'))))

compare_plot
 
saveWidget(
  compare_plot,
  'C:/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/compare_uplevelled_hll_march_june.html')


# Scatter plot & correlation coefficient
#=====================================

head(stop_uplevelled)
head(stoppers_hll)

both <- 
  stop_uplevelled %>%
  select(date_range_start, city, up_norm = normalized) %>%
  full_join(stoppers_hll %>% 
              rename(hll_norm = normalized) %>% 
              mutate(date_range_start = as.Date(date_range_start, 
                                                format = "%Y-%m-%d")),
            by = c('date_range_start', 'city'))

head(both)

ggscatter(both, x = "up_norm", y = "hll_norm",
          color = "blue", cor.coef = TRUE, 
          cor.method = "spearman",
          xlab = "stop_uplevelled", ylab = "stoppers_hll")
