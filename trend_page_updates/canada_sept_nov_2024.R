#===============================================================================
# Create dataset to update trends page for Canadian cities only, May - Nov 25
#===============================================================================

# Load packages
#=====================================

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'sf', 'lubridate', 'leaflet', 'plotly', 'htmlwidgets', 
       'broom', 'forecast'))

# Load downtown & MSA data
#=====================================

filepath1 <- '/Users/jpg23/data/downtownrecovery/stop_uplevelled_2023_2024/' 

# May 1 - Oct 13, 2024
dt1 <- read.csv(paste0(filepath1, 'downtown_raw_may-oct2024.csv')) %>%
  mutate(date = as.Date(date)) %>%
  filter(!str_detect(city, '\\W\\w{2}$')) # only keep Canada

range(dt1$date)
  
# May 1 - Oct 13, 2024
msa1 <- read.csv(paste0(filepath1, 'msa_raw_may-oct2024.csv')) %>%
  mutate(date = as.Date(date)) %>%
  filter(!str_detect(msa_name, '\\W\\w{2}$')) # only keep Canada

range(msa1$date)

filepath2 <- '/Users/jpg23/data/downtownrecovery/spectus_exports/trend_page_updates/canada_sept_nov_2024/'

# Downtowns: Sept 25 - Nov 21, 2024 (but start at Oct 14)
dt2 <-
  list.files(path = paste0(filepath2, 'downtown/')) %>% 
  map_df(~read_delim(
    paste0(filepath2, 'downtown/', .),
    delim = '\001',
    col_names = c('city', 'zone_date', 'provider_id', 'n_stops', 'n_distinct_devices'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-c(zone_date, provider_id)) %>%
  filter(date > as.Date('2024-10-13'))

range(dt2$date)
head(dt2)

dt <- rbind(dt1, dt2)
range(dt$date) # May 1 - Nov 21, 2024

# MSAs: Sept 25 - Nov 22, 2024 (but start at Oct 14 and end at Nov 21)
msa2 <-
  list.files(path = paste0(filepath2, 'msa/')) %>% 
  map_df(~read_delim(
    paste0(filepath2, 'msa/', .),
    delim = '\001',
    col_names = c('msa_name', 'zone_date', 'provider_id', 'n_stops_msa', 'n_distinct_devices_msa'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-c(zone_date, provider_id)) %>%
  filter((date > as.Date('2024-10-13')) & (date < as.Date('2024-11-22')))

range(msa2$date)

msa <- rbind(msa1, msa2)
range(msa$date) # May 1 - Nov 21, 2024

# Remove outliers in downtown data
#=====================================

dt_no_outliers <- dt %>%
  group_by(city) %>%
  mutate(
    n_stops_cleaned = tsclean(n_stops),
    n_distinct_devices_cleaned = tsclean(n_distinct_devices)
  )

# Plot with vs without outliers
dt_outliers_unique <- plot_ly() %>%
  add_lines(data = dt_no_outliers,
            x = ~date, y = ~n_distinct_devices,
            name = ~paste0(city, ': downtown'),
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ' downtown: ', round(n_distinct_devices, 3)),
            line = list(shape = "linear", color = '#b4e0a8')) %>%  
  add_lines(data = dt_no_outliers,
            x = ~date, y = ~n_distinct_devices_cleaned,
            name = ~paste0(city, ': downtown'),
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ' downtown: ', round(n_distinct_devices_cleaned, 3)),
            line = list(shape = "linear", color = '#445e3d'))

dt_outliers_unique

saveWidget(
  dt_outliers_unique,
  '/Users/jpg23/UDP/downtown_recovery/trend_updates/canada/sept_nov_2024/unique_devices_outliers_vs_not.html')

dt_outliers_total <- plot_ly() %>%
  add_lines(data = dt_no_outliers,
            x = ~date, y = ~n_stops,
            name = ~paste0(city, ': downtown'),
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ' downtown: ', round(n_stops, 3)),
            line = list(shape = "linear", color = '#b4e0a8')) %>%  
  add_lines(data = dt_no_outliers,
            x = ~date, y = ~n_stops_cleaned,
            name = ~paste0(city, ': downtown'),
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ' downtown: ', round(n_stops_cleaned, 3)),
            line = list(shape = "linear", color = '#445e3d'))

dt_outliers_total

saveWidget(
  dt_outliers_total,
  '/Users/jpg23/UDP/downtown_recovery/trend_updates/canada/sept_nov_2024/total_stops_outliers_vs_not.html')


# Join them
#=====================================

dt_goodnames <- dt_no_outliers %>%
  mutate(
    city = str_remove_all(city, '\\.'),
    city = str_replace_all(city, 'é', 'e'),
    city = case_when(
      city == "Ottawa - Gatineau (Ontario part / partie de l'Ontario)" ~ 'Ottawa',
      TRUE ~ city
    )
  )

unique(dt_goodnames$city)

msa_names <- read.csv('/Users/jpg23/data/downtownrecovery/sensitivity_analysis/msa_names.csv')

msa_cities <- unique(msa_names$city)
dt_cities <- unique(dt_goodnames$city)

setdiff(dt_cities, msa_cities)
setdiff(msa_cities, dt_cities)

dt_new <- dt_goodnames %>% left_join(msa_names) %>% data.frame()

head(dt_new)
head(msa)

dt_new %>% filter(is.na(msa_name)) # should be no rows

final_df <-
  dt_new %>%
  left_join(msa, by = c('msa_name', 'date')) %>%
  select(-c(n_stops, n_distinct_devices)) %>%
  mutate(normalized_distinct = n_distinct_devices_cleaned/n_distinct_devices_msa,
         normalized_stops = n_stops_cleaned/n_stops_msa)

head(final_df)

msa_distinct <- plot_ly() %>%
  add_lines(data = final_df,
            x = ~date, y = ~n_distinct_devices_msa,
            name = ~paste0(city, ': distinct devices (MSA)'),
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ': distinct devices (MSA):', round(n_distinct_devices_msa, 3)),
            line = list(shape = "linear", color = 'orange')) 

saveWidget(
  msa_distinct,
  '/Users/jpg23/UDP/downtown_recovery/trend_updates/canada/sept_nov_2024/msa_distinct.html')

final_df1 <- final_df %>%
  select(-c(n_stops_msa, n_distinct_devices_msa, n_stops_cleaned,
            n_distinct_devices_cleaned))

head(final_df1)

plot_ly() %>%
  add_lines(data = final_df1,
            x = ~date, y = ~normalized_stops,
            name = ~paste0(city, ': downtown'),
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ' downtown: ', round(normalized_stops, 3)),
            line = list(shape = "linear", color = 'blue')) 

norm_dist <- plot_ly() %>%
  add_lines(data = final_df1,
            x = ~date, y = ~normalized_distinct,
            name = ~paste0(city, ': downtown'),
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ' downtown: ', round(normalized_distinct, 3)),
            line = list(shape = "linear", color = 'red'))

saveWidget(
  norm_dist,
  '/Users/jpg23/UDP/downtown_recovery/trend_updates/canada/sept_nov_2024/normalized_distinct.html')

# Remove outliers for normalized data
#=====================================

final_df_no_outliers <- final_df %>%
  group_by(city) %>%
  mutate(
    normalized_distinct_clean = tsclean(normalized_distinct),
    normalized_stops_clean = tsclean(normalized_stops)
  ) %>%
  ungroup() %>%
  data.frame()

# Check the resulting dataframe
head(final_df_no_outliers)

norm2 <- plot_ly() %>%
  add_lines(data = final_df_no_outliers,
            x = ~date, y = ~normalized_stops,
            name = ~paste0(city, ': downtown'),
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ' downtown: ', round(normalized_stops, 3)),
            line = list(shape = "linear", color = '#b4e0a8')) %>%
  add_lines(data = final_df_no_outliers,
            x = ~date, y = ~normalized_stops_clean,
            name = ~paste0(city, ': downtown'),
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ' downtown: ', round(normalized_stops_clean, 3)),
            line = list(shape = "linear", color = '#445e3d'))

saveWidget(
  norm2,
  '/Users/jpg23/UDP/downtown_recovery/trend_updates/canada/sept_nov_2024/normalized_distinct_no_outliers.html')

# Export final data
#=====================================

write.csv(final_df_no_outliers,
          '/Users/jpg23/UDP/downtown_recovery/trend_updates/canada/sept_nov_2024/stopuplevelled_canada_sept_nov_2024_outliers_removed.csv',
          row.names = F)
