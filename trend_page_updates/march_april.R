#===============================================================================
# Create dataset to update trends page for March & April 2024
#===============================================================================

# Load packages
#=====================================

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'sf', 'lubridate', 'leaflet', 'plotly', 'htmlwidgets', 
       'broom', 'forecast'))

# Load downtown & MSA data
#=====================================

filepath <- '/Users/jpg23/data/downtownrecovery/spectus_exports/trend_page_updates/march_april_2024/'

# Downtowns: March 1 - April 26
dt1 <-
  list.files(path = paste0(filepath, 'downtowns/')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'downtowns/', .),
    delim = '\001',
    col_names = c('city', 'zone_date', 'provider_id', 'n_stops', 'n_distinct_devices'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-c(zone_date, provider_id)) %>%
  filter(date <= as.Date('2024-04-26'))

# Downtowns: April 27
dt2 <-
  list.files(path = paste0(filepath, 'downtowns_apr27_may2/')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'downtowns_apr27_may2/', .),
    delim = '\001',
    col_names = c('city', 'zone_date', 'provider_id', 'n_stops', 'n_distinct_devices'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-c(zone_date, provider_id)) %>%
  filter(date == as.Date('2024-04-27'))

# Downtowns: April 28 - May 2
dt3 <-
  list.files(path = paste0(filepath, 'downtowns_apr28_may2/')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'downtowns_apr28_may2/', .),
    delim = '\001',
    col_names = c('city', 'zone_date', 'provider_id', 'n_stops', 'n_distinct_devices'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-c(zone_date, provider_id)) %>%
  filter(date < as.Date('2024-05-01'))

range(dt1$date)
range(dt2$date)
range(dt3$date)

dt <- rbind(dt1, dt2, dt3)

head(dt)

# MSAs: March 1 - May 2
msa <-
  list.files(path = paste0(filepath, 'msa/')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'msa/', .),
    delim = '\001',
    col_names = c('msa_name', 'zone_date', 'provider_id', 'n_stops_msa', 'n_distinct_devices_msa'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-c(zone_date, provider_id)) %>%
  filter(date < as.Date('2024-05-01'))

range(msa$date)

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

# Join them
#=====================================

dt_goodnames <- dt_no_outliers %>%
  mutate(
    city = str_remove_all(city, '\\.'),
    city = str_replace_all(city, 'é', 'e'),
    city = case_when(
      city == 'Nashville-Davidson metropolitan government (balance) TN' ~ 'Nashville',
      city == "Ottawa - Gatineau (Ontario part / partie de l'Ontario)" ~ 'Ottawa',
      city == "Indianapolis city (balance) IN" ~ 'Indianapolis',
      city != 'Washington DC' ~ str_remove_all(city, 'Urban\\s|\\s\\w{2}$'),
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

plot_ly() %>%
  add_lines(data = final_df1,
            x = ~date, y = ~normalized_distinct,
            name = ~paste0(city, ': downtown'),
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ' downtown: ', round(normalized_distinct, 3)),
            line = list(shape = "linear", color = 'red'))


# # Remove outliers for normalized data
# #=====================================
# 
# final_df_no_outliers <- final_df %>%
#   group_by(city) %>%
#   mutate(
#     normalized_distinct_clean = tsclean(normalized_distinct),
#     normalized_stops_clean = tsclean(normalized_stops)
#   ) %>%
#   ungroup() %>%
#   data.frame()
# 
# # Check the resulting dataframe
# head(final_df_no_outliers)
# 
# plot_ly() %>%
#   add_lines(data = final_df_no_outliers,
#             x = ~date, y = ~normalized_stops,
#             name = ~paste0(city, ': downtown'),
#             opacity = .7,
#             split = ~city,
#             text = ~paste0(city, ' downtown: ', round(normalized_stops, 3)),
#             line = list(shape = "linear", color = '#b4e0a8')) %>%  
#   add_lines(data = final_df_no_outliers,
#             x = ~date, y = ~normalized_stops_clean,
#             name = ~paste0(city, ': downtown'),
#             opacity = .7,
#             split = ~city,
#             text = ~paste0(city, ' downtown: ', round(normalized_stops_clean, 3)),
#             line = list(shape = "linear", color = '#445e3d'))

# Export final data
#=====================================

write.csv(final_df1,
          '/Users/jpg23/data/downtownrecovery/stop_uplevelled_2023_2024/stopuplevelled_march_april_2024_outliers_removed.csv',
          row.names = F)
