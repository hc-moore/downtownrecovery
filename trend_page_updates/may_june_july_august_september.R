#===============================================================================
# Explore trend data May - mid-Oct 2024
#===============================================================================

# Load packages
#=====================================

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'sf', 'lubridate', 'leaflet', 'plotly', 'htmlwidgets', 
       'broom', 'forecast'))

# Load downtown & MSA data
#=====================================

filepath <- '/Users/jpg23/data/downtownrecovery/spectus_exports/trend_page_updates/'

may_fp <- paste0(filepath, 'may_2024/')
june_fp <- paste0(filepath, 'june_2024/')
july_fp <- paste0(filepath, 'july_2024/')
august_fp <- paste0(filepath, 'august_2024/')
sept_fp <- paste0(filepath, 'september_2024/')

# Downtowns: filter to all of May
dt1 <-
  list.files(path = paste0(may_fp, 'downtowns/')) %>% 
  map_df(~read_delim(
    paste0(may_fp, 'downtowns/', .),
    delim = '\001',
    col_names = c('city', 'zone_date', 'provider_id', 'n_stops', 'n_distinct_devices'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-c(zone_date, provider_id)) %>%
  filter(date < as.Date('2024-06-01'))

# Downtowns: 5/26/24 - 7/7/24 (but filter to 6/1/24 - 7/6/24)
dt2 <-
  list.files(path = paste0(june_fp, 'downtown/')) %>% 
  map_df(~read_delim(
    paste0(june_fp, 'downtown/', .),
    delim = '\001',
    col_names = c('city', 'zone_date', 'provider_id', 'n_stops', 'n_distinct_devices'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-c(zone_date, provider_id)) %>%
  filter(date >= as.Date('2024-06-01') & date <= as.Date('2024-07-06'))

# Downtowns: 7/7/24 - 8/7/24 (but filter to end at Aug 3)
dt3 <-
  list.files(path = paste0(july_fp, 'downtown/')) %>% 
  map_df(~read_delim(
    paste0(july_fp, 'downtown/', .),
    delim = '\001',
    col_names = c('city', 'zone_date', 'provider_id', 'n_stops', 'n_distinct_devices'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-c(zone_date, provider_id)) %>%
  filter(date <= as.Date('2024-08-03'))

# Downtowns: 8/4/24 - 8/31/24
dt4 <-
  list.files(path = paste0(august_fp, 'downtown/')) %>% 
  map_df(~read_delim(
    paste0(august_fp, 'downtown/', .),
    delim = '\001',
    col_names = c('city', 'zone_date', 'provider_id', 'n_stops', 'n_distinct_devices'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-c(zone_date, provider_id))

# Downtowns: 9/3/24 - 10/13/24
dt5 <-
  list.files(path = paste0(sept_fp, 'downtown/')) %>% 
  map_df(~read_delim(
    paste0(sept_fp, 'downtown/', .),
    delim = '\001',
    col_names = c('city', 'zone_date', 'provider_id', 'n_stops', 'n_distinct_devices'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-c(zone_date, provider_id))

range(dt1$date)
range(dt2$date)
range(dt3$date)
range(dt4$date)
range(dt5$date)

dt <- rbind(dt1, dt2, dt3, dt4, dt5)
range(dt$date)

head(dt)

write.csv(dt, 
          '/Users/jpg23/data/downtownrecovery/stop_uplevelled_2023_2024/downtown_raw_may-oct2024.csv',
          row.names = FALSE)

# MSAs: filter to all of May
msa1 <-
  list.files(path = paste0(may_fp, 'msa/')) %>% 
  map_df(~read_delim(
    paste0(may_fp, 'msa/', .),
    delim = '\001',
    col_names = c('msa_name', 'zone_date', 'provider_id', 'n_stops_msa', 'n_distinct_devices_msa'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-c(zone_date, provider_id)) %>%
  filter(date < as.Date('2024-06-01'))

# MSAs: 5/26/24 - 7/7/21 (but filter to end at 7/6/24)
msa2 <-
  list.files(path = paste0(june_fp, 'msa/')) %>% 
  map_df(~read_delim(
    paste0(june_fp, 'msa/', .),
    delim = '\001',
    col_names = c('msa_name', 'zone_date', 'provider_id', 'n_stops_msa', 'n_distinct_devices_msa'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-c(zone_date, provider_id)) %>%
  filter(date >= as.Date('2024-06-01') & date <= as.Date('2024-07-06'))

# MSAs: 7/7/24 - 8/7/24 (but filter to end at Aug 3)
msa3 <-
  list.files(path = paste0(july_fp, 'msa/')) %>% 
  map_df(~read_delim(
    paste0(july_fp, 'msa/', .),
    delim = '\001',
    col_names = c('msa_name', 'zone_date', 'provider_id', 'n_stops_msa', 'n_distinct_devices_msa'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-c(zone_date, provider_id)) %>%
  filter(date <= as.Date('2024-08-03'))

# MSAs: 8/4/24 - 8/31/24
msa4 <-
  list.files(path = paste0(august_fp, 'msa/')) %>% 
  map_df(~read_delim(
    paste0(august_fp, 'msa/', .),
    delim = '\001',
    col_names = c('msa_name', 'zone_date', 'provider_id', 'n_stops_msa', 'n_distinct_devices_msa'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-c(zone_date, provider_id))

# MSAs: 9/3/24 - 10/13/24
msa5 <-
  list.files(path = paste0(sept_fp, 'msa/')) %>% 
  map_df(~read_delim(
    paste0(sept_fp, 'msa/', .),
    delim = '\001',
    col_names = c('msa_name', 'zone_date', 'provider_id', 'n_stops_msa', 'n_distinct_devices_msa'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-c(zone_date, provider_id))

range(msa1$date)
range(msa2$date)
range(msa3$date)
range(msa4$date)
range(msa5$date)

msa <- rbind(msa1, msa2, msa3, msa4, msa5)
range(msa$date)

head(msa)

write.csv(msa, 
          '/Users/jpg23/data/downtownrecovery/stop_uplevelled_2023_2024/msa_raw_may-oct2024.csv',
          row.names = FALSE)

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
  paste0('/Users/jpg23/UDP/downtown_recovery/trend_updates/may_sept_2024/unique_devices_outliers_vs_not.html'))

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
  paste0('/Users/jpg23/UDP/downtown_recovery/trend_updates/may_sept_2024/total_stops_outliers_vs_not.html'))


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
  paste0('/Users/jpg23/UDP/downtown_recovery/trend_updates/may_sept_2024/msa_distinct.html'))

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
  paste0('/Users/jpg23/UDP/downtown_recovery/trend_updates/may_sept_2024/normalized_distinct.html'))

write.csv(final_df1,
          paste0('/Users/jpg23/data/downtownrecovery/stop_uplevelled_2023_2024/stopuplevelled_may_sept_2024.csv'),
          row.names = F)
