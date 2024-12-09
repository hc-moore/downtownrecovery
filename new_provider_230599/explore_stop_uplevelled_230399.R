#===============================================================================
# Explore data from provider 230399 from stop_uplevelled table 
# for all 60+ HDBSCAN downtowns -- USE THIS TO UPDATE TRENDS PAGE ON
# DOWNTOWN RECOVERY WEBSITE: https://downtownrecovery.com/charts/trends
#===============================================================================

# Load packages
#=====================================

# install.packages("forecast", repos="http://cran.us.r-project.org")

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'sf', 'lubridate', 'leaflet', 'plotly', 'htmlwidgets', 
       'broom', 'forecast', 'tidycensus', 'cancensus'))

set_cancensus_api_key('CensusMapper_bcd591107b93e609a0bb5415f58cb31b')

# Load downtown & MSA data
#=====================================

filepath <- '/Users/jpg23/data/downtownrecovery/spectus_exports/'

# Downtown 3/1/23 - 3/7/24
dt <-
  list.files(path = paste0(filepath, 'stop_uplevelled_230399_dec_feb/downtowns/')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'stop_uplevelled_230399_dec_feb/downtowns/', .),
    delim = '\001',
    col_names = c('city', 'zone_date', 'provider_id', 'n_stops', 'n_distinct_devices'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-c(zone_date, provider_id)) %>%
  filter(date <= as.Date('2024-03-01'))

# MSA 3/1/23 - 5/31/23
msa1 <-
  list.files(path = paste0(filepath, 'stop_uplevelled_230399_2023/msa_march_june/')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'stop_uplevelled_230399_2023/msa_march_june/', .),
    delim = '\001',
    col_names = c('msa_name', 'zone_date', 'provider_id', 'n_stops', 'n_distinct_devices'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-c(zone_date, provider_id)) %>%
  rename(n_stops_msa = n_stops, n_distinct_devices_msa = n_distinct_devices)

# MSA 6/1/23 - 12/16/23
msa2 <-
  list.files(path = paste0(filepath, 'stop_uplevelled_230399_2023/msa/')) %>%
  map_df(~read_delim(
    paste0(filepath, 'stop_uplevelled_230399_2023/msa/', .),
    delim = '\001',
    col_names = c('msa_name', 'zone_date', 'provider_id', 'n_stops', 'n_distinct_devices'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-c(zone_date, provider_id)) %>%
  rename(n_stops_msa = n_stops, n_distinct_devices_msa = n_distinct_devices) %>%
  filter(date < as.Date('2023-12-17'))

# MSA 12/17/23 - 2/25/24
msa3 <-
  list.files(path = paste0(filepath, 'stop_uplevelled_230399_dec_feb/msa/')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'stop_uplevelled_230399_dec_feb/msa/', .),
    delim = '\001',
    col_names = c('msa_name', 'zone_date', 'provider_id', 'n_stops', 'n_distinct_devices'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-c(zone_date, provider_id)) %>%
  rename(n_stops_msa = n_stops, n_distinct_devices_msa = n_distinct_devices)

# MSA 2/27/24 - 3/1/24
msa4 <-
  list.files(path = paste0(filepath, 'stop_uplevelled_230399_feb_march_2024/msa/')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'stop_uplevelled_230399_feb_march_2024/msa/', .),
    delim = '\001',
    col_names = c('msa_name', 'zone_date', 'provider_id', 'n_stops', 'n_distinct_devices'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-c(zone_date, provider_id)) %>%
  rename(n_stops_msa = n_stops, n_distinct_devices_msa = n_distinct_devices)

## NOTE: 2/26/24 is missing for MSA data

# Downtown
plot_ly() %>%
  add_lines(data = dt,
            x = ~date, y = ~n_distinct_devices,
            name = ~paste0(city, ': downtown'),
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ' downtown: ', round(n_distinct_devices, 3)),
            line = list(shape = "linear", color = '#445e3d'))

# MSA
plot_ly() %>%
  add_lines(data = msa1,
            x = ~date, y = ~n_distinct_devices_msa,
            name = ~paste0(msa_name, ': MSA'),
            opacity = .7,
            split = ~msa_name,
            text = ~paste0(msa_name, ' MSA: ', round(n_distinct_devices_msa, 3)),
            line = list(shape = "linear", color = 'maroon')) %>%
  add_lines(data = msa2,
            x = ~date, y = ~n_distinct_devices_msa,
            name = ~paste0(msa_name, ': MSA'),
            opacity = .7,
            split = ~msa_name,
            text = ~paste0(msa_name, ' MSA: ', round(n_distinct_devices_msa, 3)),
            line = list(shape = "linear", color = 'lightblue')) %>%
  add_lines(data = msa3,
            x = ~date, y = ~n_distinct_devices_msa,
            name = ~paste0(msa_name, ': MSA'),
            opacity = .7,
            split = ~msa_name,
            text = ~paste0(msa_name, ' MSA: ', round(n_distinct_devices_msa, 3)),
            line = list(shape = "linear", color = 'orange')) %>%
  add_lines(data = msa3,
            x = ~date, y = ~n_distinct_devices_msa,
            name = ~paste0(msa_name, ': MSA'),
            opacity = .7,
            split = ~msa_name,
            text = ~paste0(msa_name, ' MSA: ', round(n_distinct_devices_msa, 3)),
            line = list(shape = "linear", color = 'lightpink'))  

msa <- rbind(msa1, msa2, msa3, msa4)

plot_ly() %>%
  add_lines(data = msa,
            x = ~date, y = ~n_distinct_devices_msa,
            name = ~paste0(msa_name, ': MSA'),
            opacity = .7,
            split = ~msa_name,
            text = ~paste0(msa_name, ' MSA: ', round(n_distinct_devices_msa, 3)),
            line = list(shape = "linear", color = 'maroon'))

range(dt$date)
range(msa$date)

# Compare to MSA populations
#=====================================

msa_us <- get_acs(geography = "cbsa", 
              variables = "B01003_001", 
              year = 2022,
              survey = 'acs5') %>%
  select(msa_name = NAME, pop = estimate) %>%
  mutate(msa_name = str_remove(msa_name, ' Metro Area| Micro Area')) %>%
  data.frame()

# GET CANADIAN MSA POPULATION FOR 2021 (2022 NOT AVAILABLE)
# https://cran.r-project.org/web/packages/cancensus/vignettes/cancensus.html
msa_canada <- get_census(
  dataset = 'CA21', 
  # Calgary, Toronto, Ottawa, Edmonton, Montreal, Vancouver, London, Quebec,
  # Winnipeg, Halifax
  regions = list(CSD = c('48825', '35535', '505', '48835', '24462', '59933',
                         '35555', '24421', '46602', '12205')), 
  vectors = 'v_CA21_1', 
  level = 'CMA') %>%
  data.frame() %>%
  select(msa_name = Region.Name, pop = Population) %>%
  mutate(msa_name = str_remove(msa_name, '( - Gatineau)? \\(B\\)'),
         msa_name = str_replace_all(msa_name, 'é', 'e'))

msa_canada

msa_both <- rbind(msa_us, msa_canada)

head(msa_both)
  
msa_pop <- msa_both %>%  
  inner_join(msa) %>%
  mutate(samp_diff = pop - n_distinct_devices_msa)

head(msa_pop)
head(msa_pop %>% filter(samp_diff > 0))

spectus_pop <- plot_ly() %>%
  add_lines(data = msa_pop,
            x = ~date, y = ~samp_diff,
            name = ~paste0(msa_name, ': MSA'),
            opacity = .7,
            split = ~msa_name,
            text = ~paste0(msa_name, ' MSA: ', round(samp_diff, 3)),
            line = list(shape = "linear", color = '#ed5045')) %>%
  layout(title = "Census population minus unique devices from Spectus",
         xaxis = list(title = "Day", zerolinecolor = "#ffff",
                      tickformat = "%Y-%m-%d"),
         yaxis = list(title = "Difference", zerolinecolor = "#ffff",
                      ticksuffix = "  "))  

spectus_pop

saveWidget(
  spectus_pop,
  '/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/spectus_vs_MSA_pop.html')


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

saveWidget(
  dt_outliers_unique,
  '/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/dt_unique_devices_with_without_outliers.html')

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

saveWidget(
  dt_outliers_total,
  '/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/dt_total_devices_with_without_outliers.html')


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

dt1 <- dt_goodnames %>% left_join(msa_names) %>% data.frame()

head(dt1)
head(msa)

dt1 %>% filter(is.na(msa_name)) # should be no rows

final_df <-
  dt1 %>%
  left_join(msa, by = c('msa_name', 'date')) %>%
  select(-c(n_stops, n_distinct_devices)) %>%
  mutate(normalized_distinct = n_distinct_devices_cleaned/n_distinct_devices_msa,
         normalized_stops = n_stops_cleaned/n_stops_msa)

head(final_df)


# Remove outliers for normalized data
#=====================================

final_df_no_outliers <- final_df %>%
  group_by(city) %>%
  mutate(
    normalized_distinct_clean = tsclean(normalized_distinct),
    normalized_stops_clean = tsclean(normalized_stops)
  ) %>%
  ungroup() %>%
  data.frame() %>%
  select(city, date, normalized_distinct_clean, normalized_stops_clean)

# Check the resulting dataframe
head(final_df_no_outliers)


# Plot normalized unique devices
#=====================================

unique_norm_plot <-
  plot_ly() %>%
  add_lines(data = final_df,
            x = ~date, y = ~normalized_distinct,
            name = ~city,
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ': ', round(normalized_distinct, 3)),
            line = list(shape = "linear", color = '#a7a6de')) %>%
  add_lines(data = final_df_no_outliers,
            x = ~date, y = ~normalized_distinct_clean,
            name = ~city,
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ': ', round(normalized_distinct_clean, 3)),
            line = list(shape = "linear", color = '#2b2973')) %>%  
  layout(title = "Daily unique devices from provider 230399 (stop_uplevelled table), normalized by MSA",
         xaxis = list(title = "Day", zerolinecolor = "#ffff",
                      tickformat = "%Y-%m-%d"),
         yaxis = list(title = "Normalized unique devices", zerolinecolor = "#ffff",
                      ticksuffix = "  "))

unique_norm_plot

saveWidget(
  unique_norm_plot,
  '/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/normalized_unique_devices_with_without_outliers.html')


# Plot normalized total stops
#=====================================

stops_norm_plot <-
  plot_ly() %>%
  add_lines(data = final_df,
            x = ~date, y = ~normalized_stops,
            name = ~city,
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ': ', round(normalized_stops, 3)),
            line = list(shape = "linear", color = '#e8a2c0')) %>%
  add_lines(data = final_df_no_outliers,
            x = ~date, y = ~normalized_stops_clean,
            name = ~city,
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ': ', round(normalized_stops_clean, 3)),
            line = list(shape = "linear", color = '#732949')) %>%  
  layout(title = "Daily stops from provider 230399 (stop_uplevelled table), normalized by MSA",
         xaxis = list(title = "Day", zerolinecolor = "#ffff",
                      tickformat = "%Y-%m-%d"),
         yaxis = list(title = "Normalized stops", zerolinecolor = "#ffff",
                      ticksuffix = "  "))

stops_norm_plot

saveWidget(
  stops_norm_plot,
  '/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/normalized_stops_devices_with_without_outliers.html')

# Export final data (Canada)
#=====================================

canada <- final_df_no_outliers %>%
  filter(city %in% c('Edmonton', 'Winnipeg', 'Vancouver', 'London', 'Ottawa',
                      'Halifax', 'Calgary', 'Toronto', 'Quebec', 'Montreal'))

table(canada$city)

write.csv(canada,
          '/Users/jpg23/data/downtownrecovery/stop_uplevelled_2023_2024/stopuplevelled_march2023_march2024_outliers_removed_Canada_only.csv',
          row.names = F)

# Remove Canadian cities
#=====================================

no_canada <- final_df_no_outliers %>%
  filter(!city %in% c('Edmonton', 'Winnipeg', 'Vancouver', 'London', 'Ottawa',
                      'Halifax', 'Calgary', 'Toronto', 'Quebec', 'Montreal'))

unique(no_canada$city)

# Export final data (US only)
#=====================================

write.csv(no_canada,
          '/Users/jpg23/data/downtownrecovery/stop_uplevelled_2023_2024/stopuplevelled_march2023_march2024_outliers_removed_US_only.csv',
          row.names = F)








# # Plot daily unique devices
# #=====================================
# 
# # Raw numbers
# unique_raw_plot <-
#   plot_ly() %>%
#   add_lines(data = final_df,
#             x = ~date, y = ~n_distinct_devices,
#             name = ~paste0(city, ': downtown'),
#             opacity = .7,
#             split = ~city,
#             text = ~paste0(city, ' downtown: ', round(n_distinct_devices, 3)),
#             line = list(shape = "linear", color = '#445e3d')) %>%
#   add_lines(data = final_df,
#             x = ~date, y = ~n_distinct_devices_msa,
#             name = ~paste0(city, ': MSA'),
#             opacity = .7,
#             split = ~city,
#             text = ~paste0(city, ' MSA: ', round(n_distinct_devices_msa, 3)),
#             line = list(shape = "linear", color = '#b0c4ab')) %>%  
#   layout(title = "Daily unique devices from provider 230399 (stop_uplevelled table), HDBSCAN downtown and MSA",
#          xaxis = list(title = "Day", zerolinecolor = "#ffff",
#                       tickformat = "%Y-%m-%d"),
#          yaxis = list(title = "Unique devices", zerolinecolor = "#ffff",
#                       ticksuffix = "  "))
# 
# unique_raw_plot
# 
# saveWidget(
#   unique_raw_plot,
#   'C:/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/unique_plot.html')
# 
# # Normalized
# unique_norm_plot <-
#   plot_ly() %>%
#   add_lines(data = final_df,
#             x = ~date, y = ~normalized_distinct,
#             name = ~city,
#             opacity = .7,
#             split = ~city,
#             text = ~paste0(city, ': ', round(normalized_distinct, 3)),
#             line = list(shape = "linear", color = '#6665a8')) %>%
#   layout(title = "Daily unique devices from provider 230399 (stop_uplevelled table), normalized by MSA",
#          xaxis = list(title = "Day", zerolinecolor = "#ffff",
#                       tickformat = "%Y-%m-%d"),
#          yaxis = list(title = "Normalized unique devices", zerolinecolor = "#ffff",
#                       ticksuffix = "  "))
# 
# unique_norm_plot
# 
# saveWidget(
#   unique_norm_plot,
#   'C:/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/unique_plot_norm.html')
# 
# 
# # Plot daily normalized stops
# #=====================================
# 
# # Raw numbers
# stops_raw_plot <-
#   plot_ly() %>%
#   add_lines(data = final_df,
#             x = ~date, y = ~n_stops,
#             name = ~paste0(city, ': downtown'),
#             opacity = .7,
#             split = ~city,
#             text = ~paste0(city, ' downtown: ', round(n_stops, 3)),
#             line = list(shape = "linear", color = '#c2580e')) %>%
#   add_lines(data = final_df,
#             x = ~date, y = ~n_stops_msa,
#             name = ~paste0(city, ': MSA'),
#             opacity = .7,
#             split = ~city,
#             text = ~paste0(city, ' MSA: ', round(n_stops_msa, 3)),
#             line = list(shape = "linear", color = '#f09a5d')) %>%  
#   layout(title = "Daily stops from provider 230399 (stop_uplevelled table), HDBSCAN downtown and MSA",
#          xaxis = list(title = "Day", zerolinecolor = "#ffff",
#                       tickformat = "%Y-%m-%d"),
#          yaxis = list(title = "Stops", zerolinecolor = "#ffff",
#                       ticksuffix = "  "))
# 
# stops_raw_plot
# 
# saveWidget(
#   stops_raw_plot,
#   'C:/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/stops_plot.html')
# 
# # Normalized
# stops_norm_plot <-
#   plot_ly() %>%
#   add_lines(data = final_df,
#             x = ~date, y = ~normalized_stops,
#             name = ~city,
#             opacity = .7,
#             split = ~city,
#             text = ~paste0(city, ': ', round(normalized_stops, 3)),
#             line = list(shape = "linear", color = '#6e3849')) %>%
#   layout(title = "Daily stops from provider 230399 (stop_uplevelled table), normalized by MSA",
#          xaxis = list(title = "Day", zerolinecolor = "#ffff",
#                       tickformat = "%Y-%m-%d"),
#          yaxis = list(title = "Normalized stops", zerolinecolor = "#ffff",
#                       ticksuffix = "  "))
# 
# stops_norm_plot
# 
# saveWidget(
#   stops_norm_plot,
#   'C:/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/stops_plot_norm.html')
# 
# 
# # Plot weekly % change in normalized
# # stops
# #=====================================
# 
# weekly_change <-
#   final_df %>%
#   mutate(
#     date_range_start = floor_date(
#       date,
#       unit = "week",
#       week_start = getOption("lubridate.week.start", 1))) %>%
#   group_by(city, date_range_start) %>%
#   summarize(n_stops = sum(n_stops, na.rm = T),
#             n_stops_msa = sum(n_stops_msa, na.rm = T)) %>%
#   ungroup() %>%
#   mutate(normalized_stops = n_stops/n_stops_msa) %>%
#   data.frame() %>%
#   # Add lag variable (previous week's normalized_stops)
#   arrange(city, date_range_start) %>%
#   # Filter out weeks that contain 8/23 & 12/15
#   filter(date_range_start != as.Date('2023-08-21') &
#            date_range_start != as.Date('2023-08-14') &
#            date_range_start != as.Date('2023-12-11')) %>%
#   group_by(city) %>%
#   mutate(prev_norm_stops = lag(normalized_stops, n = 1),
#          perc_change = (normalized_stops - prev_norm_stops)/prev_norm_stops) %>%
#   ungroup() %>%
#   data.frame() %>%
#   select(city, date_range_start, perc_change)
# 
# head(weekly_change)
# 
# weekly_change_plot <-
#   plot_ly() %>%
#   add_lines(data = weekly_change,
#             x = ~date_range_start, y = ~perc_change,
#             name = ~city,
#             opacity = .7,
#             split = ~city,
#             text = ~paste0(city, ': ', round(perc_change, 3)),
#             line = list(shape = "linear", color = '#e89b1e')) %>%
#   layout(title = "Weekly percent change in total stops from provider 230399 (stop_uplevelled table), normalized by MSA",
#          xaxis = list(title = "Week", zerolinecolor = "#ffff",
#                       tickformat = "%Y-%m-%d"),
#          yaxis = list(title = "Percent change from previous week", zerolinecolor = "#ffff",
#                       ticksuffix = "  "))
# 
# weekly_change_plot
# 
# saveWidget(
#   weekly_change_plot,
#   'C:/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/weekly_change_plot.html')
# 
# # Plot monthly % change in normalized
# # stops
# #=====================================
# 
# monthly_change <-
#   final_df %>%
#   # Filter out problematic dates
#   filter(date != as.Date('2023-08-22') &
#            date != as.Date('2023-08-23') &
#            date != as.Date('2023-12-15')) %>%
#   mutate(
#     date_range_start = floor_date(
#       date,
#       unit = "month")) %>%
#   group_by(city, date_range_start) %>%
#   summarize(n_stops = sum(n_stops, na.rm = T),
#             n_stops_msa = sum(n_stops_msa, na.rm = T)) %>%
#   ungroup() %>%
#   mutate(normalized_stops = n_stops/n_stops_msa) %>%
#   data.frame() %>%
#   # Add lag variable (previous month's normalized_stops)
#   arrange(city, date_range_start) %>%
#   group_by(city) %>%
#   mutate(prev_norm_stops = lag(normalized_stops, n = 1),
#          perc_change = (normalized_stops - prev_norm_stops)/prev_norm_stops) %>%
#   ungroup() %>%
#   data.frame() %>%
#   select(city, date_range_start, perc_change)
# 
# head(monthly_change)
# 
# monthly_change_plot <-
#   plot_ly() %>%
#   add_lines(data = monthly_change,
#             x = ~date_range_start, y = ~perc_change,
#             name = ~city,
#             opacity = .7,
#             split = ~city,
#             text = ~paste0(city, ': ', round(perc_change, 3)),
#             line = list(shape = "linear", color = '#e89b1e')) %>%
#   layout(title = "Monthly percent change in total stops from provider 230399 (stop_uplevelled table), normalized by MSA",
#          xaxis = list(title = "Month", zerolinecolor = "#ffff",
#                       tickformat = "%b %Y"),
#          yaxis = list(title = "Percent change from previous month", zerolinecolor = "#ffff",
#                       ticksuffix = "  "))
# 
# monthly_change_plot
# 
# saveWidget(
#   monthly_change_plot,
#   'C:/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/monthly_change_plot.html')
# 
# 
# # Area plot of monthly % change
# #=====================================
# 
# monthly_area_plot <- plot_ly() %>%
#   add_trace(
#     type = 'scatter',
#     mode = 'lines',
#     fill = 'tonexty',  # Set to 'tonexty' for stacked area chart
#     data = monthly_change,
#     sort = FALSE,
#     x = ~date_range_start,
#     y = ~perc_change,
#     name = ~city,
#     stackgroup = 'one' #,
#     # text = ~paste0(city, ': ', round(n_stops_norm, 3))
#   ) %>%
#   layout(
#     title = "Monthly percent change (from previous month) in total stops,<br>normalized by MSA, from provider 230399 (stop_uplevelled table), June - November 2023",
#     xaxis = list(
#       title = "",
#       zerolinecolor = "#ffff",
#       ticktext = list("April", "May", "June", "July", "August", "September", "October", "November"),
#       tickvals = list(4, 5, 6, 7, 8, 9, 10, 11),
#       tickmode = "array"
#     ),
#     yaxis = list(
#       title = "Percent change from previous month",
#       zerolinecolor = "#ffff",
#       ticksuffix = "  "
#     )
#   )
# 
# monthly_area_plot
# 
# saveWidget(
#   monthly_area_plot,
#   'C:/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/monthly_area_plot.html')
# 
# 
# # Plot avg. weekly % change for
# # June-July-August and Sept-Oct-Nov
# #=====================================
# 
# head(weekly_change)
# 
# seasons <-
#   weekly_change %>%
#   mutate(
#     period = case_when(
#       month(date_range_start) %in% c(6, 7, 8) ~ 'summer',
#       month(date_range_start) %in% c(9, 10, 11) ~ 'fall',
#       TRUE ~ NA_character_
#     )
#   ) %>%
#   filter(!is.na(period)) %>%
#   group_by(period, city) %>%
#   summarize(avg_weekly_perc_change = mean(perc_change, na.rm = T)) %>%
#   data.frame()
# 
# head(seasons)
# 
# summer_ranking <-
#   seasons %>%
#   filter(period == 'summer') %>%
#   arrange(desc(avg_weekly_perc_change)) %>%
#   select(-period)
# 
# summer_ranking
# 
# write.csv(summer_ranking, 
#           'C:/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/summer_ranking.csv',
#           row.names = F)
# 
# fall_ranking <-
#   seasons %>%
#   filter(period == 'fall') %>%
#   arrange(desc(avg_weekly_perc_change)) %>%
#   select(-period)
# 
# fall_ranking
# 
# write.csv(fall_ranking, 
#           'C:/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/fall_ranking.csv',
#           row.names = F)
# 
# 
# # Plot monthly chunks - moving average
# #=====================================
# 
# head(final_df)
# 
# chunks <-
#   final_df %>%
#   # Filter out problematic dates
#   filter(date != as.Date('2023-08-22') &
#            date != as.Date('2023-08-23') &
#            date != as.Date('2023-12-15')) %>%
#   mutate(month = month(date)) %>%
#   select(city, month, n_stops, n_stops_msa) %>%
#   group_by(city, month) %>%
#   summarize(n_stops = sum(n_stops, na.rm = T),
#             n_stops_msa = sum(n_stops_msa, na.rm = T)) %>%
#   ungroup() %>%
#   mutate(n_stops_norm = n_stops/n_stops_msa)
# 
# chunks1 <-
#   chunks %>%
#   left_join(
#     chunks %>% filter(month == 3) %>% select(city, march_stops_norm = n_stops_norm),
#     by = c('city')
#   ) %>%
#   mutate(change_from_march = (n_stops_norm - march_stops_norm)/march_stops_norm) %>%
#   filter(!month %in% c(3, 12)) %>%
#   select(city, month, change_from_march) %>%
#   data.frame()
# 
# head(chunks1)  
# 
# chunk_plot <-
#   plot_ly() %>%
#   add_lines(data = chunks1,
#             x = ~month, y = ~change_from_march,
#             name = ~city,
#             opacity = .7,
#             split = ~city,
#             text = ~paste0(city, ': ', round(change_from_march, 3)),
#             line = list(shape = "linear", color = '#e89b1e')) %>%
#   layout(title = "Percent change in total stops compared to March 2023, from provider 230399 (stop_uplevelled table), normalized by MSA",
#          xaxis = list(title = "", zerolinecolor = "#ffff",
#                       ticktext = list("April", "May", "June", "July", "August", "September", "October", "November"),
#                       tickvals = list(4, 5, 6, 7, 8, 9, 10, 11),
#                       tickmode = "array"),
#          yaxis = list(title = "Percent change from March 2023", zerolinecolor = "#ffff",
#                       ticksuffix = "  "))
# 
# chunk_plot
# 
# saveWidget(
#   chunk_plot,
#   'C:/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/chunk_plot.html')
# 
# 
# # Monthly chunks - area plot
# #=====================================
# 
# chunk_area_plot <- plot_ly() %>%
#   add_trace(
#     type = 'scatter',
#     mode = 'lines',
#     fill = 'tonexty',  # Set to 'tonexty' for stacked area chart
#     data = chunks %>% filter(month != 12),
#     sort = FALSE,
#     x = ~month,
#     y = ~n_stops_norm,
#     name = ~city,
#     stackgroup = 'one' #,
#     # text = ~paste0(city, ': ', round(n_stops_norm, 3))
#   ) %>%
#   layout(
#     title = "Monthly stops by city, normalized by MSA, from provider 230399 (stop_uplevelled table), March - November 2023",
#     xaxis = list(
#       title = "",
#       zerolinecolor = "#ffff",
#       ticktext = list("March", "April", "May", "June", "July", "August", "September", "October", "November"),
#       tickvals = list(3, 4, 5, 6, 7, 8, 9, 10, 11),
#       tickmode = "array"
#     ),
#     yaxis = list(
#       title = "", # Monthly normalized stops
#       zerolinecolor = "#ffff",
#       ticksuffix = "  "
#     )
#   )
# 
# chunk_area_plot
# 
# saveWidget(
#   chunk_area_plot,
#   'C:/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/chunk_area_plot.html')
# 
# 
# # Plot rankings based on Nov. chunks
# # (compare to March)
# #=====================================
# 
# head(chunks1)
# 
# chunk_rank <-
#   chunks1 %>%
#   filter(month == 11) %>%
#   arrange(change_from_march)
# 
# chunk_rank_plot <- plot_ly(
#   chunk_rank, 
#   y = ~reorder(city, -change_from_march), 
#   x = ~change_from_march, 
#   type = 'bar' 
#   #text = ~city
#   ) %>%
#   layout(title = "% change from March to Nov 2023",
#          xaxis = list(title = ""),
#          yaxis = list(title = "", dtick = 1)) # , showticklabels = FALSE))
# 
# chunk_rank_plot
# 
# saveWidget(
#   chunk_rank_plot,
#   'C:/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/chunk_rank_plot.html')
# 
# 
# # # Plot rankings based on Nov. chunks
# # # (compare to July)
# # #=====================================
# # 
# # chunks_july <-
# #   chunks %>%
# #   left_join(
# #     chunks %>% filter(month == 7) %>% select(city, july_stops_norm = n_stops_norm),
# #     by = c('city')
# #   ) %>%
# #   mutate(change_from_july = (n_stops_norm - july_stops_norm)/july_stops_norm) %>%
# #   filter(!month %in% c(6, 7, 12)) %>%
# #   select(city, month, change_from_july) %>%
# #   data.frame()
# # 
# # head(chunks_july)
# # 
# # chunk_rank_july <-
# #   chunks_july %>%
# #   filter(month == 11) %>%
# #   arrange(change_from_july)
# # 
# # chunk_rank_plot_july <- plot_ly(
# #   chunk_rank_july, 
# #   y = ~reorder(city, -change_from_july), 
# #   x = ~change_from_july, 
# #   type = 'bar' 
# #   #text = ~city
# # ) %>%
# #   layout(title = "% change from July to Nov 2023",
# #          xaxis = list(title = ""),
# #          yaxis = list(title = "", dtick = 1)) # , showticklabels = FALSE))
# # 
# # chunk_rank_plot_july
# # 
# # saveWidget(
# #   chunk_rank_plot_july,
# #   'C:/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/chunk_rank_plot_july.html')
# 
# 
# # Plot CUMULATIVE monthly % change in 
# # normalized stops
# #=====================================
# 
# head(monthly_change)
# 
# cumulative_monthly <-
#   monthly_change %>%
#   mutate(month = month(date_range_start)) %>%
#   arrange(city, month) %>%
#   filter(!month %in% c(3, 12)) %>%
#   group_by(city) %>% 
#   mutate(cum_change = cumsum(perc_change))
# 
# head(cumulative_monthly, 10)
# 
# cumulative_area_plot <- plot_ly() %>%
#   add_trace(
#     type = 'scatter',
#     mode = 'lines',
#     fill = 'tonexty',  # Set to 'tonexty' for stacked area chart
#     data = cumulative_monthly,
#     sort = FALSE,
#     x = ~month,
#     y = ~cum_change,
#     name = ~city,
#     stackgroup = 'one'
#   ) %>%
#   layout(
#     title = "Cumulative monthly percent change in total stops, normalized by MSA,<br>from provider 230399 (stop_uplevelled table), April - November 2023",
#     xaxis = list(
#       title = "",
#       zerolinecolor = "#ffff",
#       ticktext = list("April", "May", "June", "July", "August", "September", "October", "November"),
#       tickvals = list(4, 5, 6, 7, 8, 9, 10, 11),
#       tickmode = "array"
#     ),
#     yaxis = list(
#       title = "Cumulative percent change from March onwards",
#       zerolinecolor = "#ffff",
#       ticksuffix = "  "
#     )
#   )
# 
# cumulative_area_plot
# 
# saveWidget(
#   cumulative_area_plot,
#   'C:/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/cumulative_area_plot.html')
# 
# 
# # Run regression (normalized total 
# # stops vs time)
# #=====================================
# 
# city_regs_tot <-
#   final_df %>% 
#   select(city, date, normalized_stops) %>%
#   nest(data = -city) %>% 
#   mutate(model = map(data, ~lm(normalized_stops ~ date, data = .)), 
#          tidied = map(model, tidy)) %>% 
#   unnest(tidied) %>%
#   filter(term == 'date') %>%
#   select(city, estimate, std.error, statistic, p.value) %>%
#   data.frame() %>%
#   mutate(stat_sig_05 = case_when(
#     p.value < .05 ~ 'yes',
#     TRUE ~ 'no'
#   )) %>%
#   arrange(desc(stat_sig_05), desc(estimate))
# 
# View(city_regs_tot)
# 
# write.csv(city_regs_tot,
#           'C:/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/norm_total_stops_regression_rank.csv',
#           row.names = F)
# 
# 
# # Run regression (normalized distinct
# # stops vs time)
# #=====================================
# 
# city_regs_distinct <-
#   final_df %>% 
#   select(city, date, normalized_distinct) %>%
#   nest(data = -city) %>% 
#   mutate(model = map(data, ~lm(normalized_distinct ~ date, data = .)), 
#          tidied = map(model, tidy)) %>% 
#   unnest(tidied) %>%
#   filter(term == 'date') %>%
#   select(city, estimate, std.error, statistic, p.value) %>%
#   data.frame() %>%
#   mutate(stat_sig_05 = case_when(
#     p.value < .05 ~ 'yes',
#     TRUE ~ 'no'
#   )) %>%
#   arrange(desc(stat_sig_05), desc(estimate))
# 
# View(city_regs_distinct)
# 
# write.csv(city_regs_distinct,
#           'C:/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/norm_unique_stops_regression_rank.csv',
#           row.names = F)
# 
# 
# # Compare rankings (total vs unique)
# #=====================================
# 
# compare_reg_rank <-
#   city_regs_tot %>%
#   filter(stat_sig_05 == 'yes') %>%
#   mutate(total_stops_rank = row_number()) %>%
#   select(city, total_stops_rank) %>%
#   inner_join(
#     city_regs_distinct %>%
#     filter(stat_sig_05 == 'yes') %>%
#     mutate(unique_stops_rank = row_number()) %>%
#     select(city, unique_stops_rank)    
#   ) %>%
#   mutate(rank_diff = total_stops_rank - unique_stops_rank) %>%
#   arrange(rank_diff)
# 
# compare_reg_rank
# 
# write.csv(compare_reg_rank,
#           'C:/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/compare_regression_rank.csv',
#           row.names = F)
# 
