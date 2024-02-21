#===============================================================================
# Explore March - Dec 2023 data from provider 700199 from stop_uplevelled table 
# for all 60+ HDBSCAN downtowns
#===============================================================================

# Load packages
#=====================================

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'sf', 'lubridate', 'leaflet', 'plotly', 'htmlwidgets', 
       'broom', 'forecast', 'caret', 'purrr', 'RColorBrewer', 'scales'))

# Load downtown & MSA data
#=====================================

filepath <- '/Users/jpg23/data/downtownrecovery/spectus_exports/stop_uplevelled_700199/'

# downtown
dt <-
  list.files(path = paste0(filepath, 'downtown/')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'downtown/', .),
    delim = '\001',
    col_names = c('city', 'zone_date', 'n_stops', 'n_distinct_devices'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-zone_date) %>%
  filter(date >= as.Date('2023-03-01') & 
           date <= as.Date('2023-12-19')) %>% 
  mutate(city = str_remove_all(city, "\\s\\w{2}$|\\.| city \\(balance\\)|-Davidson metropolitan government \\(balance\\)| - Gatineau \\(Ontario part \\/ partie de l'Ontario\\)"),
         city = str_replace_all(city, 'é', 'e'),
         city = str_remove_all(city, 'Urban '),
         city = ifelse(city == 'Washington', 'Washington DC', city))

# MSA
msa <-
  list.files(path = paste0(filepath, 'msa/')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'msa/', .),
    delim = '\001',
    col_names = c('msa_name', 'zone_date', 'n_stops', 'n_distinct_devices'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(zone_date), format = "%Y-%m-%d")) %>%
  arrange(date) %>%
  select(-zone_date) %>%
  rename(n_stops_msa = n_stops, n_distinct_devices_msa = n_distinct_devices) %>%
  filter(date >= as.Date('2023-03-01') & 
           date <= as.Date('2023-12-19'))

range(dt$date)
range(msa$date) # should both be 2023-03-01 to 2023-12-31


# Join them
#=====================================

msa_names <- read.csv('/Users/jpg23/data/downtownrecovery/sensitivity_analysis/msa_names.csv')

msa_cities <- unique(msa_names$city)
dt_cities <- unique(dt$city)

setdiff(dt_cities, msa_cities)
setdiff(msa_cities, dt_cities)

dt1 <- dt %>%
  left_join(msa_names)

head(dt1)
head(msa)

dt1 %>% filter(is.na(msa_name)) # should be no rows

final_df <-
  dt1 %>%
  left_join(msa, by = c('msa_name', 'date')) %>%
  mutate(normalized_distinct = n_distinct_devices/n_distinct_devices_msa,
         normalized_stops = n_stops/n_stops_msa)

head(final_df)


# Plot daily unique devices
#=====================================

# Raw numbers
unique_raw_plot <-
  plot_ly() %>%
  add_lines(data = final_df,
            x = ~date, y = ~n_distinct_devices,
            name = ~paste0(city, ': downtown'),
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ' downtown: ', round(n_distinct_devices, 3)),
            line = list(shape = "linear", color = '#445e3d')) %>%
  add_lines(data = final_df,
            x = ~date, y = ~n_distinct_devices_msa,
            name = ~paste0(city, ': MSA'),
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ' MSA: ', round(n_distinct_devices_msa, 3)),
            line = list(shape = "linear", color = '#b0c4ab')) %>%
  layout(title = "Daily unique devices from provider 700199 (stop_uplevelled table), HDBSCAN downtown and MSA",
         xaxis = list(title = "Day", zerolinecolor = "#ffff",
                      tickformat = "%Y-%m-%d"),
         yaxis = list(title = "Unique devices", zerolinecolor = "#ffff",
                      ticksuffix = "  "))

unique_raw_plot

saveWidget(
  unique_raw_plot,
  '/Users/jpg23/UDP/downtown_recovery/provider_700199_stop_uplevelled/unique_plot.html')

# Normalized
unique_norm_plot <-
  plot_ly() %>%
  add_lines(data = final_df,
            x = ~date, y = ~normalized_distinct,
            name = ~city,
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ': ', round(normalized_distinct, 3)),
            line = list(shape = "linear", color = '#6665a8')) %>%
  layout(title = "Daily unique devices from provider 700199 (stop_uplevelled table), normalized by MSA",
         xaxis = list(title = "Day", zerolinecolor = "#ffff",
                      tickformat = "%Y-%m-%d"),
         yaxis = list(title = "Normalized unique devices", zerolinecolor = "#ffff",
                      ticksuffix = "  "))

unique_norm_plot

saveWidget(
  unique_norm_plot,
  '/Users/jpg23/UDP/downtown_recovery/provider_700199_stop_uplevelled/unique_plot_norm.html')


# Plot daily normalized stops
#=====================================

# Raw numbers
stops_raw_plot <-
  plot_ly() %>%
  add_lines(data = final_df,
            x = ~date, y = ~n_stops,
            name = ~paste0(city, ': downtown'),
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ' downtown: ', round(n_stops, 3)),
            line = list(shape = "linear", color = '#c2580e')) %>%
  add_lines(data = final_df,
            x = ~date, y = ~n_stops_msa,
            name = ~paste0(city, ': MSA'),
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ' MSA: ', round(n_stops_msa, 3)),
            line = list(shape = "linear", color = '#f09a5d')) %>%
  layout(title = "Daily stops from provider 700199 (stop_uplevelled table), HDBSCAN downtown and MSA",
         xaxis = list(title = "Day", zerolinecolor = "#ffff",
                      tickformat = "%Y-%m-%d"),
         yaxis = list(title = "Stops", zerolinecolor = "#ffff",
                      ticksuffix = "  "))

stops_raw_plot

saveWidget(
  stops_raw_plot,
  '/Users/jpg23/UDP/downtown_recovery/provider_700199_stop_uplevelled/stops_plot.html')

# Normalized
stops_norm_plot <-
  plot_ly() %>%
  add_lines(data = final_df,
            x = ~date, y = ~normalized_stops,
            name = ~city,
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ': ', round(normalized_stops, 3)),
            line = list(shape = "linear", color = '#6e3849')) %>%
  layout(title = "Daily stops from provider 700199 (stop_uplevelled table), normalized by MSA",
         xaxis = list(title = "Day", zerolinecolor = "#ffff",
                      tickformat = "%Y-%m-%d"),
         yaxis = list(title = "Normalized stops", zerolinecolor = "#ffff",
                      ticksuffix = "  "))

stops_norm_plot

saveWidget(
  stops_norm_plot,
  '/Users/jpg23/UDP/downtown_recovery/provider_700199_stop_uplevelled/stops_plot_norm.html')

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
#   layout(title = "Weekly percent change in total stops from provider 700199 (stop_uplevelled table), normalized by MSA",
#          xaxis = list(title = "Week", zerolinecolor = "#ffff",
#                       tickformat = "%Y-%m-%d"),
#          yaxis = list(title = "Percent change from previous week", zerolinecolor = "#ffff",
#                       ticksuffix = "  "))
# 
# weekly_change_plot
# 
# saveWidget(
#   weekly_change_plot,
#   '/Users/jpg23/UDP/downtown_recovery/provider_700199_stop_uplevelled/weekly_change_plot.html')
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
#   layout(title = "Monthly percent change in total stops from provider 700199 (stop_uplevelled table), normalized by MSA",
#          xaxis = list(title = "Month", zerolinecolor = "#ffff",
#                       tickformat = "%b %Y"),
#          yaxis = list(title = "Percent change from previous month", zerolinecolor = "#ffff",
#                       ticksuffix = "  "))
# 
# monthly_change_plot
# 
# saveWidget(
#   monthly_change_plot,
#   '/Users/jpg23/UDP/downtown_recovery/provider_700199_stop_uplevelled/monthly_change_plot.html')
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
#     title = "Monthly percent change (from previous month) in total stops,<br>normalized by MSA, from provider 700199 (stop_uplevelled table), June - November 2023",
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
#   '/Users/jpg23/UDP/downtown_recovery/provider_700199_stop_uplevelled/monthly_area_plot.html')
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
#           '/Users/jpg23/UDP/downtown_recovery/provider_700199_stop_uplevelled/summer_ranking.csv',
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
#           '/Users/jpg23/UDP/downtown_recovery/provider_700199_stop_uplevelled/fall_ranking.csv',
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
#   layout(title = "Percent change in total stops compared to March 2023, from provider 700199 (stop_uplevelled table), normalized by MSA",
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
#   '/Users/jpg23/UDP/downtown_recovery/provider_700199_stop_uplevelled/chunk_plot.html')
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
#     title = "Monthly stops by city, normalized by MSA, from provider 700199 (stop_uplevelled table), March - November 2023",
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
#   '/Users/jpg23/UDP/downtown_recovery/provider_700199_stop_uplevelled/chunk_area_plot.html')
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
#   '/Users/jpg23/UDP/downtown_recovery/provider_700199_stop_uplevelled/chunk_rank_plot.html')
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
# #   '/Users/jpg23/UDP/downtown_recovery/provider_700199_stop_uplevelled/chunk_rank_plot_july.html')
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
#     title = "Cumulative monthly percent change in total stops, normalized by MSA,<br>from provider 700199 (stop_uplevelled table), April - November 2023",
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
#   '/Users/jpg23/UDP/downtown_recovery/provider_700199_stop_uplevelled/cumulative_area_plot.html')
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
#           '/Users/jpg23/UDP/downtown_recovery/provider_700199_stop_uplevelled/norm_total_stops_regression_rank.csv',
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
#           '/Users/jpg23/UDP/downtown_recovery/provider_700199_stop_uplevelled/norm_unique_stops_regression_rank.csv',
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
#           '/Users/jpg23/UDP/downtown_recovery/provider_700199_stop_uplevelled/compare_regression_rank.csv',
#           row.names = F)
# 
# 
# Remove outliers
#=====================================

final_df_no_outliers <- final_df %>%
  group_by(city) %>%
  mutate(
    ts_distinct = ts(normalized_distinct, start = c(min(date), 1)),
    ts_distinct_no_outliers = tsclean(ts_distinct),
    ts_stops = ts(normalized_stops, start = c(min(date), 1)),
    ts_stops_no_outliers = tsclean(ts_stops)
  ) %>%
  ungroup() %>%
  data.frame() %>%
  select(-c(ts_distinct, normalized_distinct, ts_stops, normalized_stops)) %>%
  rename(normalized_distinct = ts_distinct_no_outliers,
         normalized_stops = ts_stops_no_outliers)

# Check the resulting dataframe
head(final_df_no_outliers)
# 
# norm_stops_plot <-
#   plot_ly() %>%
#   add_lines(data = final_df_no_outliers,
#             x = ~date, y = ~normalized_stops,
#             name = ~city,
#             opacity = .7,
#             split = ~city,
#             text = ~paste0(city, ': ', round(normalized_stops, 3)),
#             line = list(shape = "linear", color = '#6665a8')) %>%
#   layout(title = "Daily total stops from provider 700199 (stop_uplevelled table),<br>normalized by MSA, outliers removed",
#          xaxis = list(title = "Day", zerolinecolor = "#ffff",
#                       tickformat = "%Y-%m-%d"),
#          yaxis = list(title = "Normalized total stops", zerolinecolor = "#ffff",
#                       ticksuffix = "  "))
# 
# norm_stops_plot
# 
# 
# # LOESS regression
# # (normalized total stops)
# #=====================================
# 
# # https://bio723-class.github.io/Bio723-book/non-linear-regression-models.html
# # https://stackoverflow.com/a/70915249
# 
# # # Span: "over what distance should the weight function drop to zero?"
# # # Try different values of 'span':
# # calcSSE <- function(x){
# #   loessMod <- try(loess(normalized_stops ~ as.numeric(date), 
# #                         data=final_df_no_outliers, 
# #                         span=x), silent=T)
# #   res <- try(loessMod$residuals, silent=T)
# #   if(!inherits(res, "try-error")){
# #     sse <- sum(res^2)
# #   }else{
# #     print('try-error')
# #     sse <- 99999
# #   }
# #   return(sse)
# # }
# # 
# # optimize(calcSSE, c(0.01,1))
# 
# #######
# 
# models <- final_df_no_outliers %>%
#   tidyr::nest(-city) %>%
#   dplyr::mutate(
#     # Perform loess calculation on each city
#     m = purrr::map(data, loess,
#                    formula = normalized_stops ~ as.numeric(date), 
#                    span = 0.8),
#     # Retrieve the fitted values from each model
#     fitted = purrr::map(m, `[[`, "fitted")
#   )
# 
# # Apply fitted y's as a new column
# results <- models %>%
#   dplyr::select(-m) %>%
#   tidyr::unnest()
# 
# loess_plot <-
#   plot_ly(results,
#           x = ~date,
#           y = ~fitted,
#           name = ~city,
#           opacity = 0.7,
#           split = ~city,
#           type = 'scatter',
#           mode = 'lines',
#           color = ~city) %>%
#   add_trace(
#     y = ~normalized_stops,
#     name = ~city,
#     mode = 'markers',
#     color = ~city
#   ) %>%
#   layout(title = "Daily total stops from provider 700199 (stop_uplevelled table),<br>normalized by MSA, outliers removed, with LOESS",
#          xaxis = list(title = "Day", zerolinecolor = "#ffff",
#                       tickformat = "%Y-%m-%d"),
#          yaxis = list(title = "Normalized total stops", zerolinecolor = "#ffff",
#                       ticksuffix = "  "))
# 
# loess_plot
# 
# 
# # LOESS regression - scaled
# # (normalized total stops)
# #=====================================
# 
models_scaled <- final_df_no_outliers %>%
  group_by(city) %>%
  mutate(normalized_stops = rescale(normalized_stops)) %>%
  ungroup() %>%
  tidyr::nest(-city) %>%
  dplyr::mutate(
    # Perform loess calculation on each city
    m = purrr::map(data, loess,
                   formula = normalized_stops ~ as.numeric(date),
                   span = 0.5),
    # Retrieve the fitted values from each model
    fitted = purrr::map(m, `[[`, "fitted")
  )

# Apply fitted y's as a new column
results_scaled <- models_scaled %>%
  dplyr::select(-m) %>%
  tidyr::unnest()
# 
# loess_plot_scaled <-
#   plot_ly(results_scaled,
#           x = ~date,
#           y = ~fitted,
#           name = ~city,
#           opacity = 0.7,
#           split = ~city,
#           type = 'scatter',
#           mode = 'lines',
#           color = ~city) %>%
#   add_trace(
#     y = ~normalized_stops,
#     name = ~city,
#     mode = 'markers',
#     color = ~city
#   ) %>%
#   layout(title = "Daily total stops from provider 700199 (stop_uplevelled table),<br>normalized by MSA, outliers removed, with LOESS - scaled between 0 & 1",
#          xaxis = list(title = "Day", zerolinecolor = "#ffff",
#                       tickformat = "%Y-%m-%d"),
#          yaxis = list(title = "Normalized total stops", zerolinecolor = "#ffff",
#                       ticksuffix = "  "))
# 
# loess_plot_scaled
# 
# saveWidget(
#   loess_plot_scaled,
#   '/Users/jpg23/UDP/downtown_recovery/provider_700199_stop_uplevelled/loess_scaled_span5.html')
# 
# without the dots!
loess_plot_scaled_no_dots <-
  plot_ly(results_scaled,
          x = ~date,
          y = ~fitted,
          name = ~city,
          opacity = 0.7,
          split = ~city,
          type = 'scatter',
          mode = 'lines',
          color = ~city) %>%
  layout(title = "Daily total stops from provider 700199 (stop_uplevelled table),<br>normalized by MSA, outliers removed, with LOESS - scaled between 0 & 1",
         xaxis = list(title = "Day", zerolinecolor = "#ffff",
                      tickformat = "%Y-%m-%d"),
         yaxis = list(title = "Normalized total stops", zerolinecolor = "#ffff",
                      ticksuffix = "  "))

loess_plot_scaled_no_dots

saveWidget(
  loess_plot_scaled_no_dots,
  '/Users/jpg23/UDP/downtown_recovery/provider_700199_stop_uplevelled/loess_scaled_span5_lines_only.html')
# 
# 
# 
# 
# 
# 
# # LOESS regression - scaled
# # (normalized distinct devices)
# #=====================================
# 
models_scaled_distinct <- final_df_no_outliers %>%
  group_by(city) %>%
  mutate(normalized_distinct = rescale(normalized_distinct)) %>%
  ungroup() %>%
  tidyr::nest(-city) %>%
  dplyr::mutate(
    # Perform loess calculation on each city
    m = purrr::map(data, loess,
                   formula = normalized_distinct ~ as.numeric(date),
                   span = 0.5),
    # Retrieve the fitted values from each model
    fitted = purrr::map(m, `[[`, "fitted")
  )

# Apply fitted y's as a new column
results_scaled_distinct <- models_scaled_distinct %>%
  dplyr::select(-m) %>%
  tidyr::unnest()
# 
# loess_plot_scaled_distinct <-
#   plot_ly(results_scaled_distinct,
#           x = ~date,
#           y = ~fitted,
#           name = ~city,
#           opacity = 0.7,
#           split = ~city,
#           type = 'scatter',
#           mode = 'lines',
#           color = ~city) %>%
#   add_trace(
#     y = ~normalized_distinct,
#     name = ~city,
#     mode = 'markers',
#     color = ~city
#   ) %>%
#   layout(title = "Daily unique devices from provider 700199 (stop_uplevelled table),<br>normalized by MSA, outliers removed, with LOESS - scaled between 0 & 1",
#          xaxis = list(title = "Day", zerolinecolor = "#ffff",
#                       tickformat = "%Y-%m-%d"),
#          yaxis = list(title = "Normalized unique devices", zerolinecolor = "#ffff",
#                       ticksuffix = "  "))
# 
# loess_plot_scaled_distinct
# 
# saveWidget(
#   loess_plot_scaled_distinct,
#   '/Users/jpg23/UDP/downtown_recovery/provider_700199_stop_uplevelled/loess_scaled_distinct_span5.html')

# without the dots!
loess_plot_scaled_no_dots_distinct <-
  plot_ly(results_scaled_distinct,
          x = ~date,
          y = ~fitted,
          name = ~city,
          opacity = 0.7,
          split = ~city,
          type = 'scatter',
          mode = 'lines',
          color = ~city) %>%
  layout(title = "Daily unique devices from provider 700199 (stop_uplevelled table),<br>normalized by MSA, outliers removed, with LOESS - scaled between 0 & 1",
         xaxis = list(title = "Day", zerolinecolor = "#ffff",
                      tickformat = "%Y-%m-%d"),
         yaxis = list(title = "Normalized unique devices", zerolinecolor = "#ffff",
                      ticksuffix = "  "))

loess_plot_scaled_no_dots_distinct

saveWidget(
  loess_plot_scaled_no_dots_distinct,
  '/Users/jpg23/UDP/downtown_recovery/provider_700199_stop_uplevelled/loess_scaled_distinct_span5_lines_only.html')

