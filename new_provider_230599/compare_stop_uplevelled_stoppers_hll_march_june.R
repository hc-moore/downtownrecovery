#===============================================================================
# Compare March - June 2023 data from provider 230399 from stop_uplevelled table 
# vs stoppers_hll_by_geohash for all 60+ HDBSCAN downtowns
#===============================================================================

# Load packages
#=====================================

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'sf', 'lubridate', 'leaflet', 'plotly', 'htmlwidgets',
       'ggpubr', 'forecast'))

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
  mutate(normalized_distinct = n_distinct_devices/n_distinct_devices_msa) %>%
  select(city, date, normalized_distinct) %>%
  group_by(city) %>%
  mutate(
    # replace outliers with imputed values
    ts_data = ts(normalized_distinct, start = c(min(date), 1)),
    ts_data_no_outliers = tsclean(ts_data),
    date_range_start = floor_date(
      date,
      unit = "week",
      week_start = getOption("lubridate.week.start", 1))) %>%
  select(-c(normalized_distinct, ts_data)) %>%
  rename(normalized_distinct = ts_data_no_outliers) %>%
  group_by(date_range_start, city) %>%
  # calculate weekly normalized unique device counts by taking average of the 
  # daily normalized unique device counts
  summarize(normalized = mean(normalized_distinct, na.rm = T)) %>%
  ungroup() %>%
  data.frame()

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


# Compare overall sample sizes
#=====================================

pre_impute_hll <- read.csv('C:/Users/jpg23/data/downtownrecovery/sensitivity_analysis/for_amir_weekend_weekday.csv') %>%
  filter(provider_id == '190199' & date >= as.Date('2023-03-01') &
           date <= as.Date('2023-05-31')) %>%
  mutate(date = as.Date(date, format = "%Y-%m-%d"))

pre_agg_stop_uplevelled <-
  dt1 %>%
  left_join(msa, by = c('msa_name', 'date'))

head(pre_impute_hll)
head(pre_agg_stop_uplevelled)

compare_samp <- 
  pre_impute_hll %>%
  select(city, date, hll_dt = downtown_devices, hll_msa = msa_count) %>%
  full_join(
    pre_agg_stop_uplevelled %>%
      select(city, date, up_dt = n_distinct_devices, up_msa = n_distinct_devices_msa)
  )

head(compare_samp)

compare_samp_plot <-
  plot_ly() %>%
  add_lines(data = compare_samp,
            x = ~date, y = ~hll_dt,
            name = ~paste0(city, ": downtown, stoppers_hll"),
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ', stoppers_hll downtown: ', date, ' - ', hll_dt),
            line = list(shape = "linear", color = '#eb4034')) %>%
  add_lines(data = compare_samp,
            x = ~date, y = ~hll_msa,
            name = ~paste0(city, ": MSA, stoppers_hll"),
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ', stoppers_hll MSA: ', date, ' - ', hll_msa),
            line = list(shape = "linear", color = '#eb4034', dash = 'dash')) %>%
  add_lines(data = compare_samp,
            x = ~date, y = ~up_dt,
            name = ~paste0(city, ": downtown, stop_uplevelled"),
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ', stop_uplevelled downtown: ', date, ' - ', up_dt),
            line = list(shape = "linear", color = '#492657')) %>%
  add_lines(data = compare_samp,
            x = ~date, y = ~up_msa,
            name = ~paste0(city, ": MSA, stop_uplevelled"),
            opacity = .7,
            split = ~city,
            text = ~paste0(city, ', stop_uplevelled MSA: ', date, ' - ', up_msa),
            line = list(shape = "linear", color = '#492657', dash = 'dash')) %>%  
  layout(title = "Daily unique device counts for HDBSCAN downtowns and MSAs:<br>stop_uplevelled (provider 230399) vs. stoppers_hll_by_geohash (190199)",
         xaxis = list(title = "Date", zerolinecolor = "#ffff",
                      tickformat = "%b %Y"),
         yaxis = list(title = "Unique devices", zerolinecolor = "#ffff",
                      ticksuffix = "  "))

compare_samp_plot

saveWidget(
  compare_samp_plot,
  'C:/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/compare_SAMPLE_SIZE_uplevelled_hll_march_june.html')


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

# ggscatter(both, x = "up_norm", y = "hll_norm",
#           color = "blue", cor.coef = TRUE, 
#           cor.method = "spearman",
#           xlab = "stop_uplevelled", ylab = "stoppers_hll")

scatter_both <- plot_ly(
  data = both, 
  x = ~up_norm, 
  y = ~hll_norm,
  split = ~city,
  text = ~paste0(city, ', week of ', date_range_start, '<br>stop_uplevelled: ', 
                 round(up_norm, 3), '<br>stoppers_hll: ', round(hll_norm, 3))) %>%
  layout(shapes = list(list(
    type = "line", 
    x0 = 0, 
    x1 = ~max(up_norm, hll_norm), 
    xref = "x",
    y0 = 0, 
    y1 = ~max(up_norm, hll_norm),
    yref = "y",
    line = list(color = "black")
  )))

scatter_both

saveWidget(
  scatter_both,
  'C:/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/compare_uplevelled_hll_march_june_SCATTER.html')


# Compare rates of change
#=====================================

up_trend <-
  stop_uplevelled %>%
  nest(data = -city) %>% 
  mutate(model = map(data, ~lm(normalized ~ date_range_start, data = .)), 
         tidied = map(model, tidy)) %>% 
  unnest(tidied) %>%
  filter(term == 'date_range_start') %>%
  select(city, estimate, std.error, statistic, p.value) %>%
  data.frame() %>%
  mutate(stat_sig_05 = case_when(
    p.value < .05 ~ 'yes',
    TRUE ~ 'no'
  )) %>%
  arrange(desc(stat_sig_05), desc(estimate))

up_trend  

hll_trend <-  
  stoppers_hll %>%
  mutate(date_range_start = as.Date(date_range_start, format = "%Y-%m-%d")) %>%
  nest(data = -city) %>% 
  mutate(model = map(data, ~lm(normalized ~ date_range_start, data = .)), 
         tidied = map(model, tidy)) %>% 
  unnest(tidied) %>%
  filter(term == 'date_range_start') %>%
  select(city, estimate, std.error, statistic, p.value) %>%
  data.frame() %>%
  mutate(stat_sig_05 = case_when(
    p.value < .05 ~ 'yes',
    TRUE ~ 'no'
  )) %>%
  arrange(desc(stat_sig_05), desc(estimate))

hll_trend
  
compare_trends <-
  up_trend %>% select(city, uplevelled_slope = estimate, up_sig = stat_sig_05) %>%
  left_join(hll_trend %>% select(city, hll_slope = estimate, hll_sig = stat_sig_05),
            by = 'city')

compare_trends

scatter_trends <- plot_ly(
  data = compare_trends, 
  x = ~uplevelled_slope, 
  y = ~hll_slope,
  split = ~city,
  text = ~paste0(city, '<br>stop_uplevelled slope: ', round(uplevelled_slope, 8),
                 ' - stat. sig.? ', up_sig,
                 '<br>stoppers_hll slope: ', round(hll_slope, 8),
                 ' - stat. sig.? ', hll_sig)) %>%
  layout(shapes = list(list(
    type = "line", 
    x0 = ~min(uplevelled_slope, hll_slope), 
    x1 = ~max(uplevelled_slope, hll_slope), 
    xref = "x",
    y0 = ~min(uplevelled_slope, hll_slope), 
    y1 = ~max(uplevelled_slope, hll_slope),
    yref = "y",
    line = list(color = "gray", dash = "dash")
  )))

scatter_trends

saveWidget(
  scatter_trends,
  'C:/Users/jpg23/UDP/downtown_recovery/provider_230399_stop_uplevelled/compare_SLOPES_uplevelled_hll_march_june_SCATTER.html')
