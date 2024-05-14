#===============================================================================
# Calculate recovery rates for commercial districts - WORKING HOURS VS NON-
# WORKING HOURS - in Portland, SF, NYC, Chicago and Toronto
#===============================================================================

# Load packages
#-----------------------------------------

source('~/git/timathomas/functions/functions.r')

ipak(c('tidyverse', 'lubridate', 'ggplot2', 'plotly', 
       'sf', 'leaflet', 'BAMMtools', 'gtools', 'htmlwidgets'))

# Load commercial district data (work vs
# non-work hours)
#-----------------------------------------

filepath <- '/Users/jpg23/data/downtownrecovery/spectus_exports/'

# Only keep Mon-Fri
hrs <-
  list.files(path = paste0(filepath, 'commercial_work_nonwork_hrs')) %>%
  map_df(~read_delim(
    paste0(filepath, 'commercial_work_nonwork_hrs/', .),
    delim = '\001',
    col_names = c('city', 'district', 'provider_id', 
                  'event_date', 'work_hh_devices', 'non_work_hh_devices'),
    col_types = c('ccciii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d"),
         day_week = wday(date, label = T)) %>%
  arrange(date) %>%
  select(-event_date) %>%
  filter(provider_id != '230599' & !(day_week %in% c('Sat', 'Sun')))

head(hrs)
table(hrs$provider_id)
table(hrs$day_week)

# Load commercial district data (daily)
#-----------------------------------------

# Only keep Sat & Sun
daily <-
  list.files(path = paste0(filepath, 'commercial_districts')) %>%
  map_df(~read_delim(
    paste0(filepath, 'commercial_districts/', .),
    delim = '\001',
    col_names = c('city', 'district', 'provider_id', 
                  'approx_distinct_devices_count', 'event_date'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d"),
         day_week = wday(date, label = T),
         work_hh_devices = NA) %>%
  arrange(date) %>%
  select(-event_date) %>%
  filter(provider_id != '230599' & (day_week %in% c('Sat', 'Sun'))) %>%
  rename(non_work_hh_devices = approx_distinct_devices_count)

head(daily)
table(daily$provider_id)
table(daily$day_week)

# Combine Mon-Fri & Sat-Sun data
#-----------------------------------------

comm <- rbind(hrs, daily)

head(comm)
head(comm %>% filter(day_week == 'Sat'))

# Load MSA data
#-----------------------------------------

msa_path <- '/Users/jpg23/data/downtownrecovery/spectus_exports/sensitivity_analysis/'

msa <-
  list.files(path = paste0(msa_path, 'MSA')) %>%
  map_df(~read_delim(
    paste0(msa_path, 'MSA/', .),
    delim = '\001',
    col_names = c('msa_name', 'provider_id', 'approx_distinct_devices_count',
                  'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date) %>%
  rename(msa_count = approx_distinct_devices_count) %>%
  filter(provider_id != '230599')

msa_names <- read.csv('/Users/jpg23/data/downtownrecovery/sensitivity_analysis/msa_names.csv')

head(msa)
unique(msa$provider_id)

head(msa_names)

# Join commercial district & MSA data
#-----------------------------------------

unique(comm$city)
unique(msa_names$city)

comm1 <- comm %>%
  mutate(city = case_when(
    city == 'sf' ~ 'San Francisco',
    city == 'chicago' ~ 'Chicago',
    city == 'nyc' ~ 'New York',
    city == 'portland' ~ 'Portland',
    city == 'toronto' ~ 'Toronto'
  ))

unique(comm1$city)

comm2 <- comm1 %>% left_join(msa_names)

head(comm2)
head(msa)

comm2 %>% filter(is.na(msa_name)) # should be no rows

range(comm2$date)
range(msa$date)

final_df <-
  comm2 %>%
  left_join(msa, by = c('msa_name', 'provider_id', 'date')) %>%
  mutate(comm_district = paste0(city, ': ', district)) %>%
  select(-c(city, district, msa_name))

head(final_df)

# Export normalized counts for imputation
#-----------------------------------------

for_imputation <-
  final_df %>%
  mutate(
    date_range_start = floor_date(
      date,
      unit = "week",
      week_start = getOption("lubridate.week.start", 1))) %>%
  group_by(date_range_start, comm_district, provider_id) %>%
  summarize(work_hh_devices = sum(work_hh_devices, na.rm = T),
            non_work_hh_devices = sum(non_work_hh_devices, na.rm = T),
            msa_count = sum(msa_count, na.rm = T)) %>%
  ungroup() %>%
  data.frame() %>%
  mutate(normalized_work_hrs = work_hh_devices/msa_count,
         normalized_nonwork_hrs = non_work_hh_devices/msa_count)

head(for_imputation)
range(for_imputation$date_range_start)

# write.csv(for_imputation,
#           '/Users/jpg23/data/downtownrecovery/commercial_districts/commercial_for_imputation_work_vs_nonwork_hrs.csv',
#           row.names = F)

# Plot data
#-----------------------------------------

comm_190199_plot <-
  plot_ly() %>%
  add_lines(data = for_imputation %>% filter(provider_id == '190199'),
            x = ~date_range_start, y = ~normalized_work_hrs,
            name = ~paste0(comm_district, ":  working hours"),
            opacity = .7,
            split = ~comm_district,
            text = ~paste0(comm_district, ': ', round(normalized_work_hrs, 3)),
            line = list(shape = "linear", color = '#bedcfa')) %>%
  add_lines(data = for_imputation %>% filter(provider_id == '190199'),
            x = ~date_range_start, y = ~normalized_nonwork_hrs,
            name = ~paste0(comm_district, ":  non-working hours"),
            opacity = .7,
            split = ~comm_district,
            text = ~paste0(comm_district, ': ', round(normalized_nonwork_hrs, 3)),
            line = list(shape = "linear", color = '#583863')) %>%  
  layout(title = "Weekly counts in commercial districts normalized by MSA (not yet imputed for Toronto), provider 190199, working vs non-working hours",
         xaxis = list(title = "Week", zerolinecolor = "#ffff",
                      tickformat = "%b %Y"),
         yaxis = list(title = "Normalized distinct devices", zerolinecolor = "#ffff",
                      ticksuffix = "  "),
         shapes = list(list(y0 = 0, y1 = 1, yref = "paper",
                            x0 = as.Date('2021-05-17'), x1 = as.Date('2021-05-17'),
                            line = list(color = 'black', dash = 'dash'))))

comm_190199_plot

comm_700199_plot <-
  plot_ly() %>%
  add_lines(data = for_imputation %>% filter(provider_id == '700199'),
            x = ~date_range_start, y = ~normalized_work_hrs,
            name = ~paste0(comm_district, ":  working hours"),
            opacity = .7,
            split = ~comm_district,
            text = ~paste0(comm_district, ': ', round(normalized_work_hrs, 3)),
            line = list(shape = "linear", color = '#bedcfa')) %>%
  add_lines(data = for_imputation %>% filter(provider_id == '700199'),
            x = ~date_range_start, y = ~normalized_nonwork_hrs,
            name = ~paste0(comm_district, ":  non-working hours"),
            opacity = .7,
            split = ~comm_district,
            text = ~paste0(comm_district, ': ', round(normalized_nonwork_hrs, 3)),
            line = list(shape = "linear", color = '#583863')) %>%  
  layout(title = "Weekly counts in commercial districts normalized by MSA (not yet imputed for Toronto), provider 700199, working vs non-working hours",
         xaxis = list(title = "Week", zerolinecolor = "#ffff",
                      tickformat = "%b %Y"),
         yaxis = list(title = "Normalized distinct devices", zerolinecolor = "#ffff",
                      ticksuffix = "  "),
         shapes = list(list(y0 = 0, y1 = 1, yref = "paper",
                            x0 = as.Date('2021-05-17'), x1 = as.Date('2021-05-17'),
                            line = list(color = 'black', dash = 'dash'))))

comm_700199_plot

# Calculate US cities' recovery rates:
# working hours vs non-working hours
#-----------------------------------------

rq_us <-
  for_imputation %>%
  filter(!str_detect(comm_district, 'Toronto: ') & provider_id == '190199') %>%
  filter((date_range_start >= as.Date('2019-03-04') &
            date_range_start <= as.Date('2019-06-10')) |
           (date_range_start >= as.Date('2023-02-27'))) %>%
  mutate(year = year(date_range_start)) %>%
  group_by(comm_district, year) %>%
  summarize(comm_wrk = sum(work_hh_devices, na.rm = T),
            comm_nonwrk = sum(non_work_hh_devices, na.rm = T),
            msa = sum(msa_count, na.rm = T)) %>%
  ungroup() %>%
  mutate(norm_wrk = comm_wrk/msa,
         norm_nonwrk = comm_nonwrk/msa) %>%
  pivot_wider(
    id_cols = c('comm_district'),
    names_from = 'year',
    names_prefix = 'ntv',
    values_from = c('norm_wrk', 'norm_nonwrk')
  ) %>%
  mutate(rq_wrk = norm_wrk_ntv2023/norm_wrk_ntv2019,
         rq_nonwrk = norm_nonwrk_ntv2023/norm_nonwrk_ntv2019) %>%
  data.frame() %>%
  select(comm_district, rq_wrk, rq_nonwrk) %>%
  arrange(desc(rq_wrk)) %>%
  filter(comm_district != 'San Francisco: NEIGHBORHOOD COMMERCIAL, CLUSTER - 19716')

head(rq_us)


# Calculate Toronto's recovery rates
#-----------------------------------------

# Load imputed data for Canada
imputed_canada <- read.csv('/Users/jpg23/data/downtownrecovery/commercial_districts/canada_imputed_commercial_work_nonwork.csv') %>%
  filter(str_detect(city, 'Toronto: ')) %>%
  rename(comm_district = city)

head(imputed_canada)

canada_imputed_plot <-
  plot_ly() %>%
  add_lines(data = imputed_canada %>% filter(provider_id == '190199'),
            x = ~date_range_start, y = ~normalized_work_hrs,
            name = ~paste0('190199: ', comm_district, ":  working hours"),
            opacity = .7,
            split = ~comm_district,
            text = ~paste0(comm_district, ': ', round(normalized_work_hrs, 3)),
            line = list(shape = "linear", color = '#bedcfa')) %>%
  add_lines(data = imputed_canada %>% filter(provider_id == '190199'),
            x = ~date_range_start, y = ~normalized_nonwork_hrs,
            name = ~paste0('190199: ', comm_district, ":  non-working hours"),
            opacity = .7,
            split = ~comm_district,
            text = ~paste0(comm_district, ': ', round(normalized_nonwork_hrs, 3)),
            line = list(shape = "linear", color = '#583863')) %>%  
  add_lines(data = imputed_canada %>% filter(provider_id == '700199'),
            x = ~date_range_start, y = ~normalized_work_hrs,
            name = ~paste0('700199: ', comm_district, ":  working hours"),
            opacity = .7,
            split = ~comm_district,
            text = ~paste0(comm_district, ': ', round(normalized_work_hrs, 3)),
            line = list(shape = "linear", color = '#fcba03')) %>%
  add_lines(data = imputed_canada %>% filter(provider_id == '700199'),
            x = ~date_range_start, y = ~normalized_nonwork_hrs,
            name = ~paste0('700199: ', comm_district, ":  non-working hours"),
            opacity = .7,
            split = ~comm_district,
            text = ~paste0(comm_district, ': ', round(normalized_nonwork_hrs, 3)),
            line = list(shape = "linear", color = '#0f7323')) %>%   
  layout(title = "Weekly counts in commercial districts normalized by MSA, imputed for Toronto",
         xaxis = list(title = "Week", zerolinecolor = "#ffff",
                      tickformat = "%b %Y"),
         yaxis = list(title = "Normalized distinct devices", zerolinecolor = "#ffff",
                      ticksuffix = "  "),
         shapes = list(list(y0 = 0, y1 = 1, yref = "paper",
                            x0 = as.Date('2021-05-17'), x1 = as.Date('2021-05-17'),
                            line = list(color = 'black', dash = 'dash'))))

canada_imputed_plot

map_path <- '/Users/jpg23/UDP/downtown_recovery/commercial_districts/work_nonwork_hrs/'

saveWidget(canada_imputed_plot, 
           paste0(map_path, 'imputed_Toronto_trend_plot.html'))

# NEED TO ADJUST BELOW CODE TO ADJUST FOR 2 COLUMNS INSTEAD OF ONE!

# rq_t <-
#   imputed_canada %>%
#   filter(provider_id == '190199') %>%
#   filter((date_range_start >= as.Date('2019-03-04') & 
#             date_range_start <= as.Date('2019-06-10')) | 
#            (date_range_start >= as.Date('2023-02-27'))) %>%
#   mutate(week_num = isoweek(date_range_start),
#          year = year(date_range_start)) %>%
#   select(-date_range_start) %>%
#   pivot_wider(
#     id_cols = c('comm_district', 'week_num'),
#     names_from = 'year',
#     names_prefix = 'ntv',
#     values_from = 'normalized'
#   ) %>%
#   mutate(rec2023 = ntv2023/ntv2019) %>%
#   data.frame() %>%
#   group_by(comm_district) %>%
#   summarize(rq = mean(rec2023, na.rm = T)) %>%
#   ungroup() %>%
#   data.frame() %>%
#   arrange(desc(rq))

rq_t

# Combine Canada & US RQs
#-----------------------------------------

head(rq_us)
head(rq_t)

rq <- rbind(rq_us, rq_t) %>%
  arrange(desc(rq_wrk))

head(rq)

# write.csv(rq,
#           '/Users/jpg23/UDP/downtown_recovery/commercial_districts/commercial_districts_work_nonwork_hrs_RQs.csv',
#           row.names = F)

# Map recovery rates by district
#-----------------------------------------

comm_sf <- st_read("/Users/jpg23/data/downtownrecovery/shapefiles/commercial_districts/all_cities_commercial_districts.geojson")
head(comm_sf)

comm_sf1 <- comm_sf %>%
  mutate(city = case_when(
    city == 'sf' ~ 'San Francisco',
    city == 'chicago' ~ 'Chicago',
    city == 'nyc' ~ 'New York',
    city == 'portland' ~ 'Portland',
    city == 'toronto' ~ 'Toronto'
  ),
  comm_district = paste0(city, ': ', district)) %>%
  select(comm_district)

head(comm_sf1)

# Join spatial data & recovery rates
rq_sf <- comm_sf1 %>%
  left_join(rq)

head(rq_sf)

summary(rq_sf$rq_wrk)
getJenksBreaks(rq_sf$rq_wrk, 7)
hist(rq_sf$rq_wrk)

summary(rq_sf$rq_nonwrk)
getJenksBreaks(rq_sf$rq_nonwrk, 7)
hist(rq_sf$rq_nonwrk)

comm_sf2 <- rq_sf %>%
  mutate(
    rate_wrk_cat = factor(case_when(
      rq_wrk < .75 ~ '<75%',
      rq_wrk < 1 ~ '75 - 99%',
      rq_wrk < 1.5 ~ '100 - 149%',
      TRUE ~ '150%+'
    ),
    levels = c('<75%', '75 - 99%', '100 - 149%', '150%+')),
    wrk_label = paste0(comm_district, ": ", round(rq_wrk * 100), "%"),
    rate_nonwrk_cat = factor(case_when(
      rq_nonwrk < .75 ~ '<75%',
      rq_nonwrk < 1 ~ '75 - 99%',
      rq_nonwrk < 1.5 ~ '100 - 149%',
      TRUE ~ '150%+'
    ),
    levels = c('<75%', '75 - 99%', '100 - 149%', '150%+')),    
    nonwrk_label = paste0(comm_district, ": ", round(rq_nonwrk * 100), "%")    )

head(comm_sf2)
table(comm_sf2$rate_wrk_cat)
table(comm_sf2$rate_nonwrk_cat)

pal <- c(
  "#e41822",
  "#faa09d",
  "#96cdf2",
  "#0e7bc4"
)

# Work hours
wrk_leaflet_pal <- colorFactor(
  pal,
  domain = comm_sf2$rate_wrk_cat,
  na.color = 'transparent'
)

# Non-work hours
nonwrk_leaflet_pal <- colorFactor(
  pal,
  domain = comm_sf2$rate_nonwrk_cat,
  na.color = 'transparent'
)

# Split dataset up by city
nyc <- comm_sf2 %>% filter(str_detect(comm_district, 'New York: '))
sf <- comm_sf2 %>% filter(str_detect(comm_district, 'San Francisco: '))
portland <- comm_sf2 %>% filter(str_detect(comm_district, 'Portland: '))
chicago <- comm_sf2 %>% filter(str_detect(comm_district, 'Chicago: '))
toronto <- comm_sf2 %>% filter(str_detect(comm_district, 'Toronto: '))

# Map recovery rates by district - WORKING
# HOURS
#-----------------------------------------

# New York
nyc_wrk_map <-
  leaflet(
    options = leafletOptions(minZoom = 9, maxZoom = 18, zoomControl = FALSE)
  ) %>%
  addProviderTiles("CartoDB.PositronNoLabels") %>%
  addPolygons(
    data = nyc,
    label = ~wrk_label,
    labelOptions = labelOptions(textsize = "12px"),
    fillOpacity = .8,
    color = ~wrk_leaflet_pal(rate_wrk_cat),
    stroke = TRUE,
    weight = 1,
    opacity = 1,
    highlightOptions =
      highlightOptions(
        color = "black",
        weight = 3,
        bringToFront = TRUE)
  ) %>%
  addLegend(
    data = nyc,
    position = "bottomleft",
    pal = wrk_leaflet_pal,
    values = ~rate_wrk_cat,
    title = 'Recovery rate<br>(March - mid-June,<br>2023 vs 2019),<br>working hours'
  )

nyc_wrk_map

# San Francisco
sf_wrk_map <-
  leaflet(
    options = leafletOptions(minZoom = 9, maxZoom = 18, zoomControl = FALSE)
  ) %>%
  addProviderTiles("CartoDB.PositronNoLabels") %>%
  addPolygons(
    data = sf,
    label = ~wrk_label,
    labelOptions = labelOptions(textsize = "12px"),
    fillOpacity = .8,
    color = ~wrk_leaflet_pal(rate_wrk_cat),
    stroke = TRUE,
    weight = 1,
    opacity = 1,
    highlightOptions =
      highlightOptions(
        color = "black",
        weight = 3,
        bringToFront = TRUE)
  ) %>%
  addLegend(
    data = sf,
    position = "bottomleft",
    pal = wrk_leaflet_pal,
    values = ~rate_wrk_cat,
    title = 'Recovery rate<br>(March - mid-June,<br>2023 vs 2019),<br>working hours'
  )

sf_wrk_map

# Portland
portland_wrk_map <-
  leaflet(
    options = leafletOptions(minZoom = 9, maxZoom = 18, zoomControl = FALSE)
  ) %>%
  addProviderTiles("CartoDB.PositronNoLabels") %>%
  addPolygons(
    data = portland,
    label = ~wrk_label,
    labelOptions = labelOptions(textsize = "12px"),
    fillOpacity = .8,
    color = ~wrk_leaflet_pal(rate_wrk_cat),
    stroke = TRUE,
    weight = 1,
    opacity = 1,
    highlightOptions =
      highlightOptions(
        color = "black",
        weight = 3,
        bringToFront = TRUE)
  ) %>%
  addLegend(
    data = portland,
    position = "bottomleft",
    pal = wrk_leaflet_pal,
    values = ~rate_wrk_cat,
    title = 'Recovery rate<br>(March - mid-June,<br>2023 vs 2019),<br>working hours'
  )

portland_wrk_map

# Chicago
chicago_wrk_map <-
  leaflet(
    options = leafletOptions(minZoom = 9, maxZoom = 18, zoomControl = FALSE)
  ) %>%
  addProviderTiles("CartoDB.PositronNoLabels") %>%
  addPolygons(
    data = chicago,
    label = ~wrk_label,
    labelOptions = labelOptions(textsize = "12px"),
    fillOpacity = .8,
    color = ~wrk_leaflet_pal(rate_wrk_cat),
    stroke = TRUE,
    weight = 1,
    opacity = 1,
    highlightOptions =
      highlightOptions(
        color = "black",
        weight = 3,
        bringToFront = TRUE)
  ) %>%
  addLegend(
    data = chicago,
    position = "bottomleft",
    pal = wrk_leaflet_pal,
    values = ~rate_wrk_cat,
    title = 'Recovery rate<br>(March - mid-June,<br>2023 vs 2019),<br>working hours'
  )

chicago_wrk_map

# Toronto
toronto_wrk_map <-
  leaflet(
    options = leafletOptions(minZoom = 9, maxZoom = 18, zoomControl = FALSE)
  ) %>%
  addProviderTiles("CartoDB.PositronNoLabels") %>%
  addPolygons(
    data = toronto,
    label = ~wrk_label,
    labelOptions = labelOptions(textsize = "12px"),
    fillOpacity = .8,
    color = ~wrk_leaflet_pal(rate_wrk_cat),
    stroke = TRUE,
    weight = 1,
    opacity = 1,
    highlightOptions =
      highlightOptions(
        color = "black",
        weight = 3,
        bringToFront = TRUE)
  ) %>%
  addLegend(
    data = toronto,
    position = "bottomleft",
    pal = wrk_leaflet_pal,
    values = ~rate_wrk_cat,
    title = 'Recovery rate<br>(March - mid-June,<br>2023 vs 2019),<br>working hours'
  )

toronto_wrk_map

# Save maps
saveWidget(nyc_wrk_map, paste0(map_path, 'work_hrs/nyc_wrk_commercial_RQs.html'))
saveWidget(sf_wrk_map, paste0(map_path, 'work_hrs/sf_wrk_commercial_RQs.html'))
saveWidget(portland_wrk_map, paste0(map_path, 'work_hrs/portland_wrk_commercial_RQs.html'))
saveWidget(chicago_wrk_map, paste0(map_path, 'work_hrs/chicago_wrk_commercial_RQs.html'))
saveWidget(toronto_wrk_map, paste0(map_path, 'work_hrs/toronto_wrk_commercial_RQs.html'))

# Map recovery rates by district - 
# NON-WORKING HOURS
#-----------------------------------------

# New York
nyc_nonwrk_map <-
  leaflet(
    options = leafletOptions(minZoom = 9, maxZoom = 18, zoomControl = FALSE)
  ) %>%
  addProviderTiles("CartoDB.PositronNoLabels") %>%
  addPolygons(
    data = nyc,
    label = ~nonwrk_label,
    labelOptions = labelOptions(textsize = "12px"),
    fillOpacity = .8,
    color = ~nonwrk_leaflet_pal(rate_nonwrk_cat),
    stroke = TRUE,
    weight = 1,
    opacity = 1,
    highlightOptions =
      highlightOptions(
        color = "black",
        weight = 3,
        bringToFront = TRUE)
  ) %>%
  addLegend(
    data = nyc,
    position = "bottomleft",
    pal = nonwrk_leaflet_pal,
    values = ~rate_nonwrk_cat,
    title = 'Recovery rate<br>(March - mid-June,<br>2023 vs 2019),<br>non-working hours'
  )

nyc_nonwrk_map

# San Francisco
sf_nonwrk_map <-
  leaflet(
    options = leafletOptions(minZoom = 9, maxZoom = 18, zoomControl = FALSE)
  ) %>%
  addProviderTiles("CartoDB.PositronNoLabels") %>%
  addPolygons(
    data = sf,
    label = ~nonwrk_label,
    labelOptions = labelOptions(textsize = "12px"),
    fillOpacity = .8,
    color = ~nonwrk_leaflet_pal(rate_nonwrk_cat),
    stroke = TRUE,
    weight = 1,
    opacity = 1,
    highlightOptions =
      highlightOptions(
        color = "black",
        weight = 3,
        bringToFront = TRUE)
  ) %>%
  addLegend(
    data = sf,
    position = "bottomleft",
    pal = nonwrk_leaflet_pal,
    values = ~rate_nonwrk_cat,
    title = 'Recovery rate<br>(March - mid-June,<br>2023 vs 2019),<br>non-working hours'
  )

sf_nonwrk_map

# Portland
portland_nonwrk_map <-
  leaflet(
    options = leafletOptions(minZoom = 9, maxZoom = 18, zoomControl = FALSE)
  ) %>%
  addProviderTiles("CartoDB.PositronNoLabels") %>%
  addPolygons(
    data = portland,
    label = ~nonwrk_label,
    labelOptions = labelOptions(textsize = "12px"),
    fillOpacity = .8,
    color = ~nonwrk_leaflet_pal(rate_nonwrk_cat),
    stroke = TRUE,
    weight = 1,
    opacity = 1,
    highlightOptions =
      highlightOptions(
        color = "black",
        weight = 3,
        bringToFront = TRUE)
  ) %>%
  addLegend(
    data = portland,
    position = "bottomleft",
    pal = nonwrk_leaflet_pal,
    values = ~rate_nonwrk_cat,
    title = 'Recovery rate<br>(March - mid-June,<br>2023 vs 2019),<br>non-working hours'
  )

portland_nonwrk_map

# Chicago
chicago_nonwrk_map <-
  leaflet(
    options = leafletOptions(minZoom = 9, maxZoom = 18, zoomControl = FALSE)
  ) %>%
  addProviderTiles("CartoDB.PositronNoLabels") %>%
  addPolygons(
    data = chicago,
    label = ~nonwrk_label,
    labelOptions = labelOptions(textsize = "12px"),
    fillOpacity = .8,
    color = ~nonwrk_leaflet_pal(rate_nonwrk_cat),
    stroke = TRUE,
    weight = 1,
    opacity = 1,
    highlightOptions =
      highlightOptions(
        color = "black",
        weight = 3,
        bringToFront = TRUE)
  ) %>%
  addLegend(
    data = chicago,
    position = "bottomleft",
    pal = nonwrk_leaflet_pal,
    values = ~rate_nonwrk_cat,
    title = 'Recovery rate<br>(March - mid-June,<br>2023 vs 2019),<br>non-working hours'
  )

chicago_nonwrk_map

# Toronto
toronto_nonwrk_map <-
  leaflet(
    options = leafletOptions(minZoom = 9, maxZoom = 18, zoomControl = FALSE)
  ) %>%
  addProviderTiles("CartoDB.PositronNoLabels") %>%
  addPolygons(
    data = toronto,
    label = ~nonwrk_label,
    labelOptions = labelOptions(textsize = "12px"),
    fillOpacity = .8,
    color = ~nonwrk_leaflet_pal(rate_nonwrk_cat),
    stroke = TRUE,
    weight = 1,
    opacity = 1,
    highlightOptions =
      highlightOptions(
        color = "black",
        weight = 3,
        bringToFront = TRUE)
  ) %>%
  addLegend(
    data = toronto,
    position = "bottomleft",
    pal = nonwrk_leaflet_pal,
    values = ~rate_nonwrk_cat,
    title = 'Recovery rate<br>(March - mid-June,<br>2023 vs 2019),<br>non-working hours'
  )

toronto_nonwrk_map

# Save maps
saveWidget(nyc_nonwrk_map, paste0(map_path, 'nonwork_hrs/nyc_nonwrk_commercial_RQs.html'))
saveWidget(sf_nonwrk_map, paste0(map_path, 'nonwork_hrs/sf_nonwrk_commercial_RQs.html'))
saveWidget(portland_nonwrk_map, paste0(map_path, 'nonwork_hrs/portland_nonwrk_commercial_RQs.html'))
saveWidget(chicago_nonwrk_map, paste0(map_path, 'nonwork_hrs/chicago_nonwrk_commercial_RQs.html'))
saveWidget(toronto_nonwrk_map, paste0(map_path, 'nonwork_hrs/toronto_nonwrk_commercial_RQs.html'))