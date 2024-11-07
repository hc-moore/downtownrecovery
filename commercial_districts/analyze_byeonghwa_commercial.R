#===============================================================================
# Calculate recovery rates for commercial districts in Portland, SF, NYC,
# Chicago and Toronto - using Byeonghwa's polygons
#===============================================================================

# Load packages
#-----------------------------------------

source('~/git/timathomas/functions/functions.r')

ipak(c('tidyverse', 'lubridate', 'ggplot2', 'plotly', 
       'sf', 'leaflet', 'BAMMtools', 'gtools', 'htmlwidgets'))

# Load commercial district data
#-----------------------------------------

filepath <- '/Users/jpg23/data/downtownrecovery/spectus_exports/'

comm <-
  list.files(path = paste0(filepath, 'byeonghwa_commercial')) %>%
  map_df(~read_delim(
    paste0(filepath, 'byeonghwa_commercial/', .),
    delim = '\001',
    col_names = c('city', 'district', 'provider_id', 
                  'approx_distinct_devices_count', 'event_date'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date) %>%
  filter(provider_id != '230599')

head(comm)
table(comm$provider_id)

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

comm1 <- comm %>% left_join(msa_names)
head(comm1)
head(msa)

comm1 %>% filter(is.na(msa_name)) # should be no rows

range(comm1$date)
range(msa$date)

final_df <-
  comm1 %>%
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
  summarize(district_devices = sum(approx_distinct_devices_count, na.rm = T),
            msa_count = sum(msa_count, na.rm = T)) %>%
  ungroup() %>%
  data.frame() %>%
  mutate(normalized = district_devices/msa_count)

head(for_imputation)
range(for_imputation$date_range_start)

write.csv(for_imputation,
          '/Users/jpg23/data/downtownrecovery/commercial_districts/byeonghwa_commercial_for_imputation.csv',
          row.names = F)

# Plot data
#-----------------------------------------

comm_plot <-
  plot_ly() %>%
  add_lines(data = for_imputation %>% filter(provider_id == '190199'),
            x = ~date_range_start, y = ~normalized,
            name = ~paste0(comm_district, ":  provider 190199"),
            opacity = .7,
            split = ~comm_district,
            text = ~paste0(comm_district, ': ', round(normalized, 3)),
            line = list(shape = "linear", color = '#d6ad09')) %>%
  add_lines(data = for_imputation %>% filter(provider_id == '700199'),
            x = ~date_range_start, y = ~normalized,
            name = ~paste0(comm_district, ": provider 700199"),
            opacity = .7,
            split = ~comm_district,
            text = ~paste0(comm_district, ': ', round(normalized, 3)),
            line = list(shape = "linear", color = '#8c0a03')) %>%
  layout(title = "Weekly counts in Byeonghwa's commercial districts normalized by MSA (not yet imputed for Toronto)",
         xaxis = list(title = "Week", zerolinecolor = "#ffff",
                      tickformat = "%b %Y"),
         yaxis = list(title = "Normalized", zerolinecolor = "#ffff",
                      ticksuffix = "  "),
         shapes = list(list(y0 = 0, y1 = 1, yref = "paper",
                            x0 = as.Date('2021-05-17'), x1 = as.Date('2021-05-17'),
                            line = list(color = 'black', dash = 'dash'))))

comm_plot

# Calculate US cities' recovery rates
#-----------------------------------------

rq_us <-
  for_imputation %>%
  filter(!str_detect(comm_district, 'Toronto: ') & provider_id == '190199') %>%
  filter((date_range_start >= as.Date('2019-03-04') &
            date_range_start <= as.Date('2019-06-10')) |
           (date_range_start >= as.Date('2023-02-27'))) %>%
  mutate(year = year(date_range_start)) %>%
  group_by(comm_district, year) %>%
  summarize(comm = sum(district_devices, na.rm = T),
            msa = sum(msa_count, na.rm = T)) %>%
  ungroup() %>%
  mutate(norm = comm/msa) %>%
  pivot_wider(
    id_cols = c('comm_district'),
    names_from = 'year',
    names_prefix = 'ntv',
    values_from = 'norm'
  ) %>%
  mutate(rq = ntv2023/ntv2019) %>%
  data.frame() %>%
  select(comm_district, rq) %>%
  arrange(desc(rq))

head(rq_us)

# Calculate Toronto's recovery rates
#-----------------------------------------

# LOAD BYEONGHWA'S IMPUTED DATA!
# imputed_canada <- read.csv('')

rq_t <-
  imputed_canada %>%
  filter(str_detect(comm_district, 'Toronto: ') & provider_id == '190199') %>%
  filter((date_range_start >= as.Date('2019-03-04') & 
            date_range_start <= as.Date('2019-06-10')) | 
           (date_range_start >= as.Date('2023-02-27'))) %>%
  mutate(week_num = isoweek(date_range_start),
         year = year(date_range_start)) %>%
  select(-date_range_start) %>%
  pivot_wider(
    id_cols = c('comm_district', 'week_num'),
    names_from = 'year',
    names_prefix = 'ntv',
    values_from = 'normalized'
  ) %>%
  mutate(rec2023 = ntv2023/ntv2019) %>%
  data.frame() %>%
  group_by(comm_district) %>%
  summarize(rq = mean(rec2023, na.rm = T)) %>%
  ungroup() %>%
  data.frame() %>%
  arrange(desc(rq))

rq_t

# Combine Canada & US RQs
#-----------------------------------------

head(rq_us)
head(rq_t)

rq <- rbind(rq_us, rq_t) %>%
  arrange(desc(rq))

head(rq)

write.csv(rq,
          '/Users/jpg23/UDP/downtown_recovery/commercial_districts/byeonghwa_commercial_districts_RQs.csv',
          row.names = F)

# Map recovery rates by district
#-----------------------------------------

comm_sf <- st_read("/Users/jpg23/UDP/downtown_recovery/commercial_districts/byeonghwa_commercial_5cities.geojson")
head(comm_sf)

comm_sf1 <- comm_sf %>%
  mutate(comm_district = paste0(city, ': ', district)) %>%
  select(comm_district)

head(comm_sf1)

# Join spatial data & recovery rates
rq_sf <- comm_sf1 %>%
  left_join(rq)

head(rq_sf)

summary(rq_sf$rq)
getJenksBreaks(rq_sf$rq, 7)
hist(rq_sf$rq)

# ADJUST THE BINS TO CORRESPOND WITH THE BREAKS SHOWN ABOVE!

comm_sf2 <- rq_sf %>%
  mutate(
    rate_cat = factor(case_when(
      rq < .75 ~ '40 - 74%',
      rq < 1 ~ '75 - 99%',
      rq < 1.5 ~ '100 - 149%',
      TRUE ~ '150%+'
    ),
    levels = c('40 - 74%', '75 - 99%', '100 - 149%', '150%+')),
    label = paste0(round(rq * 100), "%"))

head(comm_sf2)
table(comm_sf2$rate_cat)

pal <- c(
  "#e41822",
  "#faa09d",
  "#96cdf2",
  "#0e7bc4"
)

leaflet_pal <- colorFactor(
  pal,
  domain = comm_sf2$rate_cat,
  na.color = 'transparent'
)

# Split dataset up by city
nyc <- comm_sf2 %>% filter(str_detect(comm_district, 'New York: '))
sf <- comm_sf2 %>% filter(str_detect(comm_district, 'San Francisco: '))
portland <- comm_sf2 %>% filter(str_detect(comm_district, 'Portland: '))
chicago <- comm_sf2 %>% filter(str_detect(comm_district, 'Chicago: '))
toronto <- comm_sf2 %>% filter(str_detect(comm_district, 'Toronto: '))

# New York
nyc_map <-
  leaflet(
    options = leafletOptions(minZoom = 9, maxZoom = 18, zoomControl = FALSE)
  ) %>%
  addProviderTiles("CartoDB.PositronNoLabels") %>%
  addPolygons(
    data = nyc,
    label = ~label,
    labelOptions = labelOptions(textsize = "12px"),
    fillOpacity = .8,
    color = ~leaflet_pal(rate_cat),
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
    pal = leaflet_pal,
    values = ~rate_cat,
    title = 'Recovery rate<br>(March - mid-June,<br>2023 vs 2019)'
  )

nyc_map

# San Francisco
sf_map <-
  leaflet(
    options = leafletOptions(minZoom = 9, maxZoom = 18, zoomControl = FALSE)
  ) %>%
  addProviderTiles("CartoDB.PositronNoLabels") %>%
  addPolygons(
    data = sf,
    label = ~label,
    labelOptions = labelOptions(textsize = "12px"),
    fillOpacity = .8,
    color = ~leaflet_pal(rate_cat),
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
    pal = leaflet_pal,
    values = ~rate_cat,
    title = 'Recovery rate<br>(March - mid-June,<br>2023 vs 2019)'
  )

sf_map

# Portland
portland_map <-
  leaflet(
    options = leafletOptions(minZoom = 9, maxZoom = 18, zoomControl = FALSE)
  ) %>%
  addProviderTiles("CartoDB.PositronNoLabels") %>%
  addPolygons(
    data = portland,
    label = ~label,
    labelOptions = labelOptions(textsize = "12px"),
    fillOpacity = .8,
    color = ~leaflet_pal(rate_cat),
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
    pal = leaflet_pal,
    values = ~rate_cat,
    title = 'Recovery rate<br>(March - mid-June,<br>2023 vs 2019)'
  )

portland_map

# Chicago
chicago_map <-
  leaflet(
    options = leafletOptions(minZoom = 9, maxZoom = 18, zoomControl = FALSE)
  ) %>%
  addProviderTiles("CartoDB.PositronNoLabels") %>%
  addPolygons(
    data = chicago,
    label = ~label,
    labelOptions = labelOptions(textsize = "12px"),
    fillOpacity = .8,
    color = ~leaflet_pal(rate_cat),
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
    pal = leaflet_pal,
    values = ~rate_cat,
    title = 'Recovery rate<br>(March - mid-June,<br>2023 vs 2019)'
  )

chicago_map

# Toronto
toronto_map <-
  leaflet(
    options = leafletOptions(minZoom = 9, maxZoom = 18, zoomControl = FALSE)
  ) %>%
  addProviderTiles("CartoDB.PositronNoLabels") %>%
  addPolygons(
    data = toronto,
    label = ~label,
    labelOptions = labelOptions(textsize = "12px"),
    fillOpacity = .8,
    color = ~leaflet_pal(rate_cat),
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
    pal = leaflet_pal,
    values = ~rate_cat,
    title = 'Recovery rate<br>(March - mid-June,<br>2023 vs 2019)'
  )

toronto_map

# Save maps
map_path <- '/Users/jpg23/UDP/downtown_recovery/commercial_districts/byeonghwa_commercial/'

saveWidget(nyc_map, paste0(map_path, 'nyc_byeonghwa_commercial_RQs.html'))
saveWidget(sf_map, paste0(map_path, 'sf_byeonghwa_commercial_RQs.html'))
saveWidget(portland_map, paste0(map_path, 'portland_byeonghwa_commercial_RQs.html'))
saveWidget(chicago_map, paste0(map_path, 'chicago_byeonghwa_commercial_RQs.html'))
saveWidget(toronto_map, paste0(map_path, 'toronto_byeonghwa_commercial_RQs.html'))
