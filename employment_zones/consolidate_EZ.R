################################################################################
# Consolidate employment zone & userbase counts files exported from Spectus,
# for Laura's work on recovery rates in employment zones.
# 
# Author: Julia Greenberg
# Date: 6/28/23
################################################################################

#-----------------------------------------
# Load packages
#-----------------------------------------

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'lubridate', 'ggplot2', 'plotly', 'cancensus', 'ggmap'))

#-----------------------------------------
# Load data
#-----------------------------------------

# Userbase: 1/1/2019 - 4/25/2023

filepath_bia <- 'C:/Users/jpg23/data/downtownrecovery/spectus_exports/BIAs/'

userbase1 <- read_delim(
  paste0(filepath_bia, "bia_userbase/20230522_190855_00007_3yd5w_80fe0af5-8ae8-468e-ad40-4d1714548545.gz"),
  delim = '\001',
  col_names = c('bia', 'provider', 'n_devices', 'userbase', 'date'),
  col_types = c('cciii')
) %>%
  mutate(date = as.Date(as.character(date), format = "%Y%m%d")) %>%
  data.frame() %>%
  arrange(date) %>%
  select(-c(bia, n_devices)) %>%
  distinct()

filepath <- 'C:/Users/jpg23/data/downtownrecovery/spectus_exports/employment_zones/'

# Userbase: 4/10/23 - 6/18/23
userbase2 <-
  list.files(path = paste0(filepath, 'userbase')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'userbase/', .),
    delim = '\001',
    col_names = c('geography_name', 'userbase', 'event_date'),
    col_types = c('cii')
  )) %>%
  data.frame() %>%
  filter(geography_name == 'Ontario') %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d"),
         provider = '190199') %>%
  arrange(date) %>%
  select(-c(event_date, geography_name))

# Region-wide: 1/1/19 - 7/7/21
region1 <-
  list.files(path = paste0(filepath, 'region_20190101_20210707')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'region_20190101_20210707/', .),
    delim = '\001',
    col_names = c('ez', 'provider_id', 'approx_distinct_devices_count', 
                  'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date)

# Region-wide: 7/7/21 - 6/27/23
region2 <-
  list.files(path = paste0(filepath, 'region_20210707_20230627')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'region_20210707_20230627/', .),
    delim = '\001',
    col_names = c('ez', 'provider_id', 'approx_distinct_devices_count', 
                  'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date)

# City-wide: 1/1/19 - 8/12/22
city1 <-
  list.files(path = paste0(filepath, 'city_20190101_20220812')) %>%
  map_df(~read_delim(
    paste0(filepath, 'city_20190101_20220812/', .),
    delim = '\001',
    col_names = c('small_area', 'big_area', 'provider_id', 
                  'approx_distinct_devices_count', 'event_date'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date)

# City-wide: 8/12/22 - 6/29/23
city2 <-
  list.files(path = paste0(filepath, 'city_20220812_20230629')) %>%
  map_df(~read_delim(
    paste0(filepath, 'city_20220812_20230629/', .),
    delim = '\001',
    col_names = c('small_area', 'big_area', 'provider_id', 
                  'approx_distinct_devices_count', 'event_date'),
    col_types = c('cccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date)

#-----------------------------------------
# Combine userbase data
#-----------------------------------------

head(userbase1)
head(userbase2)

range(userbase1$date)
range(userbase2$date)

userbase <- 
  userbase1 %>% 
  filter(date < as.Date('2023-04-10')) %>%
  rbind(userbase2) %>%
  filter(date < as.Date('2023-06-01') & # through end of May
           # change providers at 5/17/21
           ((provider == '700199' & date < as.Date('2021-05-17')) | 
           (provider == '190199' & date >= as.Date('2021-05-17')))) %>%
  select(-provider)

head(userbase)
range(userbase$date)

#-----------------------------------------
# Combine region-wide data
#-----------------------------------------

head(region1)
head(region2)

range(region1$date)
range(region2$date)

region <-
  region1 %>%
  filter(date < as.Date('2021-07-07')) %>%
  rbind(region2) %>%
  filter(date < as.Date('2023-06-01') & # through end of May
           # change providers at 5/17/21
           ((provider_id == '700199' & date < as.Date('2021-05-17')) | 
              (provider_id == '190199' & date >= as.Date('2021-05-17')))) %>%
  mutate(ez = as.character(round(as.integer(ez), 0))) %>%
  select(-provider_id) %>%
  rename(n_devices = approx_distinct_devices_count) %>%
  left_join(userbase) # add userbase

head(region)
range(region$date)

#-----------------------------------------
# Combine city-wide data
#-----------------------------------------

head(city1)
head(city2)

range(city1$date)
range(city2$date)

city0 <- 
  city1 %>% 
  filter(date < as.Date('2022-08-12')) %>%
  rbind(city2) %>%
  filter(date < as.Date('2023-06-01') & # through end of May
           # change providers at 5/17/21
           (provider_id == '700199' & date < as.Date('2021-05-17')) | 
           (provider_id == '190199' & date >= as.Date('2021-05-17'))) %>%
  select(-provider_id) %>%
  mutate(
    ez = big_area,
    ez = recode(ez, 
    '31'='South of Eastern', 
    '32'='Rexdale-Airport',
    '33'='Junction-Weston-Dupont North',
    '34'='Junction-Weston-Dupont South',
    '35'='Junction-Weston-Dupont East',
    '36'='Tapscott North',
    '37'='Tapscott East',
    '38'='Tapscott South',
    '39'='Liberty Village',
    '40'='South Etobicoke - North',
    '41'='South Etobicoke - East',
    '42'='South Etobicoke - South',
    '43'='Northwest Etobicoke',
    '22'='Port Lands - Central Waterfront',
    '23'='Eastern-Carlaw-DVP-Greenwood',
    '24'='Leaside-Thorncliffe',
    '25'='Bermondsey-Railside - North',
    '9'='Bermondsey-Railside - South',
    '26'='Scarborough-Highway 401 - East',
    '27'='Scarborough-Highway 401 - Central',
    '28'='Scarborough-Highway 401 - West',
    '5'='Milliken',
    '0'='Coronation Drive',
    '1'='Golden Mile / South-Central Scarborough - North East',
    '29'='Golden Mile / South-Central Scarborough - North',
    '30'='Golden Mile / South-Central Scarborough - South',
    '2'='Scarborough Junction - North',
    '3'='Scarborough Junction - Central',
    '4'='Scarborough Junction - South-West',
    '18'='Highway 400 - North',
    '19'='Highway 400 - North-Central',
    '20'='Highway 400 - South-Central',
    '21'='Highway 400 - South',
    '10'='Downsview - North',
    '11'='Downsview - Central',
    '12'='Downsview - South',
    '13'='Caledonia - South Downsview - North',
    '14'='Caledonia - South Downsview - Central',
    '15'='Caledonia - South Downsview - South',
    '6'='Victoria Park - Steeles (eastern part)',
    '7'='Victoria Park - Steeles (western part)',
    '8'='Consumers Road',
    '16'='Duncan Mill',
    '17'='Don Mills'
  )) 

city <- city0 %>%
  # Aggregate city data to 'big_area'
  dplyr::group_by(ez, date) %>%
  dplyr::summarize(n_devices = sum(approx_distinct_devices_count, na.rm = T)) %>%
  data.frame() %>%
  left_join(userbase) # add userbase

head(city)
range(city$date)

#-----------------------------------------
# Look at trends for all 3 datasets
#-----------------------------------------

all_plotly <- 
  plot_ly() %>%
  add_lines(data = userbase, x = ~date, y = ~userbase, 
            name = "Userbase",
            opacity = .9,
            line = list(shape = "linear", color = '#d6ad09')) %>%
  add_lines(data = city, x = ~date, y = ~n_devices,
            name = "City",
            opacity = .3,
            line = list(shape = "linear", color = '#8c0a03')) %>%
  add_lines(data = region, x = ~date, y = ~n_devices,
            name = "Region", 
            opacity = .3,
            line = list(shape = "linear", color = '#6bc0c2')) %>%
  layout(title = "Trends by area",
         xaxis = list(title = "Date", zerolinecolor = "#ffff", 
                      tickformat = "%b %Y"),
         yaxis = list(title = "Devices", zerolinecolor = "#ffff",
                      ticksuffix = "  "))

all_plotly

#-----------------------------------------
# Add year & week_num to city data
#-----------------------------------------

rec_rate_city <-
  city %>%
  # Determine week and year # for each date
  mutate(
    date_range_start = floor_date(
      date,
      unit = "week",
      week_start = getOption("lubridate.week.start", 1)),
    week_num = isoweek(date_range_start),
    year = year(date_range_start)) %>%
  # Calculate # of devices by big_area, week and year
  dplyr::group_by(ez, year, week_num) %>%
  dplyr::summarize(n_devices = sum(n_devices, na.rm = T),
            userbase = sum(userbase, na.rm = T)) %>%
  dplyr::ungroup() 

head(rec_rate_city)

all_city_for_plot <-
  rec_rate_city %>%
  filter(year > 2018) %>%
  dplyr::group_by(year, week_num) %>%
  dplyr::summarize(n_devices = sum(n_devices, na.rm = T),
            userbase = sum(userbase, na.rm = T)) %>%
  dplyr::ungroup() %>%
  mutate(normalized = n_devices/userbase) %>%
  pivot_wider(
    id_cols = c('week_num'),
    names_from = 'year',
    names_prefix = 'ntv',
    values_from = 'normalized') %>%
  mutate(rec2020 = ntv2020/ntv2019,
         rec2021 = ntv2021/ntv2019,
         rec2022 = ntv2022/ntv2019,
         rec2023 = ntv2023/ntv2019) %>%
  select(-starts_with('ntv')) %>%
  pivot_longer(
    cols = rec2020:rec2023,
    names_to = 'year',
    values_to = 'rq') %>%
  filter(week_num < 53) %>%
  mutate(year = substr(year, 4, 7),
         week = as.Date(paste(year, week_num, 1, sep = '_'),
                        format = '%Y_%W_%w')) %>% # Monday of week
  filter(!(year == 2023 & week_num > 22)) %>%
  arrange(year, week_num) %>%
  mutate(rq_rolling = zoo::rollmean(rq, k = 11, fill = NA, align = 'right')) %>%
  ungroup() %>%
  data.frame() %>%
  filter(!(year == 2020 & week_num < 12))

head(all_city_for_plot)
tail(all_city_for_plot)

all_city_plot <-
  all_city_for_plot %>%
  ggplot(aes(x = week, y = rq_rolling)) +
  geom_line(size = .8) +
  ggtitle('Recovery rate for all Employment Zones in City of Toronto (11 week rolling average)') +
  scale_x_date(date_breaks = "4 month", date_labels = "%b %Y") +
  scale_y_continuous(labels = scales::percent_format(accuracy = 1),
                     limits = c(0.5, 1.3),
                     breaks = seq(.5, 1.3, .2)) +
  xlab('Month') +
  ylab('Recovery rate') +
  theme(
    # axis.text.x = element_text(angle = 90),
    panel.grid.major = element_line(color = 'light gray',
                                    linewidth = .5,
                                    linetype = 1),
    panel.grid.minor.x = element_blank(),
    panel.background = element_blank(),
    plot.title = element_text(hjust = .5),
    axis.ticks = element_blank(),
    axis.title.y = element_text(margin = margin(r = 15)),
    axis.title.x = element_text(margin = margin(t = 15))
  )

all_city_plot

# Now by employment zone
each_city_for_plot <-
  rec_rate_city %>%
  filter(year > 2018) %>%
  dplyr::group_by(year, week_num, ez) %>%
  dplyr::summarize(n_devices = sum(n_devices, na.rm = T),
            userbase = sum(userbase, na.rm = T)) %>%
  dplyr::ungroup() %>%
  mutate(normalized = n_devices/userbase) %>%
  pivot_wider(
    id_cols = c('week_num', 'ez'),
    names_from = 'year',
    names_prefix = 'ntv',
    values_from = 'normalized') %>%
  mutate(rec2020 = ntv2020/ntv2019,
         rec2021 = ntv2021/ntv2019,
         rec2022 = ntv2022/ntv2019,
         rec2023 = ntv2023/ntv2019) %>%
  select(-starts_with('ntv')) %>%
  pivot_longer(
    cols = rec2020:rec2023,
    names_to = 'year',
    values_to = 'rq') %>%
  filter(week_num < 53) %>%
  mutate(year = substr(year, 4, 7),
         week = as.Date(paste(year, week_num, 1, sep = '_'),
                        format = '%Y_%W_%w')) %>% # Monday of week
  filter(!(year == 2023 & week_num > 22)) %>%
  arrange(ez, year, week_num) %>%
  dplyr::group_by(ez) %>%
  mutate(rq_rolling = zoo::rollmean(rq, k = 11, fill = NA, align = 'right')) %>%
  dplyr::ungroup() %>%
  data.frame() %>%
  filter(!(year == 2020 & week_num < 12)) %>%
  filter(!is.na(ez)) %>%
  mutate(
    mytext = paste0(ez, '<br>Week of ', week, ': ',
                    scales::percent(rq_rolling, accuracy = 2)))

head(each_city_for_plot)

# Plotly
each_city_plotly <-
  plot_ly() %>%
  add_lines(data = each_city_for_plot,
            x = ~week, y = ~rq_rolling,
            split = ~ez,
            name = ~ez,
            text = ~mytext,
            hoverinfo = 'text',
            opacity = .3,
            line = list(shape = "linear", color = '#8c0a03')) %>%
  layout(title = "Recovery rate for all Employment Zones in City of Toronto (11 week rolling average)",
         xaxis = list(title = "Week", zerolinecolor = "#ffff",
                      tickformat = "%b %Y"),
         yaxis = list(title = "Recovery rate", zerolinecolor = "#ffff",
                      tickformat = ".0%", ticksuffix = "  "))

each_city_plotly

#-----------------------------------------
# Add year and week_num to region data
#-----------------------------------------

rec_rate_region <-
  region %>%
  # Determine week and year # for each date
  mutate(
    date_range_start = floor_date(
      date,
      unit = "week",
      week_start = getOption("lubridate.week.start", 1)),
    week_num = isoweek(date_range_start),
    year = year(date_range_start)) %>%
  # Calculate # of devices by big_area, week and year
  dplyr::group_by(ez, year, week_num) %>%
  dplyr::summarize(n_devices = sum(n_devices, na.rm = T),
                   userbase = sum(userbase, na.rm = T)) %>%
  dplyr::ungroup() 

head(rec_rate_region)

#-----------------------------------------
# Add spatial data
#-----------------------------------------

# Load city shapefile
city_sf <- st_read("C:/Users/jpg23/data/downtownrecovery/shapefiles/employment_lands/final_city_sf_dissolved.geojson") %>%
  left_join(city0 %>% select(big_area, ez) %>% distinct(), by = c('big_area')) %>%
  select(-big_area)

# city_sf <- st_read('C:/Users/jpg23/data/downtownrecovery/shapefiles/employment_lands/big_city_areas.shp') %>%
#   left_join(city0 %>% select(big_area, ez) %>% distinct(), by = c('big_area')) %>%
#   select(-big_area)

head(city_sf)

# Load region shapefile
region_sf <- st_read('C:/Users/jpg23/data/downtownrecovery/shapefiles/employment_lands/Provincially_Significant_Employment_Zones.shp') %>%
  select(ez = PSEZ_ID, municipality = MUNICIPALI, geometry) %>%
  mutate(
    label = paste0(ez, ': ', municipality),
    in_tor = case_when(
      str_detect(municipality, 'TORONTO') ~ TRUE,
      TRUE ~ FALSE
  )) %>%
  st_transform(st_crs(city_sf))

head(region_sf)
table(region_sf$in_tor)

# Load Toronto city boundary
toronto <- get_census(
  dataset='CA21', regions=list(CMA="35535"),
  level='CSD', quiet = TRUE, 
  geo_format = 'sf', labels = 'short') %>%
  filter(str_detect(name, 'Toronto')) %>%
  select(geometry)

st_write(
  toronto,
  'C:/Users/jpg23/data/downtownrecovery/shapefiles/employment_lands/toronto.geojson')

pal <- c("turquoise", "purple")

region_pal <- colorFactor(
  pal,
  domain = region_sf$in_tor,
  na.color = 'transparent'
)

interactive_region <-
  leaflet(
    options = leafletOptions(minZoom = 7, maxZoom = 18, zoomControl = FALSE)
  ) %>%
  # setView(lat = 28.72, lng = -81.97, zoom = 7) %>%
  addMapPane(name = "munic", zIndex = 410) %>%
  addMapPane(name = "polygons", zIndex = 420) %>%
  addProviderTiles("CartoDB.PositronNoLabels") %>%
  addProviderTiles("Stamen.TonerLines",
                   options = providerTileOptions(opacity = 0.3),
                   group = "Roads"
  ) %>%
  addPolygons(
    data = region_sf,
    label = ~label,
    labelOptions = labelOptions(textsize = "12px"),
    fillOpacity = .6,
    stroke = TRUE,
    color = ~region_pal(in_tor),
    weight = 2,
    opacity = 1,
    highlightOptions =
      highlightOptions(
        color = "black",
        weight = 3,
        bringToFront = TRUE),
    options = pathOptions(pane = "polygons")
  ) %>%
  addPolygons(
    data = city_sf,
    label = ~ez,
    labelOptions = labelOptions(textsize = "12px"),
    fillOpacity = .6,
    stroke = TRUE,
    weight = 2,
    opacity = 1,
    color = "orange",
    highlightOptions =
      highlightOptions(
        color = "black",
        weight = 3,
        bringToFront = TRUE),
    options = pathOptions(pane = "polygons")
  ) %>%
  addPolygons(
    data = toronto,
    fillOpacity = 0,
    stroke = TRUE,
    weight = 1,
    opacity = 1,
    color = "black",
    highlightOptions =
      highlightOptions(
        color = "black",
        weight = 3,
        bringToFront = TRUE),
    options = pathOptions(pane = "munic")
  )

interactive_region

#-----------------------------------------
# Crop city & region shapefiles
#-----------------------------------------

# # Crop city shapefiles to within boundary
# city_crop <- st_intersection(city_sf %>% st_make_valid(), 
#                              toronto %>% st_make_valid())
#
# # Crop region shapefiles to outside city shapefiles
# region_crop <- st_difference(region_sf %>% st_make_valid(),
#                              city_sf %>% st_make_valid())
# 
# interactive_crop <-
#   leaflet(
#     options = leafletOptions(minZoom = 7, maxZoom = 18, zoomControl = FALSE)
#   ) %>%
#   # setView(lat = 28.72, lng = -81.97, zoom = 7) %>%
#   addMapPane(name = "munic", zIndex = 410) %>%
#   addMapPane(name = "polygons", zIndex = 420) %>%
#   addProviderTiles("CartoDB.PositronNoLabels") %>%
#   addProviderTiles("Stamen.TonerLines",
#                    options = providerTileOptions(opacity = 0.3),
#                    group = "Roads"
#   ) %>%
#   addPolygons(
#     data = region_crop,
#     label = ~label,
#     labelOptions = labelOptions(textsize = "12px"),
#     fillOpacity = .6,
#     stroke = TRUE,
#     color = ~region_pal(in_tor),
#     weight = 2,
#     opacity = 1,
#     highlightOptions =
#       highlightOptions(
#         color = "black",
#         weight = 3,
#         bringToFront = TRUE),
#     options = pathOptions(pane = "polygons")
#   ) %>%
#   addPolygons(
#     data = city_sf,
#     label = ~ez,
#     labelOptions = labelOptions(textsize = "12px"),
#     fillOpacity = .6,
#     stroke = TRUE,
#     weight = 2,
#     opacity = 1,
#     color = "orange",
#     highlightOptions =
#       highlightOptions(
#         color = "black",
#         weight = 3,
#         bringToFront = TRUE),
#     options = pathOptions(pane = "polygons")
#   ) %>%
#   addPolygons(
#     data = toronto,
#     fillOpacity = 0,
#     stroke = TRUE,
#     weight = 1,
#     opacity = 1,
#     color = "black",
#     highlightOptions =
#       highlightOptions(
#         color = "black",
#         weight = 3,
#         bringToFront = TRUE),
#     options = pathOptions(pane = "munic")
#   )
# 
# interactive_crop

### NOTE: I CAN'T CROP A SHAPEFILE AND THEN SAY THERE WERE X DEVICES ASSOCIATED
### WITH THE CROPPED SHAPEFILE. I WOULD HAVE TO RE-QUERY THE DATA FOR THAT
### EXACT GEOGRAPHY. NEED TO FIGURE OUT A WAY TO DISPLAY ALL THE DATA TOGETHER.

#-----------------------------------------
# Map city EZ recovery rates (static): 
# 2023 vs 2019
#-----------------------------------------

head(rec_rate_city)

for_maps0 <-
  rec_rate_city %>%
  mutate(week = as.Date(
    paste(as.character(year), as.character(week_num), 1, sep = '_'),
    format = '%Y_%W_%w'))

# Make sure I'm comparing the same number of weeks:
only_23_19 <- 
  for_maps0 %>%
  filter((year == 2023 & week >= as.Date('2023-01-01') & 
            week <= as.Date('2023-05-28'))) # moved this earlier to have same # of weeks

only_19_23 <- 
  for_maps0 %>%
  filter((year == 2019 & week >= as.Date('2019-01-01') & 
            week <= as.Date('2019-05-31')))

n_distinct(only_23_19$week_num)
n_distinct(only_19_23$week_num) # yes :)

for_maps_23_19 <-
  for_maps0 %>%
  filter((year == 2023 & week >= as.Date('2023-01-01') & 
            week <= as.Date('2023-05-28')) |
           (year == 2019 & week >= as.Date('2019-01-01') & 
              week <= as.Date('2019-05-31'))) %>%
  dplyr::group_by(ez, year) %>%
  dplyr::summarize(n_devices = sum(n_devices, na.rm = T),
            userbase = sum(userbase, na.rm = T)) %>%
  mutate(normalized = n_devices/userbase) %>%
  select(-c(n_devices, userbase)) %>%
  pivot_wider(
    names_from = year,
    names_prefix = 'normalized_',
    values_from = normalized
  ) %>%
  mutate(rate = normalized_2023/normalized_2019) %>%
  filter(!is.na(ez)) %>%
  data.frame()

head(for_maps_23_19)

summary(for_maps_23_19$rate)

# Join spatial data with device count data.

nrow(city_sf)
nrow(for_maps_23_19)

n_distinct(for_maps_23_19$ez)
n_distinct(city_sf$ez)

summary(for_maps_23_19$rate)
getJenksBreaks(for_maps_23_19$rate, 7)

ez_final_23_19 <- 
  left_join(city_sf, for_maps_23_19) %>%
  mutate(
    rate_cat = factor(case_when(
      rate < .7 ~ '50 - 69%',
      rate < 1 ~ '70 - 99%',
      rate < 1.2 ~ '100 - 119%',
      rate < 1.4 ~ '120 - 139%',
      rate < 1.6 ~ '140 - 159%',
      TRUE ~ '160 - 192%'
    ),
    levels = c('50 - 69%', '70 - 99%', '100 - 119%', '120 - 139%', '140 - 159%',
               '160 - 192%')))

nrow(ez_final_23_19)
head(ez_final_23_19)

pal <- c(
  "#e41822",
  "#faa09d",
  "#5bc4fb",
  "#2c92d7",
  "#0362b0",
  "#033384"
)

basemap <-
  get_stamenmap(
    bbox = c(left = -79.65,
             bottom = 43.57,
             right = -79.12,
             top = 43.85),
    zoom = 11,
    maptype = "terrain-lines") # https://r-graph-gallery.com/324-map-background-with-the-ggmap-library.html

basemap_attributes <- attributes(basemap)

basemap_transparent <- matrix(adjustcolor(basemap, alpha.f = 0.2),
                              nrow = nrow(basemap))

attributes(basemap_transparent) <- basemap_attributes

ez_map_23_19 <-
  ggmap(basemap_transparent) +
  geom_sf(data = ez_final_23_19, 
          aes(fill = rate_cat), 
          inherit.aes = FALSE,
          # alpha = .9, 
          color = NA) +
  ggtitle('Recovery rate for all Employment Zones in City of Toronto,\nJanuary - May (2023 versus 2019)') +
  scale_fill_manual(values = pal, name = 'Recovery rate') +
  guides(fill = guide_legend(barwidth = 0.5, barheight = 10, 
                             ticks = F, byrow = T)) +
  theme(
    panel.border = element_blank(), 
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank(), 
    panel.background = element_blank(),
    axis.line = element_blank(),
    axis.text = element_blank(),
    axis.title = element_blank(),
    axis.ticks = element_blank(),
    legend.spacing.y = unit(.1, 'cm'),
    plot.title = element_text(hjust = 0.5, margin = margin(b = 20)),
    plot.caption = element_text(
      margin = margin(t = 20)),
    legend.title = element_text(
      margin = margin(b = 10)))

ez_map_23_19

#-----------------------------------------
# Map city EZ recovery rates (interactive): 
# 2023 vs 2019
#-----------------------------------------

ez_label_23_19 <-
  ez_final_23_19 %>%
  mutate(label = paste0(ez, ": ", round(rate * 100), "%"))

leaflet_pal <- colorFactor(
  pal,
  domain = ez_label_23_19$rate_cat,
  na.color = 'transparent'
)

interactive_23_19 <-
  leaflet(
    options = leafletOptions(minZoom = 9, maxZoom = 18, zoomControl = FALSE)
  ) %>%
  # setView(lat = 28.72, lng = -81.97, zoom = 7) %>%
  addMapPane(name = "polygons", zIndex = 410) %>%
  addMapPane(name = "polylines", zIndex = 420) %>%
  addMapPane(name = "Layers", zIndex = 430) %>%
  addProviderTiles("CartoDB.PositronNoLabels") %>%
  addProviderTiles("Stamen.TonerLines",
                   options = providerTileOptions(opacity = 0.3),
                   group = "Roads"
  ) %>%
  addPolygons(
    data = ez_label_23_19,
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
        bringToFront = TRUE),
    options = pathOptions(pane = "polygons")
  ) %>%
  leaflet::addLegend(
    data = ez_label_23_19,
    position = "bottomleft",
    pal = leaflet_pal,
    values = ~rate_cat,
    title = 'Recovery rate<br>(January-May,<br>2023 vs 2019)'
  )

interactive_23_19

#-----------------------------------------
# Map city EZ recovery rates (static): 
# 2023 vs 2021
#-----------------------------------------

# Make sure I'm comparing the same number of weeks:
only_23_21 <- 
  for_maps0 %>%
  filter((year == 2023 & week >= as.Date('2023-01-01') & 
            week <= as.Date('2023-05-31')))

only_21_23 <- 
  for_maps0 %>%
  filter((year == 2021 & week >= as.Date('2021-01-01') & 
            week <= as.Date('2021-05-31')))

n_distinct(only_23_21$week_num)
n_distinct(only_21_23$week_num) # yes :)

for_maps_23_21 <-
  for_maps0 %>%
  filter((year == 2023 & week >= as.Date('2023-01-01') & 
            week <= as.Date('2023-05-31')) |
           (year == 2021 & week >= as.Date('2021-01-01') & 
              week <= as.Date('2021-05-31'))) %>%
  dplyr::group_by(ez, year) %>%
  dplyr::summarize(n_devices = sum(n_devices, na.rm = T),
                   userbase = sum(userbase, na.rm = T)) %>%
  mutate(normalized = n_devices/userbase) %>%
  select(-c(n_devices, userbase)) %>%
  pivot_wider(
    names_from = year,
    names_prefix = 'normalized_',
    values_from = normalized
  ) %>%
  mutate(rate = normalized_2023/normalized_2021) %>%
  filter(!is.na(ez)) %>%
  data.frame()

head(for_maps_23_21)

summary(for_maps_23_21$rate)

# Join spatial data with device count data.

nrow(city_sf)
nrow(for_maps_23_21)

n_distinct(for_maps_23_21$ez)
n_distinct(city_sf$ez)

summary(for_maps_23_21$rate)
getJenksBreaks(for_maps_23_21$rate, 7)

ez_final_23_21 <-
  left_join(city_sf, for_maps) %>%
  mutate(
    rate_cat = factor(case_when(
      rate < .8 ~ '48 - 79%',
      rate < 1 ~ '80 - 99%',
      rate < 1.2 ~ '100 - 119%',
      rate < 1.5 ~ '120 - 149%',
      rate < 1.9 ~ '150 - 189%',
      TRUE ~ '190 - 231%'
    ),
    levels = c('48 - 79%', '80 - 99%', '100 - 119%', '120 - 149%', '150 - 189%',
               '190 - 231%')))

nrow(ez_final_23_21)
head(ez_final_23_21)

ez_map_23_21 <-
  ggmap(basemap_transparent) +
  geom_sf(data = ez_final_23_21, 
          aes(fill = rate_cat), 
          inherit.aes = FALSE,
          # alpha = .9, 
          color = NA) +
  ggtitle('Recovery rate for all Employment Zones in City of Toronto,\nJanuary - May (2023 versus 2021)') +
  scale_fill_manual(values = pal, name = 'Recovery rate') +
  guides(fill = guide_legend(barwidth = 0.5, barheight = 10, 
                             ticks = F, byrow = T)) +
  theme(
    panel.border = element_blank(), 
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank(), 
    panel.background = element_blank(),
    axis.line = element_blank(),
    axis.text = element_blank(),
    axis.title = element_blank(),
    axis.ticks = element_blank(),
    legend.spacing.y = unit(.1, 'cm'),
    plot.title = element_text(hjust = 0.5, margin = margin(b = 20)),
    plot.caption = element_text(
      margin = margin(t = 20)),
    legend.title = element_text(
      margin = margin(b = 10)))

ez_map_23_21

#-----------------------------------------
# Map city EZ recovery rates (interactive): 
# 2023 vs 2021
#-----------------------------------------

ez_label_23_21 <-
  ez_final_23_21 %>%
  mutate(label = paste0(ez, ": ", round(rate * 100), "%"))

leaflet_pal <- colorFactor(
  pal,
  domain = ez_label_23_21$rate_cat,
  na.color = 'transparent'
)

interactive_23_21 <-
  leaflet(
    options = leafletOptions(minZoom = 9, maxZoom = 18, zoomControl = FALSE)
  ) %>%
  # setView(lat = 28.72, lng = -81.97, zoom = 7) %>%
  addMapPane(name = "polygons", zIndex = 410) %>%
  addMapPane(name = "polylines", zIndex = 420) %>%
  addMapPane(name = "Layers", zIndex = 430) %>%
  addProviderTiles("CartoDB.PositronNoLabels") %>%
  addProviderTiles("Stamen.TonerLines",
                   options = providerTileOptions(opacity = 0.3),
                   group = "Roads"
  ) %>%
  addPolygons(
    data = ez_label_23_21,
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
        bringToFront = TRUE),
    options = pathOptions(pane = "polygons")
  ) %>%
  leaflet::addLegend(
    data = ez_label_23_21,
    position = "bottomleft",
    pal = leaflet_pal,
    values = ~rate_cat,
    title = 'Recovery rate<br>(January-May,<br>2023 vs 2021)'
  )

interactive_23_21

#-----------------------------------------
# Map city EZ recovery rates (static): 
# 2021 vs 2019
#-----------------------------------------

# Make sure I'm comparing the same number of weeks:
only_21_19 <- 
  for_maps0 %>%
  filter((year == 2021 & week >= as.Date('2021-01-01') & 
            week <= as.Date('2021-05-29'))) # had to move date back so there are same # of weeks

only_19_21 <- 
  for_maps0 %>%
  filter((year == 2019 & week >= as.Date('2019-01-01') & 
            week <= as.Date('2019-05-31')))

n_distinct(only_21_19$week_num)
n_distinct(only_19_21$week_num) # yes :)

for_maps_21_19 <-
  for_maps0 %>%
  filter((year == 2021 & week >= as.Date('2021-01-01') & 
            week <= as.Date('2021-05-29')) |
           (year == 2019 & week >= as.Date('2019-01-01') & 
              week <= as.Date('2019-05-31'))) %>%
  dplyr::group_by(ez, year) %>%
  dplyr::summarize(n_devices = sum(n_devices, na.rm = T),
                   userbase = sum(userbase, na.rm = T)) %>%
  mutate(normalized = n_devices/userbase) %>%
  select(-c(n_devices, userbase)) %>%
  pivot_wider(
    names_from = year,
    names_prefix = 'normalized_',
    values_from = normalized
  ) %>%
  mutate(rate = normalized_2021/normalized_2019) %>%
  filter(!is.na(ez)) %>%
  data.frame()

head(for_maps_21_19)

summary(for_maps_21_19$rate)

# Join spatial data with device count data.

nrow(city_sf)
nrow(for_maps_21_19)

n_distinct(for_maps_21_19$ez)
n_distinct(city_sf$ez)

summary(for_maps_21_19$rate)
getJenksBreaks(for_maps_21_19$rate, 7)

ez_final_21_19 <-
  left_join(city_sf, for_maps_21_19) %>%
  mutate(
    rate_cat = factor(case_when(
      rate < .4 ~ '27 - 39%',
      rate < .7 ~ '40 - 69%',
      rate < .9 ~ '70 - 89%',
      rate < 1 ~ '90 - 99%',
      rate < 1.2 ~ '100 - 119%',
      TRUE ~ '120 - 180%'
    ),
    levels = c('27 - 39%', '40 - 69%', '70 - 89%', '90 - 99%', '100 - 119%',
               '120 - 180%')))

nrow(ez_final_21_19)
head(ez_final_21_19)

pal_21_19 <- c(
  "#e41822",
  "#f0534d",
  "#f87b75",
  "#faa09d",
  "#5bc4fb",
  "#033384"
)

ez_map_21_19 <-
  ggmap(basemap_transparent) +
  geom_sf(data = ez_final_21_19, 
          aes(fill = rate_cat), 
          inherit.aes = FALSE,
          # alpha = .9, 
          color = NA) +
  ggtitle('Recovery rate for all Employment Zones in City of Toronto,\nJanuary - May (2021 versus 2019)') +
  scale_fill_manual(values = pal_21_19, name = 'Recovery rate') +
  guides(fill = guide_legend(barwidth = 0.5, barheight = 10, 
                             ticks = F, byrow = T)) +
  theme(
    panel.border = element_blank(), 
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank(), 
    panel.background = element_blank(),
    axis.line = element_blank(),
    axis.text = element_blank(),
    axis.title = element_blank(),
    axis.ticks = element_blank(),
    legend.spacing.y = unit(.1, 'cm'),
    plot.title = element_text(hjust = 0.5, margin = margin(b = 20)),
    plot.caption = element_text(
      margin = margin(t = 20)),
    legend.title = element_text(
      margin = margin(b = 10)))

ez_map_21_19

#-----------------------------------------
# Map city EZ recovery rates (interactive): 
# 2021 vs 2019
#-----------------------------------------

ez_label_21_19 <-
  ez_final_21_19 %>%
  mutate(label = paste0(ez, ": ", round(rate * 100), "%"))

leaflet_pal_21_19 <- colorFactor(
  pal_21_19,
  domain = ez_label_21_19$rate_cat,
  na.color = 'transparent'
)

interactive_21_19 <-
  leaflet(
    options = leafletOptions(minZoom = 9, maxZoom = 18, zoomControl = FALSE)
  ) %>%
  # setView(lat = 28.72, lng = -81.97, zoom = 7) %>%
  addMapPane(name = "polygons", zIndex = 410) %>%
  addMapPane(name = "polylines", zIndex = 420) %>%
  addMapPane(name = "Layers", zIndex = 430) %>%
  addProviderTiles("CartoDB.PositronNoLabels") %>%
  addProviderTiles("Stamen.TonerLines",
                   options = providerTileOptions(opacity = 0.3),
                   group = "Roads"
  ) %>%
  addPolygons(
    data = ez_label_21_19,
    label = ~label,
    labelOptions = labelOptions(textsize = "12px"),
    fillOpacity = .8,
    color = ~leaflet_pal_21_19(rate_cat),
    stroke = TRUE,
    weight = 1,
    opacity = 1,
    highlightOptions =
      highlightOptions(
        color = "black",
        weight = 3,
        bringToFront = TRUE),
    options = pathOptions(pane = "polygons")
  ) %>%
  leaflet::addLegend(
    data = ez_label_21_19,
    position = "bottomleft",
    pal = leaflet_pal_21_19,
    values = ~rate_cat,
    title = 'Recovery rate<br>(January-May,<br>2021 vs 2019)'
  )

interactive_21_19
