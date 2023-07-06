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
ipak(c('tidyverse', 'lubridate', 'ggplot2', 'plotly', 'cancensus'))

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
    '6'='Victoria Park - Steeles',
    '7'='Victoria Park - Steeles',
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
  dplyr::group_by(big_area, year, week_num) %>%
  dplyr::summarize(n_devices = sum(n_devices, na.rm = T),
            userbase = sum(userbase, na.rm = T)) %>%
  dplyr::ungroup() 

head(rec_rate_city)

# all_city_for_plot <-
#   rec_rate_city %>%
#   filter(year > 2018) %>%
#   dplyr::group_by(year, week_num) %>%
#   dplyr::summarize(n_devices = sum(n_devices, na.rm = T),
#             userbase = sum(userbase, na.rm = T)) %>%
#   dplyr::ungroup() %>%
#   mutate(normalized = n_devices/userbase) %>%
#   pivot_wider(
#     id_cols = c('week_num'),
#     names_from = 'year',
#     names_prefix = 'ntv',
#     values_from = 'normalized') %>%
#   mutate(rec2020 = ntv2020/ntv2019,
#          rec2021 = ntv2021/ntv2019,
#          rec2022 = ntv2022/ntv2019,
#          rec2023 = ntv2023/ntv2019) %>%
#   select(-starts_with('ntv')) %>%
#   pivot_longer(
#     cols = rec2020:rec2023,
#     names_to = 'year',
#     values_to = 'rq') %>%
#   filter(week_num < 53) %>%
#   mutate(year = substr(year, 4, 7),
#          week = as.Date(paste(year, week_num, 1, sep = '_'),
#                         format = '%Y_%W_%w')) %>% # Monday of week
#   filter(!(year == 2023 & week_num > 22)) %>%
#   arrange(year, week_num) %>%
#   mutate(rq_rolling = zoo::rollmean(rq, k = 11, fill = NA, align = 'right')) %>%
#   ungroup() %>%
#   data.frame() %>%
#   filter(!(year == 2020 & week_num < 12))
# 
# head(all_city_for_plot)
# tail(all_city_for_plot)
# 
# all_city_plot <-
#   all_city_for_plot %>%
#   ggplot(aes(x = week, y = rq_rolling)) +
#   geom_line(size = .8) +
#   ggtitle('Recovery rate for all Employment Zones in City of Toronto (11 week rolling average)') +
#   scale_x_date(date_breaks = "4 month", date_labels = "%b %Y") +
#   scale_y_continuous(labels = scales::percent_format(accuracy = 1),
#                      limits = c(0.5, 1.3),
#                      breaks = seq(.5, 1.3, .2)) +
#   xlab('Month') +
#   ylab('Recovery rate') +
#   theme(
#     # axis.text.x = element_text(angle = 90),
#     panel.grid.major = element_line(color = 'light gray',
#                                     linewidth = .5,
#                                     linetype = 1),
#     panel.grid.minor.x = element_blank(),
#     panel.background = element_blank(),
#     plot.title = element_text(hjust = .5),
#     axis.ticks = element_blank(),
#     axis.title.y = element_text(margin = margin(r = 15)),
#     axis.title.x = element_text(margin = margin(t = 15))
#   )
# 
# all_city_plot
# 
# # Now by employment zone
# each_city_for_plot <-
#   rec_rate_city %>%
#   filter(year > 2018) %>%
#   dplyr::group_by(year, week_num, big_area) %>%
#   dplyr::summarize(n_devices = sum(n_devices, na.rm = T),
#             userbase = sum(userbase, na.rm = T)) %>%
#   dplyr::ungroup() %>%
#   mutate(normalized = n_devices/userbase) %>%
#   pivot_wider(
#     id_cols = c('week_num', 'big_area'),
#     names_from = 'year',
#     names_prefix = 'ntv',
#     values_from = 'normalized') %>%
#   mutate(rec2020 = ntv2020/ntv2019,
#          rec2021 = ntv2021/ntv2019,
#          rec2022 = ntv2022/ntv2019,
#          rec2023 = ntv2023/ntv2019) %>%
#   select(-starts_with('ntv')) %>%
#   pivot_longer(
#     cols = rec2020:rec2023,
#     names_to = 'year',
#     values_to = 'rq') %>%
#   filter(week_num < 53) %>%
#   mutate(year = substr(year, 4, 7),
#          week = as.Date(paste(year, week_num, 1, sep = '_'),
#                         format = '%Y_%W_%w')) %>% # Monday of week
#   filter(!(year == 2023 & week_num > 22)) %>%
#   arrange(big_area, year, week_num) %>%
#   dplyr::group_by(big_area) %>%
#   mutate(rq_rolling = zoo::rollmean(rq, k = 11, fill = NA, align = 'right')) %>%
#   dplyr::ungroup() %>%
#   data.frame() %>%
#   filter(!(year == 2020 & week_num < 12)) %>%
#   filter(!is.na(big_area)) %>%
#   mutate(
#     mytext = paste0(big_area, '<br>Week of ', week, ': ',
#                     scales::percent(rq_rolling, accuracy = 2)))
# 
# head(each_city_for_plot)
# 
# # Plotly
# each_city_plotly <- 
#   plot_ly() %>%
#   add_lines(data = each_city_for_plot, 
#             x = ~week, y = ~rq_rolling, 
#             split = ~big_area,
#             name = ~big_area, 
#             text = ~mytext,
#             hoverinfo = 'text',
#             opacity = .3,
#             line = list(shape = "linear", color = '#8c0a03')) %>%
#   layout(title = "Recovery rate for all Employment Zones in City of Toronto (11 week rolling average)",
#          xaxis = list(title = "Week", zerolinecolor = "#ffff", 
#                       tickformat = "%b %Y"),
#          yaxis = list(title = "Recovery rate", zerolinecolor = "#ffff",
#                       tickformat = ".0%", ticksuffix = "  "))
# 
# each_city_plotly

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
city_sf <- st_read('C:/Users/jpg23/data/downtownrecovery/shapefiles/employment_lands/big_city_areas.shp') %>%
  left_join(city0 %>% select(big_area, ez) %>% distinct(), by = c('big_area')) %>%
  select(-big_area)

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

#-----------------------------------------
# Calculate city recovery rates
#-----------------------------------------

### NOTE: I CAN'T CROP A SHAPEFILE AND THEN SAY THERE WERE X DEVICES ASSOCIATED
### WITH THE CROPPED SHAPEFILE. I WOULD HAVE TO RE-QUERY THE DATA FOR THAT
### EXACT GEOGRAPHY. NEED TO FIGURE OUT A WAY TO DISPLAY ALL THE DATA TOGETHER.







# Combine cropped region & city data
head(region_crop)
head(city_crop)

# full_sf <-
#   region_crop %>%
#   select(ez, municipality, geometry) %>%
#   left_join(?) %>%
#   rbind()






# all_city_for_plot <-
#   rec_rate_city %>%
#   filter(year > 2018) %>%
#   dplyr::group_by(year, week_num) %>%
#   dplyr::summarize(n_devices = sum(n_devices, na.rm = T),
#             userbase = sum(userbase, na.rm = T)) %>%
#   dplyr::ungroup() %>%
#   mutate(normalized = n_devices/userbase) %>%
#   pivot_wider(
#     id_cols = c('week_num'),
#     names_from = 'year',
#     names_prefix = 'ntv',
#     values_from = 'normalized') %>%
#   mutate(rec2020 = ntv2020/ntv2019,
#          rec2021 = ntv2021/ntv2019,
#          rec2022 = ntv2022/ntv2019,
#          rec2023 = ntv2023/ntv2019) %>%
#   select(-starts_with('ntv')) %>%
#   pivot_longer(
#     cols = rec2020:rec2023,
#     names_to = 'year',
#     values_to = 'rq') %>%
#   filter(week_num < 53) %>%
#   mutate(year = substr(year, 4, 7),
#          week = as.Date(paste(year, week_num, 1, sep = '_'),
#                         format = '%Y_%W_%w')) %>% # Monday of week
#   filter(!(year == 2023 & week_num > 22)) %>%
#   arrange(year, week_num) %>%
#   mutate(rq_rolling = zoo::rollmean(rq, k = 11, fill = NA, align = 'right')) %>%
#   ungroup() %>%
#   data.frame() %>%
#   filter(!(year == 2020 & week_num < 12))
# 
# head(all_city_for_plot)
# tail(all_city_for_plot)
# 
# all_city_plot <-
#   all_city_for_plot %>%
#   ggplot(aes(x = week, y = rq_rolling)) +
#   geom_line(size = .8) +
#   ggtitle('Recovery rate for all Employment Zones in City of Toronto (11 week rolling average)') +
#   scale_x_date(date_breaks = "4 month", date_labels = "%b %Y") +
#   scale_y_continuous(labels = scales::percent_format(accuracy = 1),
#                      limits = c(0.5, 1.3),
#                      breaks = seq(.5, 1.3, .2)) +
#   xlab('Month') +
#   ylab('Recovery rate') +
#   theme(
#     # axis.text.x = element_text(angle = 90),
#     panel.grid.major = element_line(color = 'light gray',
#                                     linewidth = .5,
#                                     linetype = 1),
#     panel.grid.minor.x = element_blank(),
#     panel.background = element_blank(),
#     plot.title = element_text(hjust = .5),
#     axis.ticks = element_blank(),
#     axis.title.y = element_text(margin = margin(r = 15)),
#     axis.title.x = element_text(margin = margin(t = 15))
#   )
# 
# all_city_plot
# 
# # Now by employment zone
# each_city_for_plot <-
#   rec_rate_city %>%
#   filter(year > 2018) %>%
#   dplyr::group_by(year, week_num, big_area) %>%
#   dplyr::summarize(n_devices = sum(n_devices, na.rm = T),
#             userbase = sum(userbase, na.rm = T)) %>%
#   dplyr::ungroup() %>%
#   mutate(normalized = n_devices/userbase) %>%
#   pivot_wider(
#     id_cols = c('week_num', 'big_area'),
#     names_from = 'year',
#     names_prefix = 'ntv',
#     values_from = 'normalized') %>%
#   mutate(rec2020 = ntv2020/ntv2019,
#          rec2021 = ntv2021/ntv2019,
#          rec2022 = ntv2022/ntv2019,
#          rec2023 = ntv2023/ntv2019) %>%
#   select(-starts_with('ntv')) %>%
#   pivot_longer(
#     cols = rec2020:rec2023,
#     names_to = 'year',
#     values_to = 'rq') %>%
#   filter(week_num < 53) %>%
#   mutate(year = substr(year, 4, 7),
#          week = as.Date(paste(year, week_num, 1, sep = '_'),
#                         format = '%Y_%W_%w')) %>% # Monday of week
#   filter(!(year == 2023 & week_num > 22)) %>%
#   arrange(big_area, year, week_num) %>%
#   dplyr::group_by(big_area) %>%
#   mutate(rq_rolling = zoo::rollmean(rq, k = 11, fill = NA, align = 'right')) %>%
#   dplyr::ungroup() %>%
#   data.frame() %>%
#   filter(!(year == 2020 & week_num < 12)) %>%
#   filter(!is.na(big_area)) %>%
#   mutate(
#     mytext = paste0(big_area, '<br>Week of ', week, ': ',
#                     scales::percent(rq_rolling, accuracy = 2)))
# 
# head(each_city_for_plot)
# 
# # Plotly
# each_city_plotly <- 
#   plot_ly() %>%
#   add_lines(data = each_city_for_plot, 
#             x = ~week, y = ~rq_rolling, 
#             split = ~big_area,
#             name = ~big_area, 
#             text = ~mytext,
#             hoverinfo = 'text',
#             opacity = .3,
#             line = list(shape = "linear", color = '#8c0a03')) %>%
#   layout(title = "Recovery rate for all Employment Zones in City of Toronto (11 week rolling average)",
#          xaxis = list(title = "Week", zerolinecolor = "#ffff", 
#                       tickformat = "%b %Y"),
#          yaxis = list(title = "Recovery rate", zerolinecolor = "#ffff",
#                       tickformat = ".0%", ticksuffix = "  "))
# 
# each_city_plotly
