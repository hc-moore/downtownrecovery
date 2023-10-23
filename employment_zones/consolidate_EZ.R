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

ipak(c('tidyverse', 'lubridate', 'ggplot2', 'plotly', 'cancensus', 'ggmap', 
       'sf', 'leaflet', 'BAMMtools', 'gtools', 'htmlwidgets', 'basemaps'))

ipak_gh(c("statnmap/HatchedPolygons"))

# #-----------------------------------------
# # Load data
# #-----------------------------------------
# 
# filepath <- 'C:/Users/jpg23/data/downtownrecovery/spectus_exports/employment_zones/'
# 
# # Toronto MSA
# s_filepath <- 'C:/Users/jpg23/data/downtownrecovery/spectus_exports/sensitivity_analysis/'
# 
# msa <-
#   list.files(path = paste0(s_filepath, 'MSA')) %>%
#   map_df(~read_delim(
#     paste0(s_filepath, 'MSA/', .),
#     delim = '\001',
#     col_names = c('msa_name', 'provider_id', 'approx_distinct_devices_count',
#                   'event_date'),
#     col_types = c('ccii')
#   )) %>%
#   data.frame() %>%
#   mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
#   arrange(date) %>%
#   select(-event_date) %>%
#   rename(msa_count = approx_distinct_devices_count) %>%
#   filter(msa_name == 'Toronto') %>%
#   select(-msa_name)
# 
# # Region-wide (outside city): 1/1/2019 - 5/31/2023
# region0 <-
#   list.files(path = paste0(filepath, 'laura_region')) %>%
#   map_df(~read_delim(
#     paste0(filepath, 'laura_region/', .),
#     delim = '\001',
#     col_names = c('ez', 'provider_id', 'approx_distinct_devices_count',
#                   'event_date'),
#     col_types = c('ccii')
#   )) %>%
#   data.frame() %>%
#   mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
#   arrange(date) %>%
#   select(-event_date)
# 
# # City-wide: 1/1/19 - 8/12/22
# city1 <-
#   list.files(path = paste0(filepath, 'city_20190101_20220812')) %>%
#   map_df(~read_delim(
#     paste0(filepath, 'city_20190101_20220812/', .),
#     delim = '\001',
#     col_names = c('small_area', 'big_area', 'provider_id',
#                   'approx_distinct_devices_count', 'event_date'),
#     col_types = c('cccii')
#   )) %>%
#   data.frame() %>%
#   mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
#   arrange(date) %>%
#   select(-event_date)
# 
# # City-wide: 8/12/22 - 6/29/23
# city2 <-
#   list.files(path = paste0(filepath, 'city_20220812_20230629')) %>%
#   map_df(~read_delim(
#     paste0(filepath, 'city_20220812_20230629/', .),
#     delim = '\001',
#     col_names = c('small_area', 'big_area', 'provider_id',
#                   'approx_distinct_devices_count', 'event_date'),
#     col_types = c('cccii')
#   )) %>%
#   data.frame() %>%
#   mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
#   arrange(date) %>%
#   select(-event_date)
# 
# town_centre <-
#   list.files(path = paste0(filepath, 'town_centre_fixed')) %>%
#   map_df(~read_delim(
#     paste0(filepath, 'town_centre_fixed/', .),
#     delim = '\001',
#     col_names = c('precinct', 'provider_id', 'approx_distinct_devices_count',
#                   'event_date'),
#     col_types = c('cii')
#   )) %>%
#   data.frame() %>%
#   mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
#   arrange(date) %>%
#   select(-event_date)
# # 
# #-----------------------------------------
# # Aggregate region-wide data
# #-----------------------------------------
# 
# head(region0)
# range(region0$date)
# unique(region0$provider_id)
# 
# r_export <-
#   region0 %>%
#   filter(provider_id != '230599') %>%
#   mutate(
#     date_range_start = floor_date(
#       date,
#       unit = "week",
#       week_start = getOption("lubridate.week.start", 1))) %>%
#   group_by(ez, date_range_start, provider_id) %>%
#   summarize(n_devices = sum(approx_distinct_devices_count, na.rm = T)) %>%
#   ungroup() %>%
#   data.frame()
# 
# head(r_export)
# unique(r_export$provider_id)
# 
# #-----------------------------------------
# # Combine city-wide data
# #-----------------------------------------
# 
# head(city1)
# head(city2)
# 
# range(city1$date)
# range(city2$date)
# 
# city0 <-
#   city1 %>%
#   filter(date < as.Date('2022-08-12')) %>%
#   rbind(city2) %>%
#   filter(date < as.Date('2023-06-01')) %>% # through end of May
#   mutate(
#     ez = big_area,
#     ez = recode(ez,
#     '31'='South of Eastern',
#     '32'='Rexdale-Airport',
#     '33'='Junction-Weston-Dupont North',
#     '34'='Junction-Weston-Dupont South',
#     '35'='Junction-Weston-Dupont East',
#     '36'='Tapscott North',
#     '37'='Tapscott East',
#     '38'='Tapscott South',
#     '39'='Liberty Village',
#     '40'='South Etobicoke - North',
#     '41'='South Etobicoke - East',
#     '42'='South Etobicoke - South',
#     '43'='Northwest Etobicoke',
#     '22'='Port Lands - Central Waterfront',
#     '23'='Eastern-Carlaw-DVP-Greenwood',
#     '24'='Leaside-Thorncliffe',
#     '25'='Bermondsey-Railside - North',
#     '9'='Bermondsey-Railside - South',
#     '26'='Scarborough-Highway 401 - East',
#     '27'='Scarborough-Highway 401 - Central',
#     '28'='Scarborough-Highway 401 - West',
#     '5'='Milliken',
#     '0'='Coronation Drive',
#     '1'='Golden Mile / South-Central Scarborough - North East',
#     '29'='Golden Mile / South-Central Scarborough - North',
#     '30'='Golden Mile / South-Central Scarborough - South',
#     '2'='Scarborough Junction - North',
#     '3'='Scarborough Junction - Central',
#     '4'='Scarborough Junction - South-West',
#     '18'='Highway 400 - North',
#     '19'='Highway 400 - North-Central',
#     '20'='Highway 400 - South-Central',
#     '21'='Highway 400 - South',
#     '10'='Downsview - North',
#     '11'='Downsview - Central',
#     '12'='Downsview - South',
#     '13'='Caledonia - South Downsview - North',
#     '14'='Caledonia - South Downsview - Central',
#     '15'='Caledonia - South Downsview - South',
#     '6'='Victoria Park - Steeles (eastern part)',
#     '7'='Victoria Park - Steeles (western part)',
#     '8'='Consumers Road',
#     '16'='Duncan Mill',
#     '17'='Don Mills'
#   ))
# 
# city_export <- city0 %>%
#   mutate(
#     date_range_start = floor_date(
#       date,
#       unit = "week",
#       week_start = getOption("lubridate.week.start", 1))) %>%
#   # Calculate # of devices by week and provider
#   group_by(ez, date_range_start, provider_id) %>%
#   summarize(n_devices = sum(approx_distinct_devices_count, na.rm = T)) %>%
#   ungroup()
# 
# head(city_export)
# 
# #-----------------------------------------
# # Create Town Centre data for export
# #-----------------------------------------
# 
# tc_export <-
#   town_centre %>%
#   mutate(
#     date_range_start = floor_date(
#       date,
#       unit = "week",
#       week_start = getOption("lubridate.week.start", 1))) %>%
#   rename(ez = precinct) %>%
#   # Calculate # of devices by week and provider
#   group_by(ez, date_range_start, provider_id) %>%
#   summarize(n_devices = sum(approx_distinct_devices_count, na.rm = T)) %>%
#   ungroup() %>%
#   data.frame()
# 
# head(tc_export)
# unique(tc_export$ez)
# 
# #-----------------------------------------
# # Export for imputation
# #-----------------------------------------
# 
# head(r_export)
# head(city_export)
# head(tc_export)
# 
# # Load full names of region EZs
# region_sf0 <- st_read('C:/Users/jpg23/data/downtownrecovery/shapefiles/employment_lands/PSEZ_with_ID.geojson')
# 
# region_sf <- region_sf0 %>% filter(Precinct != 'Outside GTHA study area')
# 
# head(region_sf)
# 
# r_export1 <-
#   r_export %>%
#   inner_join(
#     region_sf %>%
#       st_drop_geometry() %>%
#       select(ez, Precinct) %>%
#       mutate(ez = as.character(ez)),
#     by = 'ez'
#   ) %>%
#   select(-ez) %>%
#   rename(ez = Precinct) %>%
#   filter(ez != 'Town Centre') # replace Town Centre with new 4 polygons!
# 
# head(r_export1)
# r_export1 %>% filter(is.na(ez)) # should be no rows
# 
# all_export <- rbind(r_export1, city_export, tc_export)
# 
# head(all_export)
# 
# # Group by *new* precinct
# 
# new_precincts <- read.csv("C:/Users/jpg23/UDP/downtown_recovery/employment_zones/toronto-updated-precinct-list.csv")
# 
# head(new_precincts)
# 
# all_export1 <-
#   all_export %>%
#   left_join(new_precincts %>% rename(ez = Employment.precinct)) %>%
#   rename(new_ez = Consolidated.precincts) %>%
#   mutate(new_ez = case_when(
#     ez == 'City of Toronto Port Lands' ~ 'Port Lands and South of Eastern',
#     ez == 'Churchill Meadows Em' ~ 'Churchill Meadows',
#     ez == '21_Town Centre_Allstate' ~ 'Allstate',
#     ez %in% c('21_Town Centre_Denison Steeles - Fourteenth Avenue',
#               '69_Town Centre_Denison Steeles - Fourteenth Avenue') ~
#       'Denison Steeles - Fourteenth Avenue',
#     ez == '69_Town Centre_Riseborough' ~ 'Riseborough',
#     TRUE ~ new_ez
#   )) %>%
#   filter(ez != 'North Leslie')
# 
# head(all_export1)
# 
# missing_new_ez <- all_export1 %>% filter(is.na(new_ez))
# unique(missing_new_ez$ez)
# 
# unique(all_export1$new_ez)
# 
# all_export2 <-
#   all_export1 %>%
#   group_by(new_ez, provider_id, date_range_start) %>%
#   summarize(n_devices = sum(n_devices, na.rm = T)) %>%
#   ungroup() %>%
#   data.frame()
# 
# head(all_export2)
# 
# head(msa)
# 
# # Add MSA data
# 
# msa_weekly <- msa %>%
#   filter(provider_id != '230599') %>%
#   mutate(
#     date_range_start = floor_date(
#       date,
#       unit = "week",
#       week_start = getOption("lubridate.week.start", 1))) %>%
#   group_by(date_range_start, provider_id) %>%
#   summarize(msa_count = sum(msa_count, na.rm = T)) %>%
#   ungroup() %>%
#   data.frame()
# 
# head(msa_weekly)
# unique(msa_weekly$provider_id)
# 
# head(all_export2)
# 
# all_export_final <-
#   all_export2 %>%
#   left_join(msa_weekly, by = c('date_range_start', 'provider_id')) %>%
#   mutate(normalized = n_devices/msa_count)
# 
# head(all_export_final)
# unique(all_export_final$new_ez)
# 
# write.csv(all_export_final,
#           "C:/Users/jpg23/UDP/downtown_recovery/employment_zones/EZs_for_imputation.csv",
#           row.names = F)
# 
# #-----------------------------------------
# # Explore non-imputed data
# #-----------------------------------------
# 
# # all_plotly <- 
# #   plot_ly() %>%
# #   add_lines(data = msa_weekly %>% filter(provider_id == '190199'), 
# #             x = ~date_range_start, y = ~msa_count, 
# #             name = "Toronto MSA  (provider 190199)",
# #             opacity = .9,
# #             line = list(shape = "linear", color = '#d6ad09')) %>%
# #   add_lines(data = msa_weekly %>% filter(provider_id == '700199'), 
# #             x = ~date_range_start, y = ~msa_count, 
# #             name = "Toronto MSA (provider 700199)",
# #             opacity = .9,
# #             line = list(shape = "linear", color = '#8c0a03')) %>%
# #   add_lines(data = all_export2 %>% filter(provider_id == '190199'), 
# #             x = ~date_range_start, y = ~n_devices,
# #             name = ~paste0(new_ez, ": provider 190199"),
# #             opacity = .3,
# #             split = ~new_ez,
# #             line = list(shape = "linear", color = 'orange')) %>%
# #   add_lines(data = all_export2 %>% filter(provider_id == '700199'), 
# #             x = ~date_range_start, y = ~n_devices,
# #             name = ~paste0(new_ez, ": provider 700199"),
# #             opacity = .3,
# #             split = ~new_ez,
# #             line = list(shape = "linear", color = 'purple')) %>%
# #   layout(title = "Trends by area",
# #          xaxis = list(title = "Date", zerolinecolor = "#ffff", 
# #                       tickformat = "%b %Y"),
# #          yaxis = list(title = "Devices", zerolinecolor = "#ffff",
# #                       ticksuffix = "  "))
# # 
# # all_plotly
# 
# norm_plotly <-
#   plot_ly() %>%
#   add_lines(data = all_export_final %>% filter(provider_id == '190199'), 
#             x = ~date_range_start, y = ~normalized, 
#             name = ~paste0(new_ez, ":  provider 190199"),
#             opacity = .7,
#             split = ~new_ez,
#             line = list(shape = "linear", color = '#d6ad09')) %>%
#   add_lines(data = all_export_final %>% filter(provider_id == '700199'), 
#             x = ~date_range_start, y = ~normalized, 
#             name = ~paste0(new_ez, ": provider 700199"),
#             opacity = .7,
#             split = ~new_ez,
#             line = list(shape = "linear", color = '#8c0a03')) %>%
#   layout(title = "Normalized trends by consolidated EZ",
#          xaxis = list(title = "Date", zerolinecolor = "#ffff", 
#                       tickformat = "%b %Y"),
#          yaxis = list(title = "Devices", zerolinecolor = "#ffff",
#                       ticksuffix = "  "))
# 
# norm_plotly

#-----------------------------------------
# Load imputed data
#-----------------------------------------

# Imputed data
imputed0 <- read.csv("C:/Users/jpg23/data/downtownrecovery/employment_zones/imputation_Canada_msa_SAITS_ez_fin.csv") %>%
  rename(new_ez = city) %>%
  filter(!new_ez %in% c('Caledonia - South Downsview - South', 
                        'North Oakville East Employment District',
                        'Southwest Milton'))

imputed_t <- read.csv('C:/Users/jpg23/data/downtownrecovery/sensitivity_analysis/imputation_Canada_msa_SAITS_hdbscan_fin.csv') %>%
  # filter out week with weird dip to not affect results
  filter(city == 'Toronto' & date_range_start != as.Date('2021-05-10')) %>%
  mutate(new_ez = 'Downtown') %>%
  select(-city)

head(imputed0)
head(imputed_t)

imputed <- rbind(imputed0, imputed_t)

head(imputed)
unique(imputed$provider_id)
unique(imputed$new_ez)

norm_plotly_imp <-
  plot_ly() %>%
  add_lines(data = imputed %>% filter(provider_id == '190199'),
            x = ~date_range_start, y = ~normalized,
            name = ~paste0(new_ez, ': ', provider_id),
            opacity = .7,
            split = ~new_ez,
            line = list(shape = "linear", color = '#d6ad09')) %>%
  add_lines(data = imputed %>% filter(provider_id == '700199'),
            x = ~date_range_start, y = ~normalized,
            name = ~paste0(new_ez, ': ', provider_id),
            opacity = .7,
            split = ~new_ez,
            line = list(shape = "linear", color = '#8c0a03')) %>%  
  layout(title = "Normalized trends by consolidated EZ - IMPUTED",
         xaxis = list(title = "Date", zerolinecolor = "#ffff", 
                      tickformat = "%b %Y"),
         yaxis = list(title = "Normalized", zerolinecolor = "#ffff",
                      ticksuffix = "  "))

norm_plotly_imp

saveWidget(
  norm_plotly_imp,
  'C:/Users/jpg23/UDP/downtown_recovery/employment_zones/imputed_norm_by_EZ.html')

#-----------------------------------------
# Calculate recovery rates
#-----------------------------------------

head(imputed)

rq <-
  imputed %>%
  filter(provider_id == '190199') %>%
  # filter to beginning of March - mid-June, 2019 & 2021 & 2023 (approx. dates)
  filter((date_range_start >= as.Date('2019-03-04') & 
            date_range_start <= as.Date('2019-06-10')) | 
           (date_range_start >= as.Date('2021-03-01') &
              date_range_start <= as.Date('2021-06-07')) |
           (date_range_start >= as.Date('2023-02-27'))) %>%
  mutate(week_num = isoweek(date_range_start),
         year = year(date_range_start)) %>%
  select(-date_range_start) %>%
  pivot_wider(
    id_cols = c('new_ez', 'week_num'),
    names_from = 'year',
    names_prefix = 'ntv',
    values_from = 'normalized'
  ) %>%
  mutate(rec23_19 = ntv2023/ntv2019,
         rec21_19 = ntv2021/ntv2019,
         rec23_21 = ntv2023/ntv2021) %>%
  data.frame() %>%
  group_by(new_ez) %>%
  summarize(rq23_19 = mean(rec23_19, na.rm = T),
            rq21_19 = mean(rec21_19, na.rm = T),
            rq23_21 = mean(rec23_21, na.rm = T)) %>%
  ungroup() %>%
  data.frame()

rq

#-----------------------------------------
# Add spatial data
#-----------------------------------------

# New HDBSCAN downtown polygon
dpt <- st_read("C:/Users/jpg23/data/downtownrecovery/sensitivity_analysis/new_downtowns/HDBSCAN_downtowns.geojson") %>%
  filter(city == 'Toronto')

dpt
plot(dpt)

# New consolidated EZs
new_sf0 <- st_read("C:/Users/jpg23/UDP/downtown_recovery/employment_zones/gtha-da-2021_fixed/gtha-da-2021_fixed.shp") %>%
  filter(!is.na(Precinct)) %>%
  rename(new_ez = Precinct) %>%
  select(-DAUID) %>%
  group_by(new_ez) %>%
  summarize()

head(new_sf0)
plot(new_sf0)

new_sf <- 
  rbind(new_sf0, dpt %>% mutate(new_ez = 'Downtown') %>% select(-city)) %>%
  filter(!new_ez %in% c('Caledonia - South Downsview - South',
                        'North Oakville East Employment District',
                        'Southwest Milton'))

sf_ez <- unique(new_sf$new_ez)
imp_ez <- unique(imputed$new_ez)

setdiff(sf_ez, imp_ez)
setdiff(imp_ez, sf_ez)

# Load Toronto city boundary
toronto <- st_read('C:/Users/jpg23/data/downtownrecovery/shapefiles/employment_lands/toronto.geojson')

plot(toronto)

final_sf <- new_sf %>% left_join(rq)

final_sf %>% data.frame()

#-----------------------------------------
# Map EZ recovery rates (static): 
# 2023 vs 2019
#-----------------------------------------

summary(rq$rq23_19)
getJenksBreaks(rq$rq23_19, 7)
hist(rq$rq23_19, breaks = 50)

ez_final_23_19 <-
  final_sf %>%
  mutate(
    rate_cat = factor(case_when(
      rq23_19 < .8 ~ '64 - 79.9%',
      rq23_19 < 1 ~ '80 - 99.9%',
      rq23_19 < 1.15 ~ '100 - 114.9%',
      rq23_19 < 1.25 ~ '115 - 124.9%',
      rq23_19 < 1.5 ~ '125 - 149.9%',
      TRUE ~ '150 - 250%'
    ),
    levels = c('64 - 79.9%', '80 - 99.9%', '100 - 114.9%', '115 - 124.9%', 
               '125 - 149.9%', '150 - 250%')))

head(ez_final_23_19 %>% data.frame())
table(ez_final_23_19$rate_cat)

pal <- c(
  "#e41822",
  "#faa09d",
  "#5bc4fb",
  "#2c92d7",
  "#0362b0",
  "#033384"
)

ez_map_23_19 <-
  ggplot() +
  geom_sf(data = ez_final_23_19 %>% filter(new_ez != 'Downtown'),
          aes(fill = rate_cat),
          color = NA) +
  ggtitle('Recovery rate for all employment zones in Toronto region,\nJanuary - May (2023 versus 2019)') +
  scale_fill_manual(values = pal, name = 'Recovery rate') +
  guides(fill = guide_legend(barwidth = 0.5, barheight = 10, 
                             ticks = F, byrow = T)) +
  geom_sf(data = toronto, fill = NA, linewidth = .7) +
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

ggsave('C:/Users/jpg23/UDP/downtown_recovery/employment_zones/static_map_23v19.png',
       plot = ez_map_23_19, 
       width = 15,
       height = 7)

#-----------------------------------------
# Map EZ recovery rates (interactive): 
# 2023 vs 2019
#-----------------------------------------

ez_label_23_19 <-
  ez_final_23_19 %>%
  mutate(label = paste0(new_ez, ": ", round(rq23_19 * 100), "%"))

head(ez_label_23_19 %>% data.frame())

leaflet_pal_23_19 <- colorFactor(
  pal,
  domain = ez_label_23_19$rate_cat,
  na.color = 'transparent'
)

# Hatched polygon (downtown)
dt_hatch_23_19 <-
  ez_label_23_19 %>%
  filter(new_ez == 'Downtown') %>%
  hatched.SpatialPolygons(density = 500, angle = 45, fillOddEven = TRUE) %>%
  cbind(ez_label_23_19 %>% filter(new_ez == 'Downtown') %>% select(rate_cat, label))

dt_hatch_23_19

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
    data = ez_label_23_19 %>% filter(new_ez != 'Downtown'),
    label = ~label,
    labelOptions = labelOptions(textsize = "12px"),
    fillOpacity = .8,
    color = ~leaflet_pal_23_19(rate_cat),
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
  addPolygons(
    data = dt_hatch_23_19,
    label = ~label,
    labelOptions = labelOptions(textsize = "12px"),
    fillOpacity = .8,
    color = ~leaflet_pal_23_19(rate_cat),
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
    pal = leaflet_pal_23_19,
    values = ~rate_cat,
    title = 'Recovery rate<br>(January-May,<br>2023 vs 2019)'
  )

interactive_23_19

saveWidget(
  interactive_23_19,
  'C:/Users/jpg23/UDP/downtown_recovery/employment_zones/interactive_map_23v19.html')

#-----------------------------------------
# Map EZ recovery rates (static): 
# 2023 vs 2021
#-----------------------------------------

summary(rq$rq23_21)
getJenksBreaks(rq$rq23_21, 10)
hist(rq$rq23_21, breaks = 50)

ez_final_23_21 <-
  final_sf %>%
  mutate(
    rate_cat = factor(case_when(
      rq23_21 < .9 ~ '67 - 89.9%',
      rq23_21 < 1 ~ '90 - 99.9%',
      rq23_21 < 1.4 ~ '100 - 139.9%',
      rq23_21 < 1.8 ~ '140 - 179.9%',
      rq23_21 < 3.3 ~ '180 - 329.9%',
      TRUE ~ '330 - 530%'
    ),
    levels = c('67 - 89.9%', '90 - 99.9%', '100 - 139.9%', '140 - 179.9%',
               '180 - 329.9%', '330 - 530%')))

head(ez_final_23_21 %>% data.frame())
table(ez_final_23_21$rate_cat)

pal <- c(
  "#e41822",
  "#faa09d",
  "#5bc4fb",
  "#2c92d7",
  "#0362b0",
  "#033384"
)

ez_map_23_21 <-
  ggplot() +
  geom_sf(data = ez_final_23_21 %>% filter(new_ez != 'Downtown'),
          aes(fill = rate_cat),
          color = NA) +
  ggtitle('Recovery rate for all employment zones in Toronto region,\nJanuary - May (2023 versus 2021)') +
  scale_fill_manual(values = pal, name = 'Recovery rate') +
  guides(fill = guide_legend(barwidth = 0.5, barheight = 10, 
                             ticks = F, byrow = T)) +
  geom_sf(data = toronto, fill = NA, linewidth = .7) +
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

ggsave('C:/Users/jpg23/UDP/downtown_recovery/employment_zones/static_map_23v21.png',
       plot = ez_map_23_21, 
       width = 15,
       height = 7)

#-----------------------------------------
# Map EZ recovery rates (interactive): 
# 2023 vs 2021
#-----------------------------------------

ez_label_23_21 <-
  ez_final_23_21 %>%
  mutate(label = paste0(new_ez, ": ", round(rq23_21 * 100), "%"))

head(ez_label_23_21 %>% data.frame())

leaflet_pal_23_21 <- colorFactor(
  pal,
  domain = ez_label_23_21$rate_cat,
  na.color = 'transparent'
)

# Hatched polygon (downtown)
dt_hatch_23_21 <-
  ez_label_23_21 %>%
  filter(new_ez == 'Downtown') %>%
  hatched.SpatialPolygons(density = 500, angle = 45, fillOddEven = TRUE) %>%
  cbind(ez_label_23_21 %>% filter(new_ez == 'Downtown') %>% select(rate_cat, label))

dt_hatch_23_21

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
    data = ez_label_23_21 %>% filter(new_ez != 'Downtown'),
    label = ~label,
    labelOptions = labelOptions(textsize = "12px"),
    fillOpacity = .8,
    color = ~leaflet_pal_23_21(rate_cat),
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
  addPolygons(
    data = dt_hatch_23_21,
    label = ~label,
    labelOptions = labelOptions(textsize = "12px"),
    fillOpacity = .8,
    color = ~leaflet_pal_23_21(rate_cat),
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
    pal = leaflet_pal_23_21,
    values = ~rate_cat,
    title = 'Recovery rate<br>(January-May,<br>2023 vs 2021)'
  )

interactive_23_21

saveWidget(
  interactive_23_21,
  'C:/Users/jpg23/UDP/downtown_recovery/employment_zones/interactive_map_23v21.html')

#-----------------------------------------
# Map EZ recovery rates (static): 
# 2021 vs 2019
#-----------------------------------------

summary(rq$rq21_19)
getJenksBreaks(rq$rq21_19, 15)
getJenksBreaks(rq$rq21_19, 5)
hist(rq$rq21_19, breaks = 50)

ez_final_21_19 <-
  final_sf %>%
  mutate(
    rate_cat = factor(case_when(
      rq21_19 < .5 ~ '24 - 49.9%',
      rq21_19 < .8 ~ '50 - 79.9%',
      rq21_19 < 1 ~ '80 - 99.9%',
      rq21_19 < 1.1 ~ '100 - 109.9%',
      rq21_19 < 1.2 ~ '110 - 119.9%',
      TRUE ~ '120 - 146%'
    ),
    levels = c('24 - 49.9%', '50 - 79.9%', '80 - 99.9%', '100 - 109.9%',
               '110 - 119.9%', '120 - 146%')))

head(ez_final_21_19 %>% data.frame())
table(ez_final_21_19$rate_cat)

pal_21_19 <- c(
  "#e41822",
  "#f46861",
  "#faa09d",
  "#5bc4fb",
  "#167ac4",
  "#033384"
)

ez_map_21_19 <-
  ggplot() +
  geom_sf(data = ez_final_21_19 %>% filter(new_ez != 'Downtown'),
          aes(fill = rate_cat),
          color = NA) +
  ggtitle('Recovery rate for all employment zones in Toronto region,\nJanuary - May (2021 versus 2019)') +
  scale_fill_manual(values = pal_21_19, name = 'Recovery rate') +
  guides(fill = guide_legend(barwidth = 0.5, barheight = 10, 
                             ticks = F, byrow = T)) +
  geom_sf(data = toronto, fill = NA, linewidth = .7) +
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

ggsave('C:/Users/jpg23/UDP/downtown_recovery/employment_zones/static_map_21v19.png',
       plot = ez_map_21_19, 
       width = 15,
       height = 7)

#-----------------------------------------
# Map EZ recovery rates (interactive): 
# 2021 vs 2019
#-----------------------------------------

ez_label_21_19 <-
  ez_final_21_19 %>%
  mutate(label = paste0(new_ez, ": ", round(rq21_19 * 100), "%"))

head(ez_label_21_19 %>% data.frame())

leaflet_pal_21_19 <- colorFactor(
  pal_21_19,
  domain = ez_label_21_19$rate_cat,
  na.color = 'transparent'
)

# Hatched polygon (downtown)
dt_hatch_21_19 <-
  ez_label_21_19 %>%
  filter(new_ez == 'Downtown') %>%
  hatched.SpatialPolygons(density = 500, angle = 45, fillOddEven = TRUE) %>%
  cbind(ez_label_21_19 %>% filter(new_ez == 'Downtown') %>% select(rate_cat, label))

dt_hatch_21_19

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
    data = ez_label_21_19 %>% filter(new_ez != 'Downtown'),
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
  addPolygons(
    data = dt_hatch_21_19,
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

saveWidget(
  interactive_21_19,
  'C:/Users/jpg23/UDP/downtown_recovery/employment_zones/interactive_map_21v19.html')


#-----------------------------------------
# Export geojson for Laura
#-----------------------------------------

head(ez_final_23_19)
head(ez_final_23_21)
head(ez_final_21_19)

for_laura <-
  ez_final_23_19 %>%
  rename(cat_23_19 = rate_cat) %>%
  left_join(
    ez_final_23_21 %>% select(new_ez, cat_23_21 = rate_cat) %>% st_drop_geometry()
  ) %>%
  left_join(
    ez_final_21_19 %>% select(new_ez, cat_21_19 = rate_cat) %>% st_drop_geometry()
  )

head(for_laura)

st_write(for_laura,
         'C:/Users/jpg23/UDP/downtown_recovery/employment_zones/final_sf.geojson')