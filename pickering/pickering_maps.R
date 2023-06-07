################################################################################
# Map recovery rates for Toronto metro area municipalities and DAs in Pickering
#
# Author: Julia Greenberg
# Date created: 6.6.2023
################################################################################
#-----------------------------------------
# Load packages
#-----------------------------------------

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'ggplot2', 'sf', 'lubridate', 'plotly', 'zoo', 
       'htmlwidgets', 'BAMMtools', 'leaflet', 'ggmap'))

#-----------------------------------------
# Load DA data from Spectus
#-----------------------------------------

filepath <- '~/data/downtownrecovery/'

da1 <- read_delim(
  paste0(filepath, 
         'spectus_exports/DAs/20230516_172448_00007_y5bth_1829c78b-40bc-443c-aea7-9f31f79b879b.gz'),
  delim = '\001',
  col_names = c('da', 'provider', 'n_devices', 'userbase', 'date'),
  col_types = c('cciii')
) %>%
  mutate(date = as.Date(as.character(date), format = "%Y%m%d")) %>%
  data.frame()

da2 <- read_delim(
  paste0(filepath, 
         'spectus_exports/DAs/20230516_172448_00007_y5bth_f2fba8fa-0f15-44b8-8429-a1b6d1350b44.gz'),
  delim = '\001',
  col_names = c('da', 'provider', 'n_devices', 'userbase', 'date'),
  col_types = c('cciii')
) %>%
  mutate(date = as.Date(as.character(date), format = "%Y%m%d")) %>%
  data.frame()

da0 <- rbind(da1, da2) %>% mutate(normalized = n_devices/userbase)

summary(da0)
head(da0)

#-----------------------------------------
# Determine which provider to use
#-----------------------------------------

# # Which provider to use? Plot each one over time
# da_plot <- da0 %>%
#   ggplot(aes(x = date, y = normalized, group = provider, color = provider,
#              alpha = .8)) +
#   geom_line() +
#   theme_bw()
# 
# da_plot

da <- da0 %>%
  filter((provider == '700199' & date < as.Date('2021-05-17')) | 
           (provider == '190199' & date >= as.Date('2021-05-17'))) %>%
  select(-provider) %>%
  # Determine week and year # for each date
  mutate(
    date_range_start = floor_date(
      date,
      unit = "week",
      week_start = getOption("lubridate.week.start", 1)),
    week_num = isoweek(date_range_start),
    year = year(date_range_start))

head(da)

#-----------------------------------------
# Load DA shapefile
#-----------------------------------------

da_sf <- read_sf(paste0(filepath, 'DAs/gtha-da-21_simplified.geojson')) %>%
  rename(da = DAUID)

head(da_sf)

nrow(da_sf)
nrow(da0)

#-----------------------------------------
# Load municipalities shapefile
#-----------------------------------------

# Shapefile of Ontario municipalities downloaded here: 
# https://geohub.lio.gov.on.ca/datasets/municipal-boundary-lower-and-single-tier/explore?location=43.865410%2C-79.317809%2C7.35

muni <- read_sf(
  paste0(filepath, 'Municipal_Boundary_-_Lower_and_Single_Tier.geojson')) %>%
  filter(str_detect(OFFICIAL_MUNICIPAL_NAME, 
                    '\\b(AJAX|AURORA|BRAMPTON|BROCK|BURLINGTON|CALEDON|CLARINGTON|EAST GWILLIMBURY|GEORGINA|HALTON HILLS|KING|MARKHAM|MILTON|MISSISSAUGA|NEWMARKET|OAKVILLE|OSHAWA|PICKERING|RICHMOND HILL|SCUGOG|TORONTO|UXBRIDGE|VAUGHAN|WHITBY|WHITCHURCH-STOUFFVILLE)\\b')) %>%
  select(municipality = OFFICIAL_MUNICIPAL_NAME)

muni$municipality
n_distinct(muni$municipality)
muni %>% group_by(municipality) %>% count() %>% arrange(desc(n))
muni %>% filter(municipality == 'CITY OF TORONTO') # 3 rows, each with different geography

#-----------------------------------------
# Map shapefile of municipalities
#-----------------------------------------

muni_map <-
  leaflet(
    options = leafletOptions(minZoom = 6, maxZoom = 16)
  ) %>%
  addMapPane(name = "polygons", zIndex = 410) %>%
  addMapPane(name = "maplabels", zIndex = 420) %>%
  addProviderTiles("CartoDB.PositronNoLabels") %>%
  addProviderTiles("Stamen.TonerLines",
                   options = providerTileOptions(opacity = 0.3),
                   group = "Roads"
  ) %>%
  addProviderTiles("CartoDB.PositronOnlyLabels",
                   options = leafletOptions(pane = "maplabels")
  ) %>%
  addPolygons(
    data = muni,
    label = ~municipality,
    options = pathOptions(pane = "polygons"),
    color = "orange",
    weight = 1,
    highlightOptions =
      highlightOptions(
        color = "black",
        weight = 2,
        bringToFront = TRUE)
  )

muni_map

#-----------------------------------------
# Remove duplicate municipalities & re-map
#-----------------------------------------

muni1 <- muni %>% 
  cbind(st_coordinates(st_centroid(muni)) %>% data.frame()) 

# For Georgina & Brock, keep most northern polygons
gb <- muni1 %>%
  filter(municipality %in% c('TOWN OF GEORGINA', 'TOWNSHIP OF BROCK')) %>%
  group_by(municipality) %>%
  arrange(Y) %>%
  slice_head() %>%
  select(municipality) %>%
  ungroup()

# For others, keep most southern polygons
other <- muni1 %>%
  filter(!municipality %in% c('TOWN OF GEORGINA', 'TOWNSHIP OF BROCK')) %>%
  group_by(municipality) %>%
  arrange(desc(Y)) %>%
  slice_head() %>%
  select(municipality) %>%
  ungroup()

muni2 <- rbind(gb, other)

head(muni2)
class(muni2)
nrow(muni2)

muni_map2 <-
  leaflet(
    options = leafletOptions(minZoom = 6, maxZoom = 16)
  ) %>%
  addMapPane(name = "polygons", zIndex = 410) %>%
  addMapPane(name = "maplabels", zIndex = 420) %>%
  addProviderTiles("CartoDB.PositronNoLabels") %>%
  addProviderTiles("Stamen.TonerLines",
                   options = providerTileOptions(opacity = 0.3),
                   group = "Roads"
  ) %>%
  addProviderTiles("CartoDB.PositronOnlyLabels",
                   options = leafletOptions(pane = "maplabels")
  ) %>%
  addLayersControl(
    position = "topright",
    overlayGroups = c(
      "municipality",
      "da"),
    options = layersControlOptions(collapsed = FALSE, maxHeight = 'auto')) %>%
  addPolygons(
    data = muni2,
    label = ~municipality,
    options = pathOptions(pane = "polygons"),
    color = "purple",
    weight = 2,
    group = "municipality",
    highlightOptions =
      highlightOptions(
        color = "black",
        weight = 3,
        bringToFront = TRUE)
  ) %>%
  addPolygons(
    data = da_sf,
    label = ~da,
    options = pathOptions(pane = "polygons"),
    color = "orange",
    weight = 1,
    group = "da",
    highlightOptions =
      highlightOptions(
        color = "black",
        weight = 2,
        bringToFront = TRUE)
  )

muni_map2

#-----------------------------------------
# Join DAs & municipalities
#-----------------------------------------

nrow(muni2)
nrow(da_sf)

da_muni_sf <- 
  st_join(muni2 %>% st_make_valid(), da_sf %>% st_make_valid(),
          join = st_contains) 

nrow(da_muni_sf)
nrow(da)

da_muni <- da_muni_sf %>% left_join(da)

nrow(da_muni)
head(da_muni %>% data.frame())

#-----------------------------------------
# Map DA recovery rates in Pickering
#-----------------------------------------

pickering <-
  da_muni %>%
  st_drop_geometry() %>%
  filter(municipality == 'CITY OF PICKERING') %>%
  # Calculate # of devices by DA, week and year
  group_by(da, year, week_num) %>%
  summarize(n_devices = sum(n_devices, na.rm = T),
            userbase = sum(userbase, na.rm = T)) %>%
  ungroup() %>%
  mutate(week = as.Date(
    paste(as.character(year), as.character(week_num), 1, sep = '_'),
    format = '%Y_%W_%w'))

# Make sure I'm comparing the same number of weeks:
only_23_pick <-
  pickering %>%
  filter((year == 2023 & week >= as.Date('2023-01-07') & # changed to Jan 7 to have same # of weeks
            week <= as.Date('2023-04-25')))

only_19_pick <-
  pickering %>%
  filter((year == 2019 & week >= as.Date('2019-01-01') &
            week <= as.Date('2019-04-25')))

n_distinct(only_23_pick$week_num)
n_distinct(only_19_pick$week_num) # yes :)

# # Which provider to use? Plot each one over time
# da_plot <- da0 %>%
#   filter(da %in% pickering$da) %>%
#   ggplot(aes(x = date, y = normalized, group = provider, color = provider,
#              alpha = .8)) +
#   geom_line() +
#   theme_bw()
# 
# ggplotly(da_plot)

# Look at DAs individually
da_plot2 <- pickering %>%
  filter(week >= as.Date('2019-01-01')) %>%
  mutate(normalized = n_devices/userbase) %>%
  ggplot(aes(x = week, y = normalized, group = da, color = da,
             alpha = .8)) +
  geom_line() +
  theme_bw()

ggplotly(da_plot2)

pickering1 <-
  pickering %>%
  filter((year == 2023 & week >= as.Date('2023-01-07') &
            week <= as.Date('2023-04-25')) |
           (year == 2019 & week >= as.Date('2019-01-01') &
              week <= as.Date('2019-04-25'))) %>%
  group_by(da, year) %>%
  summarize(n_devices = sum(n_devices, na.rm = T),
            userbase = sum(userbase, na.rm = T)) %>%
  ungroup() %>%
  mutate(normalized = n_devices/userbase) %>%
  select(-c(n_devices, userbase)) %>%
  pivot_wider(
    names_from = 'year',
    names_prefix = 'normalized_',
    values_from = 'normalized'
  ) %>%
  mutate(rate = normalized_2023/normalized_2019) %>%
  filter(!is.na(da)) %>%
  data.frame()

head(pickering1)

# Join spatial data with device count data.

nrow(da_sf)
nrow(pickering1)

summary(pickering1$rate)
# getJenksBreaks(pickering1$rate, 7)

nrow(da_sf)
nrow(pickering1)

pickering_final <-
  left_join(da_sf, pickering1 %>% st_drop_geometry(), by = 'da') %>%
  mutate(
    rate_cat = factor(case_when(
      rate < .5 ~ '<50%',
      rate < 1 ~ '50 - 99%',
      rate < 1.5 ~ '100 - 149%',
      rate < 2 ~ '150 - 199%',
      TRUE ~ '200%+'
    ),
    levels = c('<50%', '50 - 99%', '100 - 149%', '150 - 199%', '200%+'))) %>%
  filter(!is.na(rate))

nrow(pickering_final)
head(pickering_final)

pal <- c(
  "#e41822",
  "#faa09d",
  "#5bc4fb",
  "#2c92d7",
  "#0362b0"
)

basemap_p <-
  get_stamenmap(
    bbox = c(left = -79.32,
             bottom = 43.77,
             right = -78.92,
             top = 44.1),
    zoom = 11,
    maptype = "terrain-lines") # https://r-graph-gallery.com/324-map-background-with-the-ggmap-library.html

basemap_attributes_p <- attributes(basemap_p)

basemap_transparent_p <- matrix(adjustcolor(basemap_p, alpha.f = 0.4),
                              nrow = nrow(basemap_p))

attributes(basemap_transparent_p) <- basemap_attributes_p

pickering_map <-
  ggmap(basemap_transparent_p) +
  geom_sf(data = pickering_final,
          aes(fill = rate_cat),
          inherit.aes = FALSE,
          alpha = .7,
          color = 'black',
          size = .5) +
  ggtitle('Recovery rate of Dissemination Areas in\nPickering, March 1 - April 25 (2023 versus 2019)') +
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

pickering_map

#-----------------------------------------
# Map recovery rates by municipality
#-----------------------------------------

gta <-
  da_muni %>%
  st_drop_geometry() %>%
  group_by(municipality, year, week_num) %>%
  summarize(n_devices = sum(n_devices, na.rm = T),
            userbase = sum(userbase, na.rm = T)) %>%
  ungroup() %>%
  mutate(week = as.Date(
    paste(as.character(year), as.character(week_num), 1, sep = '_'),
    format = '%Y_%W_%w'))

# Make sure I'm comparing the same number of weeks:
only_23_gta <-
  gta %>%
  filter((year == 2023 & week >= as.Date('2023-01-07') &
            week <= as.Date('2023-04-25')))

only_19_gta <-
  gta %>%
  filter((year == 2019 & week >= as.Date('2019-01-01') &
            week <= as.Date('2019-04-25')))

n_distinct(only_23_gta$week_num)
n_distinct(only_19_gta$week_num) # yes :)

gta1 <-
  gta %>%
  filter((year == 2023 & week >= as.Date('2023-01-07') &
            week <= as.Date('2023-04-25')) |
           (year == 2019 & week >= as.Date('2019-01-01') &
              week <= as.Date('2019-04-25'))) %>%
  group_by(municipality, year) %>%
  summarize(n_devices = sum(n_devices, na.rm = T),
            userbase = sum(userbase, na.rm = T)) %>%
  mutate(normalized = n_devices/userbase) %>%
  select(-c(n_devices, userbase)) %>%
  ungroup() %>%
  pivot_wider(
    names_from = 'year',
    names_prefix = 'normalized_',
    values_from = 'normalized'
  ) %>%
  mutate(rate = normalized_2023/normalized_2019) %>%
  filter(!is.na(municipality)) %>%
  data.frame()

head(gta1)
summary(gta1)

# Join spatial data with device count data.

nrow(muni2)
nrow(gta1)

summary(gta1$rate)
# getJenksBreaks(gta1$rate, 7)

gta_final <-
  left_join(muni2, gta1) %>%
  mutate(
    rate_cat = factor(case_when(
      rate < .5 ~ '<50%',
      rate < 1 ~ '50 - 99%',
      rate < 1.5 ~ '100 - 149%',
      rate < 2 ~ '150 - 199%',
      TRUE ~ '200%+'
    ),
    levels = c('<50%', '50 - 99%', '100 - 149%', '150 - 199%', '200%+')))

nrow(gta_final)
head(gta_final)

basemap_gta <-
  get_stamenmap(
    bbox = c(left = -80.2,
             bottom = 43.25,
             right = -78.35,
             top = 44.52),
    zoom = 11,
    maptype = "terrain-lines") # https://r-graph-gallery.com/324-map-background-with-the-ggmap-library.html

basemap_attributes_gta <- attributes(basemap_gta)

basemap_transparent_gta <- matrix(adjustcolor(basemap_gta, alpha.f = 0.6),
                              nrow = nrow(basemap_gta))

attributes(basemap_transparent_gta) <- basemap_attributes_gta

gta_final_new <- cbind(gta_final, 
                       st_coordinates(st_centroid(gta_final)) %>% data.frame()) 

head(gta_final_new)

gta_map <-
  ggmap(basemap_transparent_gta) +
  geom_sf(data = gta_final_new,
          aes(fill = rate_cat),
          inherit.aes = FALSE,
          alpha = .7,
          color = 'black',
          size = .5) +
  ggtitle('Recovery rate of Municipalities in Greater\nToronto Area, March 1 - April 25 (2023 versus 2019)') +
  scale_fill_manual(values = pal, name = 'Recovery rate') +
  guides(fill = guide_legend(barwidth = 0.5, barheight = 10,
                             ticks = F, byrow = T)) +
  geom_text(data = gta_final_new, size = 9,
            aes(x = X, y = Y), label = gta_final_new$municipality) +
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

gta_map
