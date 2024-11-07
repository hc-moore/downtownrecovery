#===============================================================================
# Create maps showing visits by home location type in SF commercial districts
#===============================================================================

# Load packages
#----------------------------------------------------------------

source('~/git/timathomas/functions/functions.r')

ipak(c('tidyverse', 'lubridate', 'ggplot2', 'tigris', 'sf', 'ggspatial', 'scales'))

# Load visit district/home location data
#----------------------------------------------------------------

filepath <- '/Users/jpg23/data/downtownrecovery/spectus_exports/SF_commercial_home/'

# May 1, 2023 - May 1, 2024
comm_home <-
  list.files(path = filepath) %>%
  map_df(~read_delim(
    paste0(filepath, .),
    delim = '\001',
    col_names = c('visit_district', 'home_bg', 'processing_date', 'stops'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(processing_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-processing_date)

head(comm_home)
range(comm_home$date)

total <- comm_home %>%
  group_by(visit_district, home_bg) %>%
  summarize(stops = sum(stops, na.rm = T)) %>%
  data.frame() %>%
  mutate(home_bg = paste0('06', str_remove_all(substr(home_bg, 7, 18), '\\.')))

head(total)

# Load spatial data
#----------------------------------------------------------------

# Load SF commercial districts polygon
sf_comm <- st_read('/Users/jpg23/data/downtownrecovery/shapefiles/commercial_districts/san_francisco_commercial_districts.geojson') %>%
  select(district)

sf_comm$area <- st_area(sf_comm %>% st_make_valid())

head(sf_comm)

# Load block group data for SF
sf_bg <- block_groups(state = 'CA', county = 'San Francisco') %>% select(GEOID)
head(sf_bg)

# For each block group in SF, get a list of all the commercial districts that:
#   (a) intersect with it
#   (b) are within X radius of it
#----------------------------------------------------------------

# Create a half-mile buffer
sf_comm_utm <- st_transform(sf_comm, crs = 32610)
buffer_comm_utm <- st_buffer(sf_comm_utm %>% st_make_valid(), dist = 804.672)
buffer_comm <- st_transform(buffer_comm_utm, crs = 4326)

ggplot() +
  geom_sf(data = buffer_comm, fill = "yellow") +
  geom_sf(data = sf_comm, fill = "blue") +
  theme_minimal()

# # (a) Which districts intersect with each block group?
# #----------------------------------------------------------------
# 
# st_crs(sf_comm) == st_crs(sf_bg)
# sf_bg1 <- sf_bg %>% st_transform(crs = st_crs(sf_comm))
# st_crs(sf_comm) == st_crs(sf_bg1)
# 
# intersections <- st_intersects(sf_bg1, sf_comm %>% st_make_valid())
# 
# # Create a data frame with GEOID and the intersecting district values
# intersections1 <- data.frame(
#   GEOID = sf_bg1$GEOID[rep(1:length(intersections), lengths(intersections))],
#   district = sf_comm$district[unlist(intersections)]
# ) %>%
#   mutate(overlapping = 'yes')
# 
# head(intersections1)
# 
# ggplot() +
#   geom_sf(data = sf_bg %>% filter(GEOID == '060750126021'), fill = "gold",
#           alpha = .8) +
#   geom_sf(data = sf_comm %>% 
#             filter(district %in% 
#                      c('NEIGHBORHOOD COMMERCIAL, SHOPPING CENTER - 19586',
#                        'NEIGHBORHOOD COMMERCIAL, SMALL SCALE - 19572')), 
#           fill = "darkgreen", alpha = .8) +
#   theme_minimal()

# (a) Which buffered districts intersect with each block group?
#----------------------------------------------------------------

st_crs(buffer_comm) == st_crs(sf_bg1)

intersections_buff <- st_intersects(sf_bg1, buffer_comm)

# Create a data frame with GEOID and the intersecting district values
intersections_buff1 <- data.frame(
  GEOID = sf_bg1$GEOID[rep(1:length(intersections_buff), 
                           lengths(intersections_buff))],
  district = buffer_comm$district[unlist(intersections_buff)]
) %>%
  mutate(within_half_mile = 'yes')

head(intersections_buff1)

ggplot() +
  geom_sf(data = buffer_comm %>% 
            filter(district %in% 
                     c('NEIGHBORHOOD COMMERCIAL, SHOPPING CENTER - 19586',
                       'NEIGHBORHOOD COMMERCIAL, SMALL SCALE - 19572',
                       'NEIGHBORHOOD COMMERCIAL, CLUSTER - 19783',
                       'NEIGHBORHOOD COMMERCIAL, SMALL SCALE - 19548',
                       'NEIGHBORHOOD COMMERCIAL, SMALL SCALE - 19549',
                       'RESIDENTIAL- COMMERCIAL, MEDIUM DENSITY - 19510',
                       'UNION STREET NEIGHBORHOOD COMMERCIAL - 20478',
                       'NEIGHBORHOOD COMMERCIAL, SMALL SCALE - 19572',
                       'NEIGHBORHOOD COMMERCIAL, MODERATE SCALE - 19573',
                       "Fisherman's Wharf")), 
          fill = "darkgreen", alpha = .8) +
  geom_sf(data = sf_bg %>% filter(GEOID == '060750126021'), fill = "gold",
          alpha = .8) +
  theme_minimal()

# Determine spatial proximity for each block group / district
# combination
#----------------------------------------------------------------

total_int <- total %>%
  # left_join(intersections1, 
  #           by = c('home_bg' = 'GEOID', 'visit_district' = 'district')) %>%
  left_join(intersections_buff1, 
            by = c('home_bg' = 'GEOID', 'visit_district' = 'district'))

head(total_int)
# head(total_int %>% filter(!is.na(overlapping)))

#-----------------------
# How does stops/area compare for Tenderloin vs. another (e.g., Mission)?
total_stops_area <- total_int %>%
  group_by(visit_district) %>%
  summarize(stops = sum(stops, na.rm = T)) %>%
  data.frame() %>%
  arrange(desc(stops))

head(total_stops_area, 15)

#-----------------------

which_type <- total_int %>%
  mutate(how_far = case_when(
    # overlapping == 'yes' ~ 'overlap',
    within_half_mile == 'yes' ~ 'half_mile',
    substr(home_bg, 3, 5) == '075' ~ 'sf',
    TRUE ~ 'outside_sf'
  )
  ) %>%
  select(visit_district, how_far, stops) %>%
  group_by(visit_district, how_far) %>%
  summarize(stops = sum(stops, na.rm = T)) %>%
  data.frame()

head(which_type)
table(which_type$how_far)

# How many total stops for each type of visitor?
tot_stops_type <- which_type %>%
  group_by(how_far) %>%
  summarize(stops = sum(stops, na.rm = T)) %>%
  data.frame()

ggplot(data=tot_stops_type, aes(x=how_far, y=stops)) +
  geom_bar(stat="identity") +
  coord_flip() +
  theme_minimal() +
  theme(axis.title = element_blank()) +
  scale_y_continuous(label=comma)

# Join back to spatial district data
for_maps <- sf_comm %>%
  left_join(which_type, by = c('district' = 'visit_district')) %>%
  mutate(num_points = stops %/% 100,
         stops_per_m2 = as.numeric(stops/area)) %>%
  st_make_valid()

head(for_maps)

# Create maps
#----------------------------------------------------------------

# Get bounding box for San Francisco
san_francisco_bbox <- st_bbox(
  c(xmin = -122.52, ymin = 37.7, xmax = -122.35, ymax = 37.82), 
  crs = st_crs(4326))

san_francisco <- st_as_sfc(san_francisco_bbox)

map_path <- '/Users/jpg23/UDP/downtown_recovery/commercial_districts/commercial_home_locations/'

# # Stops from overlapping home block groups
# #------------------------------------------
# 
# overlap <- for_maps %>% filter(how_far == 'overlap')
# 
# # Generate random points for each polygon
# overlap_pts <- lapply(1:nrow(overlap), function(i) {
#   if (overlap$num_points[i] > 0) {
#     st_sample(overlap[i, ], size = overlap$num_points[i], type = "random")
#   } else {
#     NULL
#   }
# })
# 
# # Filter out NULL values and flatten the list of points
# overlap_pts_flat <- do.call(c, overlap_pts)
# 
# # Create an sf object from the points
# overlap_pts_sf <- st_sf(geometry = overlap_pts_flat)
# 
# st_write(overlap_pts_sf, paste0(map_path, 'overlap.geojson'))
# 
# # head(overlap_pts_sf)
# 
# overlap_map <- ggplot(overlap_pts_sf) +
#   annotation_map_tile(type = "cartolight", zoom = 12) +
#   geom_sf(color = "#075E62", alpha = .2, size = .3) +
#   theme_minimal() +
#   coord_sf(xlim = c(st_bbox(san_francisco)[1], st_bbox(san_francisco)[3]),
#            ylim = c(st_bbox(san_francisco)[2], st_bbox(san_francisco)[4]),
#            expand = FALSE) +
#   theme(axis.text.x = element_blank(),
#         axis.text.y = element_blank()) +
#   ggtitle('Visitor lives in block group that intersects with commercial district')
# 
# ggsave(plot = overlap_map,
#        filename = paste0(map_path, 'overlap.png'))

# Stops from home block groups within .5 miles
#------------------------------------------

half_mile <- for_maps %>% filter(how_far == 'half_mile')

st_write(half_mile, paste0(map_path, 'half_mile_polygons.geojson'))

# Generate random points for each polygon
half_mile_pts <- lapply(1:nrow(half_mile), function(i) {
  if (half_mile$num_points[i] > 0) {
    st_sample(half_mile[i, ], size = half_mile$num_points[i], type = "random")
  } else {
    NULL
  }
})

# Filter out NULL values and flatten the list of points
half_mile_pts_flat <- do.call(c, half_mile_pts)

# Create an sf object from the points
half_mile_pts_sf <- st_sf(geometry = half_mile_pts_flat)

st_write(half_mile_pts_sf, paste0(map_path, 'half_mile_inclusive.geojson'))

# head(half_mile_pts_sf)

half_mile_map <- ggplot(half_mile_pts_sf) +
  annotation_map_tile(type = "cartolight", zoom = 12) +
  geom_sf(color = "#075E62", alpha = .2, size = .3) +
  theme_minimal() +
  coord_sf(xlim = c(st_bbox(san_francisco)[1], st_bbox(san_francisco)[3]),
           ylim = c(st_bbox(san_francisco)[2], st_bbox(san_francisco)[4]),
           expand = FALSE) +
  theme(axis.text.x = element_blank(),
        axis.text.y = element_blank()) +
  ggtitle('Visitor lives in a block group within half a mile of - but not intersecting with - commercial district')

ggsave(plot = half_mile_map,
       filename = paste0(map_path, 'half_mile.png'))

# Stops from home block groups elsewhere in SF
#------------------------------------------

sf <- for_maps %>% filter(how_far == 'sf')

st_write(sf, paste0(map_path, 'SF_polygons.geojson'))

# Generate random points for each polygon
sf_pts <- lapply(1:nrow(sf), function(i) {
  if (sf$num_points[i] > 0) {
    st_sample(sf[i, ], size = sf$num_points[i], type = "random")
  } else {
    NULL
  }
})

# Filter out NULL values and flatten the list of points
sf_pts_flat <- do.call(c, sf_pts)

# Create an sf object from the points
sf_pts_sf <- st_sf(geometry = sf_pts_flat)

st_write(sf_pts_sf, paste0(map_path, 'SF.geojson'))

# head(sf_pts_sf)

sf_map <- ggplot(sf_pts_sf) +
  annotation_map_tile(type = "cartolight", zoom = 12) +
  geom_sf(color = "#075E62", alpha = .2, size = .3) +
  theme_minimal() +
  coord_sf(xlim = c(st_bbox(san_francisco)[1], st_bbox(san_francisco)[3]),
           ylim = c(st_bbox(san_francisco)[2], st_bbox(san_francisco)[4]),
           expand = FALSE) +
  theme(axis.text.x = element_blank(),
        axis.text.y = element_blank()) +
  ggtitle('Visitor lives elsewhere in San Francisco')

ggsave(plot = sf_map,
       filename = paste0(map_path, 'elsewhere_in_SF.png'))

# Stops from home block groups outside SF
#------------------------------------------

outside_sf <- for_maps %>% filter(how_far == 'outside_sf')

st_write(outside_sf, paste0(map_path, 'outside_sf_polygons.geojson'))

# Generate random points for each polygon
outside_sf_pts <- lapply(1:nrow(outside_sf), function(i) {
  if (outside_sf$num_points[i] > 0) {
    st_sample(outside_sf[i, ], size = outside_sf$num_points[i], type = "random")
  } else {
    NULL
  }
})

# Filter out NULL values and flatten the list of points
outside_sf_pts_flat <- do.call(c, outside_sf_pts)

# Create an sf object from the points
outside_sf_pts_sf <- st_sf(geometry = outside_sf_pts_flat)

st_write(outside_sf_pts_sf, paste0(map_path, 'outside_SF.geojson'))

# head(outside_sf_pts_sf)

outside_SF_map <- ggplot(outside_sf_pts_sf) +
  annotation_map_tile(type = "cartolight", zoom = 12) +
  geom_sf(color = "#075E62", alpha = .2, size = .3) +
  theme_minimal() +
  coord_sf(xlim = c(st_bbox(san_francisco)[1], st_bbox(san_francisco)[3]),
           ylim = c(st_bbox(san_francisco)[2], st_bbox(san_francisco)[4]),
           expand = FALSE) +
  theme(axis.text.x = element_blank(),
        axis.text.y = element_blank()) +
  ggtitle('Visitor lives outside of San Francisco')

ggsave(plot = outside_SF_map,
       filename = paste0(map_path, 'outside_SF.png'))
