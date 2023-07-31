################################################################################
# Compare LEHD jobs concentration areas with downtown polygons currently used 
# for Downtown Recovery website in San Francisco. LEHD data downloaded at 
# https://lehd.ces.census.gov/data/#lodes.
#
# Author: Julia Greenberg
# Date: 7.19.23
################################################################################

# Load packages
#=====================================

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'sf', 'sp', 'data.table', 'tigris', 'leaflet', 'spdep',
       'htmlwidgets'))

# Load current downtown polygons
#=====================================

dp <- st_read("C:/Users/jpg23/data/downtownrecovery/sensitivity_analysis/current/study_area_downtowns.shp")

head(dp)

dp_sf <- dp %>% filter(city == 'San Francisco')

dp_sf

# Load LEHD data
#=====================================

# See https://github.com/urban-displacement/edr-ca/blob/8de94d59dede0381c83e2631f71004ed70ac370a/code/d8_merge_model_data.R#L53
# for code to load in data for multiple states at once.

sf_raw <- fread("C:/Users/jpg23/data/downtownrecovery/sensitivity_analysis/lehd/ca_wac_S000_JT00_2019.csv.gz") %>%
  select(block = w_geocode, jobs = C000) %>%
  mutate(block = paste0('0', as.character(block))) %>%
  filter(substr(block, 1, 5) == '06075')

glimpse(sf_raw)
table(nchar(sf_raw$block))

# # Aggregate to census tract level
# #=====================================
# 
# sf_agg <- sf_raw %>% 
#   mutate(tract = substr(block, 1, 11)) %>%
#   group_by(tract) %>%
#   summarize(jobs = sum(jobs, na.rm = T)) %>% 
#   data.frame()
# 
# glimpse(sf_agg)

# Aggregate to block group level
#=====================================

sf_agg <- sf_raw %>% 
  mutate(blgr = substr(block, 1, 12)) %>%
  group_by(blgr) %>%
  summarize(jobs = sum(jobs, na.rm = T)) %>% 
  data.frame()

glimpse(sf_agg)

# Join with shapefile
#=====================================

# sf_tracts <- tracts(state = 'CA', county = 'San Francisco', year = 2019) %>% 
#   select(tract = GEOID)
# 
# glimpse(sf_tracts)
# 
# sf <- sf_tracts %>% 
#   left_join(sf_agg) %>%
#   mutate(
#     jobs_avg = mean(sf_agg$jobs, na.rm = T),
#     jobs_cat = factor(case_when(
#       jobs < jobs_avg ~ '<100% of city avg.',
#       jobs < 2*jobs_avg ~ '100 - 199% of city avg.',
#       jobs < 4*jobs_avg ~ '200 - 399% of city avg.',
#       jobs < 6*jobs_avg ~ '400 - 599% of city avg.',
#       jobs >= 6*jobs_avg ~ '600%+ of city avg.',
#       TRUE ~ NA_character_
#     ),
#     levels = c('<100% of city avg.', '100 - 199% of city avg.', 
#                '200 - 399% of city avg.', '400 - 599% of city avg.', 
#                '600%+ of city avg.')),
#     jobs_lab = paste0(jobs, ' jobs: ', 100*(round(jobs/jobs_avg, 2)), '% of avg.'))

sf_blgr0 <- block_groups(state = 'CA', county = 'San Francisco', year = 2019) %>% 
  select(blgr = GEOID, ALAND) 

sf_blgr <- sf_blgr0 %>%
  cbind(st_coordinates(st_centroid(sf_blgr0$geometry))) %>%
  filter(X != min(X)) %>% # get rid of outlying western block group
  select(-c(X, Y))

glimpse(sf_blgr)

sf0 <- sf_blgr %>% left_join(sf_agg) %>% mutate(job_dens = jobs/ALAND)

sf <- sf0 %>%
  mutate(
    jobs_avg = mean(sf0$job_dens, na.rm = T),
    jobs_cat = factor(case_when(
      job_dens < jobs_avg ~ '<100% of city avg.',
      job_dens < 2*jobs_avg ~ '100 - 199% of city avg.',
      job_dens < 5*jobs_avg ~ '200 - 499% of city avg.',
      job_dens < 10*jobs_avg ~ '500 - 999% of city avg.',
      job_dens >= 10*jobs_avg ~ '1000%+ of city avg.',
      TRUE ~ NA_character_
    ),
    levels = c('<100% of city avg.', '100 - 199% of city avg.', 
               '200 - 499% of city avg.', '500 - 999% of city avg.', 
               '1000%+ of city avg.')),
    jobs_lab = paste0(job_dens, ' jobs per square meters: ', 100*(round(job_dens/jobs_avg, 2)), '% of avg.'))

# sf <- sf_blgr %>% 
#   left_join(sf_agg) %>%
#   mutate(
#     jobs_avg = mean(sf_agg$jobs, na.rm = T),
#     jobs_cat = factor(case_when(
#       jobs < jobs_avg ~ '<100% of city avg.',
#       jobs < 2*jobs_avg ~ '100 - 199% of city avg.',
#       jobs < 5*jobs_avg ~ '200 - 499% of city avg.',
#       jobs < 10*jobs_avg ~ '500 - 999% of city avg.',
#       jobs >= 10*jobs_avg ~ '1000%+ of city avg.',
#       TRUE ~ NA_character_
#     ),
#     levels = c('<100% of city avg.', '100 - 199% of city avg.', 
#                '200 - 499% of city avg.', '500 - 999% of city avg.', 
#                '1000%+ of city avg.')),
#     jobs_lab = paste0(jobs, ' jobs: ', 100*(round(jobs/jobs_avg, 2)), '% of avg.'))

glimpse(sf)

# Create clusters
#=====================================

# See documentation on SKATER algorithm:
# https://www.dshkol.com/post/spatially-constrained-clustering-and-regionalization/

sf_simp <- sf %>% 
  select(job_dens) %>%
  filter(!is.na(job_dens))
  # select(jobs) %>% 
  # filter(!is.na(jobs))

head(sf_simp)

sf_scale <- sf %>% 
  select(blgr, job_dens) %>% 
  st_drop_geometry() %>%
  mutate(job_dens = scale(job_dens)) %>%
  filter(!is.na(job_dens))
  # select(blgr, jobs) %>% 
  # st_drop_geometry() %>%
  # mutate(jobs = scale(jobs)) %>%
  # filter(!is.na(jobs))

class(sf_scale)
head(sf_scale)

sf_nb <- poly2nb(as_Spatial(sf_simp), queen = F)

# Create adjacency neighbor structure
plot(as_Spatial(sf_simp), main = "Neighbors (without queen)")
plot(sf_nb, coords = coordinates(as_Spatial(sf_simp)), col="red", add = TRUE)

# Calculate edge costs based on statistical distance between each node
costs <- nbcosts(sf_nb, data = sf_scale[,-1])

# Transform edge costs into spatial weights to supplement neighbor list
sf_w <- nb2listw(sf_nb, costs, style = "B")

# Create minimal spanning tree that turns adjacency graph into subgraph
# with n nodes and n-1 edges
sf_mst <- mstree(sf_w)

# Plot minimal spanning tree
plot(sf_mst, coordinates(as_Spatial(sf_simp)), col="blue", cex.lab = 0.5)
plot(as_Spatial(sf_simp), add=TRUE)

pal <-
  colorFactor(c(
    "white",
    "#9ce4f6",
    "#00b2f9",
    "#0077f7",
    "#0123d1"
  ),
  domain = sf$jobs_cat,
  na.color = 'transparent'
  )

# Map 10 clusters
#=====================================

# Partition the minimal spanning tree to create 10 clusters
clus10 <- skater(edges = sf_mst[,1:2], data = sf_scale[,-1], ncuts = 9)

with_clust10 <- sf_simp %>% 
  mutate(clus = clus10$groups) %>%
  mutate(job_lab = paste0(format(job_dens, big.mark = ','), ' jobs per square mile'))
  # mutate(job_lab = paste0(format(jobs, big.mark = ','), ' jobs'))

clustpal10 <-
  colorFactor(c(
    "#88e99a", "#277a35", "#99def9", "#19477d", "#df72ef", "#ad0599", "#7d9af7", 
    "#6f3a93", "#11a0aa", "#cadba5"
  ),
  domain = with_clust10$clus,
  na.color = 'transparent'
  )

cluster_map10 <-
  leaflet(
    options = leafletOptions(minZoom = 6, maxZoom = 16)
  ) %>%
  addMapPane(name = "lehd_pane", zIndex = 410) %>%
  addMapPane(name = "current_pane", zIndex = 420) %>%
  addMapPane(name = "maplabels", zIndex = 430) %>%
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
    baseGroups = c("lehd", "lehdcluster"),
    overlayGroups = c("current"),
    options = layersControlOptions(collapsed = FALSE, maxHeight = 'auto')) %>%
  addPolygons(
    data = sf,
    group = "lehd",
    label = ~jobs_lab,
    labelOptions = labelOptions(textsize = "12px"),
    fillOpacity = .6,
    color = ~pal(jobs_cat),
    stroke = TRUE,
    weight = 1,
    opacity = .3,
    highlightOptions =
      highlightOptions(
        color = "black",
        weight = 3,
        bringToFront = TRUE),
    options = pathOptions(pane = "lehd_pane")
  ) %>%
  addPolygons(
    data = with_clust10,
    group = "lehdcluster",
    label = ~job_lab,
    labelOptions = labelOptions(textsize = "12px"),
    fillOpacity = .8,
    color = ~clustpal10(clus),
    stroke = TRUE,
    weight = 1,
    opacity = .3,
    highlightOptions =
      highlightOptions(
        color = "black",
        weight = 3,
        bringToFront = TRUE),
    options = pathOptions(pane = "lehd_pane")
  )  %>%
  addLegend(
    data = sf,
    position = "bottomright",
    pal = pal,
    values = ~jobs_cat,
    group = "lehd",
    title = "LEHD (2019)",
    className = 'info legend lehd'
  ) %>%
  addPolylines(
    data = dp_sf,
    group = "current",
    fillOpacity = 0,
    stroke = TRUE,
    weight = 4,
    opacity = .8,
    color = 'black',
    highlightOptions =
      highlightOptions(
        color = "white",
        weight = 5,
        bringToFront = TRUE),
    options = pathOptions(pane = "current_pane")
  )

cluster_map10

saveWidget(
  cluster_map10,
  'C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/lehd_jobdensity_clusters_10_sf.html')

# Map 4 clusters
#=====================================

# Partition the minimal spanning tree to create 10 clusters
clus4 <- skater(edges = sf_mst[,1:2], data = sf_scale[,-1], ncuts = 3)

with_clust4 <- sf_simp %>% 
  mutate(clus = clus4$groups) %>%
  mutate(job_lab = paste0(format(job_dens, big.mark = ','), ' jobs per square mile'))
  # mutate(job_lab = paste0(format(jobs, big.mark = ','), ' jobs'))

clustpal4 <-
  colorFactor(c(
    "#cadba5", "#11a0aa", 
    "#19477d", "#df72ef"#, 
    # "#ad0599", "#7d9af7",
    # "#6f3a93", "#11a0aa", "#cadba5"
  ),
  domain = with_clust4$clus,
  na.color = 'transparent'
  )

cluster_map4 <-
  leaflet(
    options = leafletOptions(minZoom = 6, maxZoom = 16)
  ) %>%
  addMapPane(name = "lehd_pane", zIndex = 410) %>%
  addMapPane(name = "current_pane", zIndex = 420) %>%
  addMapPane(name = "maplabels", zIndex = 430) %>%
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
    baseGroups = c("lehd", "lehdcluster"),
    overlayGroups = c("current"),
    options = layersControlOptions(collapsed = FALSE, maxHeight = 'auto')) %>%
  addPolygons(
    data = sf,
    group = "lehd",
    label = ~jobs_lab,
    labelOptions = labelOptions(textsize = "12px"),
    fillOpacity = .6,
    color = ~pal(jobs_cat),
    stroke = TRUE,
    weight = 1,
    opacity = .3,
    highlightOptions =
      highlightOptions(
        color = "black",
        weight = 3,
        bringToFront = TRUE),
    options = pathOptions(pane = "lehd_pane")
  ) %>%
  addPolygons(
    data = with_clust4,
    group = "lehdcluster",
    label = ~job_lab,
    labelOptions = labelOptions(textsize = "12px"),
    fillOpacity = .8,
    color = ~clustpal4(clus),
    stroke = TRUE,
    weight = 1,
    opacity = .3,
    highlightOptions =
      highlightOptions(
        color = "black",
        weight = 3,
        bringToFront = TRUE),
    options = pathOptions(pane = "lehd_pane")
  )  %>%
  addLegend(
    data = sf,
    position = "bottomright",
    pal = pal,
    values = ~jobs_cat,
    group = "lehd",
    title = "LEHD (2019)",
    className = 'info legend lehd'
  ) %>%
  addPolylines(
    data = dp_sf,
    group = "current",
    fillOpacity = 0,
    stroke = TRUE,
    weight = 4,
    opacity = .8,
    color = 'black',
    highlightOptions =
      highlightOptions(
        color = "white",
        weight = 5,
        bringToFront = TRUE),
    options = pathOptions(pane = "current_pane")
  )

cluster_map4

saveWidget(
  cluster_map4,
  'C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/lehd_jobdensity_clusters_4_sf.html')

# Map 6 clusters
#=====================================

# Partition the minimal spanning tree to create 10 clusters
clus6 <- skater(edges = sf_mst[,1:2], data = sf_scale[,-1], ncuts = 5)

with_clust6 <- sf_simp %>% 
  mutate(clus = clus6$groups) %>%
  mutate(job_lab = paste0(format(job_dens, big.mark = ','), ' jobs per square mile'))
# mutate(job_lab = paste0(format(jobs, big.mark = ','), ' jobs'))

clustpal6 <-
  colorFactor(c(
    "#cadba5", "#11a0aa", 
    "#19477d", "#df72ef", 
    "#6f3a93", "#7d9af7"
  ),
  domain = with_clust6$clus,
  na.color = 'transparent'
  )

cluster_map6 <-
  leaflet(
    options = leafletOptions(minZoom = 6, maxZoom = 16)
  ) %>%
  addMapPane(name = "lehd_pane", zIndex = 410) %>%
  addMapPane(name = "current_pane", zIndex = 420) %>%
  addMapPane(name = "maplabels", zIndex = 430) %>%
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
    baseGroups = c("lehd", "lehdcluster"),
    overlayGroups = c("current"),
    options = layersControlOptions(collapsed = FALSE, maxHeight = 'auto')) %>%
  addPolygons(
    data = sf,
    group = "lehd",
    label = ~jobs_lab,
    labelOptions = labelOptions(textsize = "12px"),
    fillOpacity = .6,
    color = ~pal(jobs_cat),
    stroke = TRUE,
    weight = 1,
    opacity = .3,
    highlightOptions =
      highlightOptions(
        color = "black",
        weight = 3,
        bringToFront = TRUE),
    options = pathOptions(pane = "lehd_pane")
  ) %>%
  addPolygons(
    data = with_clust6,
    group = "lehdcluster",
    label = ~job_lab,
    labelOptions = labelOptions(textsize = "12px"),
    fillOpacity = .8,
    color = ~clustpal6(clus),
    stroke = TRUE,
    weight = 1,
    opacity = .3,
    highlightOptions =
      highlightOptions(
        color = "black",
        weight = 3,
        bringToFront = TRUE),
    options = pathOptions(pane = "lehd_pane")
  )  %>%
  addLegend(
    data = sf,
    position = "bottomright",
    pal = pal,
    values = ~jobs_cat,
    group = "lehd",
    title = "LEHD (2019)",
    className = 'info legend lehd'
  ) %>%
  addPolylines(
    data = dp_sf,
    group = "current",
    fillOpacity = 0,
    stroke = TRUE,
    weight = 4,
    opacity = .8,
    color = 'black',
    highlightOptions =
      highlightOptions(
        color = "white",
        weight = 5,
        bringToFront = TRUE),
    options = pathOptions(pane = "current_pane")
  )

cluster_map6

saveWidget(
  cluster_map6,
  'C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/lehd_jobdensity_clusters_6_sf.html')
