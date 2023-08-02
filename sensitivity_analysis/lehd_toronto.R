################################################################################
# Compare jobs concentration areas with downtown polygons currently used 
# for Downtown Recovery website in Toronto.
#
# Author: Julia Greenberg
# Date: 8.1.23
################################################################################

# Load packages
#=====================================

source('~/git/timathomas/functions/functions.r')

ipak(c('tidyverse', 'sf', 'sp', 'data.table', 'tigris', 'leaflet', 'spdep',
       'htmlwidgets', 'cancensus'))

set_cancensus_api_key('CensusMapper_bcd591107b93e609a0bb5415f58cb31b')

select <- dplyr::select

# Load current downtown polygons
#=====================================

dp <- st_read("C:/Users/jpg23/data/downtownrecovery/sensitivity_analysis/current/study_area_downtowns.shp")

head(dp)

dp_t <- dp %>% filter(city == 'Toronto')

dp_t

# Load jobs data
#=====================================

# Downloaded here (link from Jeff): 
# http://odesi2.scholarsportal.info/documentation/CENSUS/2016/cen16labour.html

# Employed Labour Force aged 15 years and over by Place of Work Census Divisions, 
# Census subdivisions and Dissemination areas for Ontario showing Industrial 
# sectors (NAICS 2012), 2016 Census, 25% sample.

t_raw <- read.csv("C:/Users/jpg23/data/downtownrecovery/sensitivity_analysis/toronto_jobs_2016.csv")

glimpse(t_raw)
head(t_raw)

# Delete the aggregate total jobs values in the 'CD'/'CSD' rows
t_agg <- t_raw %>% 
  filter(!str_detect(DAUID, '\\D')) %>% 
  mutate(jobs = as.numeric(str_remove_all(total_jobs, ','))) %>% 
  select(-total_jobs)

head(t_agg)
nrow(t_agg)/nrow(t_raw)
nrow(t_agg %>% filter(is.na(jobs)))/nrow(t_agg)

# Join with shapefile
#=====================================

# Dissemination areas
t_da0 <- st_read('C:/Users/jpg23/data/downtownrecovery/shapefiles/DAs/toronto/gtha-da-21_simplified.geojson') %>%
  mutate(ALAND = as.numeric(st_area(st_make_valid(geometry))))

glimpse(t_da0)
t_da0 %>% filter(is.na(ALAND) | ALAND == 0) # any NAs or 0s in ALAND column?

t_da <- t_da0 %>%
  cbind(st_coordinates(st_centroid(st_make_valid(t_da0$geometry)))) %>%
  select(-c(X, Y))

glimpse(t_da)

leaflet() %>%
  addPolygons(
    data = t_da,
    label = ~DAUID,
    labelOptions = labelOptions(textsize = "12px")
  )

t0 <- t_da %>% left_join(t_agg) %>% mutate(job_dens = jobs/ALAND)

head(t0)

t <- t0 %>%
  mutate(
    jobs_avg = mean(t0$job_dens, na.rm = T),
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

head(t)
table(t$jobs_cat)

# Create clusters
#=====================================

# See documentation on SKATER algorithm:
# https://www.dshkol.com/post/spatially-constrained-clustering-and-regionalization/

t_sub0 <- t %>% filter(!is.na(job_dens))
# 
# t_sub <- 
#   tibble::rowid_to_column(t_sub0, "index") %>%
#   filter(!index %in% c(45, 284, 407, 559, 579, 707, 717, 953, 1407, 1472, 1481,
#                        1935, 2320, 2346, 2864, 2894, 2940, 3003, 3160, 3260,
#                        3320, 3552, 3916, 3979, 4051, 4068, 4085, 4297, 4303,
#                        4357, 4562, 4583, 4585, 4588, 4602, 4675, 4680, 4775,
#                        4834, 4877, 5216, 5259, 5272, 5326, 5540, 5736))
# 
# t_simp <- t_sub %>% select(job_dens)
t_simp <- t_sub0 %>% select(job_dens) %>% st_make_valid()

head(t_simp)
nrow(t_simp)

# t_scale <- t_sub %>%
t_scale <- t_sub0 %>%
  select(DAUID, job_dens) %>% 
  st_drop_geometry() %>%
  mutate(job_dens = scale(job_dens))

class(t_scale)
head(t_scale)
nrow(t_scale)


# overlapmat <- st_overlaps(t_simp, sparse=FALSE)
# ovnb <- mat2listw(overlapmat)
# plot(t_simp$geom);plot(ovnb, st_coordinates(st_centroid(t_simp)),add=TRUE)
# ovnb$neighbours


# Create adjacency neighbor structure
t_nb <- poly2nb(as_Spatial(t_simp), queen = F) # , snap = 1000

plot(as_Spatial(t_simp), main = "Neighbors (without queen)")
plot(t_nb, coords = coordinates(as_Spatial(t_simp)), col="red", add = TRUE)

# Calculate edge costs based on statistical distance between each node
costs <- nbcosts(t_nb, data = t_scale[,-1])

# Transform edge costs into spatial weights to supplement neighbor list
t_w <- nb2listw(t_nb, costs, style = "B")

# Create minimal spanning tree that turns adjacency graph into subgraph
# with n nodes and n-1 edges
t_mst <- mstree(t_w)

# Plot minimal spanning tree
plot(t_mst, coordinates(as_Spatial(t_simp)), col="blue", cex.lab = 0.5)
plot(as_Spatial(t_simp), add=TRUE)


################################################################################
# EVERYTHING BELOW MUST BE ADJUSTED FOR TORONTO


pal <-
  colorFactor(c(
    "white",
    "#9ce4f6",
    "#00b2f9",
    "#0077f7",
    "#0123d1"
  ),
  domain = slc$jobs_cat,
  na.color = 'transparent'
  )

# Map 10 clusters
#=====================================

# Partition the minimal spanning tree to create 10 clusters
clus10 <- skater(edges = slc_mst[,1:2], data = slc_scale[,-1], ncuts = 9)

with_clust10 <- slc_simp %>% 
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
    data = slc,
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
    data = slc,
    position = "bottomright",
    pal = pal,
    values = ~jobs_cat,
    group = "lehd",
    title = "LEHD (2019)",
    className = 'info legend lehd'
  ) %>%
  addPolylines(
    data = dp_slc,
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
  'C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/lehd_jobdensity_clusters_10_slc.html')

# Map 4 clusters
#=====================================

# Partition the minimal spanning tree to create 10 clusters
clus4 <- skater(edges = slc_mst[,1:2], data = slc_scale[,-1], ncuts = 3)

with_clust4 <- slc_simp %>% 
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
    data = slc,
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
    data = slc,
    position = "bottomright",
    pal = pal,
    values = ~jobs_cat,
    group = "lehd",
    title = "LEHD (2019)",
    className = 'info legend lehd'
  ) %>%
  addPolylines(
    data = dp_slc,
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
  'C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/lehd_jobdensity_clusters_4_slc.html')

# Map 6 clusters
#=====================================

# Partition the minimal spanning tree to create 10 clusters
clus6 <- skater(edges = slc_mst[,1:2], data = slc_scale[,-1], ncuts = 5)

with_clust6 <- slc_simp %>% 
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
    data = slc,
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
    data = slc,
    position = "bottomright",
    pal = pal,
    values = ~jobs_cat,
    group = "lehd",
    title = "LEHD (2019)",
    className = 'info legend lehd'
  ) %>%
  addPolylines(
    data = dp_slc,
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
  'C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/lehd_jobdensity_clusters_6_slc.html')
