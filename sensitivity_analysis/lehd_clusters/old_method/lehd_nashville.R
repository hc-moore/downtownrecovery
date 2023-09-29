################################################################################
# Compare LEHD jobs concentration areas with downtown polygons currently used 
# for Downtown Recovery website in Nashville. LEHD data downloaded at 
# https://lehd.ces.census.gov/data/#lodes.
#
# Author: Julia Greenberg
# Date: 8.18.23
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

dp_n <- dp %>% filter(city == 'Nashville')

dp_n

# Load LEHD data
#=====================================

# See https://github.com/urban-displacement/edr-ca/blob/8de94d59dede0381c83e2631f71004ed70ac370a/code/d8_merge_model_data.R#L53
# for code to load in data for multiple states at once.

n_raw <- fread("C:/Users/jpg23/data/downtownrecovery/sensitivity_analysis/lehd/tn_wac_S000_JT00_2019.csv.gz") %>%
  select(block = w_geocode, jobs = C000) %>%
  mutate(block = as.character(block)) %>%
  filter(substr(block, 1, 5) == '47037')

glimpse(n_raw)
table(nchar(n_raw$block))

# Aggregate to block group level
#=====================================

n_agg <- n_raw %>% 
  mutate(blgr = substr(block, 1, 12)) %>%
  group_by(blgr) %>%
  summarize(jobs = sum(jobs, na.rm = T)) %>% 
  data.frame()

glimpse(n_agg)

n_agg %>% filter(jobs == 0 | is.na(jobs))

# Join with shapefile
#=====================================

n_blgr0 <- block_groups(state = 'TN', county = 'Davidson', year = 2019) %>% 
  select(blgr = GEOID, ALAND) 

n_blgr <- n_blgr0 %>%
  cbind(st_coordinates(st_centroid(n_blgr0$geometry))) %>%
  select(-c(X, Y))

glimpse(n_blgr)

leaflet() %>%
  addPolygons(
    data = n_blgr,
    label = ~blgr,
    labelOptions = labelOptions(textsize = "12px")
  )

n0 <- n_blgr %>% left_join(n_agg) %>% mutate(job_dens = jobs/ALAND)

n <- n0 %>%
  mutate(
    jobs_avg = mean(n0$job_dens, na.rm = T),
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

glimpse(n)

# Create clusters
#=====================================

# See documentation on SKATER algorithm:
# https://www.dshkol.com/post/spatially-constrained-clustering-and-regionalization/

n_simp <- n %>% 
  filter(!blgr %in% c('470370128022', '470370128023')) %>%
  select(job_dens) %>%
  filter(!is.na(job_dens))

head(n_simp)

n_scale <- n %>% 
  filter(!blgr %in% c('470370128022', '470370128023')) %>%
  select(blgr, job_dens) %>% 
  st_drop_geometry() %>%
  mutate(job_dens = scale(job_dens)) %>%
  filter(!is.na(job_dens))

class(n_scale)
head(n_scale)

n_nb <- poly2nb(as_Spatial(n_simp), queen = F) # , snap = 1000

# Create adjacency neighbor structure
plot(as_Spatial(n_simp), main = "Neighbors (without queen)")
plot(n_nb, coords = coordinates(as_Spatial(n_simp)), col="red", add = TRUE)

# Calculate edge costs based on statistical distance between each node
costs <- nbcosts(n_nb, data = n_scale[,-1])

# Transform edge costs into spatial weights to supplement neighbor list
n_w <- nb2listw(n_nb, costs, style = "B")

# Create minimal spanning tree that turns adjacency graph into subgraph
# with n nodes and n-1 edges
n_mst <- mstree(n_w)

# Plot minimal spanning tree
plot(n_mst, coordinates(as_Spatial(n_simp)), col="blue", cex.lab = 0.5)
plot(as_Spatial(n_simp), add=TRUE)

pal <-
  colorFactor(c(
    "white",
    "#9ce4f6",
    "#00b2f9",
    "#0077f7",
    "#0123d1"
  ),
  domain = n$jobs_cat,
  na.color = 'transparent'
  )

# Map 10 clusters
#=====================================

# Partition the minimal spanning tree to create 10 clusters
clus10 <- skater(edges = n_mst[,1:2], data = n_scale[,-1], ncuts = 9)

with_clust10 <- n_simp %>% 
  mutate(clus = clus10$groups) %>%
  mutate(job_lab = paste0(format(job_dens, big.mark = ','), ' jobs per square mile'))

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
    data = n,
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
    data = n,
    position = "bottomright",
    pal = pal,
    values = ~jobs_cat,
    group = "lehd",
    title = "LEHD (2019)",
    className = 'info legend lehd'
  ) %>%
  addPolylines(
    data = dp_n,
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
  'C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/lehd_jobdensity_clusters_10_n.html')
