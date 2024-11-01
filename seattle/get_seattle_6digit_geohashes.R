#===============================================================================
# Create list of all 6-digit geohashes that intersect with Seattle
#===============================================================================

# Load packages
#=====================================

source('~/git/timathomas/functions/functions.r')
ipak(c('sf', 'geohashTools', 'tigris', 'tidyverse', 'leaflet'))

# Get shapefile of Seattle
#=====================================

s <- places(state = 'WA') %>% 
  filter(NAME == 'Seattle') %>% 
  select(geometry) %>%
  st_transform(crs = 4326)

s

# Get geohashes that intersect
#=====================================

gh_s <- gh_covering(s)
gh_s
plot(gh_s)

gh_s <- rownames_to_column(gh_s, var = "geohashid")

# Plot
#=====================================

leaflet() %>%
  addProviderTiles("CartoDB.Positron") %>%
  addPolygons(
    data = s,
    color = "blue", # Outline color
    fillColor = "blue", # Fill color
    fillOpacity = 0.5, # Transparency for fill
    opacity = 0.7 # Transparency for outline
  ) %>%
  addPolygons(
    data = gh_s,
    color = "black", # Outline color
    fillOpacity = 0, # Transparency for fill
    weight = .5,
    opacity = 0.7 # Transparency for outline
  )

# Export
#=====================================

write.csv(gh_s %>% select(geohashid) %>% st_drop_geometry(),
          '/Users/jpg23/data/downtownrecovery/seattle/seattle_6digit_geohashes.csv',
          row.names = F)
