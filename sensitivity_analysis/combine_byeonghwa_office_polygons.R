#===============================================================================
# Combine all of Byeonghwa's office-only downtown polygons into one file
#===============================================================================

# Overall folder
data_folder <- "C:/Users/jpg23/data/downtownrecovery/sensitivity_analysis/new_downtowns/byeonghwa_office_districts"

# List of files within folder
shp_files <- list.files(data_folder, full.names = TRUE, pattern = "\\.gpkg$")

shp_files

# Create empty list
shp_list <- list()

# For each file in list...
for (filename in shp_files) {
  
  if (str_detect(filename, '_to\\.') == FALSE) {
    
    #...read in the '.gpkg' file in the folder
    gpkg <- st_read(filename)
    
    #...add city name and select only that variable...
    with_city <- gpkg %>%
      st_transform(4326) %>%
      mutate(city0 = str_match(filename, 'office_district_(.*)\\.gpkg$')[2],
             city = case_when(
               city0 == 'sf' ~ 'San Francisco',
               city0 == 'cl' ~ 'Cleveland',
               city0 == 'pt' ~ 'Portland',
               city0 == 'salt' ~ 'Salt Lake City',
               city0 == 'cal' ~ 'Calgary',
               city0 == 'ed' ~ 'Edmonton',
               city0 == 'mon' ~ 'Montreal',
               city0 == 'ot' ~ 'Ottawa',
               city0 == 'van' ~ 'Vancouver',
               TRUE ~ NA_character_
             )) %>%
      select(city)
    
    #...add this shapefile to the list of files
    shp_list[[filename]] <- with_city    
  }
}

# Read in second layer of Toronto file
to <- st_read(paste0(data_folder, "/office_district_to.gpkg"), 
              layer = 'office_district_to') %>%
  st_transform(4326) %>%
  mutate(city = 'Toronto') %>%
  select(city)

head(to)
plot(to$geom)

# Combine all stacked shapefiles into a single sf object
all_stacked_sf <- do.call(rbind, shp_list) %>% rbind(to)

unique(all_stacked_sf$city)

st_write(all_stacked_sf, paste0(data_folder, '/byeonghwa_office.geojson'))
