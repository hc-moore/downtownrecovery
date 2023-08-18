#===============================================================================
# Combine all of Byeonghwa's downtown polygons into one file
#===============================================================================

# Overall folder
data_folder <- "C:/Users/jpg23/data/downtownrecovery/sensitivity_analysis/new_downtowns/byeonghwa_downtowns"

# List of files within folder
shp_files <- list.files(data_folder, full.names = TRUE, pattern = "\\.gpkg$")

shp_files

# Create empty list
shp_list <- list()

# For each subfolder in list...
for (filename in shp_files) {
  
  #...read in the '.gpkg' file in the folder
  gpkg <- st_read(filename)
    
  #...add city name and select only that variable...
  with_city <- gpkg %>%
    st_transform(4326) %>%
    mutate(city0 = str_match(filename, '\\/downtow?n_(.*)\\.gpkg$')[2],
           city = case_when(
             city0 == 'sf' ~ 'San Francisco',
             city0 == 'cl' ~ 'Cleveland',
             city0 == 'pt' ~ 'Portland',
             city0 == 'st' ~ 'Salt Lake City',
             city0 == 'to' ~ 'Toronto',
             TRUE ~ NA_character_
           )) %>%
    select(city)
  
  #...add this shapefile to the list of files
  shp_list[[filename]] <- with_city
}

# Combine all stacked shapefiles into a single sf object
all_stacked_sf <- do.call(rbind, shp_list)

unique(all_stacked_sf$city)

st_write(all_stacked_sf, paste0(data_folder, '/byeonghwa_sf_cl_p_slc_t.geojson'))
