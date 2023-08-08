#===============================================================================
# Combine all of Sarah's city-defined downtown polygons into one file
#===============================================================================

# Overall folder
data_folder <- "C:/Users/jpg23/data/downtownrecovery/sensitivity_analysis/new_downtowns"

# List of subfolders within overall folder
subfolders <- list.dirs(data_folder, full.names = TRUE, recursive = FALSE)

# Subset subfolders to ones that start with 'citydefined_'
citydefined_subfolders <- subfolders[regexpr("^citydefined_", subfolders)]

# Create empty list
shp_list <- list()

# For each subfolder in list...
for (subfolder in citydefined_subfolders) {
  
  #...list all '.shp' files in subfolder...
  shp_files <- list.files(subfolder, pattern = "\\.shp$", full.names = TRUE)
  
  #...if there's at least 1 '.shp' file...
  if (length(shp_files) > 0) {
    
    #...read in all '.shp' files in the folder
    sf_objects <- lapply(shp_files, st_read)
    
    #...stack them all together...
    stacked_sf <- do.call(rbind, sf_objects) %>% 
      st_transform(4326) %>%
      mutate(city = str_match(subfolder, 'citydefined_(.*)$')[2]) %>%
      select(city)
    
    #...add this stacked shapefile to the overall list (where each item in list
    # is from a different subfolder)
    shp_list[[subfolder]] <- stacked_sf
  }
}

# Combine all stacked shapefiles into a single sf object
all_stacked_sf <- do.call(rbind, shp_list)

unique(all_stacked_sf$city)

st_write(all_stacked_sf, paste0(data_folder, '/all_citydefined.geojson'))
