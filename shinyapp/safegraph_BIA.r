library(readxl)
library(stringr)
library(zoo)
library(tidyverse)
library(broom)
library(dplyr)
library(sf)
library(sp)
library(geojsonio)

rm(list = ls())
gc()

# this is 21 million rows- filter it down as you go to save on computational resources/ram/etc
all_canada_device_count_0317 <- read.csv("~/data/downtownrecovery/safegraph/all_canada_device_count_0317.csv")
all_canada_device_count_0317 %>% glimpse()

# filter to just ontario
ontario_device_count_0317 <- all_canada_device_count_0317 %>%
                              filter(substr(as.character(poi_cbg), 1, 2) == '35')

ontario_device_count_0317 %>% glimpse()

# remove the 21 million row df 
rm(all_canada_device_count_0317)
gc()

# the set of placekeys used to query canada from safegraph w/ geocoding
toronto_safegraph_geocoded <- read.csv("~/data/downtownrecovery/safegraph/toronto_data_geocoded.csv")
toronto_safegraph_geocoded %>% glimpse()

geocoded_visits <- ontario_device_count_0317 %>%
                    # useless column
                    select(-X) %>%
                    # inner_join with geocoded data by placekey
                    inner_join(toronto_safegraph_geocoded %>% select(placekey, location_name, address:ExInfo))

geocoded_visits %>% glimpse()

# now turn this into a sf object

geocoded_visits_sf <- geocoded_visits %>%
                        st_as_sf(coords = c("X", "Y"), crs = 4326)

geocoded_visits_sf %>% glimpse()

# the bia shapefile
bia_shapefile <- st_read("~/data/downtownrecovery/BIAs/Business Improvement Areas Data - 4326.shp")
bia_shapefile %>% glimpse()

# now get the subset of placekeys that are in the bia and to which one they belong

geocoded_visits_bia_sf <- read.csv("~/data/downtownrecovery/safegraph/placekey_BIA_st_join.csv")
                            
geocoded_visits_bia_placekey <- geocoded_visits_bia_sf %>% filter(!is.na(X_id1))

bia_safegraph <- ontario_device_count_0317 %>%
                  select(-X) %>%
                  inner_join(geocoded_visits_bia_placekey %>% select(-X) %>% distinct(placekey, .keep_all = TRUE), by = 'placekey') %>%
                  select(placekey:score, Addr_type, DisplayX, DisplayY, X_id1:OBJECTI10) %>%
                  distinct()

bia_safegraph %>% glimpse()

write.csv(bia_safegraph, "~/data/downtownrecovery/safegraph_toronto_BIA.csv")
# details the minor cleaning and transformations of csvs downloaded from databricks 
# for the sake of recordkeeping and reproducability

#study_das$country <- "CAN"
#write.csv(study_das, paste0(input_path, "input_data/study_area_canada.csv"))

#study_postal_codes$country <- "USA"
#write.csv(study_postal_codes, paste0(input_path, "input_data/study_area_us.csv"))
#all_city_index <- rbind(study_das %>% dplyr::select(-X), study_postal_codes)
#write.csv(all_city_index, paste0(input_path, "input_data/all_city_index.csv"))

#can_shapefile <- st_transform(can_shapefile, st_crs("+proj=longlat +datum=WGS84"))
#(can_shapefile) <- c("postal_code", "geometry")
#us_shapefile <- st_transform(us_shapefile, st_crs("+proj=longlat +datum=WGS84"))
#colnames(us_shapefile) <- c("postal_code", "geometry")
#all_shapefile <- rbind(can_shapefile, us_shapefile)








