################################################################################
# adapted from: Create maps and plots using census data for Albuquerque, for RWJF EDDIT.
# will pull city level variables by municipality in downtown recovery study
# Author: Julia Greenberg
# Date: 2023.06.08
# edits: Hannah Moore
################################################################################

# Load packages & API key
#=====================================

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'tidycensus', 'tigris', 'sf', 'plyr','scales', 'stringr'))

census_api_key('b4e4c4e4d51c88141658e8b8499a81b6522f87bb')
options(width=Sys.getenv("COLUMNS"), setWidthOnResize=TRUE)




# load city information
# these are the placeids:
city_placeids <- read.csv("~/data/downtownrecovery/geographies/us_city_placeids.csv", header = FALSE)

names(city_placeids) <- c("city", "state", "STATEFP", "PLACEFP")

city_placeids <- city_placeids %>%
                    mutate(
                        # the original PLACEFP for Louisville, 48003, does not actually exist- this was probably a typo
                        PLACEFP = case_when(city == "Louisville" ~ "48006",
                                            TRUE ~ str_pad(as.character(PLACEFP), side = "left", 5, "0")),
                        STATEFP = str_pad(as.character(STATEFP), side = "left", 2, "0"),
                        GEOID = paste0(STATEFP, PLACEFP)
                    )


city_placeids %>% glimpse()

city_placeids %>%
    filter(nchar(PLACEFP) != 5)
# city names should keep punctuation
# states should be abbreviations
city_info <- read.csv("~/git/downtownrecovery/shinyapp/input_data/regions.csv") %>%
                filter(region != "Canada") %>%
                select(city, display_title) %>%
                mutate(
                        state = str_extract(display_title, "\\S+$")
                                          ) %>%
                left_join(city_placeids)

city_info %>% glimpse()

# check if the left_join worked - this should be zero rows
city_info %>% filter(is.na(PLACEFP))

og_model_data <- read.csv("~/data/downtownrecovery/curated_data/all_model_features_20230714.csv") %>%
                    select(-X, -X.1) %>%
                    inner_join(city_info, by = 'city')
og_model_data %>% glimpse()

# Load ACS variables
#=====================================

# # 2015-2019
v19 <- load_variables(2019, "acs5")

v19 %>% glimpse()

og_vars <- c("B25024_001E","B25024_002E","B25024_003E","B25024_004E","B25024_005E","B25024_006E","B25024_007E","B25024_008E","B25024_009E","B25024_010E","B25024_011E","B25003_001E","B25003_003E","B01002_001E","B15003_001E","B15003_022E","B15003_023E","B15003_024E","B15003_025E","B19013_001E","B25064_001E","B25002_001E","B25002_002E","B25002_003E","B03002_001E","B03002_003E","B03002_004E","B03002_006E","B03002_012E","B08301_001E","B08301_002E","B08301_010E","B08301_016E","B08301_017E","B08301_018E","B08301_019E","B08301_020E","B08301_021E","B08135_001E","B08303_001E")

length(og_vars)

og_vars <- str_remove(og_vars, "E$")

# they are all B variables
og_vars

v19 %>%
    filter(name == "B25018_001") %>%
    print(n = Inf)

vars <- c(
  "med_year_built" = "B25035_001",     # median year housing structure built
  "med_num_rooms" = "B25018_001",      # median number of rooms
  "pop" = "B01003_001",                # total population

  "units_tot" = "B25024_001",          # total (housing units in structure)
  "units_1_det" = "B25024_002",        # 1 unit, detached
  "units_1_att" = "B25024_003",        # 1 unit, attached
  "units_2" = "B25024_004",            # 2 units
  "units_3_4" = "B25024_005",          # 3-4 units
  "units_5_9" = "B25024_006",          # 5-9 units
  "units_10_19" = "B25024_007",        # 10-19 units
  "units_20_49" = "B25024_008",        # 20-49 units
  "units_50" = "B25024_009",           # 50+ units
  "units_mobile_home" = "B25024_010",  # mobile home units
  "units_rv_other" = "B25024_011",     # boat, rv, van, etc units

  "ten_tot" = "B25003_001",            # total units (tenure)
  "ten_own" = "B25003_002",            # owner-occupied units
  "ten_rent" = "B25003_003",           # renter-occupied units

  "med_age" = "B01002_001",            # median age

  "bach_tot" = "S1501_C01_006",        # total pop. 25+
  "bach_bach" = "S1501_C01_012",       # pop. 25+ with Bachelor's degree or higher

  "med_hh_inc" = "B19013_001",         # median household income
  "med_rent" = "B25064_001",           # median gross rent

  "vac_tot" = "B25002_001",            # total housing units (vacancy status)
  "vac_occ" = "B25002_002",            # occupied housing units
  "vac_vac" = "B25002_003",            # vacant housing units

  "race_tot" = "B03002_001",           # total pop. (race/ethnicity)
  "race_hisp" = "B03002_012",          # hispanic/latino
  "race_white" = "B03002_003",         # white (non-hispanic/latino)
  "race_black" = "B03002_004",         # black (non-hispanic/latino)
  "race_asian" = "B03002_006",         # asian (non-hispanic/latino)

 


  "commute_tot" = "B08301_001",        # total pop. (means of transportation to work)
  "commute_drive" = "B08301_002",      # car, truck or van
  "commute_transit" = "B08301_010",    # public transit
  "commute_taxi" = "B08301_016",       # taxi
  "commute_motor" = "B08301_017",      # motorcycle
  "commute_bike" = "B08301_018",       # bicycle
  "commute_walk" = "B08301_019",       # walked
  "commute_other" = "B08301_020",      # other means
  "commute_home" = "B08301_021",       # worked at home

  "units" = "B25024_001",               # total housing units
  "travel_time_work_agg" = "B08135_001",# AGGREGATE TRAVEL TIME TO WORK (IN MINUTES) OF WORKERS BY TRAVEL TIME TO WORK
  "travel_time_work" = "B08303_001"     # TRAVEL TIME TO WORK 
)

# what of the original queried variables are missing? (educational attainment is superceded by the convenient subject table variables)
# ok to proceed
v19 %>%
    filter(name %in% (og_vars[!(og_vars %in% vars)])) %>%
    print(n = Inf)

acs_place_data_19 <- get_acs(
            geography = "place", 
            variables = vars,
            year = 2019,
            geometry = TRUE,
            keep_geo_vars = TRUE,
            survey = "acs5",
            output = "wide"
        ) %>%
        st_drop_geometry() %>%
        data.frame() %>%
        inner_join(city_info, by = "GEOID")

# good? (yes)
acs_place_data_19 %>% nrow() == city_info %>% nrow()

acs_place_data_19 %>% filter(GEOID == "0603526")

dput(acs_place_data_19 %>% select(ends_with("E")) %>% colnames())

acs_place_data_21 <- get_acs(
            geography = "place", 
            variables = vars,
            year = 2021,
            geometry = TRUE,
            keep_geo_vars = TRUE,
            survey = "acs5",
            output = "wide"
        ) %>%
        st_drop_geometry() %>%
        data.frame() %>%
        inner_join(city_info, by = "GEOID")

# good? (yes, for both year = 2019 and 2021)
acs_place_data_21 %>% nrow() == city_info %>% nrow()

acs_place_data_21 %>% filter(GEOID == "0603526")

dput(acs_place_data_21 %>% select(ends_with("E")) %>%colnames())

# Calculate % variables
#=====================================

# Create function that adds new percent variables
mutate_vars <- function(df){
  df_new <- df %>%
    mutate(
               total_pop_city=popE,
               pct_singlefam_city=100 * ((units_1_detE + units_1_attE) / units_totE),
               pct_multifam_city=100 * ((units_2E + units_3_4E + units_5_9E + units_10_19E + units_20_49E + units_50E) / units_totE),
               pct_mobile_home_and_others_city=100 * (( units_mobile_homeE + units_rv_otherE) / units_totE),
               pct_renter_city=100* (ten_rentE / ten_totE),
               median_age_city=med_ageE,
               bachelor_plus_city=100 * (bach_bachE / bach_totE),
               median_hhinc_city=med_hh_incE,
               median_rent_city=med_rentE,
               pct_vacant_city=100 * (vac_vacE / vac_totE),
               pct_nhwhite_city=100 * (race_whiteE / race_totE),
               pct_nhblack_city=100 * (race_blackE / race_totE),
               pct_nhasian_city=100 * (race_asianE / race_totE),
               pct_hisp_city=100 * (race_hispE / race_totE),
               pct_others_city = 100 - pct_hisp_city - pct_nhwhite_city - pct_nhblack_city - pct_nhasian_city,
               pct_commute_auto_city=100 * (commute_driveE / commute_totE),
               pct_commute_public_transit_city=100 * (commute_transitE / commute_totE),
               pct_commute_bicycle_city=100 * (commute_bikeE / commute_totE),
               pct_commute_walk_city=100 * (commute_walkE / commute_totE),
               pct_wfh_city=100 * (commute_homeE / commute_totE),
               # previously this included commute from home, to add it back just add pct_wfh_city back to this variable
               pct_commute_others_city=100 - pct_commute_auto_city - pct_commute_public_transit_city - pct_commute_bicycle_city - pct_commute_walk_city - pct_wfh_city,
               
               housing_units_city=units_totE,
               average_commute_time_city = travel_time_work_aggE / travel_time_workE,
               housing_density_city = units_totE / ALAND,
               population_density_city = popE / ALAND,

    )
  
  return(df_new)
}

acs_place_data1_19 <- mutate_vars(acs_place_data_19)

# adjust for inflation from 2019 dollars -> 2021 dollars
R_CPI_U_RS_2019 <- 376.5
R_CPI_U_RS_2021 <- 399.0

acs_place_data1_19_adjusted <- acs_place_data1_19 %>%
                                mutate(median_hhinc_city = median_hhinc_city * (R_CPI_U_RS_2021 / R_CPI_U_RS_2019),
                                       median_rent_city = median_rent_city * (R_CPI_U_RS_2021 / R_CPI_U_RS_2019) )



acs_place_data1_21 <- mutate_vars(acs_place_data_21)

# compare to og_model_data for the _city variables
# note that income comparisons will have to take inflation into account
# pct_commute_others includes wfh...................

comparison_df <- og_model_data %>%
    select(ends_with("city")) %>%
    pivot_longer(cols = !city, values_to = "zipcodes") %>%
    left_join(

acs_place_data1_21 %>%
    select(ends_with("city")) %>%
    pivot_longer(cols = !city, values_to = "place_id")
    ) %>%
    mutate(difference = place_id - zipcodes,
           pct_change = 100 * abs(difference) / zipcodes)

comparison_df %>%
    # these are known to be off
    filter(!str_detect(name, "density")) %>%
    arrange(-pct_change) %>%
    print(n = 50)

comparison_df %>% glimpse()

comparison_df 

og_model_data %>% colnames()

# save them
acs_place_data_subset_19 <- acs_place_data1_19 %>% 
                            select(contains("_city"),GEOID) %>%
                            inner_join(city_info, by = "GEOID")

acs_place_data_subset_19 %>% glimpse()

write.csv(acs_place_data_subset_19, paste0("~/data/downtownrecovery/curated_data/us_city_acs_vars_2019.csv"), row.names = F)



acs_place_data_subset_21 <- acs_place_data1_21 %>% 
                            select(contains("_city"),GEOID) %>%
                            inner_join(city_info, by = "GEOID")

acs_place_data_subset_21 %>% glimpse()

write.csv(acs_place_data_subset_21, paste0("~/data/downtownrecovery/curated_data/us_city_acs_vars_2021.csv"), row.names = F)

# something possibly interesting- change between 2019 - 2021...



comparison_df_19_21 <- acs_place_data_subset_19 %>%
    select(ends_with("city")) %>%
    pivot_longer(cols = !city, values_to = "est_2019") %>%
    left_join(

acs_place_data_subset_21 %>%
    select(ends_with("city")) %>%
    pivot_longer(cols = !city, values_to = "est_2021")
    ) %>%
    mutate(difference = est_2021 - est_2019,
           pct_change = 100 * abs(difference) / est_2019)


comparison_df_19_21 %>%
    arrange(pct_change) %>%
    print(n = 100)

