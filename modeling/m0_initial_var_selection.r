# ==========================================================================
# Title: Initial Variable Selection
# Summary: When starting with v1 BART modeling or any variables are added or
# created, we need to run an initial evaluation of variables for the first
# run of BART modeling. This script runs through that process.
# ==========================================================================

librarian::shelf(tidyverse)

options(width=Sys.getenv("COLUMNS"), setWidthOnResize=TRUE, scipen = 999, digits = 5)
# ==========================================================================
# Data load
# ==========================================================================

test_df <- read.csv("~/data/downtownrecovery/downtownrecovery/full_ntv.csv")

test_df %>% glimpse()



m_df <- read.csv(paste0("~/data/downtownrecovery/curated_data/all_model_features_20230714.csv"))

glimpse(m_df)

m_df <- m_df %>%
    # scale units per km2 to units per m2
#    mutate_at(vars(contains("_density")), ~.x / 1000000) #%>%
    # exclude the outlier cities
    filter(!(city %in% c("Dallas", "Mississauga", "Oklahoma City", "Orlando")))

m_df %>% distinct(city) %>% nrow()

prev_m_df <- read.csv((paste0("~/git/downtownrecovery/shinyapp/input_data/all_model_features_1015_weather.csv")))

prev_m_df %>% semi_join(m_df, by = 'city') %>% distinct(city) %>% nrow()

prev_m_df <- prev_m_df %>% semi_join(m_df, by = 'city')

all_variables <- c("pct_jobs_agriculture_forestry_fishing_hunting", 
"pct_jobs_mining_quarrying_oil_gas", "pct_jobs_utilities", "pct_jobs_construction", 
"pct_jobs_manufacturing", "pct_jobs_wholesale_trade", "pct_jobs_retail_trade", 
"pct_jobs_transport_warehouse", "pct_jobs_information", "pct_jobs_finance_insurance", 
"pct_jobs_real_estate", "pct_jobs_professional_science_techical", 
"pct_jobs_management_of_companies_enterprises", "pct_jobs_administrative_support_waste", 
"pct_jobs_educational_services", "pct_jobs_healthcare_social_assistance", 
"pct_jobs_arts_entertainment_recreation", "pct_jobs_accomodation_food_services", 
"pct_jobs_public_administration", "employment_density_downtown", "employment_entropy", 

"total_pop_downtown", "population_density_downtown", 
"median_age_downtown", "median_hhinc_downtown", "median_rent_downtown",

"pct_singlefam_downtown", "pct_multifam_downtown","pct_vacant_downtown",
"bachelor_plus_downtown","housing_units_downtown", "median_year_structure_built", "median_no_rooms","housing_density_downtown",

"average_commute_time_city", "pct_commute_auto_city", "pct_commute_public_transit_city", "pct_commute_bicycle_city", "pct_commute_walk_city",

"days_school_closing", "days_workplace_closing", "days_cancel_large_events", "days_cancel_all_events", 
"days_stay_home_requirements", "days_mask_mandates", "days_income_support",

"winter_avg_temp","spring_avg_temp","summer_avg_temp","fall_avg_temp")

prev_data <- prev_m_df %>%
                select(city, all_of(all_variables)) %>%
                pivot_longer(!city, names_to = "variable", values_to = "previous")

curr_data <- m_df %>%
                select(city, all_of(all_variables)) %>%
                pivot_longer(!city, names_to = "variable", values_to = "current")

all_data <- prev_data %>%
                left_join(curr_data) %>%
                mutate(difference = current - previous,
                       pct_of_prev = abs(difference / previous) * 100) %>%
                arrange(-pct_of_prev)

all_data %>% 
    filter((pct_of_prev > 1) & str_detect(variable, "density")) %>%
    print(n = Inf)


dt_variables <- c('city','total_pop_downtown', 'pct_singlefam_downtown', 'pct_multifam_downtown','pct_renter_downtown','median_age_downtown','bachelor_plus_downtown','median_hhinc_downtown','median_rent_downtown','pct_vacant_downtown','median_year_structure_built','median_no_rooms','pct_commute_auto_city','pct_commute_public_transit_city','pct_commute_bicycle_city','pct_commute_walk_city','housing_units_downtown','average_commute_time_city','pct_jobs_agriculture_forestry_fishing_hunting','pct_jobs_mining_quarrying_oil_gas', 'pct_jobs_utilities','pct_jobs_construction', 'pct_jobs_manufacturing','pct_jobs_wholesale_trade', 'pct_jobs_retail_trade','pct_jobs_transport_warehouse', 'pct_jobs_information','pct_jobs_finance_insurance','pct_jobs_real_estate','pct_jobs_professional_science_techical','pct_jobs_management_of_companies_enterprises','pct_jobs_administrative_support_waste','pct_jobs_educational_services','pct_jobs_healthcare_social_assistance','pct_jobs_arts_entertainment_recreation','pct_jobs_accomodation_food_services','pct_jobs_public_administration', 'employment_entropy', 'population_density_downtown', 'employment_density_downtown', 'housing_density_downtown','days_school_closing', 'days_workplace_closing','days_cancel_large_events', 'days_cancel_all_events','days_stay_home_requirements', 'days_income_support','days_mask_mandates', 'winter_avg_temp', 'summer_avg_temp')

city_variables <- c('city','total_pop_city', 'pct_singlefam_city', 'pct_multifam_city','pct_renter_city','median_age_city','bachelor_plus_city','median_hhinc_city','median_rent_city','pct_vacant_city','pct_commute_auto_city','pct_commute_public_transit_city','pct_commute_bicycle_city','pct_commute_walk_city','housing_units_city','average_commute_time_city','pct_jobs_agriculture_forestry_fishing_hunting','pct_jobs_mining_quarrying_oil_gas', 'pct_jobs_utilities','pct_jobs_construction', 'pct_jobs_manufacturing','pct_jobs_wholesale_trade', 'pct_jobs_retail_trade','pct_jobs_transport_warehouse', 'pct_jobs_information','pct_jobs_finance_insurance','pct_jobs_real_estate','pct_jobs_professional_science_techical','pct_jobs_management_of_companies_enterprises','pct_jobs_administrative_support_waste','pct_jobs_educational_services','pct_jobs_healthcare_social_assistance','pct_jobs_arts_entertainment_recreation','pct_jobs_accomodation_food_services','pct_jobs_public_administration', 'employment_entropy', 'population_density_city', 'employment_density_downtown', 'housing_density_city', 'days_school_closing', 'days_workplace_closing','days_cancel_large_events', 'days_cancel_all_events','days_stay_home_requirements', 'days_income_support','days_mask_mandates', 'winter_avg_temp', 'summer_avg_temp')

lq_variables <- c('city','total_pop_downtown', 'pct_singlefam_downtown', 'pct_multifam_downtown','pct_renter_downtown','median_age_downtown','bachelor_plus_downtown','median_hhinc_downtown','median_rent_downtown','pct_vacant_downtown','median_year_structure_built','median_no_rooms','pct_commute_auto_city','pct_commute_public_transit_city','pct_commute_bicycle_city','pct_commute_walk_city','housing_units_downtown','average_commute_time_city','pct_jobs_agriculture_forestry_fishing_hunting','pct_jobs_mining_quarrying_oil_gas', 'pct_jobs_utilities','pct_jobs_construction', 'pct_jobs_manufacturing','pct_jobs_wholesale_trade', 'pct_jobs_retail_trade','pct_jobs_transport_warehouse', 'pct_jobs_information','pct_jobs_finance_insurance','pct_jobs_real_estate','pct_jobs_professional_science_techical','pct_jobs_management_of_companies_enterprises','pct_jobs_administrative_support_waste','pct_jobs_educational_services','pct_jobs_healthcare_social_assistance','pct_jobs_arts_entertainment_recreation','pct_jobs_accomodation_food_services','pct_jobs_public_administration', 'employment_entropy', 'population_density_downtown', 'employment_density_downtown', 'housing_density_downtown','days_school_closing', 'days_workplace_closing','days_cancel_large_events', 'days_cancel_all_events','days_stay_home_requirements', 'days_income_support','days_mask_mandates', 'winter_avg_temp', 'summer_avg_temp')

downtown_rfc_table <- m_df %>%
    select(all_of(dt_variables))

city_rfc_table <- m_df %>%
    select(all_of(city_variables))

lq_rfc_table <- m_df %>%
    select(all_of(lq_variables))

#
# Top correlated variables, all and then by subset
# --------------------------------------------------------------------------
 
m_df %>% glimpse()
# current data
strongly_correlated_vars <- cor(m_df %>%
    filter(!(city %in% c("Dallas", "Mississauga", "Oklahoma City", "Orlando"))) %>%
    select(all_of(all_variables)) %>%
    select_if(is.numeric)) %>%
     as.data.frame() %>%
      mutate(var1 = rownames(.)) %>%
      gather(var2, value, -var1) %>%
      arrange(desc(value)) %>%
      group_by(value) %>%
      filter(row_number()==1) %>%
      filter((abs(value) > .75) & (var1 != var2)) %>%
      data.frame()



strongly_correlated_var_names <- unique(c(strongly_correlated_vars %>% pull(var1), strongly_correlated_vars %>% pull(var2)))
strongly_correlated_var_names

correlation_matrix <- cor(m_df %>%
    filter(!(city %in% c("Dallas", "Mississauga", "Oklahoma City", "Orlando"))) %>%
    select(all_of(strongly_correlated_var_names)) %>%
    select_if(is.numeric))

correlation_matrix

write.csv(correlation_matrix, "~/data/downtownrecovery/correlation_matrix_75.csv")

write.csv(m_df, "~/data/downtownrecovery/curated_data/all_model_features_20230714.csv")




# 2954 combinations of them are below .5
# 150 combinations of them are above .6
# 39 combinations are above .8

m_df %>%
    select(city, pct_hisp_downtown, pct_hisp_city) %>%
    print(n = Inf, na.print = "")


# previous data
cor(prev_m_df %>%
    select(all_of(all_variables)) %>%
    select_if(is.numeric)) %>%
     as.data.frame() %>%
      mutate(var1 = rownames(.)) %>%
      gather(var2, value, -var1) %>%
      arrange(desc(value)) %>%
      group_by(value) %>%
      filter(row_number()==1) %>%
      filter((abs(value) > .8) & (var1 != var2)) %>%
      data.frame()

 # downtown
 
cor(downtown_rfc_table %>% select_if(is.numeric)) %>%
      as.data.frame() %>%
      mutate(var1 = rownames(.)) %>%
      gather(var2, value, -var1) %>%
      arrange(desc(value)) %>%
      group_by(value) %>%
      filter(row_number()==1) %>%
      filter(abs(value) > .8) %>%
      data.frame()

# city

 cor(city_rfc_table %>% select_if(is.numeric)) %>%
      as.data.frame() %>%
      mutate(var1 = rownames(.)) %>%
      gather(var2, value, -var1) %>%
      arrange(desc(value)) %>%
      group_by(value) %>%
      filter(row_number()==1) %>%
      filter(abs(value) > .8) %>%
      data.frame()

# lq

 cor(lq_rfc_table %>% select_if(is.numeric)) %>%
      as.data.frame() %>%
      mutate(var1 = rownames(.)) %>%
      gather(var2, value, -var1) %>%
      arrange(desc(value)) %>%
      group_by(value) %>%
      filter(row_number()==1) %>%
      filter(abs(value) > .8) %>%
      data.frame()
