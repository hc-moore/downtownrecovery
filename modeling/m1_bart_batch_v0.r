# ==========================================================================
# BART Big Memory script for cluster
# downtown recovery
# Run:
# sbatch -C mem768g --job-name=downtown_recovery downtown_recovery_bart.sh
# Author: Tim Thomas
# Edits: Hannah Moore
# ==========================================================================
# options(scipen=10, width=system("tput cols", intern=TRUE), tigris_use_cache = TRUE) # avoid scientific notation
source("~/git/timathomas/functions/functions.r")
# run java.parameters memory increase prior to loading bartMachine
options(java.parameters = paste0("-Xmx100g")) # Increase Java memory
librarian::shelf(qs, dplyr, bartMachine, colorout, stringr)

#
# set number of cores for BART
# --------------------------------------------------------------------------
parallel::detectCores()
set_bart_machine_num_cores(1)

# geography <- 'state'
metric <- "lq"
period <- ""
y_var = "cluster"

data_path <- '~/data/downtownrecovery/model_outputs/'

if (metric == "downtown") {

    y_vals <- read.csv(paste0('~/data/downtownrecovery/recovery_clusters/rq_dwtn_clusters_1015_period_', period, '.csv')) %>%
    select(-X) %>%
    mutate(city = str_replace(city, "é", "e"))
} else {
    y_vals <- read.csv(paste0('~/data/downtownrecovery/recovery_clusters/lq_clusters_single_period_1015.csv')) %>%
    select(-X) %>%
    mutate(city = str_replace(city, "é", "e"))
}

y_vals %>% glimpse()

#
# Create directories if they don't exist
# --------------------------------------------------------------------------
if(!dir.exists(data_path)){
  print("Creating directory")
  dir.create(data_path)
} else {print("Directory exists")}

if(!dir.exists(paste0(data_path, metric))){
  print("Creating Directory")
  dir.create(paste0(data_path, metric))
  dir.create(paste0(data_path, metric,'/bart_models'))
  dir.create(paste0(data_path, metric,'/var_importance'))
  setwd(paste0(data_path, metric, '/'))
} else {
  print("Directory exists")
  setwd(paste0(data_path, metric, '/'))
}

#
# Define select variables
# --------------------------------------------------------------------------
sel_var <- c("median_year_structure_built", "median_no_rooms", "days_school_closing", 
"days_workplace_closing", "days_cancel_large_events", "days_cancel_all_events", 
"days_stay_home_requirements", "days_income_support", "days_mask_mandates", 
"winter_avg_temp", "spring_avg_temp", "summer_avg_temp", "fall_avg_temp", 
"total_pop_downtown", "total_pop_city", "pct_singlefam_downtown", 
"pct_singlefam_city", "pct_multifam_downtown", "pct_multifam_city", 
"pct_mobile_home_and_others_downtown", "pct_mobile_home_and_others_city", 
"pct_renter_downtown", "pct_renter_city", "median_age_downtown", 
"median_age_city", "bachelor_plus_downtown", "bachelor_plus_city", 
"median_hhinc_downtown", "median_hhinc_city", "median_rent_downtown", 
"median_rent_city", "pct_vacant_downtown", "pct_vacant_city", 
"pct_nhwhite_downtown", "pct_nhwhite_city", "pct_nhblack_downtown", 
"pct_nhblack_city", "pct_hisp_downtown", "pct_hisp_city", "pct_nhasian_downtown", 
"pct_nhasian_city", "pct_commute_auto_downtown", "pct_commute_auto_city", 
"pct_commute_public_transit_downtown", "pct_commute_public_transit_city", 
"pct_commute_walk_downtown", "pct_commute_walk_city", "pct_commute_bicycle_downtown", 
"pct_commute_bicycle_city", "pct_commute_others_downtown", "pct_commute_others_city", 
"housing_units_downtown", "housing_units_city", "average_commute_time_downtown", 
"average_commute_time_city", "pct_jobs_agriculture_forestry_fishing_hunting", 
"pct_jobs_mining_quarrying_oil_gas", "pct_jobs_utilities", "pct_jobs_construction", 
"pct_jobs_manufacturing", "pct_jobs_wholesale_trade", "pct_jobs_retail_trade", 
"pct_jobs_transport_warehouse", "pct_jobs_information", "pct_jobs_finance_insurance", 
"pct_jobs_real_estate", "pct_jobs_professional_science_techical", 
"pct_jobs_management_of_companies_enterprises", "pct_jobs_administrative_support_waste", 
"pct_jobs_educational_services", "pct_jobs_healthcare_social_assistance", 
"pct_jobs_arts_entertainment_recreation", "pct_jobs_accomodation_food_services", 
"pct_jobs_other", "pct_jobs_public_administration", "employment_entropy", 
"population_density_downtown", "population_density_city", "housing_density_downtown", 
"housing_density_city", "employment_density_downtown")

# ==========================================================================
# Run BART CV
# ==========================================================================
#
# Top correlated variables
# --------------------------------------------------------------------------
    # cor(m_df %>% select_if(is.numeric)) %>%
    #   as.data.frame() %>%
    #   mutate(var1 = rownames(.)) %>%
    #   gather(var2, value, -var1) %>%
    #   arrange(desc(value)) %>%
    #   group_by(value) %>%
    #   filter(row_number()==1) %>%
    #   filter(abs(value) > .8) %>%
    #   data.frame()

#
# BartMachine cross validated model
# --------------------------------------------------------------------------
m_df <-
      read.csv(paste0("~/data/downtownrecovery/curated_data/all_model_features_20230714.csv")) %>%
      left_join(y_vals) %>%
      select(
        all_of(sel_var),
        y = all_of(y_var) # specific y value
        ) %>%
      na.omit() %>%
      data.frame()

dim(m_df)
summary(m_df)

  seed <- floor(runif(1, min = 1, max = 100000))
  # 90806
    bartcv <-
      bartMachineCV(
        Xy = m_df,
        mem_cache_for_speed=TRUE,
        seed = seed,
        use_missing_data=FALSE)
     bartcv$cv_stats
     bartcv$PseudoRsq
     k = bartcv$k
     nu = bartcv$nu
     q = bartcv$q
     nt = bartcv$num_trees
     qsave(bartcv, paste0(data_path, metric, '/bart_models/bartcv_rate_', period, '_', metric, '_', Sys.Date(), ".qs"))

     gc()
     rm(bartcv)

# ==========================================================================
# Loop model run
# ==========================================================================
for (i in 1:20){
  cat("*** run", i, " ***\n")
  gc()

#
# Data
# --------------------------------------------------------------------------

 m_df <-
      read.csv(paste0("~/data/downtownrecovery/curated_data/all_model_features_20230714.csv")) %>%
      left_join(y_vals) %>%
      select(
        all_of(sel_var),
        y = all_of(y_var) # specific y value
        ) %>%
      na.omit() %>%
      data.frame()


  gc()

  seed <- floor(runif(1, min = 1, max = 100000))
  cat("Running seed", seed, "\n")

#
# Bart model
# --------------------------------------------------------------------------
  system.time(
  bart <-
    	bartMachine(
    		Xy = m_df,
    		mem_cache_for_speed=TRUE,
    		seed = seed,
    		use_missing_data=FALSE,
        serialize = TRUE,
        run_in_sample = TRUE,
    		k=k,
        nu=nu,
        q=q,
        num_trees = nt) # optimal options based on cv
  )
    gc()

    qsave(bart, paste0(data_path,metric, '/bart_models/bart_rate_', period, '_', seed, '_', Sys.Date(), ".qs"))

    gc()

#
# Variable Selection
# --------------------------------------------------------------------------
    start <- Sys.time()
    imp <- investigate_var_importance(bart, plot = FALSE)
    imp_permute <- var_selection_by_permute(bart, bottom_margin = 10, num_permute_samples = 10, plot = FALSE)
    imp_permute$important_vars_local_names
    imp_permute$important_vars_global_max_names
    imp_permute$important_vars_global_se_names
    gc()
# DO NOT RUN - takes too long to run
    # imp_permute_cv <- var_selection_by_permute_cv(bart)
    # imp_permute_cv$best_method
    # imp_permute_cv$important_vars_cv
    # gc()
# END DO NOT RUN
    end <- Sys.time()
    print(end - start)

  impdf <-
  	data.frame(imp, row.names = c()) %>% mutate(var = names(imp$avg_var_props)) %>%
    mutate(
      local = case_when(var %in% imp_permute$important_vars_local_names ~ 1),
      global_max = case_when(var %in% imp_permute$important_vars_global_max_names ~ 1),
      global_se = case_when(var %in% imp_permute$important_vars_global_se_names ~ 1),
      best_method = ifelse(exists("imp_permute_cv") == TRUE, imp_permute_cv$best_method, NA_character_)
    )

  head(impdf, 20)

  write.csv(impdf, paste0(data_path,metric, "/var_importance",  "/vi_", period, '_', seed, '_', Sys.Date(), ".csv"), row.names = F)
  rm("m_df")
  rm("bart")
  # rm("imp_permute")
  gc()
}
# quit(save = "no")