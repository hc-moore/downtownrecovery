# ==========================================================================
# Variable selection script
# Author: Tim Thomas
# determine which variables predict downtown recovery clusters
# ==========================================================================
  librarian::shelf(BAMMtools, janitor, qs, tidyverse, tidyfst, bartMachine,colorout, dtplyr, plotly)


#
# Set user options: state and version
# --------------------------------------------------------------------------

# geography <- 'state'
metric <- "downtown"
period <- "1"
y_var = "cluster"

data_path <- '~/data/downtownrecovery/model_outputs/'

# Gather all BART variable importance files and find the ranks
# --------------------------------------------------------------------------
if (metric == "downtown") {
    vi_path <- paste0(data_path, metric, "/var_importance/period_", period, "/")
} else {
    vi_path <- paste0(data_path, metric, "/var_importance/")
}



l_files <- list.files(vi_path)

ranks_l <- data.frame()
for (file in l_files){
  d <-
    read.csv(paste0(vi_path, file)) %>%
    arrange(-avg_var_props) %>%
    mutate(rank = nrow(.):1)
  ranks_l <- rbind(ranks_l, d)
}

ranksSum_l <-
  ranks_l %>%
  group_by(var) %>%
  summarise(
    TotalRank = sum(rank, na.rm = TRUE),
    MeanRank = mean(rank, na.rm = TRUE),
    mean_avg_var_props = mean(avg_var_props, na.rm = TRUE),
    LocalSum = sum(local, na.rm = TRUE),
    GlobalMaxSum = sum(global_max, na.rm = TRUE),
    GlobalSESum = sum(global_se, na.rm = TRUE),
    run_count = n()) %>%
  filter(run_count > 1) %>%
  arrange(-TotalRank) %>%
  mutate(updated = Sys.time())

ranksSum_l %>%
    pull(var)

#
# *** FOR RSTUDIO ***

#
# Save file and upload a version to google drive to view
# --------------------------------------------------------------------------

# ranksSum %>%
#   mutate(plot_link =
#     paste0("https://urban-displacement.github.io/displacement-measure/plots/var_plots/", var, ".html"))

save_csv_to_gd <-
  function(x, y){
    save_path = paste0("")
    write_csv(
      x,
      save_path)

    drive_upload(
      save_path,
      as_dribble(""),
      overwrite = TRUE,
      type = 'spreadsheet')
  }

save_csv_to_gd(ranksSum_l, 'l')
save_csv_to_gd(ranksSum_vl, 'vl')
save_csv_to_gd(ranksSum_el, 'el')

# ==========================================================================
# Plot trends of top variables
# ==========================================================================

jenks_rank_l <- BAMMtools::getJenksBreaks(ranksSum_l$mean_avg_var_props, k = 10)

jenks_rank_l
# Make rank plot
rank_plot <- function(x, y){
  ggplot(
    x
  ) +
  geom_bar(aes(y = reorder(var, mean_avg_var_props), x = mean_avg_var_props), stat = "identity") +
  ylab("") +
  geom_hline(
    yintercept = x %>% filter(mean_avg_var_props %in% y) %>% pull(var),
    color = "Red"
  )}

rank_plot(ranksSum_l, jenks_rank_l)
rank_plot(ranksSum_vl, jenks_rank_vl)
rank_plot(ranksSum_el, jenks_rank_el)

#
# Correlation Matrix
# --------------------------------------------------------------------------

  m_df <-
    qread(paste0("~/data/urban-displacement/edr-us/output/data8_displaced_rate_df_", study_region, "_", data_ver, ".qs")) %>%
    # left_join(region) %>% # join region for modeling
    select(all_of(ranksSum_l %>% filter(!grepl("region", var), !grepl("tcac", var)) %>% pull(var))) %>%
    select_if(is.numeric) %>%
    data.frame()

cor(m_df) %>%
  as.data.frame() %>%
  mutate(var1 = rownames(.)) %>%
  gather(var2, value, -var1) %>%
  arrange(desc(value)) %>%
  group_by(value) %>%
  filter(row_number()==1) %>%
  filter(abs(value) > .7) %>%
  data.frame()

