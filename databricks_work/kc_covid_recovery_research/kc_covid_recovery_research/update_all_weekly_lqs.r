# Databricks notebook source
# Package names
packages <- c("broom", "ggplot2", "ggrepel", "readxl", "dplyr", "tidyr",  "MASS", "scales", "SparkR", "tidyverse", "zoo")

# Packages loading
invisible(lapply(packages, library, character.only = TRUE))

hiveContext <- sparkR.session()

get_table_as_r_df <- function(table_name) {
  SparkR::as.data.frame(SparkR::sql(paste0("select * from ", table_name, " table")))
  }

create_and_save_as_table <- function(df, table_name) {
    spark_df <- SparkR::createDataFrame(df)
    spark_df %>%
      SparkR::saveAsTable(table_name)
  }

# COMMAND ----------

## original final weekly rq/lqs by metric:
downtown_rec <- get_table_as_r_df("0713_downtown_rec_df_totaldevs")
metro_rec <- get_table_as_r_df("0713_metro_rec_df_totaldevs")
relative_rec <- get_table_as_r_df("0713_localized_lq_df_totaldevs")

# get geographic data - regions, metro_size, city
regions_df <- get_table_as_r_df("regions_csv")
regions_df <- regions_df %>%
  dplyr::select(city, display_title, region, metro_size)

# create metric col
downtown_rec$metric <- "downtown"
metro_rec$metric <- "city"
relative_rec$metric <- "relative"

# COMMAND ----------

# the rec_2020.53 column is null
# numbers in rec_year_XX are ISO week standard numbers from pandas .week accessor 
# except for rec_2020, which is iso week number - 1 
all_weekly_lqs <- rbind(downtown_rec, metro_rec, relative_rec) %>%
                      dplyr::select(city, display_title, metric, starts_with("rec_"))
all_weekly_lqs$rec_2020.53 <- 0

# COMMAND ----------

display(all_weekly_lqs)

# COMMAND ----------

new_data <- get_table_as_r_df("0714_combined_metrics_df")
display(new_data)

# COMMAND ----------

all_weekly_lqs_longer <- all_weekly_lqs %>%
   dplyr::select(-rec_2020.53) %>%
   dplyr::select(city, metric, starts_with("rec")) %>%
   pivot_longer(cols = starts_with("rec"), names_to = "week", values_to = "weekly_lq")
 
all_weekly_lqs_longer$week <- str_pad(string = all_weekly_lqs_longer$week, width = 11, side = "right", pad = "0")
all_weekly_lqs_longer$week <- substr(all_weekly_lqs_longer$week, 5, 11)
all_weekly_lqs_longer$week <- as.Date(paste(all_weekly_lqs_longer$week, "1", sep = "."), "%Y.%U.%u")

# COMMAND ----------

max(all_weekly_lqs_longer$week)

# COMMAND ----------

all_weekly_lqs_longer$week <- as.character(all_weekly_lqs_longer$week)

# COMMAND ----------

new_all_weekly_lq <- new_data
# new_all_weekly_lq <- bind_rows(all_weekly_lqs_longer, new_data)

# COMMAND ----------

# checking to see if safe to change data type 
sort(unique(as.Date(new_all_weekly_lq$week)))

# COMMAND ----------

# re-run this command for all results and download this as all_weekly_lq.csv
# don't trust this as truly having all the weeks, since databricks cuts off the display after the first 1000 rows, so what you sort will be the max/min week of those first 1000 rows, which by design is the old data
new_all_weekly_lq$city <- str_replace(new_all_weekly_lq$city, "é", "e")
new_all_weekly_lq <- inner_join(new_all_weekly_lq, regions_df, by = "city")
display(new_all_weekly_lq)

# COMMAND ----------

regions_choices <- lapply(split(regions_df$city, regions_df$region), as.list)

# COMMAND ----------

# now that the all_weekly_lq df has been updated, visualize it!
# this is NOT identical to the shinyapp - some changes had to be made to get this to work in databricks
# setup widgets to make this somewhat interactable

# creates a list of regions with each element in the list being a list of cities in that region
dbutils.widgets.multiselect(name = "cities", 
                            defaultValue = unlist(regions_choices)[base::sample(seq(1:length(unique(regions_df$city))), 1)], 
                            choices = as.list(unique(regions_df$city)))

dbutils.widgets.dropdown(name = "patterns_metric",
                         defaultValue = "downtown",
                         choices = list("downtown", "city", "relative"))

dbutils.widgets.dropdown(name = "rolling_avg_window",
                         defaultValue = "15",
                         choices = as.list(as.character(1:25)))

selected_cities <- unlist(str_split(dbutils.widgets.get("cities"), ","))

# databricks compatible version of reactive shiny function to create the df to plot based on user input
getRawRecoveryDF <- function(cities, metric_val, rolling_avg_window) {
  new_all_weekly_lq %>%
      dplyr::mutate(week = base::as.Date(new_all_weekly_lq$week)) %>%
      dplyr::filter((city %in% cities) & 
                    (week >= base::as.Date("2020-03-01")) & 
                    (metric == metric_val)) %>%
      dplyr::arrange(week) %>%
      dplyr::group_by(city) %>%
      dplyr::mutate(rolling_avg = rollmean(
        weekly_lq,
        as.numeric(rolling_avg_window),
        na.pad = TRUE,
        align = "center"
      )) %>%
      ungroup()
  }

new_df <- getRawRecoveryDF(selected_cities, dbutils.widgets.get("patterns_metric"), dbutils.widgets.get("rolling_avg_window"))

    df <- na.omit(new_df)
    starting_lqs <- df %>%
      dplyr::filter(week == min(week)) %>%
      dplyr::select(city, region, week, rolling_avg) %>%
      dplyr::arrange(desc(rolling_avg))
    
    ending_lqs <- df %>%
      dplyr::group_by(city) %>%
      dplyr::mutate(latest_data = max(week)) %>%
      dplyr::ungroup() %>%
      dplyr::filter(week == latest_data) %>%
      dplyr::select(city, region, week, rolling_avg) %>%
      dplyr::arrange(desc(rolling_avg))
    
    total_cities <- length(unique(df$city))
    total_weeks <- length(unique(df$week))
    

    
    g1 <- ggplot(df) + aes(
      x = week,
      y = rolling_avg,
      group = city,
      color = region,
      label = city
    ) + geom_line(size = 1) +
      geom_label_repel(
        data = starting_lqs,
        size = 5,
        direction = "y",
        hjust = "right",
        force = 1,
        na.rm  = TRUE,
        min.segment.length = 0,
        segment.curvature = 1e-20,
        segment.angle = 20,
        # this was determined to be a decent offset based on the commented out line below
        # leaving it in as future reference 
        nudge_x = rep(-35, times = total_cities),
        show.legend = FALSE
        #nudge_x = rep(-total_weeks / as.numeric(input$rolling_window[1]), times = total_cities),
      ) +
      geom_label_repel(
        data = ending_lqs,
        size = 5,
        direction = "y",
        hjust = "left",
        force = 1,
        na.rm = TRUE,
        min.segment.length = 0,
        segment.curvature =  1e-20,
        segment.angle = 20,
        # this was determined to be a decent offset based on the commented out line below
        # leaving it in as future reference 
        nudge_x = rep(35, times = total_cities),
        show.legend = FALSE
        #nudge_x = rep(total_weeks / as.numeric(input$rolling_window[1]), times = total_cities),
      ) +
      labs(title = paste(dbutils.widgets.get("patterns_metric"), "weekly average"),
           subtitle = paste(dbutils.widgets.get("rolling_avg_window"), "week rolling average"),
           color = "Region",
           y = "Metric",
           x = "Week"
      ) +
      theme(
        #legend.position = "none",
        axis.text.x = element_text(size = 10, angle = 45, vjust = 1, hjust = 1),
        axis.title = element_text(size = 12, hjust = .5),
        plot.title = element_text(size = 16, hjust = .5),
        plot.subtitle = element_text(size = 12, hjust = .5)
      ) +
      scale_x_date(
        breaks = "4 weeks",
        date_labels = "%Y.%m",
        expand = expansion(mult = .15)
      ) +
      scale_color_manual(values = c("Canada" = "#e41a1c",
                                    "Midwest" = "#377eb8",
                                    "Northeast" = "#4daf4a",
                                    "Pacific" = "#984ea3",
                                    "Southeast" = "#ff7f00",
                                    "Southwest" = "#e6ab02"))
g1

# COMMAND ----------

# save this to databricks so a single consolidated notebook can visualize all updated data
new_all_weekly_lq$normalized_visits_by_total_visits <- as.numeric(new_all_weekly_lq$normalized_visits_by_total_visits)
new_all_weekly_lq$raw_visit_counts <- as.numeric(new_all_weekly_lq$raw_visit_counts)
create_and_save_as_table(new_all_weekly_lq, "shinyapp_export_all_weekly_metrics")

# COMMAND ----------

# if the results are acceptable, save as all_weekly_lq.csv for the ~final shiny app to read
display(new_all_weekly_lq)

# COMMAND ----------

all_weekly_lqs <- get_table_as_r_df("shinyapp_export_all_weekly_metrics")
display(all_weekly_lqs)

# COMMAND ----------

