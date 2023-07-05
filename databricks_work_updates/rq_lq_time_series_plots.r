# Databricks notebook source
# Package names
packages <- c("broom",  "ggplot2", "ggrepel", "readxl", "dplyr", "tidyr",  "MASS", "nnet", "scales", "stats", "SparkR", "tidyverse", "zoo")

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

# from stackoverflow to convert several columns from character -> numeric
# https://stackoverflow.com/questions/22772279/converting-multiple-columns-from-character-to-numeric-format-in-r
is_all_numeric <- function(x) {
  !any(is.na(suppressWarnings(as.numeric(na.omit(x))))) & is.character(x)
}

# COMMAND ----------

# load in data
y <- get_table_as_r_df('0714_combined_metrics_df')
y$week <- base::as.Date(y$week)

y <- y %>%
  dplyr::mutate_if(is_all_numeric, as.numeric)

old_table <- y %>%
  dplyr::filter((is.na(raw_visit_counts)) & 
                (!is.na(normalized_visits_by_total_visits))) %>%
  dplyr::select(-raw_visit_counts, -normalized_visits_by_state_scaling)

new_table <- y %>%
  dplyr::filter((!is.na(raw_visit_counts)) &
                (week > max(old_table$week)) &
               (week < base::as.Date("2022-06-13"))) %>%
  dplyr::select(-raw_visit_counts, -normalized_visits_by_state_scaling) %>%
  dplyr::distinct()

# now concat old_table and new_table
y <- dplyr::bind_rows(old_table, new_table)

y$city <- str_replace(y$city, "é", "e")

# exclude the outlier cities from modeling
outlier_cities <- c("Dallas", "Mississauga", "Orlando", "Oklahoma City", "Hamilton")

y <- y %>%
  dplyr::filter(!(city %in% outlier_cities))

regions_df <- get_table_as_r_df("regions_csv")
regions_df <- regions_df %>%
  dplyr::select(city, region, metro_size, display_title)
regions_df$city <- str_replace(regions_df$city, "é", "e")

y <- y %>%
  dplyr::distinct() %>%
  dplyr::filter(!is.na(normalized_visits_by_total_visits)) %>% # drop the lone NA week - the 53rd week of 2020
  dplyr::ungroup() %>% # because R
  left_join(regions_df, by = "city") # geographic and display options
display(y)

# COMMAND ----------

recovery_patterns_df <- function(selected_metric, selected_cities, rolling_window) {
  colors = c("Canada" = "#e41a1c",
             "Midwest" = "#377eb8",
             "Northeast" = "#4daf4a",
             "Pacific" = "#984ea3",
             "Southeast" = "#ff7f00",
             "Southwest" = "#e6ab02")
  na.omit(y %>%
    dplyr::filter((metric == selected_metric) &
                  (city %in% selected_cities))%>%
    dplyr::arrange(week) %>%
    dplyr::group_by(city) %>%
    dplyr::mutate(rolling_avg = rollmean(
      normalized_visits_by_total_visits,
      as.numeric(rolling_window),
      na.pad = TRUE,
      align = "center"
    )) %>%
    dplyr::ungroup() %>%
      dplyr::mutate(color = colors[region]))
}

recovery_patterns_plot <- function(df, metric, n) {
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
    label = city,
    
  ) + geom_line(aes(data_id = city), size = 1) +
    geom_point(aes(tooltip =
                                 paste0(
                                   "<b>City:</b> ", city, "<br>",
                                   "<b>Week:</b> ", week, "<br>",
                                   n, " <b>week rolling average:</b> ", percent(round(rolling_avg, 2), 1), "<br>"
                                 ),
                               data_id = city), size = 1, alpha = .1) +
     geom_label_repel(
       data = starting_lqs,
       size = 3,
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
       size = 3,
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
    labs(#title = paste(names(named_metrics[named_metrics == metric])),
         subtitle = paste(n, " week rolling average"),
         color = "Region",
         y = "Metric",
         x = "Month"
    ) +
    theme(
      axis.text.x = element_text(size = 10, angle = 45, vjust = 1, hjust = 1),
      axis.title = element_text(size = 10, hjust = .5),
      plot.title = element_text(size = 12, hjust = .5),
      plot.subtitle = element_text(size = 10, hjust = .5)
    ) +
    scale_x_date(
      breaks = "3 month",
      date_labels = "%Y.%m",
      expand = expansion(mult = .15)
    ) +
    scale_y_continuous("Metric", labels = scales::percent) +
    scale_color_manual(values = c("Canada" = "#e41a1c",
                                  "Midwest" = "#377eb8",
                                  "Northeast" = "#4daf4a",
                                  "Pacific" = "#984ea3",
                                  "Southeast" = "#ff7f00",
                                  "Southwest" = "#e6ab02"))
  g1
  
}

# COMMAND ----------

metric = 'downtown'
rolling = 9

plot_df <- recovery_patterns_df(metric, c('San Francisco', 'Miami', 'Omaha', 'Los Angeles', 'Chicago', 'Vancouver', 'Toronto', 'New York'), rolling)
recovery_patterns_plot(plot_df, metric, rolling)

# COMMAND ----------

