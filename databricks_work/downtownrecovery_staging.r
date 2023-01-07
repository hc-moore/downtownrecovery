# Databricks notebook source
## includes code adapted from the following sources:
# https://github.com/rstudio/shiny-examples/blob/master/087-crandash/
# https://github.com/eparker12/nCoV_tracker
# https://rviews.rstudio.com/2019/10/09/building-interactive-world-maps-in-shiny/
# https://github.com/rstudio/shiny-examples/tree/master/063-superzip-example

# Package names
packages <- c("broom", "ggplot2", "ggrepel", "readxl", "dplyr", "tidyr",  "MASS", "plotly", "scales", "shiny", "shinyWidgets", "SparkR", "tidyverse", "zoo")

# Packages loading
invisible(lapply(packages, library, character.only = TRUE))

hiveContext <- sparkR.session()

get_table_as_r_df <- function(table_name) {
  SparkR::as.data.frame(SparkR::sql(paste0("select * from ", table_name, " table")))
  }

# COMMAND ----------

# load in data
# see update_metrics_df for origins of all_seasonal_metrics and all_weekly_metrics
regions_df <- get_table_as_r_df("regions_csv")
explanatory_vars <- get_table_as_r_df("shinyapp_export_explanatory_vars")
all_seasonal_metrics <- get_table_as_r_df("shinyapp_export_all_seasonal_metrics")
all_weekly_metrics <- get_table_as_r_df("shinyapp_export_all_weekly_metrics")

# omit all references to outlier cities
#outlier_cities <-
#  c("Dallas", "Mississauga", "Orlando", "Oklahoma City")

#explanatory_vars <- explanatory_vars %>%
#  dplyr::filter(!(city %in% outlier_cities))

#all_weekly_lqs <- all_weekly_lqs %>%
#  dplyr::filter(!(city %in% outlier_cities))

#regions_df <- regions_df %>%
#  dplyr::filter(!(city %in% outlier_cities))

#metrics_df <- metrics_df %>%
#  dplyr::filter(!(city %in% outlier_cities))


# the 26th most populous city is denver, that is the cutoff point for large/medium
n = 26
largest_n_cities <- regions_df %>%
  dplyr::arrange(-population) %>%
  dplyr::mutate(pop_rank = rank(-population)) %>%
  dplyr::filter(pop_rank <= n) %>%
  dplyr::select(city)

all_weekly_metrics$week <-
  base::as.Date(all_weekly_metrics$week, format = "%Y-%m-%d")



named_factors <- c(
  # census vars
  "Total Jobs in Downtown" = "total_jobs", # always downtown
  "Total Population" = "total_pop", 
  "Population Density" = "population_density",
  "Total Housing Stock" = "housing_units", 
  "Housing Density" = "housing_density", 
  "Percentage of Rented-Occupied Units" = "pct_renter", 
  "Percentage of Single-Family Homes" = "pct_singlefam", 
  "Percentage of Multi-Family Homes" = "pct_multifam",
  "Percentage of Mobile Homes and Others" = "pct_mobile_home_and_others",
  "Median Age of Residents" = "median_age",
  "Percentage of Residents with a Bachelor's Degree or Higher" = "bachelor_plus",
  "Percentage of Vacant Housing Units" = "pct_vacant",
  "Median Rent of Housing Units" = "median_rent",
  "Median Household Income of Residents" = "median_hhinc",
  "Percentage of White Residents" = "pct_nhwhite", # always city
  "Percentage of Black Residents" = "pct_nhblack", # always city
  "Percentage of Hispanic Residents" = "pct_hisp", # always city
  "Percentage of Asian Residents" = "pct_nhasian", # always city
  "Percentage of Residents with Other Races" = "pct_others", # always city
  "Percentage of Residents who Commute to Work by Car" = "pct_commute_auto", # always city
  "Percentage of Residents who Commute to Work by Public Transit" = "pct_commute_public_transit", # always city
  "Percentage of Residents who Commute to Work by Bicycle" = "pct_commute_bicycle", # always city
  "Percentage of Residents who Commute to Work by Walking" = "pct_commute_walk", # always city
  "Percentage of Residents who Commute to Work by Other Modes" = "pct_commute_others", # always city
  "Average Commute Time" = "average_commute_time", #always city
  # LEHD / employment vars, always downtown
  "Employment Density in Downtown" = "employment_density",
  "Employment Entropy in Downtown" = "employment_entropy",
  "Percentage of Jobs in Agriculture, Forestry, Fishing, and Hunting in Downtown" = "pct_jobs_agriculture_forestry_fishing_hunting",
  "Percentage of Jobs in Mining, Quarrying, Oil, and Gas in Downtown" = "pct_jobs_mining_quarrying_oil_gas",
  "Percentage of Jobs in Utilities in Downtown" = "pct_jobs_utilities",
  "Percentage of Jobs in Construction in Downtown" = "pct_jobs_construction",
  "Percentage of Jobs in Manufacturing in Downtown" = "pct_jobs_manufacturing",
  "Percentage of Jobs in Wholesale Trade in Downtown" = "pct_jobs_wholesale_trade",
  "Percentage of Jobs in Retail Trade in Downtown" = "pct_jobs_retail_trade",
  "Percentage of Jobs in Transportation and Warehousing in Downtown" = "pct_jobs_transport_warehouse",
  "Percentage of Jobs in Information in Downtown" = "pct_jobs_information",
  "Percentage of Jobs in Finance & Insurance in Downtown" = "pct_jobs_finance_insurance",
  "Percentage of Jobs in Real Estate in Downtown" = "pct_jobs_real_estate",
  "Percentage of Jobs in Professional, Scientific, and Management in Downtown" = "pct_jobs_professional_science_techical",
  "Percentage of Jobs in Management in Downtown" = "pct_jobs_management_of_companies_enterprises",
  "Percentage of Jobs in Administrative Support & Waste Management in Downtown" = "pct_jobs_administrative_support_waste",
  "Percentage of Jobs in Educational Services in Downtown" = "pct_jobs_educational_services",
  "Percentage of Jobs in Healthcare & Social Assistance in Downtown" = "pct_jobs_healthcare_social_assistance",
  "Percentage of Jobs in Arts, Entertainment, and Recreation in Downtown" = "pct_jobs_arts_entertainment_recreation",
  "Percentage of Jobs in Accommodation & Food Services in Downtown" = "pct_jobs_accomodation_food_services",
  "Percentage of Jobs in Public Administration in Downtown" = "pct_jobs_public_administration",
  "Percentage of Jobs in Other Categories in Downtown" = "pct_jobs_other",
  # covid policies, all at city level
  "Composite of Closing" = "composite_closing",
  "Composite of Economic" = "composite_economic",
  "Average of C1_School closing" = "school_closing",
  "Average of C2_Workplace closing" = "workplace_closing",
  "Average of C3_Cancel public events" = "cancel_public_events",
  "Average of C4_Restrictions on gatherings" = "restrict_gatherings",
  "Average of C6_Stay at home requirements" = "stay_at_home",
  "Average of E1_Income support" = "income_support",
  "Average of E2_Debt relief" = "debt_relief",
  "Average of H6_Facial Coverings" = "facial_coverings",
  "Average of H7_Vaccination policy" = "vaccination_policy",
  # political leaning - perhaps rename pct_other to pct_other_leaning after saving the csv for the sake of clarity, all at city level
  "Percentage Liberal Leaning" = "pct_liberal_leaning",
  "Percentage Conservative Leaning" = "pct_conservative_leaning",
  "Percentage Other Leaning" = "pct_other"
  #weather - all at city level
)

named_periods <- c(
  "Spring: Mar 2020 - May 2020" = "Season_1",
  "Summer: June 2020 - Aug 2020" = "Season_2",
  "Fall: Sept 2020 - Nov 2020" = "Season_3",
  "Winter: Dec 2020 - Feb 2021" = "Season_4",
  "Spring: Mar 2021 - May 2021" = "Season_5",
  "Summer: June 2021 - Aug 2021" = "Season_6",
  "Fall: Sept 2021 - Nov 2021" = "Season_7",
  "Winter: Dec 2021 - Feb 2022" = "Season_8",
  "Spring: Mar 2022 - May 2022" = "Season_9"
)

named_metrics <- c(
  "Recovery Quotient (RQ) Downtown" = "downtown",
  "Recovery Quotient (RQ) City" = "city",
  "Location Quotient (LQ)" = "relative"
)

named_sg_field_type <- c(
  "Normalized visits by state scaling" = "normalized_visits_by_state_scaling",
  "Normalized visits by total visits" = "normalized_visits_by_total_visits"
)

# selects the top 3 of each region to plot by default
explanatory_cities <- regions_df %>%
  dplyr::group_by(region, metro_size) %>%
  dplyr::arrange(-population) %>%
  dplyr::slice_head(n = 3)

patterns_cities <- regions_df %>%
  dplyr::group_by(region, metro_size) %>%
  dplyr::arrange(-population) %>%
  dplyr::slice_head(n = 1)

ranking_cities <- regions_df %>%
  dplyr::group_by(region, metro_size) %>%
  dplyr::arrange(-population) %>%
  dplyr::slice_head(n = 5)

# get random subset of those cities to initialize city select plots
regions_choices <- lapply(split(regions_df$display_title, regions_df$region), as.list)

selected_regions <- unlist(lapply(regions_choices, base::sample, 5))

# COMMAND ----------

# ui function
### SHINY UI ###
ui <- navbarPage("Downtown Recovery",
         tabPanel(value = "recovery_rankings", "Recovery Rankings",
             fillRow(
               sidebarLayout(
                 sidebarPanel(
                   pickerInput(
                     inputId = "rankings_metro_size",
                     label = "Select which city size to include in ranking. Multiple sizes can be selected at a time.",
                     choices = c("Large" = "large",
                                 "Medium" = "medium"),
                     selected = c("large", "medium"),
                     multiple = TRUE,
                     options = list(
                       `actions-box` = FALSE,
                       `none-selected-text` = "Please make a selection!"
                     )
                   ),
                   pickerInput(
                     inputId = "rankings_regions",
                     label =  "Select which regions to include in ranking. Multiple regions can be selected at a time.",
                     choices = sort(unique(regions_df$region)),
                     selected = sort(unique(regions_df$region)),
                     multiple = TRUE, 
                     options = list(
                       `actions-box` = FALSE,
                       `none-selected-text` = "Please make a selection!"
                     )
                   ),
                    pickerInput(
                      "rankings_cities",
                      label = "Select which cities to include in ranking. Cities are filtered according to size and region.",
                      choices = regions_choices,
                      options = list(`actions-box` = TRUE, `none-selected-text` = "Please make a selection!"),
                      multiple = TRUE,
                      selected = unlist(regions_choices),
                      pickerOptions(mobile = TRUE)
                    ),
                   pickerInput(
                     "rankings_sg_field",
                     label = "Select which Safegraph field to use. Only one field can be selected at a time.",
                     choices = named_sg_field_type,
                     selected = "state",
                     multiple = FALSE,
                     pickerOptions(mobile = TRUE)
                   ),
                   pickerInput(
                     "recovery_rankings_metric",
                     label = "Select which metric to use in the ranking. Only one metric can be selected at a time.",
                     choices = named_metrics,
                     selected = "downtown",
                     multiple = FALSE,
                     pickerOptions(mobile = TRUE)
                   ),
                   sliderTextInput(
                     "recovery_rankings_period",
                     label = "Select the comparison period. Only one period can be selected at a time.",
                     choices = named_periods,
                     selected = named_periods[4]
                   )
                 ),
                 mainPanel(
                   tags$style(
                      "#recovery_ranking {height: calc(100vh) !important;}"
                    ),
                   plotOutput("recovery_ranking")
                 )
               )
             )
    ),
    tabPanel(value = "recovery_patterns", "Recovery Patterns",
             fillRow(
               sidebarLayout(
                 sidebarPanel(
                   pickerInput(
                     "recovery_patterns_cities",
                     label = "Select cities to include in the plot. Multiple cities can be selected at a time.",
                     choices = regions_choices,
                     options = list(`actions-box` = TRUE, `none-selected-text` = "Please make a selection!"),
                     selected = patterns_cities$display_title,
                     multiple = TRUE,
                     pickerOptions(mobile = TRUE)
                   ),
                   pickerInput(
                     "recovery_patterns_sg_field",
                     label = "Select which Safegraph field to use. Only one field can be selected at a time.",
                     choices = named_sg_field_type,
                     selected = named_sg_field_type[1],
                     multiple = FALSE,
                     pickerOptions(mobile = TRUE)
                   ),
                   pickerInput(
                     "recovery_patterns_metric",
                     label = "Select which metric to use in the ranking. Only one metric can be selected at a time.",
                     choices = named_metrics,
                     selected = "downtown",
                     multiple = FALSE,
                     pickerOptions(mobile = TRUE)
                   ),
                   sliderInput(
                     "rolling_window",
                     label = "Selecting rolling average window to smooth weekly variation.",
                     min = 1,
                     max = 25,
                     value = 15,
                     step = 2
                   )
                 ),
                 mainPanel(
                   tags$style(
                     "#raw_recovery_plot {height: calc(100vh - 125px) !important;}"
                   ),
                   plotlyOutput("raw_recovery_plot")
                 )
               )
             )
    ),
    tabPanel(value = "explanatory_variables", "Explanatory Variables",
             fillRow(
               sidebarLayout(
                 sidebarPanel(
                   pickerInput(
                     "slr_metric",
                     label = "Select which metric to plot. Only one metric can be selected at a time.",
                     choices = named_metrics,
                     selected = "downtown",
                     multiple = FALSE,
                     pickerOptions(mobile = TRUE)
                   ),
                   pickerInput(
                     "explanatory_sg_field",
                     label = "Select which Safegraph field to use. Only one field can be selected at a time.",
                     choices = named_sg_field_type,
                     selected = named_sg_field_type[1],
                     multiple = FALSE,
                     pickerOptions(mobile = TRUE)
                   ),
                   pickerInput(
                     "x",
                     label = "Select independent variable. Only one variable can be selected at a time.",
                     choices = named_factors,
                     selected = "pct_jobs_information",
                     multiple = FALSE,
                     pickerOptions(mobile = TRUE)
                   ),
                   sliderTextInput(
                     "y",
                     label = "Select dependent variable. Only one variable can be selected at a time.",
                     choices = named_periods,
                     selected = named_periods[4]
                   ),
                   pickerInput(
                     "slr_cities",
                     label = "Select which cities to highlight in the plot. Multiple cities can be selected at a time.",
                     choices = regions_choices,
                     options = list(`actions-box` = TRUE, `none-selected-text` = "Please make a selection!"),
                     selected = explanatory_cities$display_title,
                     multiple = TRUE,
                     pickerOptions(mobile = TRUE)
                   ),
                   verbatimTextOutput("slr_summary")
                 ),
                 mainPanel(
                   tabsetPanel(
                   tabPanel(
                     "Single variable plot",
                     tags$style(
                       type = "text/css",
                       "#explanatory_plot {height: calc(100vh - 140px) !important;}"
                     ),
                     plotlyOutput(
                       "explanatory_plot"
                       ),
                     
                   ),
                     tabPanel("Table of coefficients",
                             verbatimTextOutput("slr_coeffs")
                     )
                     
                   )
                    
                 )
                   )
                 )
               )
)

# COMMAND ----------

# server function
### SHINY SERVER ###

server = function(input, output) {
  # reactive helper 'get' functions that respond to user selections
  # access user inputs by using input$name_of_ui_element[1]

  getRegressionDF <- reactive({
    # getRegressionDF is meant to be used with slr.
    # allows multi-city selection, but only one x or y can be selected at a time
    # filters on user selected metric as well as multi-city choice
    
    y <- all_seasonal_metrics %>%
      dplyr::filter((metric == input$slr_metric) &
                    (sg_field == input$explanatory_sg_field[1]) &
                   (Season == input$y[1])) %>%
      dplyr::select(city, metric, Season)
    
    colnames(y) <- c("city", "metric", "y")
    
    if (input$slr_metric != "relative") {
      X <- explanatory_vars %>%
        dplyr::filter(geography == input$slr_metric[1]) %>%
        dplyr::filter(Season == input$y[1]) %>%
        dplyr::select(city, display_title, metro_size, region, starts_with(input$x[1]), geography)
    } else {
    X <- explanatory_vars %>%
        dplyr::filter(Season == input$y[1]) %>%
        dplyr::select(city, display_title, metro_size, region, starts_with(input$x[1]), geography)
      }
    X <- explanatory_vars %>%
        dplyr::filter(Season == input$y[1]) %>%
        dplyr::select(city, display_title, metro_size, region, starts_with(input$x[1]), geography)
    colnames(X) <- c("city", "display_title", "metro_size", "region", "x", "geography")
    
    unique(y %>%
      inner_join(X, by = "city") %>%
      dplyr::mutate(key_study_case = display_title %in% input$slr_cities)) 
      
  })
  
  getRawRecoveryDF <- reactive({
    # getRawRecoveryDF is meant to be used with raw recovery plots
    # allows multi-city selection
    # filters on user selected metric as well as multi-city choice
    all_weekly_metrics[!is.na(all_weekly_metrics$value),] %>%
      dplyr::filter((display_title %in% input$recovery_patterns_cities) &
                    (week >= as.Date("2020-03-01")) & 
                    (metric == input$recovery_patterns_metric[1]) &
                    (sg_field == input$recovery_patterns_sg_field[1])) %>%
      dplyr::select(city, metric, week, sg_field, value, display_title, region) %>%
      dplyr::arrange(week) %>%
      dplyr::group_by(city) %>%
      dplyr::mutate(rolling_avg = rollmean(
        value,
        as.numeric(input$rolling_window[1]),
        na.pad = TRUE,
        align = "center"
      )) %>%
      dplyr::ungroup()
  })
  
  getRawRankingDF <- reactive({
    # creates the df to be used in generating bar chart ranking plot
    # the first filter is on city size
    # from that, the user is allowed to select cities to include or exclude
    # selecting large will omit all medium sized cities from the selection and vice versa
    # to mix and match, the user must select all then manually deselect cities
    # this is to avoid confusion between dropdowns and removes the need for multiple legends 
    
    # largest_n_cities is defined globally with a hard coded number for largest. this should not be up to the user to define,
    # but rather something that is adjusted by the authors of the script at their discretion and what aligns with 
    # definitions used in the study
    
    ranking_df <- all_seasonal_metrics %>%
      dplyr::filter(
        (metric == input$recovery_rankings_metric[1]) &
                    (metro_size %in% input$rankings_metro_size) &
                    (region %in% input$rankings_regions) &
                    (display_title %in% input$rankings_cities) &
                    (sg_field == input$rankings_sg_field[1])) %>%
      dplyr::select(display_title,
                    metro_size,
                    input$recovery_rankings_period[1],
                    region,
                    metric,
                    city) %>%
    dplyr::distinct(city, .keep_all = TRUE)

    colnames(ranking_df) <-
      c("display_title",
        "metro_size",
        "recovery_rankings_period",
        "region",
        "metric",
        "city")
    ranking_df
  })
  
  # explanatory plot - reactive
  output$explanatory_plot <- renderPlotly({
    plot_df <- getRegressionDF() %>%
      dplyr::distinct(city, x, y, geography, .keep_all = TRUE)
    
    g1 <- ggplot(plot_df, aes(x = x, y = y)) +
      geom_smooth(
        method = "lm",
        formula = "y~x",
        alpha = 0.3,
        linetype = 0,
        na.rm = TRUE,
        fullrange = TRUE
      ) +
       stat_smooth(
         geom = "line",
         method = "lm",
         formula = "y~x",
         alpha = .75,
         linetype = "dashed",
         na.rm = TRUE,
         fullrange = TRUE
       ) +
      geom_point(aes(color = region,
                    text = paste0("<b>City: </b>", city, "<br>",
                                 "<b>", input$x[1], ": </b>", round(x, 2), "<br>",
                                 "<b>", input$y[1], ": </b>", round(y, 2), "<br>"),
                     size = 5)) +
      theme(plot.title = element_text(size = 16, hjust = .5),
            axis.title = element_text(size = 12),
            plot.subtitle = element_text(size = 14, hjust = .5)) +
      labs(x = names(named_factors[named_factors == input$x[1]]),
           y = names(named_periods[named_periods == input$y[1]]),
           color = "Region") +
      scale_color_manual(values = c("Canada" = "#e41a1c",
                                    "Midwest" = "#377eb8",
                                    "Northeast" = "#4daf4a",
                                    "Pacific" = "#984ea3",
                                    "Southeast" = "#ff7f00",
                                    "Southwest" = "#e6ab02"))
    if (length(unique(plot_df$geography)) > 1) {
      g1 <- g1 +
      facet_wrap(.~geography, scales = "free")
    }
    ggplotly(g1, tooltip = "text")
    
    })

  output$slr_summary <- renderPrint({
    model_df <- getRegressionDF()
    
    model.formula <- as.formula("y ~ x")
    model.ols <- stats::lm(model.formula, model_df)
    print(summary(model.ols))
  })
  
  
  # raw recovery patterns plot - reactive
  output$raw_recovery_plot <- renderPlotly({
    df <- na.omit(getRawRecoveryDF())
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
      text = paste0("<b>City: </b>", city, "<br>",
                   "<b>Week: </b>", week, "<br>",
                  "<b>Rolling average: </b>", rolling_avg)
    ) + geom_line(size = 1) +
      labs(title = paste(names(named_metrics[named_metrics == input$recovery_patterns_metric[1]]), "weekly average"),
           subtitle = paste(input$rolling_window[1], "week rolling average"),
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
    ggplotly(g1, tooltip = "text")
    })
  
  output$slr_coeffs <- renderPrint({
    models <- data.frame(matrix(ncol = 5, nrow = 0))
    
    y <- all_seasonal_metrics %>%
      dplyr::filter((metric == input$slr_metric) &
                    (sg_field == input$explanatory_sg_field[1]) &
                    (Season == input$y[1])) %>%
      dplyr::select(city, metric, Season)
    
    if (input$slr_metric != "relative") {
      X <- explanatory_vars %>%
        dplyr::filter(geography == input$slr_metric[1]) %>%
        dplyr::filter(Season == input$y[1])
    } else {
    X <- explanatory_vars %>%
        dplyr::filter(Season == input$y[1])
      }
    model_df <- unique(y %>%
      inner_join(X, by = "city")) 
    
    for (x in named_factors) {
      model.formula <- as.formula(paste(Season, "~", x, sep = " "))
      model.ols <- lm(model.formula, model_df)
      models <-
        do.call(rbind, list(models, tidy(summary(model.ols))))
    }
    print(models %>%
            dplyr::filter(term != "(Intercept)"), n = Inf)
  })
  
  
  
  
  # recovery ranking plot - reactive
  output$recovery_ranking <- renderPlot({
    df <- na.omit(getRawRankingDF()) %>%
      dplyr::filter(!is.na(recovery_rankings_period)) %>%
      dplyr::arrange(-recovery_rankings_period) %>%
      dplyr::mutate(lq_rank = rank(-recovery_rankings_period, ties.method = "first")) %>%
      dplyr::ungroup()
    g1 <-
      ggplot(df) + aes(lq_rank,
                       group = display_title,
                       fill = region) +
      geom_tile(
        aes(y = recovery_rankings_period / 2, height = recovery_rankings_period, width = 1), 
        alpha = .8,
        color = "white") +
      
      geom_text(
        aes(y = 0, label = paste("", lq_rank, ":", display_title,  ":", round(recovery_rankings_period, 2))),
        color = "white",
        hjust = "inward",
        size = 6
      ) +
      coord_flip(clip = "off", expand = FALSE) +
      labs(title = paste(names(named_metrics[named_metrics == input$recovery_rankings_metric[1]]), "Recovery Rankings"),
           subtitle = names(named_periods[named_periods == input$recovery_rankings_period[1]]),
           fill = "Region") +
      scale_y_continuous("", labels = scales::comma) +
      scale_x_reverse("") +
      theme(panel.grid = element_blank(),
            axis.text.y = element_blank(),
            axis.title = element_blank(),
            axis.title.y = element_blank(),
            plot.title = element_text(size = 20, hjust = .5),
            plot.subtitle = element_text(size = 14, hjust = .5)
            #plot.margin = unit(c(1, 1, 1, 3), "cm")
            ) +
      scale_fill_manual(values = c("Canada" = "#e41a1c",
                                    "Midwest" = "#377eb8",
                                    "Northeast" = "#4daf4a",
                                    "Pacific" = "#984ea3",
                                    "Southeast" = "#ff7f00",
                                    "Southwest" = "#e6ab02"))
    g1
  })
}

# COMMAND ----------

# actually run the app
#shinyApp(ui = ui, server = server)

# COMMAND ----------

