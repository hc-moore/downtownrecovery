# Databricks notebook source
# Package names
packages <- c("astsa", "broom", "fable", "forecast", "ggplot2", "readxl", "dplyr", "tidyr",  "MASS", "scales", "smoother", "stats", "SparkR", "tidyverse", "vars", "zoo")

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

all_weekly_metrics <- get_table_as_r_df('all_weekly_metrics')
full_model_df <- get_table_as_r_df('model_data_full')

outlier_cities <- c("Hamilton")

all_weekly_metrics <- all_weekly_metrics %>%
  dplyr::filter(!(city %in% outlier_cities))

full_model_df <- full_model_df %>%
  dplyr::filter(!(city %in% outlier_cities))

# COMMAND ----------

dbutils.widgets.dropdown("metric", "downtown", list("downtown", "city", "relative")) # default to downtown
dbutils.widgets.dropdown("city", "San Francisco", unique(all_weekly_metrics$city) %>% as.list())
dbutils.widgets.dropdown("lag", "1", as.character(seq(1:52)) %>% as.list())
dbutils.widgets.dropdown("samples", "1", as.character(seq(1:52)) %>% as.list())
dbutils.widgets.dropdown("difference_order", "1", as.character(seq(1:52)) %>% as.list())
dbutils.widgets.dropdown("forecast", "2", as.character(seq(2:52)) %>% as.list())

# COMMAND ----------

# EVERY city 
all_time_series <- ts(all_weekly_metrics %>%
  dplyr::filter((metric == dbutils.widgets.get('metric') &
                (week > base::as.Date("2020-03-16")))) %>%
  dplyr::arrange(week) %>%
  dplyr::select(city, normalized_visits_by_total_visits, week) %>%
  tidyr::pivot_wider(id_cols = week, values_from = normalized_visits_by_total_visits, names_from = city) %>%
  dplyr::select(-week))

display(as.data.frame(all_time_series))

# COMMAND ----------

# MAGIC %md
# MAGIC # Autocorrelation function of a series

# COMMAND ----------

X <- all_time_series[, dbutils.widgets.get('city')]
acf2(X, gg = TRUE, col=2:7, lwd=4, pacf = TRUE, main =  dbutils.widgets.get('city'));

# COMMAND ----------

# MAGIC %md
# MAGIC # AR(lag) model
# MAGIC 
# MAGIC Regresses activity at time \\(t\\) on activity at time \\(t-n\\)

# COMMAND ----------

# simple little AR(1) model
# one week's lag
X <- all_time_series[, dbutils.widgets.get('city')]
lag1.plot(X, as.numeric(dbutils.widgets.get('lag')), main = dbutils.widgets.get('city'))

# COMMAND ----------

X <- all_time_series[, dbutils.widgets.get('city')]
Xlag1=lag(X, -as.numeric(dbutils.widgets.get('lag')))
y=cbind(X,Xlag1) 
ar1fit=lm(y[,1]~y[,2])
summary(ar1fit)

# COMMAND ----------

# MAGIC %md
# MAGIC #  Residuals and fitted values of AR(1) model

# COMMAND ----------

plot(ar1fit$fit, ar1fit$residuals) 

# COMMAND ----------

# MAGIC %md
# MAGIC # ACF and PACF of residuals

# COMMAND ----------

acf2(ar1fit$residuals, xlim=c(1,52))

# COMMAND ----------


X <- all_time_series[, dbutils.widgets.get('city')]
plot(decompose(ts(X, freq = as.numeric(dbutils.widgets.get('samples'))), type = "additive"))

# COMMAND ----------

plot(decompose(ts(all_time_series[, dbutils.widgets.get('city')], freq = as.numeric(dbutils.widgets.get('samples'))), type = "multiplicative"))

# COMMAND ----------

X <- all_time_series[, dbutils.widgets.get('city')]
diff_n <- diff(X, as.numeric(dbutils.widgets.get('difference_order')))
acf2(diff_n)

# COMMAND ----------

# MAGIC %md
# MAGIC # N-th difference plots of randomly selected cities

# COMMAND ----------

display(all_weekly_metrics)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # N-th difference plots

# COMMAND ----------

# nth-difference plots
nth_diff <- all_weekly_metrics %>%
        dplyr::filter(week > base::as.Date('2020-03-16')) %>%
        dplyr::select(city, week, normalized_visits_by_total_visits) %>%
        dplyr::group_by('city', 'week') %>%
        dplyr::mutate(lag_n = normalized_visits_by_total_visits - dplyr::lag(normalized_visits_by_total_visits, as.numeric(dbutils.widgets.get('difference_order')))) %>%
        ungroup()

ggplot(data = nth_diff, aes(x = week, group = city)) + 
  geom_line(aes(y = normalized_visits_by_total_visits)) + 
  facet_wrap(city ~ .)

# COMMAND ----------

# MAGIC %md
# MAGIC # SARIMA(n, 0, 0)

# COMMAND ----------

X <- all_time_series[, dbutils.widgets.get('city')]
sarima(X, as.numeric(dbutils.widgets.get('difference_order')), 0, 0)

# COMMAND ----------

# MAGIC %md
# MAGIC # ARIMA forecasting

# COMMAND ----------

# forecasting
X <- all_time_series[, dbutils.widgets.get('city')]
Fx <- sarima.for(X, as.numeric(dbutils.widgets.get('forecast')), as.numeric(dbutils.widgets.get('difference_order')), 0, 0)

# COMMAND ----------

X <- all_time_series[, dbutils.widgets.get('city')]
spectral_values <- mvspec(X, log='no') # no smoothing
vals <- spectral_values$details

# COMMAND ----------

vals[order(vals[,'spectrum'], decreasing = TRUE),]# get max spectral density, get frequency, get period of city's recovery pattern

# COMMAND ----------

# individually tune each city- then do regression
y <- mean(as.vector(Fx$pred))
X_feat <- full_model_df %>%
  dplyr::filter(city == dbutils.widgets.get('city')) %>%
  dplyr::select(ends_with('city'), ends_with('downtown'), starts_with('pct_jobs')) %>%
  dplyr::distinct(city, .keep_all = TRUE)

# COMMAND ----------

# city's weekly average should follow a normal distribution
X <- all_time_series[, dbutils.widgets.get('city')]
acf2(diff(X, as.numeric(dbutils.widgets.get('difference_order'))), gg = TRUE, col=2:7, lwd=4, pacf = TRUE, main =  paste(dbutils.widgets.get('city'), 'diff order', dbutils.widgets.get('difference_order')))

# COMMAND ----------

# how similar is this week to the previous week
# very far from the expected error value (dotted lines) -- probably need a higher order autoregression 
# the farther out the lag, the closer that week is to the week of comparison
# aka, corr(y_t, y_{t-k}), k = 1,2,...
sampled_cities <- all_time_series[, c('Oakland', 'San Francisco', 'Los Angeles', 'San Jose', 'San Diego')]
acfm(diff(sampled_cities, as.numeric(dbutils.widgets.get('difference_order'))), gg = TRUE, col=2:7, pacf = TRUE, lwd=4)

# COMMAND ----------

# MAGIC %md
# MAGIC # Automatic ARIMA fitting

# COMMAND ----------

X <- all_time_series[, dbutils.widgets.get('city')]
arimafit <- auto.arima(X)
fcast <- forecast(arimafit)
plot(fcast)

# COMMAND ----------

summary(forecast(arimafit))

# COMMAND ----------

