#===============================================================================
# Create plots and maps showing recovery patterns for Business Improvement Areas
# for Toronto City Council presentation.
#
# Author: Julia Greenberg
# Date created: 5/22/23
#===============================================================================

#-----------------------------------------
# Load packages
#-----------------------------------------

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'ggplot2', 'sf', 'lubridate', 'plotly', 'zoo', 
       'htmlwidgets', 'BAMMtools', 'leaflet', 'caret'))

#-----------------------------------------
# Load data
#-----------------------------------------

filepath_sf <- "C:/Users/jpg23/data/downtownrecovery/shapefiles/"

## Shapefile of BIAs

bia_sf <- read_sf(paste0(filepath_sf, "business_improvement_areas_simplified.geojson")) %>%
  select(bia = AREA_NAME)

head(bia_sf)
class(bia_sf)
n_distinct(bia_sf$bia) # there are 85 BIAs

## Study area downtowns filtered to Toronto ('inner core')

core <- read_sf(paste0(filepath_sf, "study_area_downtowns.shp")) %>%
  filter(city == 'Toronto')

core

## Former municipal boundaries of Toronto ('outer core')
## https://open.toronto.ca/dataset/former-municipality-boundaries/

inner_ring <- read_sf(paste0(filepath_sf, "Former_Municipality_Boundaries.geojson")) %>%
  filter(AREA_NAME == 'TORONTO') %>%
  select(geometry) %>%
  st_as_sf()

# inner_ring <- st_union(inner_ring) %>% st_as_sf()

head(inner_ring)
plot(inner_ring$geometry)

## 1/1/2019 - 4/25/2023 (userbase + BIAs)

filepath <- 'C:/Users/jpg23/data/downtownrecovery/spectus_exports/BIAs/'

both <- read_delim(
  paste0(filepath, "bia_userbase/20230522_190855_00007_3yd5w_80fe0af5-8ae8-468e-ad40-4d1714548545.gz"),
  delim = '\001',
  col_names = c('bia', 'provider', 'n_devices', 'userbase', 'date'),
  col_types = c('cciii')
) %>%
  mutate(date = as.Date(as.character(date), format = "%Y%m%d")) %>%
  data.frame() %>%
  arrange(date)

## 4/25/2023 - 5/19/2023 (userbase)

userbase <-
  list.files(path = paste0(filepath, 'userbase')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'userbase/', .),
    delim = '\001',
    col_names = c('province', 'userbase', 'provider', 'date'),
    col_types = c('cici')
    )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(date), format = "%Y%m%d")) %>%
  arrange(date)

## 5/15/2023 - 5/19/2023 (BIAs)

bia <-
  list.files(path = paste0(filepath, 'bia')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'bia/', .),
    delim = '\001',
    col_names = c('bia', 'provider', 'n_devices', 'date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(date), format = "%Y%m%d")) %>%
  arrange(date)

head(both)
glimpse(both)
summary(both)

head(userbase)
glimpse(userbase)
summary(userbase)

head(bia)
glimpse(bia)
summary(bia)

range(both$date)
range(userbase$date)
range(bia$date)

# Combine them
b0 <-
  userbase %>%
  filter(province == 'Ontario') %>%
  select(-province) %>%
  left_join(bia, by = c('provider', 'date')) %>%
  filter(date > as.Date('2023-04-25')) %>%
  rbind(both) %>%
  # Switch providers on 5/17/2021
  filter((provider == '700199' & date < as.Date('2021-05-17')) | 
           (provider == '190199' & date >= as.Date('2021-05-17'))) %>%
  select(-provider) %>%
  mutate(date_range_start = 
           lubridate::floor_date(date, 
                                 unit = "week", 
                                 week_start = getOption("lubridate.week.start", 
                                                        1))) %>%
  group_by(bia, date_range_start) %>%
  summarize(n_devices = sum(n_devices, na.rm = T),
            userbase = sum(userbase, na.rm = T)) %>%
  ungroup() %>%
  mutate(normalized = n_devices/userbase) %>%
  mutate(source = 'spectus') %>%
  data.frame()

head(b0)
summary(b0)
range(b0$date)

## Replace 2019 data with Safegraph
safegraph_raw <- read.csv('C:/Users/jpg23/data/downtownrecovery/safegraph_toronto_BIA.csv')

View(safegraph_raw)
head(safegraph_raw)
glimpse(safegraph_raw)

safegraph <-
  safegraph_raw %>%
  mutate(date_range_start = as.Date(date_range_start),
         userbase = raw_visit_counts/normalized_visits_by_total_visits) %>%
  group_by(bia = AREA_NA8, date_range_start) %>%
  summarize(
    n_devices = sum(raw_visit_counts, na.rm = T),
    userbase = sum(userbase, na.rm = T)
  ) %>%
  ungroup() %>%
  mutate(source = 'safegraph',
         normalized = n_devices/userbase) %>%
  select(userbase, source, date_range_start, bia, n_devices, normalized) %>%
  data.frame()

head(safegraph)

#---------------------------------------------------------------------

range(b0$date_range_start)
range(safegraph19$date_range_start)

shared_start <- "2021-11-01"
shared_end <- "2022-02-21"

# Filter safegraph to only the last 3 months (to see overlap with spectus)
safegraph_subset <- safegraph %>%
  filter((date_range_start <= shared_end) & (date_range_start >= shared_start))

all_counts <- bind_rows(safegraph_subset, b0) %>%
  filter((date_range_start <= shared_end) & (date_range_start >= shared_start)) %>%
  distinct(date_range_start, bia, source, .keep_all = TRUE)

head(all_counts)

n_weeks <- all_counts %>% distinct(date_range_start) %>% nrow()

n_weeks

all_counts1 <- all_counts %>%
  group_by(source, date_range_start) %>%
  summarize(userbase = sum(userbase, na.rm = T),
            n_devices = sum(n_devices, na.rm = T),
            normalized = n_devices/userbase) %>%
  ungroup() %>%
  data.frame()

head(all_counts1)

ggplotly(all_counts1 %>%
           ggplot(aes(x = date_range_start, 
                      y = normalized, 
                      color = source, 
                      alpha = .75)) +
           geom_line())

minmax_normalizer <- function(x, min_x, max_x) {
  return((x- min_x) /(max_x-min_x))
}

bia_source_scaling_df <- all_counts %>%
  group_by(bia, source) %>%
  summarise(min_val = min(normalized, na.rm = TRUE),
            max_val = max(normalized, na.rm = TRUE)) %>%
  ungroup() %>% data.frame()

head(bia_source_scaling_df)

comparisons_df <- all_counts %>% 
  left_join(bia_source_scaling_df) %>%
  mutate(minmax_scaled = minmax_normalizer(normalized, min_val, max_val))

head(comparisons_df)

comparisons_with_diff <- comparisons_df %>%
  pivot_wider(
    id_cols = c('date_range_start', 'bia'),
    names_from = 'source',
    # change this to test different comparison metrics
    values_from = 'normalized') %>%
  arrange(date_range_start) %>%
  group_by(bia) %>%
  mutate(
    normalized_safegraph_spectus_diff = spectus - safegraph
  ) %>%
  data.frame()

head(comparisons_with_diff)

normalized_spectus <-  b0 %>%
  # Keep spectus data only after 2022-02-21
  filter(date_range_start >= shared_end) %>%
  left_join(comparisons_with_diff %>%
              ungroup() %>%
              filter(
                (date_range_start >= '2022-01-01') &
                       (date_range_start <= shared_end)) %>%
              group_by(bia) %>%
              summarise(
                #' on average, what would spectus's normalized visits have to be multiplied by to
                #' get safegraph's normalized visits for the last ___? 
                ratio = mean(safegraph / spectus)) %>% 
              filter(!is.na(ratio)) %>%
              distinct()
  ) %>%
  group_by(date_range_start, bia) %>%
  mutate(normalized = ratio * normalized,
         source = 'normalized_spectus') %>%
  ungroup() %>%
  select(-ratio)

normalized_spectus %>% glimpse()

all_together <- rbind(safegraph_subset, b0, normalized_spectus)

head(all_together)

ggplotly(all_together %>% 
           filter(bia == 'Financial District' & source != 'spectus') %>%
           ggplot(aes(x = date_range_start, 
                      y = normalized, 
                      color = source,
                      alpha = .8)) +
           geom_line() +
           facet_wrap(.~bia, ncol = 6, scales = 'free'))

which_region <-
  bia_sf %>%
  st_join(core %>% mutate(core = 'yes') %>% select(core)) %>%
  st_join(inner_ring %>% mutate(inner_ring = 'yes')) %>%
  st_drop_geometry() %>%
  data.frame()

# By BIA
rq_spectus <- rbind(safegraph %>%
                      ungroup() %>%
                      filter(date_range_start <= shared_end) %>%
                      select(date_range_start, bia, normalized),
                    normalized_spectus %>%
                      filter(date_range_start > shared_end) %>%
                      select(date_range_start, bia, normalized)) %>%
  mutate(week_num = lubridate::isoweek(date_range_start),
         year = lubridate::year(date_range_start)) %>%
  select(-date_range_start) %>%
  pivot_wider(id_cols = c('bia', 'week_num'),
              names_from = 'year',
              names_prefix = 'ntv_',
              values_from = 'normalized') %>%
  mutate(rec_2020 = ntv_2020 / ntv_2019,
         rec_2021 = ntv_2021 / ntv_2019,
         rec_2022 = ntv_2022 / ntv_2019,
         rec_2023 = ntv_2023 / ntv_2019) %>%
  pivot_longer(cols = starts_with('rec_'),
               names_to = "year",
               values_to = "rq") %>%
  mutate(year = substr(year, 5, 9),
         week = as.Date(paste(year, week_num, 1, sep = "_"), format = "%Y_%W_%w")) %>%
  filter(!is.na(rq)) %>%
  data.frame() %>%
  arrange(bia, year, week_num) %>%
  group_by(bia) %>%
  mutate(rq_rolling = zoo::rollmean(rq, k = 11, fill = NA, align = 'right')) %>%
  ungroup() %>%
  data.frame() %>%
  filter(!is.na(rq_rolling)) %>%
  left_join(which_region) %>%
  mutate(
    dark_alpha = case_when(
      bia == 'Financial District' ~ 1,
      TRUE ~ 0
    ),
    region = case_when(
      core == 'yes' ~ 0, # core
      inner_ring == 'yes' ~ 1, # inner ring
      TRUE ~ 2 # outer ring
    )) %>%
  filter(!is.na(bia)) %>%
  mutate(
    mytext = paste0(bia, '<br>Week of ', week, ': ',
                    scales::percent(rq_rolling, accuracy = 2)),
    bia_region = case_when(
      region == 0 ~ paste0('Core (', bia, ')'),
      region == 1 ~ paste0('Inner ring (', bia, ')'),
      TRUE ~ paste0('Outer ring (', bia, ')')
    ))

head(rq_spectus)
range(rq_spectus$week)

# Not by BIA
rq_spectus_overall <- rbind(safegraph %>%
                      ungroup() %>%
                      filter(date_range_start <= shared_end) %>%
                      select(date_range_start, bia, normalized),
                    normalized_spectus %>%
                      filter(date_range_start > shared_end) %>%
                      select(date_range_start, bia, normalized)) %>%
  mutate(week_num = lubridate::isoweek(date_range_start),
         year = lubridate::year(date_range_start)) %>%
  select(-date_range_start) %>%
  group_by(week_num, year) %>%
  summarize(normalized = mean(normalized, na.rm = T)) %>%
  pivot_wider(id_cols = c('week_num'),
              names_from = 'year',
              names_prefix = 'ntv_',
              values_from = 'normalized') %>%
  mutate(rec_2020 = ntv_2020 / ntv_2019,
         rec_2021 = ntv_2021 / ntv_2019,
         rec_2022 = ntv_2022 / ntv_2019,
         rec_2023 = ntv_2023 / ntv_2019) %>%
  pivot_longer(cols = starts_with('rec_'),
               names_to = "year",
               values_to = "rq") %>%
  filter(week_num < 53) %>%
  mutate(year = substr(year, 5, 9),
         week = as.Date(paste(year, week_num, 1, sep = "_"), 
                        format = "%Y_%W_%w")) %>%
  filter(!is.na(rq)) %>%
  data.frame() %>%
  arrange(year, week_num) %>%
  mutate(rq_rolling = zoo::rollmean(rq, k = 11, fill = NA, align = 'right')) %>%
  ungroup() %>%
  data.frame() %>%
  filter(!is.na(rq_rolling))

head(rq_spectus_overall)

all_bia_plot <-
  rq_spectus_overall %>%
  ggplot(aes(x = week, y = rq_rolling)) +
  geom_line(size = .8) +
  ggtitle('Recovery rate for all Business Improvement Areas in Toronto (11 week rolling average)') +
  scale_x_date(date_breaks = "4 month", date_labels = "%b %Y") +
  scale_y_continuous(labels = scales::percent_format(accuracy = 1)) +
  #                    limits = c(0.4, 1.2),
  #                    breaks = seq(.4, 1.2, .2)) +
  xlab('Month') +
  ylab('Recovery rate') +
  theme(
    # axis.text.x = element_text(angle = 90),
    panel.grid.major = element_line(color = 'light gray',
                                    linewidth = .5,
                                    linetype = 1),
    panel.grid.minor.x = element_blank(),
    panel.background = element_blank(),
    plot.title = element_text(hjust = .5),
    axis.ticks = element_blank(),
    axis.title.y = element_text(margin = margin(r = 15)),
    axis.title.x = element_text(margin = margin(t = 15))
  )

all_bia_plot

each_bia_plot <-
  rq_spectus %>%
  ggplot(aes(x = week, y = rq_rolling, group = bia, 
             color = interaction(as.factor(region), as.factor(dark_alpha)))) + # text = mytext
  geom_line(size = 1) +
  ggtitle('Recovery rate for all Business Improvement Areas in Toronto (11 week rolling average)') +
  scale_x_date(date_breaks = "4 month", date_labels = "%b %Y") +
  scale_y_continuous(labels = scales::percent_format(accuracy = 1)) +
  xlab('Month') +
  ylab('Recovery rate') +
  labs(color = '') +
  geom_hline(yintercept = 1, linetype = "dashed", color = "black", size = .7) +
  scale_color_manual(
    name = 'Region',
    values = c(
      "0.1" = alpha("#8c0a03", .9),
      "0.0" = alpha("#8c0a03", .2),
      "1.0" = alpha("#d6ad09", .2),
      "1.1" = alpha("#d6ad09", .9),
      "2.0" = alpha("#6bc0c2", .3),
      "2.1" = alpha("#6bc0c2", .9)
    ),
    labels = c(
      'Core',
      'Inner ring', 
      'Outer ring',
      'Financial District',
      'doesnt exist',
      'doesnt exist'
    )) +
  theme(
    panel.grid.major = element_line(color = 'light gray',
                                    linewidth = .5,
                                    linetype = 1),
    panel.grid.minor.x = element_blank(),
    panel.background = element_blank(),
    plot.title = element_text(hjust = .5),
    axis.ticks = element_blank(),
    # legend.title = element_blank(),
    legend.key = element_rect(fill = "white"),
    legend.title = element_text(margin = margin(b = 10)),
    axis.title.y = element_text(margin = margin(r = 20)),
    axis.title.x = element_text(margin = margin(t = 20))
  )

each_bia_plot

# Now plotly

fd <- rq_spectus %>% filter(bia == 'Financial District')

not_fd_core <- rq_spectus %>% 
  filter(bia != 'Financial District' & region == 0)

inner <- rq_spectus %>% 
  filter(region == 1)

outer <- rq_spectus %>% 
  filter(region == 2)

each_bia_plotly <- 
  plot_ly() %>%
  add_lines(data = fd, x = ~week, y = ~rq_rolling, 
            name = "Financial District", 
            text = ~mytext,
            hoverinfo = 'text',
            opacity = .9,
            line = list(shape = "linear", color = '#8c0a03')) %>%
  add_lines(data = not_fd_core, x = ~week, y = ~rq_rolling, split = ~bia,
            name = ~bia_region, 
            text = ~mytext,
            hoverinfo = 'text',
            opacity = .3,
            line = list(shape = "linear", color = '#8c0a03')) %>%
  add_lines(data = inner, x = ~week, y = ~rq_rolling, split = ~bia,
            name = ~bia_region, 
            text = ~mytext,
            hoverinfo = 'text',
            opacity = .3,
            line = list(shape = "linear", color = '#d6ad09')) %>%
  add_lines(data = outer, x = ~week, y = ~rq_rolling, split = ~bia,
            name = ~bia_region, 
            text = ~mytext,
            hoverinfo = 'text',
            opacity = .3,
            line = list(shape = "linear", color = '#6bc0c2')) %>%
  layout(title = "Recovery rate for all Business Improvement Areas in Toronto (11 week rolling average)",
         xaxis = list(title = "Week", zerolinecolor = "#ffff", 
                      tickformat = "%b %Y"),
         yaxis = list(title = "Recovery rate", zerolinecolor = "#ffff",
                      tickformat = ".0%", ticksuffix = "  "))

each_bia_plotly

#---------------------------------------------------------------------

# 
# 
# head(safegraph19)
# 
# # Scale both datasets
# 
# process_s19 <- preProcess(safegraph19 %>% select(normalized),
#                           method = c("range"))
# 
# safegraph19_scale <- predict(process_s19, safegraph19)
# 
# head(safegraph19_scale)
# nrow(safegraph19_scale)
# nrow(safegraph19)
# range(safegraph19_scale$date)
# range(safegraph19_scale$normalized) # should be 0-1
# 
# process_b0 <- preProcess(b0 %>% select(normalized),
#                          method = c("range"))
# 
# b0_scale <- predict(process_b0, b0)
# 
# head(b0_scale)
# nrow(b0_scale)
# nrow(b0)
# range(b0_scale$date)
# range(b0_scale$normalized) # should be 0-1
# 
# b <-
#   b0_scale %>%
#   filter(date >= as.Date('2020-01-01')) %>%
#   rbind(safegraph19_scale)
# 
# range(b$date)
# head(b)
# 
# #-----------------------------------------
# # Plots (exploratory)
# #-----------------------------------------
# 
# # First look at trends over time, by provider, for all BIAs.
# 
# trend_all <- 
#   b %>%
#   ggplot(aes(x = date, y = n_devices, group = provider, color = provider,
#              alpha = .8)) +
#   geom_line() +
#   theme_bw()
# 
# ggplotly(trend_all)
# 
# normalized_all <- 
#   b %>%
#   ggplot(aes(x = date, y = normalized, group = provider, color = provider,
#              alpha = .8)) +
#   geom_line() +
#   theme_bw()
# 
# ggplotly(normalized_all)
# 
# one_provider <- 
#   b %>%
#   # Use Safegraph data for 2019, '700199' until 5/17/21, then '190199' after
#   filter((provider == 'safegraph' & date < as.Date('2020-01-01')) | 
#            (provider == '700199' & date >= as.Date('2020-01-01') & 
#               date < as.Date('2021-05-17')) | 
#            (provider == '190199' & date >= as.Date('2021-05-17')))
# 
# head(one_provider)
# 
# one_prov_plot <- 
#   one_provider %>%
#   ggplot(aes(x = date, y = normalized, alpha = .8)) +
#   geom_line() +
#   theme_bw()
# 
# ggplotly(one_prov_plot)
# 

#-----------------------------------------
# Trend plot: recovery rate (Downtown Yonge)
#-----------------------------------------

yonge_plot <-
  rq_spectus %>%
  filter(bia == 'Downtown Yonge') %>%
  ggplot(aes(x = week, y = rq_rolling)) +
  geom_line(size = .8) +
  ggtitle('Recovery rate for Downtown Yonge (11 week rolling average)') +
  scale_x_date(date_breaks = "4 month", date_labels = "%b %Y") +
  scale_y_continuous(labels = scales::percent_format(accuracy = 1),
                     limits = c(0.2, 1.2),
                     breaks = seq(.2, 1.2, .2)) +
  xlab('Month') +
  ylab('Recovery rate') +
  theme(
    # axis.text.x = element_text(angle = 90),
    panel.grid.major = element_line(color = 'light gray',
                                    linewidth = .5,
                                    linetype = 1),
    panel.grid.minor.x = element_blank(),
    panel.background = element_blank(),
    plot.title = element_text(hjust = .5),
    axis.ticks = element_blank(),
    axis.title.y = element_text(margin = margin(r = 15)),
    axis.title.x = element_text(margin = margin(t = 15))
  )

yonge_plot

#-----------------------------------------
# Trend plot: recovery rate (Financial District)
#-----------------------------------------

financial_plot <-
  rq_spectus %>%
  filter(bia == 'Financial District') %>%
  ggplot(aes(x = week, y = rq_rolling)) +
  geom_line(size = .8) +
  ggtitle('Recovery rate for Financial District (11 week rolling average)') +
  scale_x_date(date_breaks = "4 month", date_labels = "%b %Y") +
  scale_y_continuous(labels = scales::percent_format(accuracy = 1),
                     limits = c(0, 1),
                     breaks = seq(0, 1, .2)) +
  xlab('Month') +
  ylab('Recovery rate') +
  theme(
    panel.grid.major = element_line(color = 'light gray',
                                    linewidth = .5,
                                    linetype = 1),
    panel.grid.minor.x = element_blank(),
    panel.background = element_blank(),
    plot.title = element_text(hjust = .5),
    axis.ticks = element_blank(),
    axis.title.y = element_text(margin = margin(r = 15)),
    axis.title.x = element_text(margin = margin(t = 15))
  )

financial_plot

#-----------------------------------------
# Maps
#-----------------------------------------

# # Compare overall NORMALIZED device count for period of March 1, 2023 through 
# # latest date available to same time period in 2019.
# 
# for_maps0 <-
#   rec_rate %>%
#   mutate(week = as.Date(
#     paste(as.character(year), as.character(week_num), 1, sep = '_'),
#           format = '%Y_%W_%w'))
# 
# # Make sure I'm comparing the same number of weeks:
# only_23 <- 
#   for_maps0 %>%
#   filter((year == 2023 & week >= as.Date('2023-03-01') & 
#             week <= as.Date('2023-05-19')))
# 
# only_19 <- 
#   for_maps0 %>%
#   filter((year == 2019 & week >= as.Date('2019-03-01') & 
#             week <= as.Date('2019-05-19')))
# 
# nrow(only_23)
# n_distinct(only_23$week_num)
# 
# nrow(only_19)
# n_distinct(only_19$week_num) # yes :)
# 
# for_maps <-
#   for_maps0 %>%
#   filter((year == 2023 & week >= as.Date('2023-03-01') & 
#             week <= as.Date('2023-05-19')) |
#            (year == 2019 & week >= as.Date('2019-03-01') & 
#               week <= as.Date('2019-05-19'))) %>%
#   group_by(bia, year) %>%
#   summarize(n_devices = sum(n_devices, na.rm = T),
#             userbase = sum(userbase, na.rm = T)) %>%
#   mutate(normalized = n_devices/userbase) %>%
#   select(-c(n_devices, userbase)) %>%
#   pivot_wider(
#     names_from = year,
#     names_prefix = 'normalized_',
#     values_from = normalized
#   ) %>%
#   mutate(rate = normalized_2023/normalized_2019) %>%
#   filter(!is.na(bia)) %>%
#   data.frame()
# 
# head(for_maps)
# 
# 
# # 1. Choropleth map of "recovery rate" for all BIAs
# #-----------------------------------------------------------------
# 
# # Join spatial data with device count data.
# 
# nrow(bia_sf)
# nrow(for_maps)
# 
# summary(for_maps$rate)
# getJenksBreaks(for_maps$rate, 7)
# 
# bia_final <- 
#   left_join(bia_sf, for_maps) %>%
#   mutate(
#     rate_cat = factor(case_when(
#       rate < .8 ~ '50 - 79%',
#       rate < 1 ~ '80 - 99%',
#       rate < 1.2 ~ '100 - 119%',
#       rate < 1.5 ~ '120 - 149%',
#       rate < 1.8 ~ '150 - 179%',
#       TRUE ~ '180 - 240%'
#     ),
#     levels = c('50 - 79%', '80 - 99%', '100 - 119%', '120 - 149%', '150 - 179%',
#                '180 - 240%')))
# 
# nrow(bia_final)
# head(bia_final)
# 
# pal <- c(
#   "#e41822",
#   "#faa09d",
#   "#5bc4fb",
#   "#2c92d7",
#   "#0362b0",
#   "#033384"
# )
# 
# basemap <-
#   get_stamenmap(
#     bbox = c(left = -79.58,
#              bottom = 43.59,
#              right = -79.2,
#              top = 43.81),
#     zoom = 11,
#     maptype = "terrain-lines") # https://r-graph-gallery.com/324-map-background-with-the-ggmap-library.html
# 
# basemap_attributes <- attributes(basemap)
# 
# basemap_transparent <- matrix(adjustcolor(basemap, alpha.f = 0.2),
#                               nrow = nrow(basemap))
# 
# attributes(basemap_transparent) <- basemap_attributes
# 
# bia_map <-
#   ggmap(basemap_transparent) +
#   geom_sf(data = bia_final, 
#           aes(fill = rate_cat), 
#           inherit.aes = FALSE,
#           # alpha = .9, 
#           color = NA) +
#   ggtitle('Recovery rate of Business Improvement Areas in\nToronto, March 1 - May 19 (2023 versus 2019)') +
#   scale_fill_manual(values = pal, name = 'Recovery rate') +
#   guides(fill = guide_legend(barwidth = 0.5, barheight = 10, 
#                              ticks = F, byrow = T)) +
#   theme(
#     panel.border = element_blank(), 
#     panel.grid.major = element_blank(),
#     panel.grid.minor = element_blank(), 
#     panel.background = element_blank(),
#     axis.line = element_blank(),
#     axis.text = element_blank(),
#     axis.title = element_blank(),
#     axis.ticks = element_blank(),
#     legend.spacing.y = unit(.1, 'cm'),
#     plot.title = element_text(hjust = 0.5, margin = margin(b = 20)),
#     plot.caption = element_text(
#       margin = margin(t = 20)),
#     legend.title = element_text(
#       margin = margin(b = 10)))
# 
# bia_map
# 
# # 2. Map zoomed in on downtown yonge and financial district BIAs
# #-----------------------------------------------------------------
# 
# basemap_zoom <-
#   get_stamenmap(
#     bbox = c(left = -79.39,
#              bottom = 43.64,
#              right = -79.373,
#              top = 43.665),
#     zoom = 15,
#     maptype = "terrain-lines") # https://r-graph-gallery.com/324-map-background-with-the-ggmap-library.html
# 
# basemap_attributes_zoom <- attributes(basemap_zoom)
# 
# basemap_transparent_zoom <- matrix(adjustcolor(basemap_zoom, alpha.f = 0.2),
#                                    nrow = nrow(basemap_zoom))
# 
# attributes(basemap_transparent_zoom) <- basemap_attributes_zoom
# 
# # ggmap(basemap_transparent_zoom)
# 
# # Subset data to only Financial District & Downtown Yonge
# bia_zoom <-
#   bia_final %>%
#   filter(bia %in% c('Financial District', 'Downtown Yonge')) %>%
#   mutate(bia_lab = factor(paste0(bia, ': ', round(rate * 100), '%'),
#                           levels = c('Financial District: 71%',
#                                      'Downtown Yonge: 109%')))
# 
# zoom_pal <- c(
#   "#e41822",
#   "#5bc4fb"
# )
# 
# zoom_map <-
#   ggmap(basemap_transparent_zoom) +
#   geom_sf(data = bia_zoom, 
#           aes(fill = bia_lab), 
#           inherit.aes = FALSE,
#           color = NA) +
#   ggtitle('Recovery rate of Financial District and Downtown\nYonge, March 1 - May 19 (2023 versus 2019)') +
#   scale_fill_manual(values = zoom_pal, name = 'Recovery rate') +
#   guides(fill = guide_legend(barwidth = 0.5, barheight = 10, 
#                              ticks = F, byrow = T)) +
#   theme(
#     panel.border = element_blank(), 
#     panel.grid.major = element_blank(),
#     panel.grid.minor = element_blank(), 
#     panel.background = element_blank(),
#     axis.line = element_blank(),
#     axis.text = element_blank(),
#     axis.title = element_blank(),
#     axis.ticks = element_blank(),
#     legend.spacing.y = unit(.1, 'cm'),
#     plot.title = element_text(hjust = 0.5, margin = margin(b = 20),
#                               size = 11),
#     plot.caption = element_text(
#       margin = margin(t = 20)),
#     legend.title = element_text(
#       margin = margin(b = 10)))
# 
# zoom_map
# 
# # 3. Interactive map of BIAs
# #-----------------------------------------------------------------
# 
# bia_final_label <-
#   bia_final %>%
#   mutate(label = paste0(bia, ": ", round(rate * 100), "%"))
# 
# leaflet_pal <- colorFactor(
#   pal,
#   domain = bia_final_label$rate_cat,
#   na.color = 'transparent'
#   )
# 
# interactive <-
#   leaflet(
#     options = leafletOptions(minZoom = 9, maxZoom = 18, zoomControl = FALSE)
#   ) %>%
#   # setView(lat = 28.72, lng = -81.97, zoom = 7) %>%
#   addMapPane(name = "polygons", zIndex = 410) %>%
#   addMapPane(name = "polylines", zIndex = 420) %>%
#   addMapPane(name = "Layers", zIndex = 430) %>%
#   addProviderTiles("CartoDB.PositronNoLabels") %>%
#   addProviderTiles("Stamen.TonerLines",
#                    options = providerTileOptions(opacity = 0.3),
#                    group = "Roads"
#   ) %>%
#   addPolygons(
#     data = bia_final_label,
#     label = ~label,
#     labelOptions = labelOptions(textsize = "12px"),
#     fillOpacity = .8,
#     color = ~leaflet_pal(rate_cat),
#     stroke = TRUE,
#     weight = 1,
#     opacity = 1,
#     highlightOptions =
#       highlightOptions(
#         color = "black",
#         weight = 3,
#         bringToFront = TRUE),
#     options = pathOptions(pane = "polygons")
#   ) %>%
#   leaflet::addLegend(
#     data = bia_final_label,
#     position = "bottomleft",
#     pal = leaflet_pal,
#     values = ~rate_cat,
#     title = 'Recovery rate<br>(March 1 - May 19,<br>2023 vs 2019)'
#   )
# 
# interactive
