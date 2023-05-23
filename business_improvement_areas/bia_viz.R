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
       'htmlwidgets', 'BAMMtools'))

#-----------------------------------------
# Load data
#-----------------------------------------

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

# Combine them
b <-
  userbase %>%
  filter(province == 'Ontario') %>%
  select(-province) %>%
  left_join(bia, by = c('provider', 'date')) %>%
  filter(date > as.Date('2023-04-25')) %>%
  rbind(both)

head(b)
summary(b)
range(b$date)

#-----------------------------------------
# Plots (exploratory)
#-----------------------------------------

# First look at trends over time, by provider, for all BIAs.

all_bia <-
  b %>%
  group_by(bia, provider, date) %>%
  summarize(n_devices = sum(n_devices, na.rm = T),
            userbase = sum(userbase, na.rm = T)) %>%
  data.frame() %>%
  mutate(normalized = n_devices/userbase)

head(all_bia)

trend_all <- 
  all_bia %>%
  ggplot(aes(x = date, y = n_devices, group = provider, color = provider,
             alpha = .8)) +
  geom_line() +
  theme_bw()

ggplotly(trend_all)

normalized_all <- 
  all_bia %>%
  ggplot(aes(x = date, y = normalized, group = provider, color = provider,
             alpha = .8)) +
  geom_line() +
  theme_bw()

ggplotly(normalized_all)

one_provider <- 
  all_bia %>%
  # Starting 5/17/21, switch providers
  filter((provider == '700199' & date < as.Date('2021-05-17')) | 
           (provider == '190199' & date >= as.Date('2021-05-17')))

head(one_provider)

one_prov_plot <- 
  one_provider %>%
  ggplot(aes(x = date, y = normalized, alpha = .8)) +
  geom_line() +
  theme_bw()

ggplotly(one_prov_plot)

#-----------------------------------------
# Trend plot: recovery rate (all BIAs)
#-----------------------------------------

# 11-week rolling average of weekly normalized counts

rec_rate <-
  b %>%
  # Starting 5/17/21, switch providers
  filter((provider == '700199' & date < as.Date('2021-05-17')) | 
           (provider == '190199' & date >= as.Date('2021-05-17'))) %>%
  # Determine week and year # for each date
  mutate(
    date_range_start = floor_date(
      date,
      unit = "week",
      week_start = getOption("lubridate.week.start", 1)),
    week_num = isoweek(date_range_start),
    year = year(date_range_start)) %>%
  # Calculate # of devices by BIA, week and year
  group_by(bia, year, week_num) %>%
  summarize(n_devices = sum(n_devices, na.rm = T),
            userbase = sum(userbase, na.rm = T)) %>%
  ungroup()

head(rec_rate)
summary(rec_rate$week_num)
rec_rate %>% filter(week_num == 53) %>% head()

# 1. Group by week and year number and sum device counts in both BIA and in 
#    userbase.
# 2. Calculate normalized device counts by dividing BIA count by userbase count.
# 3. Calculate recovery quotient for each week in year 202X by dividing 
#    normalized device count by normalized device count in equivalent week in 
#    2019.
# 4. Calculate 11-week rolling average of recovery quotient.

# Make sure to omit BIAs with less than 10 devices in the weekly aggregates.

all_bia_for_plot <-
  rec_rate %>%
  filter(year > 2018) %>%
  group_by(year, week_num) %>%
  summarize(n_devices = sum(n_devices, na.rm = T),
            userbase = sum(userbase, na.rm = T)) %>%
  ungroup() %>%
  mutate(normalized = n_devices/userbase) %>%
  pivot_wider(
    id_cols = c('week_num'),
    names_from = 'year',
    names_prefix = 'ntv',
    values_from = 'normalized') %>%
  mutate(rec2020 = ntv2020/ntv2019,
         rec2021 = ntv2021/ntv2019,
         rec2022 = ntv2022/ntv2019,
         rec2023 = ntv2023/ntv2019) %>%
  select(-starts_with('ntv')) %>%
  pivot_longer(
    cols = rec2020:rec2023,
    names_to = 'year',
    values_to = 'rq') %>%
  filter(week_num < 53) %>%
  mutate(year = substr(year, 4, 7),
         week = as.Date(paste(year, week_num, 1, sep = '_'),
                        format = '%Y_%W_%w')) %>% # Monday of week
  filter(!(year == 2023 & week_num > 17)) %>%
  arrange(year, week_num) %>%
  mutate(rq_rolling = zoo::rollmean(rq, k = 11, fill = NA, align = 'right')) %>%
  ungroup() %>%
  data.frame() %>%
  filter(!(year == 2020 & week_num < 12))

head(all_bia_for_plot)
tail(all_bia_for_plot)
summary(all_bia_for_plot$rq)
summary(all_bia_for_plot$week)

all_bia_plot <-
  all_bia_for_plot %>%
  ggplot(aes(x = week, y = rq_rolling)) +
  geom_line(size = .8) +
  ggtitle('Recovery rate for all Business Improvement Areas in Toronto (11 week rolling average)') +
  scale_x_date(date_breaks = "4 month", date_labels = "%b %Y") +
  scale_y_continuous(labels = scales::percent_format(accuracy = 1),
                     limits = c(0.4, 1.2),
                     breaks = seq(.4, 1.2, .2)) +
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

# ggsave(
#   '~/git/downtownrecovery/viz/all_bia_plot.png',
#   plot = all_bia_plot,
#   width = 6,
#   height = 2.5,
#   units = "in")

each_bia_for_plot <-
  rec_rate %>%
  filter(year > 2018) %>%
  group_by(year, week_num, bia) %>%
  summarize(n_devices = sum(n_devices, na.rm = T),
            userbase = sum(userbase, na.rm = T)) %>%
  ungroup() %>%
  mutate(normalized = n_devices/userbase) %>%
  pivot_wider(
    id_cols = c('week_num', 'bia'),
    names_from = 'year',
    names_prefix = 'ntv',
    values_from = 'normalized') %>%
  mutate(rec2020 = ntv2020/ntv2019,
         rec2021 = ntv2021/ntv2019,
         rec2022 = ntv2022/ntv2019,
         rec2023 = ntv2023/ntv2019) %>%
  select(-starts_with('ntv')) %>%
  pivot_longer(
    cols = rec2020:rec2023,
    names_to = 'year',
    values_to = 'rq') %>%
  filter(week_num < 53) %>%
  mutate(year = substr(year, 4, 7),
         week = as.Date(paste(year, week_num, 1, sep = '_'),
                        format = '%Y_%W_%w')) %>% # Monday of week
  filter(!(year == 2023 & week_num > 17)) %>%
  arrange(bia, year, week_num) %>%
  group_by(bia) %>%
  mutate(rq_rolling = zoo::rollmean(rq, k = 11, fill = NA, align = 'right')) %>%
  ungroup() %>%
  data.frame() %>%
  filter(!(year == 2020 & week_num < 12))

head(each_bia_for_plot)

each_bia_plot <-
  each_bia_for_plot %>%
  ggplot(aes(x = week, y = rq_rolling, group = bia, color = bia,
             text = str_c(bia, ',<br>week of ', week, ':<br>', 
                          scales::percent(rq_rolling, accuracy = 2)))) +
  geom_line(size = .8) +
  ggtitle('Recovery rate for all Business Improvement Areas in Toronto (11 week rolling average)') +
  scale_x_date(date_breaks = "4 month", date_labels = "%b %Y") +
  scale_y_continuous(labels = scales::percent_format(accuracy = 1)) +
  xlab('Month') +
  ylab('Recovery rate') +
  labs(color = 'Business Improvement Area') +
  theme(
    panel.grid.major = element_line(color = 'light gray',
                                    linewidth = .5,
                                    linetype = 1),
    panel.grid.minor.x = element_blank(),
    panel.background = element_blank(),
    plot.title = element_text(hjust = .5),
    axis.ticks = element_blank(),
    legend.title = element_text(margin = margin(b = 20)),
    axis.title.y = element_text(margin = margin(r = 20)),
    axis.title.x = element_text(margin = margin(t = 20))
  )

each_bia_plotly <- ggplotly(each_bia_plot, tooltip = "text")

each_bia_plotly

# saveWidget(each_bia_plotly, '~/git/downtownrecovery/viz/each_bia_plot.html')

#-----------------------------------------
# Trend plot: recovery rate (Downtown Yonge)
#-----------------------------------------

yonge_plot <-
  each_bia_for_plot %>%
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
  each_bia_for_plot %>%
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

# Compare overall NORMALIZED device count for period of March 1, 2023 through 
# latest date available to same time period in 2019.

for_maps <-
  rec_rate %>%
  mutate(week = as.Date(
    paste(as.character(year), as.character(week_num), 1, sep = '_'),
          format = '%Y_%W_%w')) %>%
  
  # MAKE SURE I'M COMPARING THE SAME # OF WEEKS HERE:
  
  filter((year == 2023 & week >= as.Date('2023-03-01') & 
            week <= as.Date('2023-05-19')) |
           (year == 2019 & week >= as.Date('2019-03-01') & 
              week <= as.Date('2019-05-19'))) %>%
  group_by(bia, year) %>%
  summarize(n_devices = sum(n_devices, na.rm = T),
            userbase = sum(userbase, na.rm = T)) %>%
  mutate(normalized = n_devices/userbase) %>%
  select(-c(n_devices, userbase)) %>%
  pivot_wider(
    names_from = year,
    names_prefix = 'normalized_',
    values_from = normalized
  ) %>%
  mutate(rate = normalized_2023/normalized_2019) %>%
  filter(!is.na(bia)) %>%
  data.frame()

head(for_maps)


# 1. Choropleth map of "recovery rate" for all BIAs
#-----------------------------------------------------------------

## Shapefile of BIAs

bia_sf <- read_sf("C:/Users/jpg23/data/downtownrecovery/business_improvement_areas/business_improvement_areas_simplified.geojson") %>%
  select(bia = AREA_NAME)

head(bia_sf)
class(bia_sf)
n_distinct(bia_sf$bia) # there are 85 BIAs

# Join spatial data with device count data.

nrow(bia_sf)
nrow(for_maps)

summary(for_maps$rate)
getJenksBreaks(for_maps$rate, 7)

bia_final <- 
  left_join(bia_sf, for_maps) %>%
  mutate(
    rate_cat = factor(case_when(
      rate < .8 ~ '50 - 79%',
      rate < 1 ~ '80 - 99%',
      rate < 1.2 ~ '100 - 119%',
      rate < 1.5 ~ '120 - 149%',
      rate < 1.8 ~ '150 - 179%',
      TRUE ~ '180 - 240%'
    ),
    levels = c('50 - 79%', '80 - 99%', '100 - 119%', '120 - 149%', '150 - 179%',
               '180 - 240%')))

nrow(bia_final)
head(bia_final)

pal <- c(
  "#e41822",
  "#faa09d",
  "#5bc4fb",
  "#2c92d7",
  "#0362b0",
  "#033384"
)

basemap <-
  get_stamenmap(
    bbox = c(left = -79.58,
             bottom = 43.59,
             right = -79.2,
             top = 43.81),
    zoom = 11,
    maptype = "terrain-lines") # https://r-graph-gallery.com/324-map-background-with-the-ggmap-library.html

basemap_attributes <- attributes(basemap)

basemap_transparent <- matrix(adjustcolor(basemap, alpha.f = 0.2),
                              nrow = nrow(basemap))

attributes(basemap_transparent) <- basemap_attributes

bia_map <-
  ggmap(basemap_transparent) +
  geom_sf(data = bia_final, 
          aes(fill = rate_cat), 
          inherit.aes = FALSE,
          # alpha = .9, 
          color = NA) +
  ggtitle('Recovery rate of Business Improvement Areas in Toronto\n2019 to 2023 (March 1 - May 19 (??))') +
  scale_fill_manual(values = pal, name = 'Recovery rate') +
  guides(fill = guide_legend(barwidth = 0.5, barheight = 10, 
                             ticks = F, byrow = T)) +
  theme(
    panel.border = element_blank(), 
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank(), 
    panel.background = element_blank(),
    axis.line = element_blank(),
    axis.text = element_blank(),
    axis.title = element_blank(),
    axis.ticks = element_blank(),
    legend.spacing.y = unit(.1, 'cm'),
    plot.title = element_text(hjust = 0.5, margin = margin(b = 20)),
    plot.caption = element_text(
      margin = margin(t = 20)),
    legend.title = element_text(
      margin = margin(b = 10)))

bia_map

# 2. Map zoomed in on downtown yonge and financial district BIAs
#-----------------------------------------------------------------

basemap_zoom <-
  get_stamenmap(
    bbox = c(left = -79.39,
             bottom = 43.64,
             right = -79.373,
             top = 43.665),
    zoom = 15,
    maptype = "terrain-lines") # https://r-graph-gallery.com/324-map-background-with-the-ggmap-library.html

basemap_attributes_zoom <- attributes(basemap_zoom)

basemap_transparent_zoom <- matrix(adjustcolor(basemap_zoom, alpha.f = 0.2),
                                   nrow = nrow(basemap_zoom))

attributes(basemap_transparent_zoom) <- basemap_attributes_zoom

# ggmap(basemap_transparent_zoom)

# Subset data to only Financial District & Downtown Yonge
bia_zoom <-
  bia_final %>%
  filter(bia %in% c('Financial District', 'Downtown Yonge')) %>%
  mutate(bia_lab = paste0(bia, ': ', round(rate * 100), '%'))

zoom_map <-
  ggmap(basemap_transparent_zoom) +
  geom_sf(data = bia_zoom, 
          aes(fill = bia_lab), 
          inherit.aes = FALSE,
          color = NA) +
  ggtitle('Recovery rate of Financial District and\nDowntown Yonge Business Improvement Areas in\nToronto 2019 to 2023 (March 1 - May 19 (??))') +
  scale_fill_manual(values = pal, name = 'Recovery rate') +
  guides(fill = guide_legend(barwidth = 0.5, barheight = 10, 
                             ticks = F, byrow = T)) +
  theme(
    panel.border = element_blank(), 
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank(), 
    panel.background = element_blank(),
    axis.line = element_blank(),
    axis.text = element_blank(),
    axis.title = element_blank(),
    axis.ticks = element_blank(),
    legend.spacing.y = unit(.1, 'cm'),
    plot.title = element_text(hjust = 0.5, margin = margin(b = 20)),
    plot.caption = element_text(
      margin = margin(t = 20)),
    legend.title = element_text(
      margin = margin(b = 10)))

zoom_map
