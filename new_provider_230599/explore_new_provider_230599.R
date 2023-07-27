################################################################################
# Explore data from new provider 230599.
#
# Author: Julia Greenberg
# Date: 7.25.23
################################################################################

# Load packages
#=====================================

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'readr', 'ggplot2', 'plotly'))

# Load data
#=====================================

filepath <- 'C:/Users/jpg23/data/downtownrecovery/spectus_exports/new_provider_230599/'

userbase <-
  list.files(path = filepath) %>% 
  map_df(~read_delim(
    paste0(filepath, .),
    delim = '\001',
    col_names = c('geography_name', 'provider_id', 'userbase', 'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date)

head(userbase)
glimpse(userbase)
range(userbase$date)
unique(userbase$provider_id)
unique(userbase$geography_name)

# LOAD DOWNTOWN DATA HERE!!!!!!!!!!!!

# Aggregate by week
#=====================================

u <-
  userbase %>%
  # Determine week and year # for each date
  mutate(
    date_range_start = floor_date(
      date,
      unit = "week",
      week_start = getOption("lubridate.week.start", 1))) %>%
  # Calculate # of devices by big_area, week and year
  group_by(geography_name, provider_id, date_range_start) %>%
  summarize(userbase = sum(userbase, na.rm = T)) %>%
  ungroup() 

head(u)

# Plot
#=====================================

u_plot <-
  u %>%
  ggplot(aes(x = date_range_start, y = userbase, color = provider_id, 
             group = provider_id)) +
  geom_line(linewidth = .8) +
  scale_x_date(date_breaks = "1 year", date_labels = "%b %Y") +
  xlab('Week') +
  ylab('# of unique devices') +
  theme(
    axis.text.x = element_text(angle = 90),
    panel.grid.major = element_line(color = 'light gray',
                                    linewidth = .5,
                                    linetype = 1),
    panel.grid.minor.x = element_blank(),
    panel.background = element_blank(),
    plot.title = element_text(hjust = .5),
    axis.ticks = element_blank(),
    axis.title.y = element_text(margin = margin(r = 15)),
    axis.title.x = element_text(margin = margin(t = 15))
  ) +
  facet_wrap(. ~ geography_name)

ggplotly(u_plot)
