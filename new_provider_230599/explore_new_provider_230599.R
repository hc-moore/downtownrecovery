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

select <- dplyr::select

# Load data
#=====================================

filepath <- 'C:/Users/jpg23/data/downtownrecovery/spectus_exports/new_provider_230599/'

userbase1 <-
  list.files(path = paste0(filepath, 'userbase_20190101_20230721')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'userbase_20190101_20230721/', .),
    delim = '\001',
    col_names = c('geography_name', 'provider_id', 'userbase', 'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date)

userbase2 <-
  list.files(path = paste0(filepath, 'userbase_20230722_20230804')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'userbase_20230722_20230804/', .),
    delim = '\001',
    col_names = c('geography_name', 'provider_id', 'userbase', 'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date)

userbase <- rbind(userbase1, userbase2)

head(userbase)
glimpse(userbase)
range(userbase$date)
unique(userbase$provider_id)
unique(userbase$geography_name)

downtown1 <-
  list.files(path = paste0(filepath, 'downtown_20190101_20230721')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'downtown_20190101_20230721/', .),
    delim = '\001',
    col_names = c('city', 'provider_id', 'approx_distinct_devices_count', 
                  'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date) %>%
  filter(date <= as.Date('2023-07-21'))

downtown2 <-
  list.files(path = paste0(filepath, 'downtown_20230722_20230804')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'downtown_20230722_20230804/', .),
    delim = '\001',
    col_names = c('city', 'provider_id', 'approx_distinct_devices_count', 
                  'event_date'),
    col_types = c('ccii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date)

downtown <- rbind(downtown1, downtown2)

head(downtown)
glimpse(downtown)
range(downtown$date)
unique(downtown$provider_id)
unique(downtown$city)

# Join downtowns with userbase
#=====================================

city_to_state <- data.frame(
  Albuquerque=c('New Mexico'),
  Atlanta=c('Georgia'),
  Austin=c('Texas'),
  Bakersfield=c('California'),
  Baltimore=c('Maryland'),
  Boston=c('Massachusetts'),
  Calgary=c('Alberta'),
  Charlotte=c('North Carolina'),
  Chicago=c('Illinois'),
  Cincinnati=c('Ohio'),
  Cleveland=c('Ohio'),
  `Colorado Springs`=c('Colorado'),
  Columbus=c('Ohio'),
  Dallas=c('Texas'),
  Denver=c('Colorado'),
  Detroit=c('Michigan'),
  Edmonton=c('Alberta'),
  `El Paso`=c('Texas'),
  `Fort Worth`=c('Texas'),
  Fresno=c('California'),
  Halifax=c('Nova Scotia'),
  Honolulu=c('Hawaii'),
  Houston=c('Texas'),
  Indianapolis=c('Indiana'),
  Jacksonville=c('Florida'),
  `Kansas City`=c('Missouri'),
  `Las Vegas`=c('Nevada'),
  `London`=c('Ontario'),
  `Los Angeles`=c('California'),
  Louisville=c('Kentucky'),
  Memphis=c('Tennessee'),
  Miami=c('Florida'),
  Milwaukee=c('Wisconsin'),
  Minneapolis=c('Minnesota'),
  Mississauga=c('Ontario'),
  Montreal=c('Quebec'),
  Nashville=c('Tennessee'),
  `New Orleans`=c('Louisiana'),
  `New York`=c('New York'),
  Oakland=c('California'),
  `Oklahoma City`=c('Oklahoma'),
  Omaha=c('Nebraska'),
  Orlando=c('Florida'),
  Ottawa=c('Ontario'),
  Philadelphia=c('Pennsylvania'),
  Phoenix=c('Arizona'),
  Pittsburgh=c('Pennsylvania'),
  Portland=c('Oregon'),
  Quebec=c('Quebec'),
  Raleigh=c('North Carolina'),
  Sacramento=c('California'),
  `Salt Lake City`=c('Utah'),
  `San Antonio`=c('Texas'),
  `San Diego`=c('California'),
  `San Francisco`=c('California'), 
  `San Jose`=c('California'),
  Seattle=c('Washington'),
  `St Louis`=c('Missouri'),
  Tampa=c('Florida'),
  Toronto=c('Ontario'),
  Tucson=c('Arizona'),
  Tulsa=c('Oklahoma'),
  Vancouver=c('British Columbia'),
  `Washington DC`=c('District of Columbia'),
  Wichita=c('Kansas'),
  Winnipeg=c('Manitoba')) %>%
  pivot_longer(
    cols = everything(),
    names_to = 'city',
    values_to = 'state'
  ) %>%
  mutate(city = str_replace_all(city, '\\.', ' '))

downtown1 <- downtown %>%
  left_join(city_to_state)

head(downtown1)
head(userbase)

final_df <- 
  downtown1 %>% 
  left_join(userbase, by = c('state' = 'geography_name', 'provider_id', 'date')) %>%
  rename(downtown_devices = approx_distinct_devices_count) %>%
  mutate(normalized = downtown_devices/userbase)

head(final_df)

# Aggregate by week
#=====================================

by_week <-
  final_df %>%
  # Determine week and year # for each date
  mutate(
    date_range_start = floor_date(
      date,
      unit = "week",
      week_start = getOption("lubridate.week.start", 1))) %>%
  group_by(city, state, provider_id, date_range_start) %>%
  summarize(userbase = sum(userbase, na.rm = T),
            downtown_devices = sum(downtown_devices, na.rm = T)) %>%
  ungroup() %>%
  mutate(normalized = downtown_devices/userbase) %>%
  filter(date_range_start >= as.Date('2023-01-01'))

head(by_week)
range(by_week$date_range_start)

# Split into 3 even chunks
#=====================================

all_cities <- unique(by_week$city)

all_cities

chunks <- split(all_cities, cut(seq_along(all_cities), 3, labels = FALSE))

by_week1 <- by_week %>% filter(city %in% unlist(chunks[1]))
by_week2 <- by_week %>% filter(city %in% unlist(chunks[2]))
by_week3 <- by_week %>% filter(city %in% unlist(chunks[3]))

# Plot downtowns
#=====================================

d_plot <-
  by_week %>%
  ggplot(aes(x = date_range_start, y = downtown_devices, 
             color = provider_id, group = provider_id)) +
  geom_line(linewidth = .8) +
  scale_x_date(date_breaks = "1 year") + #, date_labels = waiver()) + # date_labels = "%b %Y") +
  xlab('Week') +
  ylab('# of unique devices') +
  theme(
    axis.text.x = element_blank(),
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
  ggtitle('Downtown (2023)') +
  facet_wrap(. ~ city, scales = "free")

d <- ggplotly(d_plot)

saveWidget(
  d,
  'C:/Users/jpg23/UDP/downtown_recovery/new_provider_230599/downtown_by_provider_2023.html')

d1_plot <-
  by_week1 %>%
  ggplot(aes(x = date_range_start, y = downtown_devices, 
             color = provider_id, group = provider_id)) +
  geom_line(linewidth = .8) +
  scale_x_date(date_breaks = "1 year", date_labels = "%b %Y") +
  xlab('Week') +
  ylab('# of unique devices') +
  theme(
    axis.text.x = element_blank(),
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
  ggtitle('Downtown (2023)') +
  facet_wrap(. ~ city, scales = "free")

d1 <- ggplotly(d1_plot)

saveWidget(
  d1,
  'C:/Users/jpg23/UDP/downtown_recovery/new_provider_230599/downtown1_by_provider_2023.html')

d2_plot <-
  by_week2 %>%
  ggplot(aes(x = date_range_start, y = downtown_devices, 
             color = provider_id, group = provider_id)) +
  geom_line(linewidth = .8) +
  scale_x_date(date_breaks = "1 year", date_labels = "%b %Y") +
  xlab('Week') +
  ylab('# of unique devices') +
  theme(
    axis.text.x = element_blank(),
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
  ggtitle('Downtown (2023)') +
  facet_wrap(. ~ city, scales = "free")

d2 <- ggplotly(d2_plot)

saveWidget(
  d2,
  'C:/Users/jpg23/UDP/downtown_recovery/new_provider_230599/downtown2_by_provider_2023.html')

d3_plot <-
  by_week2 %>%
  ggplot(aes(x = date_range_start, y = downtown_devices, 
             color = provider_id, group = provider_id)) +
  geom_line(linewidth = .8) +
  scale_x_date(date_breaks = "1 year", date_labels = "%b %Y") +
  xlab('Week') +
  ylab('# of unique devices') +
  theme(
    axis.text.x = element_blank(),
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
  ggtitle('Downtown (2023)') +
  facet_wrap(. ~ city, scales = "free")

d3 <- ggplotly(d3_plot)

saveWidget(
  d3,
  'C:/Users/jpg23/UDP/downtown_recovery/new_provider_230599/downtown3_by_provider_2023.html')

# Plot userbase
#=====================================

u_plot <-
  by_week %>%
  ggplot(aes(x = date_range_start, y = userbase, color = provider_id, 
             group = provider_id)) +
  geom_line(linewidth = .8) +
  scale_x_date(date_breaks = "1 year", date_labels = "%b %Y") +
  xlab('Week') +
  ylab('# of unique devices') +
  theme(
    axis.text.x = element_blank(),
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
  ggtitle('Userbase (2023)') +
  facet_wrap(. ~ state, scales = "free")

u <- ggplotly(u_plot)

saveWidget(
  u,
  'C:/Users/jpg23/UDP/downtown_recovery/new_provider_230599/userbase_by_provider_2023.html')

u1_plot <-
  by_week1 %>%
  ggplot(aes(x = date_range_start, y = userbase, color = provider_id, 
             group = provider_id)) +
  geom_line(linewidth = .8) +
  scale_x_date(date_breaks = "1 year", date_labels = "%b %Y") +
  xlab('Week') +
  ylab('# of unique devices') +
  theme(
    axis.text.x = element_blank(),
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
  ggtitle('Userbase (2023)') +
  facet_wrap(. ~ state, scales = "free")

u1 <- ggplotly(u1_plot)

saveWidget(
  u1,
  'C:/Users/jpg23/UDP/downtown_recovery/new_provider_230599/userbase1_by_provider_2023.html')

u2_plot <-
  by_week2 %>%
  ggplot(aes(x = date_range_start, y = userbase, color = provider_id, 
             group = provider_id)) +
  geom_line(linewidth = .8) +
  scale_x_date(date_breaks = "1 year", date_labels = "%b %Y") +
  xlab('Week') +
  ylab('# of unique devices') +
  theme(
    axis.text.x = element_blank(),
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
  ggtitle('Userbase (2023)') +
  facet_wrap(. ~ state, scales = "free")

u2 <- ggplotly(u2_plot)

saveWidget(
  u2,
  'C:/Users/jpg23/UDP/downtown_recovery/new_provider_230599/userbase2_by_provider_2023.html')

u3_plot <-
  by_week3 %>%
  ggplot(aes(x = date_range_start, y = userbase, color = provider_id, 
             group = provider_id)) +
  geom_line(linewidth = .8) +
  scale_x_date(date_breaks = "1 year", date_labels = "%b %Y") +
  xlab('Week') +
  ylab('# of unique devices') +
  theme(
    axis.text.x = element_blank(),
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
  ggtitle('Userbase (2023)') +
  facet_wrap(. ~ state, scales = "free")

u3 <- ggplotly(u3_plot)

saveWidget(
  u3,
  'C:/Users/jpg23/UDP/downtown_recovery/new_provider_230599/userbase3_by_provider_2023.html')

# Plot normalized
#=====================================

n_plot <-
  by_week %>%
  ggplot(aes(x = date_range_start, y = normalized, color = provider_id, 
             group = provider_id)) +
  geom_line(linewidth = .8) +
  scale_x_date(date_breaks = "1 year", date_labels = "%b %Y") +
  xlab('Week') +
  ylab('# of unique devices') +
  theme(
    axis.text.x = element_blank(),
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
  ggtitle('Normalized (2023)') +
  facet_wrap(. ~ city, scales = "free")

ggplotly(n_plot)

n <- ggplotly(n_plot)

saveWidget(
  n,
  'C:/Users/jpg23/UDP/downtown_recovery/new_provider_230599/normalized_by_provider_2023.html')

n1_plot <-
  by_week1 %>%
  ggplot(aes(x = date_range_start, y = normalized, color = provider_id, 
             group = provider_id)) +
  geom_line(linewidth = .8) +
  scale_x_date(date_breaks = "1 year", date_labels = "%b %Y") +
  xlab('Week') +
  ylab('# of unique devices') +
  theme(
    axis.text.x = element_blank(),
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
  ggtitle('Normalized (2023)') +
  facet_wrap(. ~ city, scales = "free")

ggplotly(n_plot)

n1 <- ggplotly(n1_plot)

saveWidget(
  n1,
  'C:/Users/jpg23/UDP/downtown_recovery/new_provider_230599/normalized1_by_provider_2023.html')

n2_plot <-
  by_week2 %>%
  ggplot(aes(x = date_range_start, y = normalized, color = provider_id, 
             group = provider_id)) +
  geom_line(linewidth = .8) +
  scale_x_date(date_breaks = "1 year", date_labels = "%b %Y") +
  xlab('Week') +
  ylab('# of unique devices') +
  theme(
    axis.text.x = element_blank(),
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
  ggtitle('Normalized (2023)') +
  facet_wrap(. ~ city, scales = "free")

ggplotly(n2_plot)

n2 <- ggplotly(n2_plot)

saveWidget(
  n2,
  'C:/Users/jpg23/UDP/downtown_recovery/new_provider_230599/normalized2_by_provider_2023.html')

n3_plot <-
  by_week3 %>%
  ggplot(aes(x = date_range_start, y = normalized, color = provider_id, 
             group = provider_id)) +
  geom_line(linewidth = .8) +
  scale_x_date(date_breaks = "1 year", date_labels = "%b %Y") +
  xlab('Week') +
  ylab('# of unique devices') +
  theme(
    axis.text.x = element_blank(),
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
  ggtitle('Normalized (2023)') +
  facet_wrap(. ~ city, scales = "free")

ggplotly(n3_plot)

n3 <- ggplotly(n3_plot)

saveWidget(
  n3,
  'C:/Users/jpg23/UDP/downtown_recovery/new_provider_230599/normalized3_by_provider_2023.html')
