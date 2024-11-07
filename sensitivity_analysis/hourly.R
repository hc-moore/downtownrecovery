#===============================================================================
# Examine trends based on time of day and day of week for Cleveland, Halifax, 
# Nashville, Portland, Salt Lake City, San Diego, San Francisco, St Louis and 
# Toronto
#===============================================================================

# Load packages
#=====================================

source('~/git/timathomas/functions/functions.r')
ipak(c('tidyverse', 'sf', 'lubridate', 'leaflet', 'plotly', 'htmlwidgets', 
       'ggplot', 'forcats'))

# Load hourly data
#=====================================

filepath <- 'C:/Users/jpg23/data/downtownrecovery/spectus_exports/sensitivity_analysis/'

hourly <-
  list.files(path = paste0(filepath, 'hourly')) %>% 
  map_df(~read_delim(
    paste0(filepath, 'hourly/', .),
    delim = '\001',
    col_names = c('city', 'date_unique_devices', 'h0_unique_devices', 
                  'h1_unique_devices', 'h2_unique_devices', 'h3_unique_devices', 
                  'h4_unique_devices', 'h5_unique_devices', 'h6_unique_devices', 
                  'h7_unique_devices', 'h8_unique_devices', 'h9_unique_devices', 
                  'h10_unique_devices', 'h11_unique_devices', 'h12_unique_devices', 
                  'h13_unique_devices', 'h14_unique_devices', 'h15_unique_devices', 
                  'h16_unique_devices', 'h17_unique_devices', 'h18_unique_devices', 
                  'h19_unique_devices', 'h20_unique_devices', 'h21_unique_devices', 
                  'h22_unique_devices', 'h23_unique_devices', 'event_date'),
    col_types = c('ciiiiiiiiiiiiiiiiiiiiiiiiii')
  )) %>%
  data.frame() %>%
  mutate(date = as.Date(as.character(event_date), format = "%Y%m%d")) %>%
  arrange(date) %>%
  select(-event_date)

head(hourly)

hourly_long <- hourly %>%
  pivot_longer(
    cols = starts_with('h'),
    names_to = 'hour',
    names_pattern = 'h(.*)_unique_devices',
    values_to = 'hourly_devices'
  ) %>%
  mutate(day_of_week = wday(date, label = TRUE, abbr = FALSE),
         hour = as.integer(hour)) %>%
  group_by(city, day_of_week, hour) %>%
  summarize(avg_unique_devices = mean(hourly_devices, na.rm = TRUE)) %>%
  ungroup() %>%
  mutate(time_of_day0 = as.factor(case_when(
           hour == 0 ~ '12 am',
           hour >= 1 & hour < 12 ~ paste0(hour, ' am'),
           hour == 12 ~ '12 pm',
           hour > 12 ~ paste0(as.integer(hour - 12), ' pm')
         )),
         time_of_day = fct_relevel(time_of_day0,
           '12 am', '1 am', '2 am', '3 am', '4 am', '5 am',
           '6 am', '7 am', '8 am', '9 am', '10 am', '11 am',
           '12 pm', '1 pm', '2 pm', '3 pm', '4 pm', '5 pm',
           '6 pm', '7 pm', '8 pm', '9 pm', '10 pm', '11 pm'))

head(hourly_long)
table(hourly_long$day_of_week)
table(hourly_long$time_of_day)

hourly_plot <-
  ggplot(hourly_long, aes(x=time_of_day, y=avg_unique_devices, color=city, 
                          group=city)) +
  geom_line() +
  facet_grid(day_of_week ~ ., scale = "free_y") +
  theme_bw() +
  theme(axis.text.x = element_text(angle = 90),
        axis.title.y = element_text(margin = margin(r = 10)),
        axis.title.x = element_text(margin = margin(t = 10))) +
  ylab('Average unique devices') +
  ggtitle('Unique devices by time of day and day of week, 2022')

hourly_plotly <- ggplotly(hourly_plot)

hourly_plotly

saveWidget(
  hourly_plotly,
  'C:/Users/jpg23/UDP/downtown_recovery/sensitivity_analysis/hourly_dayofweek.html')
