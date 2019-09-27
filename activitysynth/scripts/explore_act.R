#source("code/prep_act.R")

library(ggplot2)

# activities started later in the day have short duration due to the fact
# that the diary end time is pre-set to 02:59 next morning
time_vs_duration <- ggplot(act_plc1p) +
  geom_point(aes(x=STIME, y=act_duration, color=purp)) +
  ggtitle("activity start time v activity duration (minutes)")

time_vs_duration %>% print()
#ggsave("output/time-v-act-duration.pdf", plot=time_vs_duration)

# home_time_vs_duration <- ggplot(act_plc1p %>% filter(purp == "Home")) +
#   geom_point(aes(x=STIME, y=act_duration))
# 
# ggsave("home-time-v-act-duration.pdf", plot=home_time_vs_duration)


adatetime <- as_datetime("2018-01-01 00:00:00")
ts10m_seq <- adatetime %--% seq(adatetime, length.out=162, by="10 min") %>%
  hms::hms() %>%
  tibble(time=., .id=1L)

#ts10m_seq <- ts10m_seq %>% mutate(.id=1L)
act_plc1p_ts10m <- act_plc1p %>% 
  select(HHPER, act_id, purp, STIME, ETIME) %>% 
  mutate(.id=1L) %>% 
  left_join(ts10m_seq, by=".id") %>% 
  filter(STIME <= time, ETIME > time) %>% 
  select(-.id) %>% 
  as_tibble()

# use data.table for non-equi joins
# library(data.table) #v>=1.9.8
#setDT(sdata); setDT(fdata) # converting to data.table in place

#fdata[sdata, on = .(fyear >= byear, fyear < eyear), nomatch = 0,
#      .(id, x.fyear, byear, eyear, val)]
# adatetime <- as_datetime("2018-01-01 00:00:00")
# ts10m_seq <- adatetime %--% seq(adatetime, length.out=162, by="10 min") %>% 
#   hms::hms() %>%
#   tibble(time=.)
# 
# ts10m_seq_dt <- ts10m_seq %>% setDT()
# act_plc1p_dt <- act_plc1p %>% setDT()
# #ts10m_seq_dt[act_plc1p_dt, on = .(time>=STIME, time<ETIME), nomatch = 0, .(time, STIME, ETIME, purp)]
# ts10m_seq_dt[act_plc1p_dt, on = .(time>=STIME, time<ETIME), nomatch = 0, .(HHPER, time, STIME, ETIME, purp), allow.cartesian=TRUE]


act_plc1p_plot <- act_plc1p_ts10m %>% 
  group_by(time, purp) %>% 
  summarize(n=n()) %>% 
  group_by(time) %>% 
  mutate(`%`=n/sum(n))

time_v_act_pct_by_purp <- ggplot(act_plc1p_plot, aes(x = time, y = `%`, fill = purp)) + 
  geom_area(position = 'stack') +
  ggtitle("% population by activity type & time") +
  scale_y_continuous(name=NULL, labels = scales::percent)

time_v_act_pct_by_purp %>% print()
#ggsave("output/time-v-act-pct-by-purp.pdf", plot=time_v_act_pct_by_purp)
