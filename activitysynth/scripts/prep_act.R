if (!require(pacman, quietly = TRUE)) {install.packages("pacman"); library(pacman)}
# need to install libcurl for conda w/ command:
# conda install -c anaconda libcurl 
p_load(tidyverse, stringr, readr, furrr, tictoc, lubridate)
require(dplyr)


# Data Loading
data_dir <- "/home/data/fall_2018/CHTS_csv_format/data"

hh <- read_csv(file.path(data_dir, "Deliv_HH.csv"), 
               #col_types=c(SAMPN="c")
               col_types=cols_only(SAMPN="c", AREA="i", HHVEH="i", 
                                   HHBIC="i", OWN="i", INCOM="i", HHSIZ="i")) %>% 
  #select(SAMPN, AREA, HHVEH, HHBIC, OWN, INCOM, HHSIZ) %>% 
  rename(cars=HHVEH, income=INCOM, persons=HHSIZ, bikes=HHBIC) %>% 
  mutate(#tenure = if_else(OWN == 1, 1L, 2L),
         income = if_else(income %in% c(98, 99), NA_integer_, income),
         income = factor(income, 
                         levels = 1:10, 
                         labels=c("<$10k", "$10-25k", "$25-35k", "$35-50k", "$50-75k", 
                                  "$75-100k", "$100-150k", "$150-200k","$200-250k", "$250k+"))
         ) %>% 
  select(-OWN)  

#only load MTC data for speed (there may be reason for using all/more data)
hh.sub <- hh %>% select(SAMPN, AREA) %>% 
  filter(AREA==22)

pp <- read_csv(file.path(data_dir, "Deliv_PER.csv"),
               #col_types=c(SAMPN="c", PERNO="c")
               col_types=cols_only(SAMPN="c", PERNO="c", GEND="i", 
                                   RELAT="i", AGE="i", HISP="i", EMPLY="i",
                                   WMODE="i", EDUCA="i", HOURS="i", 
                                   STUDE="i", RACE1="i")) %>%
  #select(SAMPN, PERNO, GEND, RELAT, AGE, HISP, EMPLY, WMODE, EDUCA, HOURS, STUDE, RACE1) %>% 
  mutate(PERNO=str_pad(PERNO, width=2, pad="0"), 
         HHPER=str_c(SAMPN, PERNO),
         student=if_else(STUDE %in% c(1, 2), 1L, 0L),
         sex=if_else(GEND==9, NA_integer_, GEND),
         worker=if_else(EMPLY==1, 1L, 0L)) %>% 
  rename(relate=RELAT, age=AGE, hispanic=HISP, 
         primary_commute_mode=WMODE,
         edu=EDUCA, hours=HOURS, race_id=RACE1) %>%
  mutate(edu = if_else(edu %in% c(8, 9), NA_integer_, edu),
         edu = factor(edu, 
                      levels=1:7,
                      labels=c("less than HS", "HS", "Some College", "Associate Degree", "Undergraduate", 
                               "Graduate", "Other"))
  ) %>% 
  select(-STUDE, -EMPLY, -GEND)

act <- read_csv(file.path(data_dir, "Deliv_ACTIVITY.csv"), 
                col_types=c(SAMPN="c", PERNO="c", STIME="t", ETIME="t"))

plc <- read_csv(file.path(data_dir, "Deliv_PLACE.csv"), 
                col_types=c(SAMPN="c", PERNO="c"))

act.sub <- hh.sub %>% 
  left_join(act, by="SAMPN") %>% 
  mutate(PERNO=str_pad(PERNO, width=2, pad="0"), 
         HHPER=str_c(SAMPN, PERNO)) %>% 
  select(HHPER, PLANO, ACTNO, APURP, O_APURP, STIME, ETIME)

plc <- plc %>% mutate(PERNO=str_pad(PERNO, width=2, pad="0"), 
                      HHPER=str_c(SAMPN, PERNO))

act_plc <- act.sub %>% left_join(plc, by= c("HHPER", "PLANO"))

#act_plc %>% filter(HHPER == "10352742") %>% 
#            select(HHPER, PLANO, TRIPNO.x, MODE, ARR_HR, ARR_MIN, DEP_HR, DEP_MIN, PNAME, APURP)

#' x <- c(1, 1, 0, 0, 1, 1, 1, 0)
#' seq_id(x)
#' 1, 1, 2, 3, 4, 4, 4, 5
#' x <- c(1, 1, 0, 0, 1, 1, 1, 0)
#' seq_id(x, nlag=1)
#' 1, 1, 1, 2, 3, 3, 3, 3
#' # these inputs don't make sense, but they should still work
#' seq_id(rep(0L, 10))
#' 1  2  3  4  5  6  7  8  9 10
#' x2 <- c(1, 0, 1, 1, 0, 0, 1)
#' seq_id(x2)
#' 1, 2, 3, 3, 4, 5, 6
#' seq_id(x2, nlag=1)
#' 1, 1, 2, 2, 2, 3, 4

seq_id <- function(x, nlead=0, nlag=0) {
  xl <- as.logical(x)
  xl_lag1 <- lag(xl, default=FALSE)
  y <- rep(1L, length(xl))
  
  y[(xl & xl_lag1)] <- 0L
  
  if (nlead !=0) {
    xl_leadn <- lead(xl, n=nlead, default=FALSE)
    y[!x & xl_leadn] <- 0L    
  }
  
  if (nlag != 0) {
    xl_lagn <- lag(xl, n=nlag, default=FALSE)
    y[!x & xl_lagn] <- 0L
  }
  
  cumsum(y)
  
}

act_plc <- act_plc %>% 
  mutate(
    mode=case_when(
      MODE %in% c(1) ~ "walk",
      MODE %in% c(2) ~ "bike",
      MODE %in% c(5, 8, 9, 10) ~ "drive alone",
      MODE %in% c(6, 7) ~ "drive shared",
      MODE %in% c(15, 16, 17, 19:29) ~ "transit",
      MODE %in% c(18) ~ "transit", # should school bus be coded as transit?
      is.na(MODE) ~ NA_character_,
      TRUE ~ "other"),
    #use the last digit of SAMPN to assign paritition for parallel processing
    partition = str_sub(SAMPN, -1, -1),
    
    PNAME = str_to_upper(PNAME),
    purp=case_when(
      APURP %in% c(21) ~ "ChangeMode",
      APURP %in% c(31, 23) ~ "EatOut",
      APURP %in% c(22) ~ "Escort",
      APURP %in% c(24, 26, 29, 30) ~ "PersonalBus",
      APURP %in% c(4, 14, 32, 34, 35, 36) ~ "Recreation",
      APURP %in% c(17) ~ "School",
      APURP %in% c(18, 19, 20) ~ "SchoolRelated",
      APURP %in% c(27, 28) ~ "Shopping",
      APURP %in% c(3, 13, 33, 37) ~ "SocialRec",
      APURP %in% c(9, 15) ~ "Work",
      APURP %in% c(6) ~ "WorkAtHome",
      APURP %in% c(10, 11, 12, 16) ~ "WorkRelated",
      str_detect(O_APURP, "^OUT OF") ~ "Out of Area",
      PNAME == "HOME" ~ "Home",
      TRUE ~ "Other")
  )

# Data cleanup
#' there are 33 persons who's last activity is ChangeMode. 28 of them
#' are taking flights, the rest is due to either incorrect coding of purp 
#' (SAMPN="1832032", PERNO="1") 
#' or incomplete diary (diary ends earlier than 02:59)
#' ("1186446" "2"; "2312406" "4"; "2494270" "1"; "3022295" "2")
#' 
#' SOLUTION: 
#' 1. Change the purp code of the last activity for the 28 persons to "Out of Area" (?);
act_plc <- act_plc %>% 
  group_by(HHPER) %>% 
  mutate(purp=if_else(row_number() == n() & 
                        purp == "ChangeMode" & 
                        !(PNAME %in% c("MUNI", "HOME", "STARBUCKS")), 
                      "Out of Area", purp)) %>% 
  ungroup()

#' 2. The purp of the last activity for (SAMPN="1832032", PERNO="01") is changed to "Home";
act_plc <- act_plc %>% mutate(purp=if_else(HHPER=="183203201" & PLANO == 15, "Home", purp))

#' 3. activities for ("1186446" "02"; "2312406" "04"; "2494270" "01"; "3022295" "02") are removed;
act_plc <- act_plc %>% filter(!(HHPER %in% c("118644602", "231240604", "249427001", "302229502")))

# Data Processing

step1_setup <- . %>% 
    arrange(HHPER, PLANO, ACTNO, TRIPNO) %>% 
    #filter(APURP==21) %>%  # change of modes
    group_by(HHPER) %>% 
    transmute(PNAME = PNAME,
              prevpurp = lag(purp, default = ""),
              purp = purp,
              nextpurp = lead(purp, default = ""),
              STIME = STIME,   ##
              nextSTIME = lead(STIME),
              prevETIME = lag(ETIME),
              ETIME = ETIME,   ##
              mode = mode,     ##
              
              ## intermediate variables for collapsing multi-leg trips
              ctrip_id = seq_id(purp == "ChangeMode", nlag=1)
    ) 

step2_collapse_trips  <- . %>%
    group_by(HHPER, ctrip_id) %>% 
    summarize(PNAME = last(PNAME),
              ._prevpurp = first(prevpurp), # for diagnostics only
              purp = last(purp),            
              ._nextpurp = last(nextpurp),  # for diagnostics only
              STIME = last(STIME),   ##
              nextSTIME = last(nextSTIME),
              prevETIME = first(prevETIME),
              ETIME = last(ETIME),   ##
              
              mode = paste0(mode, collapse="-"),
              #transfers = str_count(mode, "transit") - 1,
              #max_nextstime = max(nextSTIME), 
              #min_prevetime = min(prevETIME),
              travel_time = STIME - prevETIME
              #travel_dist = sum(),
              #start_time = last(STIME),
              #end_time = last(ETIME) 
    ) %>% 
  ungroup()

step2b_collapse_activities  <- . %>%
  group_by(HHPER) %>% 
  mutate(
    prevpurp = lag(purp, default = ""),
    nextpurp = lead(purp, default = ""),     
    # intermediate variables for collapsing multi-records of continuing activities
    cont_act = if_else((purp == nextpurp & ETIME == nextSTIME) | 
                         (prevpurp == purp & prevETIME == STIME),
                       1L, 0L),
    act_id = seq_id(cont_act)) %>% 
  group_by(HHPER, act_id) %>% 
  summarize(purp = last(purp), 
            PNAME = last(PNAME),
            # there should be no mode (NA) or a single mode for observations
            # sharing the same act_id. If true, use mode=last(mode)
            mode = paste0(mode, collapse="-"),
            #mode = last(mode),
            #transfers = sum(transfers),
            #max_nextstime = max(nextSTIME), 
            #min_prevetime = min(prevETIME),
            travel_time = sum(travel_time),
            
            STIME = first(STIME),
            ETIME = last(ETIME)
  ) %>% 
  ungroup()

step2c_wrap_act_timing  <- . %>%
  arrange(HHPER, act_id) %>% 
  group_by(HHPER) %>% 
  mutate(
    firstpurp = first(purp),
    lastpurp = last(purp),
    firstSTIME = first(STIME),
    firstETIME = first(ETIME),
    lastSTIME = last(STIME),
    lastETIME = last(ETIME),
    
    # change the start time for the first act
    STIME = if_else(row_number() == 1 &
                      firstpurp == lastpurp & 
                      firstSTIME == lastETIME + hms::hms(0, 1, 0), 
                    lastSTIME, STIME),
    # change the end time for the last act
    ETIME = if_else(row_number() == n() &
                      firstpurp == lastpurp &
                      firstSTIME == lastETIME + hms::hms(0, 1, 0), 
                    firstETIME, ETIME)) %>% 
  ungroup() %>% 
  select(-(firstpurp:lastETIME))

step3_postproc <- . %>%
    mutate(
      transfers = str_count(mode, "transit") - 1,
      transfers = ifelse(transfers < 0, 0L, transfers),
      travel_time = time_length(travel_time, unit="minute"), 
      travel_time = ifelse(travel_time < 0, travel_time + 24*60, travel_time),

      act_duration = time_length(ETIME - STIME, unit="minute"),
      act_duration = ifelse(act_duration < 0, act_duration + 24*60, act_duration),
      )

steps <- function(.data) {
  .data %>% 
    step1_setup %>% 
    step2_collapse_trips %>% 
    step2b_collapse_activities %>% 
    step2c_wrap_act_timing %>% 
    step3_postproc
}

# parallel

# split the data by partition & process data for each partition in parallel
act_plc_split <- split(act_plc, act_plc$partition) 
length(act_plc_split)

future::plan(multiprocess)
tic("parallel processing")
act_plc1p <- furrr::future_map_dfr(act_plc_split, steps)
toc()

# non-parallel
if (compare <- FALSE) {
    tic("non-parallel processing")
    #act_plc1s <- purrr::map_dfr(act_plc_split, steps)
    act_plc1s <- steps(act_plc)
    toc()

    identical(act_plc1p, act_plc1s)
}

# check results
options(tibble.width = Inf)
options(tibble.print_max = 50)

act_plc1p %>% group_by(purp) %>% tally() %>% arrange(n)
act_plc1p %>% group_by(mode) %>% tally() %>% arrange(n) %>% print(n=400)
act_plc1p %>% filter(mode %in% c("NA-NA", "NA-drive shared"))

act_plc1p %>% filter(HHPER %in% c("10456721", "10488521", "11202202", "11704701"))
act_plc1p %>% filter(HHPER %in% c("13373541", "25038951", "26550704", "14302341"))

# prep data for modeling

hh_plus <- pp %>% 
  group_by(SAMPN) %>% 
  summarize(nchildren=sum(age < 18),
            n65plus=sum(age > 65)) %>% 
  ungroup()

pp_hh <- pp %>% 
  left_join(hh, by="SAMPN") %>% 
  left_join(hh_plus, by="SAMPN")

## Create data structure for number of activities by purp
hhper <- act_plc1p %>% distinct(HHPER) %>% mutate(.id=1)
purp <- act_plc1p %>% distinct(purp) %>% mutate(.id=1)
hhper_purp <- hhper %>% 
  left_join(purp, by=".id") %>% 
  select(-.id)

# collapse work-related and school-related activities
nact_df <- act_plc1p %>% 
  group_by(HHPER, purp) %>% 
  summarize(nact=n())

hhper_purp_nact <- hhper_purp %>% 
  left_join(nact_df, by=c("HHPER", "purp")) %>% 
  mutate(nact=ifelse(is.na(nact), 0L, nact))

hhper_purp_pp <- hhper_purp_nact %>% 
  left_join(pp_hh %>% select(-SAMPN, -PERNO), 
            by="HHPER")
#

act_plc1p <- act_plc1p %>% 
  left_join(pp_hh %>% dplyr::select(-SAMPN, -PERNO), 
            by="HHPER") %>% 
  mutate(start_mins=as.numeric(STIME)/60)

write_csv(act_plc1p, "../data/activities.csv")
write_csv(hhper_purp_pp, "../data/hhper_purp_nact.csv")
