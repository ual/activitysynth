require(pscl)

#source("code/prep_act.R")

workers <- hhper_purp_pp %>% 
  filter(worker == 1)

workers <- workers %>% 
  mutate(edu_undergrad = if_else(as.character(edu) == "Undergraduate", 1L, 0L),
         edu_graduate = if_else(as.character(edu) == "Graduate", 1L, 0L))

m1_work <- zeroinfl(nact ~ age + sex + edu_undergrad + edu_graduate + persons + hours | hours,
                    data = workers %>% filter(purp=="Work") %>% drop_na(), dist = "negbin", EM = TRUE)
summary(m1_work)

workers <- workers %>% 
  mutate(nact_f = if_else(nact > 5, 5L, nact) %>% as.factor )

m2_work <- MASS::polr(nact_f ~ age + sex + edu_undergrad + edu_graduate + persons + hours, 
                data = workers %>% filter(purp=="Work") %>% drop_na(), Hess=TRUE)
summary(m2_work)


# students <- hhper_purp_pp %>% 
#   filter(student == 1) #%>% 
#   #mutate(nact=if_else(nact>5, 5L, nact))
# 
# m1_school <- zeroinfl(nact ~ age,
#                       data = students %>% filter(purp=="School") %>% drop_na(), 
#                       dist = "negbin", EM = TRUE)
# summary(m1_school)

nact_purp_lcdf <- hhper_purp_pp %>% 
  filter(purp %in% c("Escort", "Shopping", "Recreation", "PersonalBus", "SocialRec", "EatOut")) %>% 
  group_by(purp) %>% 
  nest()

nact_purp_lcdf <- nact_purp_lcdf %>% 
  mutate(#zinb_model=map(data, ~zeroinfl(nact ~ persons,
         #                               data = ., dist = "negbin", EM = TRUE)),
         polr_model=map(data, ~MASS::polr(as.factor(nact) ~ age + sex + edu + income + persons + worker + student,
                                    data = ., Hess = TRUE))    
         )

act_time_dur_purp_lcdf <- act_plc1p %>% 
  filter(purp %in% c("Escort", "Shopping", "Recreation", "PersonalBus", "SocialRec", "EatOut")) %>% 
  group_by(purp) %>% 
  nest()

# Model of activity stime & duration (separate models)
act_time_dur_purp_lcdf <- act_time_dur_purp_lcdf %>% 
  mutate(st_min_model=map(data, ~lm(start_mins ~ age + sex + income + edu + worker + student + persons + nchildren + n65plus, data = .)),
         duration_model=map(data, ~lm(act_duration ~ age + sex + income + edu + worker + student + persons + nchildren + n65plus, data = .))
         )


# model of activity stime & duration (joint model)

# model of activity choice
# library(mlogit)
# act_mldf <- act_plc1p %>% 
#   mutate(act_id=str_pad(act_id, width=2, pad="0"),
#          hhper_act_id = str_c(HHPER, act_id)) %>% 
#   #select(HHPER, act_id, hhper_act_id)
#   as.data.frame() %>% 
#   mlogit.data(shape="wide", choice="purp", id.var="hhper_act_id")
# 
# act_mldf$worker_x_work_related <- act_mldf$worker * act_mldf$alt %in% c("Work", "WorkAtHome", "WorkRelated")
# 
# mlogit(purp ~ act_mldf, data=act_mldf) %>% summary()
# 
# MDCEV with Apollo
# ### Load Apollo library
# pacman::p_load(apollo)
# 
# ### Initialise code
# apollo_initialise()
# 
# ### Set core controls
# apollo_control  <-  list(
#   modelName  ="activity-choice-model",
#   modelDescr ="MNL model with socio-demographics on activity participation RP data",
#   indivID    ="HHPER" #+act_id
# )
# 
# ### Vector of parameters, including any that are kept fixed in estimation
# apollo_beta <- c(asc_home               = 0,
#                  asc_               = 0,
#               asc_air               = 0,
#               asc_rail              = 0,
#               asc_bus_shift_female  = 0,
#               asc_air_shift_female  = 0,
#               asc_rail_shift_female = 0,
#               b_tt_car              = 0,
#               b_tt_bus              = 0,
#               b_tt_air              = 0,
#               b_tt_rail             = 0,
#               b_tt_shift_business   = 0,
#               b_access              = 0,
#               b_cost                = 0,
#               b_cost_shift_business = 0,
#               cost_income_elast     = 0,
#               b_no_frills           = 0,
#               b_wifi                = 0,
#               b_food                = 0)
# 
# ### Vector with names (in quotes) of parameters to be kept fixed at their starting value in apollo_beta, use apollo_beta_fixed = c() if none
# apollo_fixed = c("asc_car","b_no_frills")