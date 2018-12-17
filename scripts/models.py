import orca
import pandana as pdna
import pandas as pd
import scipy.stats as st

from urbansim.utils import networks
from urbansim_templates import modelmanager as mm
from urbansim_templates.models import LargeMultinomialLogitStep


# Set data directory
d = '/home/data/fall_2018/'

if 'data_directory' in orca.list_injectables():
    d = orca.get_injectable('data_directory')

# load existing model steps from the model manager
mm.initialize()


@orca.step()
def test_manual_registration():
    print("Model step is running")


@orca.step()
def initialize_network_small():
    """
    This will be turned into a data loading template.
    """

    @orca.injectable('netsmall', cache=True)
    def build_networksmall():
        nodessmall = pd.read_csv(d + 'bay_area_tertiary_strongly_nodes.csv') \
            .set_index('osmid')
        edgessmall = pd.read_csv(d + 'bay_area_tertiary_strongly_edges.csv')
        netsmall = pdna.Network(nodessmall.x, nodessmall.y, edgessmall.u,
                                edgessmall.v, edgessmall[['length']],
                                twoway=False)
        netsmall.precompute(25000)
        return netsmall


@orca.step()
def initialize_network_walk():
    """
    This will be turned into a data loading template.

    """

    @orca.injectable('netwalk', cache=True)
    def build_networkwalk():
        nodeswalk = pd.read_csv(d + 'bayarea_walk_nodes.csv') \
            .set_index('osmid')
        edgeswalk = pd.read_csv(d + 'bayarea_walk_edges.csv')
        netwalk = pdna.Network(nodeswalk.x, nodeswalk.y, edgeswalk.u,
                               edgeswalk.v, edgeswalk[['length']], twoway=True)
        netwalk.precompute(2500)
        return netwalk


@orca.step()
def initialize_network_beam():
    """
    This will be turned into a data loading template.

    """

    @orca.injectable('netbeam', cache=True)
    def build_networkbeam():
        nodesbeam = pd.read_csv(d + 'physsim-network-nodes.csv') \
            .set_index('id')
        edgesbeam = pd.read_csv(d + 'physsim-network-links.csv')
        netbeam = pdna.Network(
            nodesbeam['x'], nodesbeam['y'], edgesbeam['from'],
            edgesbeam['to'], edgesbeam[['travelTime']], twoway=False)
        netbeam.precompute(7200)
        return netbeam


@orca.step()
def network_aggregations_small(netsmall):
    """
    This will be turned into a network aggregation template.
    """
    nodessmall = networks.from_yaml(
        netsmall, 'network_aggregations_small.yaml')
    nodessmall = nodessmall.fillna(0)
    print(nodessmall.describe())
    orca.add_table('nodessmall', nodessmall)


@orca.step()
def network_aggregations_walk(netwalk):
    """
    This will be turned into a network aggregation template.

    """

    nodeswalk = networks.from_yaml(netwalk, 'network_aggregations_walk.yaml')
    nodeswalk = nodeswalk.fillna(0)
    print(nodeswalk.describe())
    orca.add_table('nodeswalk', nodeswalk)


@orca.step()
def network_aggregations_beam(netbeam):
    """
    This will be turned into a network aggregation template.

    """

    nodesbeam = networks.from_yaml(netbeam, 'network_aggregations_beam.yaml')
    nodesbeam = nodesbeam.fillna(0)
    print(nodesbeam.describe())
    orca.add_table('nodesbeam', nodesbeam)


@orca.step()
def wlcm_simulate(persons):
    """
    Generate workplace location choices for the synthetic pop. This is just
    a temporary workaround until the model templates themselves can handle
    interaction terms. Otherwise the model template would normally not need
    an addtional orca step wrapper such as is defined here.

    """
    interaction_terms_tt = pd.read_csv(
        './data/WLCM_interaction_terms_tt.csv', index_col=[
            'zone_id_home', 'zone_id_work'])
    interaction_terms_dist = pd.read_csv(
        './data/WLCM_interaction_terms_dist.csv', index_col=[
            'zone_id_home', 'zone_id_work'])
    interaction_terms_cost = pd.read_csv(
        './data/WLCM_interaction_terms_cost.csv', index_col=[
            'zone_id_home', 'zone_id_work'])

    m = mm.get_step('WLCM')

    m.run(chooser_batch_size=200000, interaction_terms=[
        interaction_terms_tt, interaction_terms_dist, interaction_terms_cost])
    
    
@orca.step()
def TOD_choice_simulate():
    """
    Generate time of day period choices for the synthetic population
    home-work and work-home trips.
    
    """
    TOD_obs = orca.merge_tables('persons', ['persons','jobs'])
    
#     TOD_obs = pd.read_csv('./data/persons_w_jobs.csv')
#     TOD_obs.index.name = 'TOD_obs_id'
    
#     jobs = pd.read_csv('/home/data/fall_2018/jobs_v2.csv')
#     hh = pd.read_csv('/home/data/fall_2018/households_v2.csv')
#     buildings = pd.read_csv('/home/data/fall_2018/buildings_v2.csv')
#     parcels = pd.read_csv('/home/data/fall_2018/parcel_attr.csv')
    
#     merge = buildings.merge(parcels,how = 'left',left_on='parcel_id',
#                             right_on='primary_id')

#     jobs = jobs.merge(merge,on = 'building_id',how = 'left').rename(columns={'zone_id': 
#                                                                           'zone_id_work'})

#     hh = hh.merge(merge,on = 'building_id',how = 'left').rename(columns={'zone_id':
#                                                                          'zone_id_home'})
    
#     TOD_obs = TOD_obs.merge(jobs,on = 'job_id',how = 'left')

#     TOD_obs = TOD_obs.merge(hh,on = 'household_id',how = 'left')
    
    TOD_obs = TOD_obs[[
        'zone_id_home', 'zone_id_work',
        'age', 'edu', 'sex','hours','hispanic','race_id',
        'income','persons',
        'sector_id','occupation_id']]
    
    TOD_obs.dropna(inplace = True)
    
    skims = pd.read_csv('./data/skims_110118.csv')
    
    TOD_obs = pd.merge(TOD_obs, skims, how = 'left', 
                       left_on=['zone_id_home','zone_id_work'], 
                       right_on=['orig','dest'])

    TOD_obs = pd.merge(TOD_obs, skims, how = 'left',
                       left_on=['zone_id_work','zone_id_home'], 
                       right_on=['orig','dest'], suffixes=('_HW', '_WH'))
    
    TOD_list = ['EA','AM','MD','PM','EV']

    for tod1 in TOD_list:
        for tod2 in TOD_list:
            col_name = f'da_Time_{tod1}_{tod2}'
            TOD_obs[col_name] = TOD_obs[f'da_Time_{tod1}_HW'] + TOD_obs[f'da_Time_{tod2}_WH']
    
    TOD_obs = TOD_obs[[
        'zone_id_home', 'zone_id_work',
        'age', 'edu', 'sex','hours','hispanic','race_id',
        'income','persons',
        'sector_id','occupation_id',    
        'da_Time_AM_MD',
        'da_Time_AM_PM',
        'da_Time_AM_EV',
        'da_Time_MD_MD',
        'da_Time_MD_PM',
        'da_Time_MD_EV']]
    
#     TOD_obs['hh_inc_150kplus'] = ((TOD_obs['income'] > 150000) | (TOD_obs['income'] == 150000)).astype(int)

#     TOD_obs['lessGED'] = (TOD_obs['edu'] < 16).astype(int)
#     TOD_obs['GED'] = TOD_obs['edu'].isin([16,17]).astype(int)
#     TOD_obs['somebach'] = TOD_obs['edu'].isin([18,19]).astype(int)
#     TOD_obs['Assoc'] = TOD_obs['edu'].isin([20]).astype(int)
#     TOD_obs['Bach'] = TOD_obs['edu'].isin([21]).astype(int)

#     TOD_obs['female'] = TOD_obs['sex'] - 1

#     TOD_obs['white'] = TOD_obs['race_id'].isin([1.0]).astype(int)
#     TOD_obs['minority'] = (TOD_obs['white'] == 1).astype(int)
#     TOD_obs['age_16less25'] = ((TOD_obs.age.between(16,25,inclusive = False)) | (TOD_obs.age==16)).astype(int)
#     TOD_obs['hh_size_1per'] = TOD_obs.persons.isin([1.0]).astype(int)

#     TOD_obs['sector_retail'] = TOD_obs['sector_id'].isin([44, 45]).astype(int)
#     TOD_obs['sector_healthcare'] = TOD_obs['sector_id'].isin([62]).astype(int)
#     TOD_obs['info'] = TOD_obs['sector_id'].isin([51]).astype(int)
#     TOD_obs['scitech'] = TOD_obs['sector_id'].isin([54]).astype(int)
#     TOD_obs['sector_mfg'] = TOD_obs['sector_id'].isin([31, 32, 33]).astype(int)
#     TOD_obs['sector_edu_serv'] = TOD_obs['sector_id'].isin([61]).astype(int)
#     TOD_obs['sector_oth_serv'] = TOD_obs['sector_id'].isin([81]).astype(int)
#     TOD_obs['sector_constr'] = TOD_obs['sector_id'].isin([23]).astype(int)
#     TOD_obs['sector_gov'] = TOD_obs['sector_id'].isin([92]).astype(int)
#     TOD_obs['finance'] = TOD_obs['sector_id'].isin([52]).astype(int)

    TOD_obs['TOD'] = None
    
    m = mm.get_step('TOD_choice')
    
    @orca.table(cache=True)
    def tripsA():
        return TOD_obs
    
    m.run()
    results = orca.get_table('tripsA').to_frame()
    persons = orca.get_table('persons').to_frame()
    persons = pd.merge(persons, results[['TOD']], left_index=True, right_index=True)
    orca.add_table('persons', persons)

    
@orca.step()
def TOD_distribution_simulate():
    """
    Generate specific time of day choices for the synthetic population
    home-work and work-home trips.
    
    """
    persons = orca.get_table('persons').to_frame()
    
    trips02 = persons.loc[persons['TOD'].isin([2])]
    trips03 = persons.loc[persons['TOD'].isin([3])]
    trips12 = persons.loc[persons['TOD'].isin([12])]
    trips13 = persons.loc[persons['TOD'].isin([13])]
    trips14 = persons.loc[persons['TOD'].isin([14])]
    trips22 = persons.loc[persons['TOD'].isin([22])]
    trips23 = persons.loc[persons['TOD'].isin([23])]
    trips24 = persons.loc[persons['TOD'].isin([24])]
    
    trips02['HW_ST'] = st.burr.rvs(size= len(trips02), 
                                   c=104.46,d=0.03,loc=2.13,scale=3.72)
    trips02['WH_ST'] = st.argus.rvs(size= len(trips02), 
                                    chi=3.02,loc=7.70,scale=7.66)

    trips03['HW_ST'] = st.genlogistic.rvs(size= len(trips03), c=0.08,loc=5.86,scale=0.05)
    trips03['WH_ST'] = st.bradford.rvs(size= len(trips03), c=8.91, loc=15.50, scale=3.01)

    trips12['HW_ST'] = st.vonmises_line.rvs(size= len(trips12), 
                                            kappa=0.33,loc=7.48,scale=0.47)
    trips12['WH_ST'] = st.johnsonsb.rvs(size= len(trips12), 
                                        a=-0.95, b=0.71, loc=8.69, scale=6.80)

    trips13['HW_ST'] = st.vonmises_line.rvs(size= len(trips13), 
                                            kappa=0.46,loc=7.48,scale=0.47)
    trips13['WH_ST'] = st.vonmises_line.rvs(size= len(trips13), 
                                            kappa=0.41, loc=16.99, scale=0.47)

    trips14['HW_ST'] = st.beta.rvs(size= len(trips14), a=1.58,b=1.14,loc=5.90,scale=3.07)
    trips14['WH_ST'] = st.pareto.rvs(size= len(trips14), b=19.93, loc=-0.36, scale=18.86)

    trips22['HW_ST'] = st.weibull_min.rvs(size= len(trips22), c=0.95,loc=9.00,scale=1.04)
    trips22['WH_ST'] = st.burr.rvs(size= len(trips22), 
                                   c=263.97, d=0.03, loc=-1.00, scale=16.33)

    trips23['HW_ST'] = st.levy.rvs(size= len(trips23), loc=8.93,scale=0.30)
    trips23['WH_ST'] = st.triang.rvs(size= len(trips23), c=0.90, loc=15.17, scale=3.34)

    trips24['WH_ST'] = st.bradford.rvs(size= len(trips24), c=21.60, loc=18.50, scale=7.76)
    
    #make sure start times are within the correct period of day:
    while len(trips02.loc[(trips02['HW_ST'] < 3) | (trips02['HW_ST'] >= 6)]) > 0:
        trips02.loc[ (trips02['HW_ST'] < 3) | (trips02['HW_ST'] >= 6),
           'HW_ST'] = st.burr.rvs(size= len(trips02.loc[(trips02['HW_ST'] < 3) |
                                                        (trips02['HW_ST'] >= 6)]), 
                                  c=104.46,d=0.03,loc=2.13,scale=3.72)

    while len(trips03.loc[(trips03['HW_ST'] < 3) | (trips03['HW_ST'] >= 6)]) > 0:
        trips03.loc[ (trips03['HW_ST'] < 3) | (trips03['HW_ST'] >= 6),
           'HW_ST'] = st.genlogistic.rvs(size= len(trips03.loc[(trips03['HW_ST'] < 3) |
                                                               (trips03['HW_ST'] >= 6)]), 
                                         c=0.08,loc=5.86,scale=0.05)
    while len(trips12.loc[(trips12['HW_ST'] < 6) | (trips12['HW_ST'] >= 9)]) > 0:
        trips12.loc[ (trips12['HW_ST'] < 6) | (trips12['HW_ST'] >= 9),
           'HW_ST'] = st.vonmises_line.rvs(size= len(trips12.loc[(trips12['HW_ST'] < 6) | 
                                                                 (trips12['HW_ST'] >= 9)]), 
                                           kappa=0.33,loc=7.48,scale=0.47)

    while len(trips13.loc[(trips13['HW_ST'] < 6) | (trips13['HW_ST'] >= 9)]) > 0:
        trips13.loc[ (trips13['HW_ST'] < 6) | (trips13['HW_ST'] >= 9),
           'HW_ST'] = st.vonmises_line.rvs(size= len(trips13.loc[(trips13['HW_ST'] < 6) | 
                                                                 (trips13['HW_ST'] >= 9)]), 
                                           kappa=0.46,loc=7.48,scale=0.47)

    while len(trips14.loc[(trips14['HW_ST'] < 6) | (trips14['HW_ST'] >= 9)]) > 0:
        trips14.loc[ (trips14['HW_ST'] < 6) | (trips14['HW_ST'] >= 9),
           'HW_ST'] = st.beta.rvs(size= len(trips14.loc[(trips14['HW_ST'] < 6) | 
                                                        (trips14['HW_ST'] >= 9)]), 
                                  a=1.58,b=1.14,loc=5.90,scale=3.07)

    while len(trips22.loc[(trips22['HW_ST'] < 9) | (trips22['HW_ST'] >= 15.5)]) > 0:
        trips22.loc[ (trips22['HW_ST'] < 9) | (trips22['HW_ST'] >= 15.5),
           'HW_ST'] = st.weibull_min.rvs(size= len(trips22.loc[(trips22['HW_ST'] < 9) | 
                                                               (trips22['HW_ST'] >= 15.5)]), 
                                         c=0.95,loc=9.00,scale=1.04)

    while len(trips23.loc[(trips23['HW_ST'] < 9) | (trips23['HW_ST'] >= 15.5)]) > 0:
        trips23.loc[ (trips23['HW_ST'] < 9) | (trips23['HW_ST'] >= 15.5),
           'HW_ST'] = st.levy.rvs(size= len(trips23.loc[(trips23['HW_ST'] < 9) | 
                                                        (trips23['HW_ST'] >= 15.5)]), 
                                  loc=8.93,scale=0.30)
    
    while len(trips02.loc[(trips02['WH_ST'] < 9) | (trips02['WH_ST'] >= 15.5)]) > 0:
        trips02.loc[ (trips02['WH_ST'] < 9) | (trips02['WH_ST'] >= 15.5),
           'WH_ST'] = st.argus.rvs(size= len(trips02.loc[(trips02['WH_ST'] < 9) | 
                                                         (trips02['WH_ST'] >= 15.5)]), 
                                   chi=3.02,loc=7.70,scale=7.66)

    while len(trips03.loc[(trips03['WH_ST'] < 15.5) | (trips03['WH_ST'] >= 18.5)]) > 0:
        trips03.loc[ (trips03['WH_ST'] < 15.5) | (trips03['WH_ST'] >= 18.5),
           'WH_ST'] = st.bradford.rvs(size= len(trips03.loc[(trips03['WH_ST'] < 15.5) | 
                                                            (trips03['WH_ST'] >= 18.5)]), 
                                      c=8.91, loc=15.50, scale=3.01)

    while len(trips12.loc[(trips12['WH_ST'] < 9) | (trips12['WH_ST'] >= 15.5)]) > 0:
        trips12.loc[ (trips12['WH_ST'] < 9) | (trips12['WH_ST'] >= 15.5),
           'WH_ST'] = st.johnsonsb.rvs(size= len(trips12.loc[(trips12['WH_ST'] < 9) | 
                                                             (trips12['WH_ST'] >= 15.5)]), 
                                       a=-0.95, b=0.71, loc=8.69, scale=6.80)

    while len(trips13.loc[(trips13['WH_ST'] < 15.5) | (trips13['WH_ST'] >= 18.5)]) > 0:
        trips13.loc[ (trips13['WH_ST'] < 15.5) | (trips13['WH_ST'] >= 18.5),
           'WH_ST'] = st.vonmises_line.rvs(size= len(
            trips13.loc[(trips13['WH_ST'] < 15.5) | (trips13['WH_ST'] >= 18.5)]), 
                                           kappa=0.41, loc=16.99, scale=0.47)
 
    while len(trips14.loc[(trips14['WH_ST'] < 18.5) | (trips14['WH_ST'] >= 27)]) > 0:
        trips14.loc[ (trips14['WH_ST'] < 18.5) | (trips14['WH_ST'] >= 27),
           'WH_ST'] = st.pareto.rvs(size= len(trips14.loc[(trips14['WH_ST'] < 18.5) | 
                                                          (trips14['WH_ST'] >= 27)]), 
                                    b=19.93, loc=-0.36, scale=18.86)

    trips14.loc[ (trips14['WH_ST'] > 24),'WH_ST'] = trips14['WH_ST'] - 24
    
    while len(trips22.loc[(trips22['WH_ST'] < 9) | (trips22['WH_ST'] >= 15.5)]) > 0:
        trips22.loc[ (trips22['WH_ST'] < 9) | (trips22['WH_ST'] >= 15.5),
           'WH_ST'] = st.burr.rvs(size= len(trips22.loc[(trips22['WH_ST'] < 9) | 
                                                        (trips22['WH_ST'] >= 15.5)]), 
                                  c=263.97, d=0.03, loc=-1.00, scale=16.33)
    #make sure HW time is before WH time for people in period 22:
    while len(trips22.loc[(trips22['HW_ST'] >= trips22['WH_ST'])]) > 0:
        trips22.loc[ (trips22['HW_ST'] >= trips22['WH_ST']),
           'WH_ST'] = st.burr.rvs(size= len(trips22.loc[(trips22['HW_ST'] >= 
                                                         trips22['WH_ST'])]), 
                                  c=263.97, d=0.03, loc=-1.00, scale=16.33)

        trips22.loc[ (trips22['HW_ST'] >= trips22['WH_ST']),
           'HW_ST'] = st.weibull_min.rvs(size= len(trips22.loc[(trips22['HW_ST'] >= 
                                                                trips22['WH_ST'])]), 
                                         c=0.95,loc=9.00,scale=1.04)
    
    while len(trips23.loc[(trips23['WH_ST'] < 15.5) | (trips23['WH_ST'] >= 18.5)]) > 0:
        trips23.loc[ (trips23['WH_ST'] < 15.5) | (trips23['WH_ST'] >= 18.5),
           'WH_ST'] = st.triang.rvs(size= len(trips23.loc[(trips23['WH_ST'] < 15.5) | 
                                                          (trips23['WH_ST'] >= 18.5)]), 
                                    c=0.90, loc=15.17, scale=3.34)

    while len(trips24.loc[(trips24['WH_ST'] < 18.5) | (trips24['WH_ST'] >= 27)]) > 0:
        trips24.loc[ (trips24['WH_ST'] < 18.5) | (trips24['WH_ST'] >= 27),
           'WH_ST'] = st.bradford.rvs(size= len(trips24.loc[(trips24['WH_ST'] < 18.5) | 
                                                            (trips24['WH_ST'] >= 27)]), 
                                      c=21.60, loc=18.50, scale=7.76)
    
    trips24.loc[ (trips24['WH_ST'] > 24),'WH_ST'] = trips24['WH_ST'] - 24
    
    #set up separate HW distribution assignment for 9am-12pm and 12-3:29pm:
    trips24a = trips24.sample(int(round(len(trips24)*(241/377))))

    AM = trips24a.index.unique()

    trips24b = trips24[~trips24.index.isin(AM)] 
    
    trips24a['HW_ST'] = st.bradford.rvs(size= len(trips24a), c=9.63, loc=9.00, scale=2.83)
    trips24b['HW_ST'] = st.exponweib.rvs(size= len(trips24b), 
                                         a=0.05, c=21.50, loc=11.99, scale=3.23)
    
    while len(trips24a.loc[(trips24a['HW_ST'] < 9) | (trips24a['HW_ST'] >= 12)]) > 0:
        trips24a.loc[ (trips24a['HW_ST'] < 9) | (trips24a['HW_ST'] >= 12),
           'HW_ST'] = st.bradford.rvs(size= len(trips24a.loc[(trips24a['HW_ST'] < 9) | 
                                                             (trips24a['HW_ST'] >= 12)]), 
                                      c=9.63, loc=9.00, scale=2.83)

    while len(trips24b.loc[(trips24b['HW_ST'] < 12) | (trips24b['HW_ST'] >= 15.5)]) > 0:
        trips24b.loc[ (trips24b['HW_ST'] < 12) | (trips24b['HW_ST'] >= 15.5),
           'HW_ST'] = st.exponweib.rvs(size= len(trips24b.loc[(trips24b['HW_ST'] < 12) | 
                                                              (trips24b['HW_ST'] >= 15.5)]), 
                                       a=0.05, c=21.50, loc=11.99, scale=3.23)

    cols = list(trips02.columns.values)

    frames = [
        trips02, trips03, trips12, trips13, trips14, trips22, trips23, trips24a, trips24b]

    TOD_obs2 = pd.concat(frames)

    TOD_obs2 = TOD_obs2[cols]
    
    persons = pd.merge(persons, TOD_obs2[['HW_ST','WH_ST']], left_index=True, right_index=True)
    orca.add_table('persons', persons)