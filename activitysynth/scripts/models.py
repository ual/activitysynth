import orca
import pandana as pdna
import pandas as pd
import scipy.stats as st
import numpy as np
from datetime import datetime
import os

from urbansim.utils import networks
from urbansim_templates import modelmanager as mm
from urbansim_templates.models import LargeMultinomialLogitStep
from urbansim_templates.utils import update_column

from activitysynth.scripts import utils


# load existing model steps from the model manager
mm.initialize()


@orca.step()
def initialize_imputed_skims(mtc_skims):

    # if imputed skims exist, just load them
    try:
        df = orca.get_table('beam_skims_imputed').to_frame()

    # otherwise, impute the raw skims
    except FileNotFoundError:
        print('No imputed skims found. Creating them now.')

        try:
            raw_skims = orca.get_table('beam_skims_raw')
            df = utils.impute_missing_skims(mtc_skims, raw_skims)
        except FileNotFoundError:
            print(
                "Couldn't find raw skims either. Make sure there "
                "is a file of skims present in the data directory.")

    orca.add_table('beam_skims_imputed', df, cache=True)


@orca.step()
def skims_aggregations(beam_skims_imputed):

    for impedance in ['gen_tt_WALK_TRANSIT', 'gen_tt_CAR']:

        # each of these columns must be defined for the
        # zones table since the skims are reported at
        # the zone level. currently they get created in
        # variables.py under the section commented as
        # "ZONES VARIABLES"
        for col in [
                'total_jobs', 'sum_persons', 'sum_income',
                'sum_residential_units']:
            for tt in [15, 45]:
                utils.register_skim_access_variable(
                    col + '_{0}_'.format(impedance) + str(tt),
                    col, impedance, tt, beam_skims_imputed)

        for col in ['avg_income']:
            for tt in [30]:
                utils.register_skim_access_variable(
                    col + '_{0}_'.format(impedance) + str(tt),
                    col, impedance, tt, beam_skims_imputed, np.mean)


@orca.step()
def test_manual_registration():
    print("Model step is running")


@orca.step()
def initialize_network_small():
    """
    This will be turned into a data loading template.
    """
    @orca.injectable('netsmall', cache=True)
    def build_networksmall(drive_nodes, drive_edges):
        drive_nodes = drive_nodes.to_frame()
        drive_edges = drive_edges.to_frame()
        netsmall = pdna.Network(
            drive_nodes.x, drive_nodes.y, drive_edges.u,
            drive_edges.v, drive_edges[['length']],
            twoway=False)
        netsmall.precompute(25000)
        return netsmall


@orca.step()
def initialize_network_walk():
    """
    This will be turned into a data loading template.

    """
    @orca.injectable('netwalk', cache=True)
    def build_networkwalk(walk_nodes, walk_edges):
        walk_nodes = walk_nodes.to_frame()
        walk_edges = walk_edges.to_frame()
        netwalk = pdna.Network(
            walk_nodes.x, walk_nodes.y, walk_edges.u,
            walk_edges.v, walk_edges[['length']], twoway=True)
        netwalk.precompute(2500)
        return netwalk


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
def wlcm_simulate(beam_skims_imputed):
    """
    Generate workplace location choices for the synthetic pop. This is just
    a temporary workaround until the model templates themselves can handle
    interaction terms. Otherwise the model template would normally not need
    an addtional orca step wrapper such as is defined here.

    """
    interaction_terms = beam_skims_imputed.to_frame().rename_axis(
        ['zone_id_home', 'zone_id_work'])

    m = mm.get_step('WLCM_gen_tt_simple')

    m.run(chooser_batch_size=200000, interaction_terms=[interaction_terms])

    orca.broadcast(
        'jobs', 'persons', cast_index=True, onto_on='job_id')


@orca.step()
def auto_ownership_simulate(households):
    """
    Generate auto ownership choices for the synthetic pop households.
    The categories are:
    - 0: no vehicle
    - 1: one vehicle
    - 2: two vehicles
    - 3: three or more vehicles
    """
    m = mm.get_step('auto_ownership')

    # remove filters, specify out tables
    m.filters = None
    m.tables = [
        'households', 'units', 'buildings', 'parcels', 'nodessmall',
        'nodeswalk']
    m.out_tables = [
        'households', 'units', 'buildings', 'parcels', 'nodessmall',
        'nodeswalk']
    m.run()


@orca.step()
def primary_mode_choice_simulate(persons):
    """
    Generate primary mode choices for the synthetic population.
    The choices are:
    - 0: drive alone
    - 1: shared
    - 2: walk-transit-walk
    - 3: drive-transit-walk
    - 4: walk-transit-drive
    - 5: bike
    - 6: walk
    """

    @orca.table(cache=True)
    def persons_CHTS_format(mtc_skims):
    # use persons with jobs for persons
        persons = orca.get_table('persons').to_frame()
        persons.index.name = 'person_id'
        persons.reset_index(inplace=True)
        persons = persons[['person_id','sex','age','race_id','worker','edu','household_id','job_id', 'TOD']]

        hh_df = orca.get_table('households').to_frame().reset_index()[['household_id','cars','tenure','income','persons','building_id']]
        jobs_df = orca.get_table('jobs').to_frame().reset_index()[['job_id','building_id']]
        buildings_df = orca.get_table('buildings').to_frame().reset_index()[['building_id','parcel_id']]
        try:
            parcels_df = orca.get_table('parcels').to_frame().reset_index()[['parcel_id','zone_id']]
        except KeyError:
            parcels_df = orca.get_table('parcels').to_frame().reset_index()[['primary_id','zone_id']]
            parcels_df.rename(columns = {'primary_id':'parcel_id'}, inplace = True)

        # rename columns/change values to match CHTS
        persons.columns = ['person_id','GEND','AGE','RACE1','JOBS','EDUCA','household_id','job_id', 'TOD']
        persons.RACE1 = persons.RACE1.map({1:1,2:2,3:3,4:3,5:3,6:4,7:5,8:97,9:97})
        persons.EDUCA = persons.EDUCA.map({0:1,1:1,2:1,3:1,4:1,5:1,6:1,7:1,8:1,9:1,
                                                            10:1,11:1,12:1,13:1,14:1,15:1,16:2,17:2,18:3,19:3,
                                                            20:4,21:5,22:6,23:6,24:6})
        persons.TOD = persons.TOD.map({2:'EA',3:'EA',12:'AM',14:'AM',22:'MD',23:'MD',24:'MD'})

        # read skim
        skim = orca.get_table('mtc_skims').to_frame()
        
        skim.columns = skim.columns.str.replace('_distance','_Distance') # capitalization issues
        skim.columns = skim.columns.str.replace('_cost','_Cost')
        
        EA_skim = skim[['orig','dest']+list(skim.filter(like = 'EA').columns)]
        EA_skim.columns = EA_skim.columns.str.replace('_EA','')
        EA_skim['TOD'] = 'EA'
        AM_skim = skim[['orig','dest']+list(skim.filter(like = 'AM').columns)]
        AM_skim.columns = AM_skim.columns.str.replace('_AM','')
        AM_skim['TOD'] = 'AM'
        MD_skim = skim[['orig','dest']+list(skim.filter(like = 'MD').columns)]
        MD_skim.columns = MD_skim.columns.str.replace('_MD','')
        MD_skim['TOD'] = 'MD'

        skim_combined = pd.concat([EA_skim,AM_skim,MD_skim])

        MTC_acc = pd.read_csv('./data/MTC_TAZ_accessibility.csv')

        # merge attributes onto persons
        # want household as origin zone and job as destination zone.

        hh_df = hh_df.merge(buildings_df, how = 'left', on = 'building_id').merge(parcels_df, how = 'left', on = 'parcel_id')
        hh_df.rename(columns = {'zone_id':'orig'},inplace = True)

        jobs_df = jobs_df.merge(buildings_df,how = 'left', on = 'building_id').merge(parcels_df, how = 'left', on = 'parcel_id')
        jobs_df.rename(columns = {'zone_id':'dest'}, inplace = True)

        persons = persons.merge(hh_df, how = 'left', on = 'household_id')
        persons.drop(['building_id','parcel_id'],axis = 1,inplace = True)

        persons = persons.merge(jobs_df, how = 'inner',on = 'job_id')
        persons.drop(['building_id','parcel_id'],axis = 1,inplace = True)


        persons = persons.merge(MTC_acc, how = 'left',left_on = 'orig', right_on = 'taz1454')
        persons[MTC_acc.columns] = persons[MTC_acc.columns].fillna(0)

        persons = persons.merge(skim_combined, how = 'left', on = ['orig','dest','TOD'])

        
        # rename the remaning attributes
        persons['OWN'] = (persons['tenure']==1).astype(int)
        persons.rename(columns = {'cars':'HHVEH','income':'INCOM','persons':'HHSIZ'},inplace = True)
        return persons
    
    
    m = mm.get_step('primary_mode_choice')
    
    # remove filters, specify out table, out column
    m.filters = None
    m.out_filters = None
    m.tables = ['persons_CHTS_format']
    m.out_tables = 'persons_CHTS_format'
    m.out_column = 'primary_commute_mode'

    m.run()


@orca.step()
def TOD_choice_simulate(mtc_skims):
    """
    Generate time of day period choices for the synthetic population
    home-work and work-home trips.
    """
    TOD_obs = orca.merge_tables('persons', ['persons', 'households', 'jobs'])
    
    # TOD_obs.dropna(inplace = True)
    TOD_obs.reset_index(inplace=True)

    mtc_skims = orca.get_table('mtc_skims').to_frame()
    
    TOD_obs = pd.merge(TOD_obs, mtc_skims, how = 'left', 
                       left_on=['zone_id_home', 'zone_id_work'], 
                       right_on=['orig', 'dest'])

    TOD_obs = pd.merge(TOD_obs, mtc_skims, how = 'left',
                       left_on=['zone_id_work','zone_id_home'], 
                       right_on=['orig', 'dest'], suffixes=('_HW', '_WH'))
    
    TOD_list = ['EA','AM','MD','PM','EV']

    for tod1 in TOD_list:
        for tod2 in TOD_list:
            col_name = f'da_Time_{tod1}_{tod2}'
            TOD_obs[col_name] = TOD_obs[f'da_Time_{tod1}_HW'] + TOD_obs[f'da_Time_{tod2}_WH']

    # TOD_obs['TOD'] = None
    
    m = mm.get_step('TOD_choice')
    
    @orca.table(cache=True)
    def tripsA():
        return TOD_obs
    
    m.run()

    results = orca.get_table('tripsA').to_frame().set_index('person_id')
    persons = orca.get_table('persons').to_frame()

    #####UPDATE COLUMN#######
    update_column('persons', 'TOD', results['TOD'])
    # persons = pd.merge(
    #     persons, results[['TOD']], how='left',
    #     left_index=True, right_index=True)
    # orca.add_table('persons', persons)

    
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

    for col in ['HW_ST', 'WH_ST']:
        update_column('persons', col, TOD_obs2[col])
    # persons = pd.merge(
    #     persons, TOD_obs2[['HW_ST', 'WH_ST']], how='left',
    #     left_index=True, right_index=True)
    # orca.add_table('persons', persons)


@orca.step()
def generate_activity_plans():

    time = str(datetime.now().strftime('%Y-%m-%d_%H:%M:%S'))

    persons = orca.get_table('persons').to_frame().reset_index().rename(
        columns={'index': 'person_id'})

    job_coords = orca.merge_tables('jobs', ['jobs', 'buildings', 'parcels'])
    job_coords = job_coords[['x', 'y']]

    hh_coords = orca.merge_tables(
        'households', ['households', 'units', 'buildings', 'parcels'])
    hh_coords = hh_coords[['x', 'y']]

    trips = persons[[
        'person_id', 'household_id', 'job_id', 'HW_ST',
        'WH_ST']].rename(
            columns={'HW_ST': 'Home', 'WH_ST': 'Work'})

    trip_data = trips.merge(
        hh_coords, left_on='household_id', right_index=True).merge(
        job_coords, left_on='job_id', right_index=True,
        suffixes=('_home', '_work'))
    trip_data = trip_data[[
        'person_id', 'Home', 'Work', 'x_home', 'y_home', 'x_work',
        'y_work']]

    melted = trip_data.melt(
        id_vars=['person_id', 'x_home', 'y_home', 'x_work', 'y_work'],
        var_name='activityType', value_name='endTime')
    melted['x'] = None
    melted['y'] = None
    melted.loc[melted['activityType'] == 'Home', 'x'] = melted.loc[
        melted['activityType'] == 'Home', 'x_home']
    melted.loc[melted['activityType'] == 'Home', 'y'] = melted.loc[
        melted['activityType'] == 'Home', 'y_home']
    melted.loc[melted['activityType'] == 'Work', 'x'] = melted.loc[
        melted['activityType'] == 'Work', 'x_work']
    melted.loc[melted['activityType'] == 'Work', 'y'] = melted.loc[
        melted['activityType'] == 'Work', 'y_work']

    plans = melted.sort_values(['person_id', 'endTime'])[[
        'person_id', 'activityType', 'endTime', 'x',
        'y']].reset_index(drop=True)
    plans['planElement'] = 'activity'
    plans['planElementIndex'] = plans.groupby('person_id').cumcount() * 2 + 1

    returnActivity = plans[plans['planElementIndex'] == 1]
    returnActivity.loc[:, 'planElementIndex'] = 5
    returnActivity.loc[:, 'endTime'] = None

    plans = plans.append(
        returnActivity, ignore_index=True).sort_values(
        ['person_id', 'planElementIndex'])

    legs = plans[plans['planElementIndex'].isin([1, 3])]
    legs.loc[:, 'planElementIndex'] = legs.loc[:, 'planElementIndex'] + 1
    legs.loc[:, 'activityType'] = ''
    legs.loc[:, 'endTime'] = None
    legs.loc[:, 'x'] = None
    legs.loc[:, 'y'] = None
    legs.loc[:, 'planElement'] = 'leg'

    plans = plans.append(legs, ignore_index=True).sort_values(
        ['person_id', 'planElementIndex']).rename(
        columns={'person_id': 'personId'}).reset_index(
        drop=True)
    plans = plans[[
        'personId', 'planElement', 'planElementIndex', 'activityType',
        'x', 'y', 'endTime']]
    plans['x']
    # plans.loc[plans['planElement'] == 'activity', 'mode'] = ''
    orca.add_table('plans', plans, cache=True)
