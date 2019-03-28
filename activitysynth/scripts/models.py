import orca
import pandana as pdna
import pandas as pd
import scipy.stats as st
import numpy as np
from datetime import datetime

from urbansim.utils import networks
from urbansim_templates import modelmanager as mm
from urbansim_templates.models import LargeMultinomialLogitStep


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
    def build_networksmall(data_mode, store, s3_input_data_url, local_data_dir):
        if data_mode == 's3':
            nodes = pd.read_parquet(s3_input_data_url.format('nodessmall'))
            edges = pd.read_parquet(s3_input_data_url.format('edgessmall'))
        elif data_mode == 'h5':
            nodes = store['nodessmall']
            edges = store['edgessmall']
        elif data_mode == 'csv':
            nodes = pd.read_csv(
                local_data_dir + 'bay_area_tertiary_strongly_nodes.csv').set_index('osmid')
            edges = pd.read_csv(
                local_data_dir + 'bay_area_tertiary_strongly_edges.csv').set_index('uniqueid')
        netsmall = pdna.Network(
            nodes.x, nodes.y, edges.u,
            edges.v, edges[['length']],
            twoway=False)
        netsmall.precompute(25000)
        return netsmall


@orca.step()
def initialize_network_walk():
    """
    This will be turned into a data loading template.

    """

    @orca.injectable('netwalk', cache=True)
    def build_networkwalk(data_mode, store, s3_input_data_url, local_data_dir):
        if data_mode == 's3':
            nodes = pd.read_parquet(s3_input_data_url.format('nodeswalk'))
            edges = pd.read_parquet(s3_input_data_url.format('edgeswalk'))
        elif data_mode == 'h5':
            nodes = store['nodeswalk']
            edges = store['edgeswalk']
        elif data_mode == 'csv':
            nodes = pd.read_csv(
                local_data_dir + 'bayarea_walk_nodes.csv').set_index('osmid')
            edges = pd.read_csv(
                local_data_dir + 'bayarea_walk_edges.csv').set_index(
                'uniqueid')
        netwalk = pdna.Network(
            nodes.x, nodes.y, edges.u,
            edges.v, edges[['length']], twoway=True)
        netwalk.precompute(2500)
        return netwalk


@orca.step()
def initialize_network_beam():
    """
    This will be turned into a data loading template.

    """

    @orca.injectable('netbeam', cache=True)
    def build_networkbeam(s3_input_data_url):
        nodes = pd.read_parquet(s3_input_data_url.format('nodesbeam'))
        edges = pd.read_parquet(s3_input_data_url.format('edgesbeam'))
        netbeam = pdna.Network(
            nodes['x'], nodes['y'], edges['from'],
            edges['to'], edges[['travelTime']], twoway=False)
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
def wlcm_simulate():
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

    orca.broadcast(
        'jobs', 'persons', cast_index=True, onto_on='job_id')


@orca.step()
def auto_ownership_simulate(households):
    """
    Generate auto ownership choices for the synthetic pop households. The categories are:
    - 0: no vehicle
    - 1: one vehicle
    - 2: two vehicles
    - 3: three or more vehicles
    """
    
       
    m = mm.get_step('auto_ownership')
    
    # remove filters, specify out tables
#     households['cars_alt'] = None
    
    m.filters = None
    m.tables = ['households','units','buildings','parcels' ,'nodessmall','nodeswalk']
    m.out_tables = ['households','units','buildings','parcels' ,'nodessmall','nodeswalk']
    
    m.run()

#     results = orca.get_table('tripsA').to_frame().set_index('person_id')
#     households = orca.get_table('households').to_frame()
#     households = pd.merge(
#         households, results[['cars_alt']], how='left',
#         left_index=True, right_index=True)
#     orca.add_table('households', households)
    
@orca.step()
def primary_mode_choice_simulate(persons):
    """
    Generate primary mode choices for the synthetic population. The choices are:
    - 0: drive alone
    - 1: shared
    - 2: walk-transit-walk
    - 3: drive-transit-walk
    - 4: walk-transit-drive
    - 5: bike
    - 6: walk
    """    
    @orca.table(cache=True)
    def persons_CHTS_format(skims):
    # use persons with jobs for persons
        persons = orca.get_table('persons').to_frame()
        persons.index.name = 'person_id'
        persons.reset_index(inplace=True)
        persons = persons[['person_id','sex','age','race_id','worker','edu','household_id','job_id', 'TOD']]

        hh_df = orca.get_table('households').to_frame().reset_index()[['household_id','cars_alt','tenure','income','persons','building_id']]
        jobs_df = orca.get_table('jobs').to_frame().reset_index()[['job_id','building_id']]
        buildings_df = orca.get_table('buildings').to_frame().reset_index()[['building_id','parcel_id']]
        parcels_df = orca.get_table('parcels').to_frame().reset_index()[['primary_id','zone_id']]
        parcels_df.rename(columns = {'primary_id':'parcel_id'}, inplace = True)

        # rename columns/change values to match CHTS
        persons.columns = ['person_id','GEND','AGE','RACE1','JOBS','EDUCA','household_id','job_id', 'TOD']
        persons.RACE1 = persons.RACE1.map({1:1,2:2,3:3,4:3,5:3,6:4,7:5,8:97,9:97})
        persons.EDUCA = persons.EDUCA.map({0:1,1:1,2:1,3:1,4:1,5:1,6:1,7:1,8:1,9:1,
                                                            10:1,11:1,12:1,13:1,14:1,15:1,16:2,17:2,18:3,19:3,
                                                            20:4,21:5,22:6,23:6,24:6})
        persons.TOD = persons.TOD.map({0:'EA',1:'AM',2:'MD',3:'PM',4:'EV'})

        # read skim
        skim = orca.get_table('skims').to_frame()
        
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

        
        # rename the remaining attributes
        persons['OWN'] = (persons['tenure']==1).astype(int)
        persons.rename(columns = {'cars_alt':'HHVEH','income':'INCOM','persons':'HHSIZ'},inplace = True)
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
def TOD_category_simulate(skims):
    """
    Generate time of day period choices for the synthetic population
    home-work and work-home trips.
    
    """
    TOD_obs = orca.merge_tables('persons', ['persons', 'households', 'jobs'])

    TOD_obs.dropna(subset = ['age', 'edu', 'sex','hours','race_id', 
                             'income','persons', 'tenure','sector_id'])
    
    TOD_obs.reset_index(inplace=True)
    
    m = mm.get_step('work_TOD_choice')
    
    @orca.table(cache=True)
    def tripsA():
        return TOD_obs
    
    m.run()

    results = orca.get_table('tripsA').to_frame().set_index('person_id')
    persons = orca.get_table('persons').to_frame()
    persons = pd.merge(
        persons, results[['TOD']], how='left',
        left_index=True, right_index=True)
    orca.add_table('persons', persons)

@orca.step()
def TOD_dwell_simulate(skims):
    """
    Generate time of day period choices for the synthetic population
    home-work and work-home trips.
    
    """
    TOD_obs = orca.merge_tables('persons', ['persons', 'households', 'jobs'])

    TOD_obs.dropna(subset = ['age', 'edu', 'sex','hours','race_id', 
                             'income','persons', 'tenure','sector_id'])
    
    TOD_obs.reset_index(inplace=True)
    
    m = mm.get_step('dwell_work')
    
    @orca.table(cache=True)
    def tripsA():
        return TOD_obs
    
    m.run()

    results = orca.get_table('tripsA').to_frame().set_index('person_id')
    persons = orca.get_table('persons').to_frame()
    persons = pd.merge(
        persons, results[['dwell_work']], how='left',
        left_index=True, right_index=True)
    orca.add_table('persons', persons)
    
@orca.step()
def TOD_distribution_simulate():
    """
    Generate specific time of day choices for the synthetic population
    home-work and work-home trips.
    
    """
    persons = orca.get_table('persons').to_frame()
    
    TOD_obs = persons.copy()
    
    TOD_obs['HW_ET'] = st.johnsonsu.rvs(size= len(TOD_obs), a=-0.71, b=1.00, loc=7.12, scale=1.31)
    
    TOD_obs['dwell_exact'] = st.johnsonsu.rvs(size= len(TOD_obs), a=0.49, b=0.94, loc=9.29, scale=1.26)
    
    #make sure start times are within the correct period of day:
    while len(TOD_obs.loc[(TOD_obs['TOD'] == 0) & ((TOD_obs['HW_ET'] < 3) | (TOD_obs['HW_ET'] >= 6))]) > 0:
        TOD_obs.loc[(TOD_obs['TOD'] == 0) &  ((TOD_obs['HW_ET'] < 3) | (TOD_obs['HW_ET'] >= 6)),
           'HW_ET'] = st.johnsonsu.rvs(size= len(TOD_obs.loc[(TOD_obs['TOD'] == 0) & ((TOD_obs['HW_ET'] < 3) | (TOD_obs['HW_ET'] >= 6))]), 
                                       a=-0.71, b=1.00, loc=7.12, scale=1.31)

    while len(TOD_obs.loc[(TOD_obs['TOD'] == 1) & ((TOD_obs['HW_ET'] < 6) | (TOD_obs['HW_ET'] >= 9))]) > 0:
        TOD_obs.loc[ (TOD_obs['TOD'] == 1) & ((TOD_obs['HW_ET'] < 6) | (TOD_obs['HW_ET'] >= 9)),
           'HW_ET'] = st.johnsonsu.rvs(size= len(TOD_obs.loc[(TOD_obs['TOD'] == 1) & ((TOD_obs['HW_ET'] < 6) | (TOD_obs['HW_ET'] >= 9))]), 
                                       a=-0.71, b=1.00, loc=7.12, scale=1.31)

    while len(TOD_obs.loc[(TOD_obs['TOD'] == 2) & ((TOD_obs['HW_ET'] < 9) | (TOD_obs['HW_ET'] >= 15.5))]) > 0:
        TOD_obs.loc[(TOD_obs['TOD'] == 2) & ((TOD_obs['HW_ET'] < 9) | (TOD_obs['HW_ET'] >= 15.5)),
           'HW_ET'] = st.johnsonsu.rvs(size= len(TOD_obs.loc[(TOD_obs['TOD'] == 2) & 
                                                             ((TOD_obs['HW_ET'] < 9) | (TOD_obs['HW_ET'] >= 15.5))]), 
                                       a=-0.71, b=1.00, loc=7.12, scale=1.31)

    while len(TOD_obs.loc[(TOD_obs['TOD'] == 3) & ((TOD_obs['HW_ET'] < 15.5) | (TOD_obs['HW_ET'] >= 18.5))]) > 0:
        TOD_obs.loc[(TOD_obs['TOD'] == 3) & ((TOD_obs['HW_ET'] < 15.5) | (TOD_obs['HW_ET'] >= 18.5)),
           'HW_ET'] = st.johnsonsu.rvs(size= len(TOD_obs.loc[(TOD_obs['TOD'] == 3) & 
                                                             ((TOD_obs['HW_ET'] < 15.5) | (TOD_obs['HW_ET'] >= 18.5))]), 
                                       a=-0.71, b=1.00, loc=7.12, scale=1.31)

    while len(TOD_obs.loc[(TOD_obs['TOD'] == 4) & ((TOD_obs['HW_ET'] < 18.5) | (TOD_obs['HW_ET'] >= 27))]) > 0:
        TOD_obs.loc[(TOD_obs['TOD'] == 4) & ((TOD_obs['HW_ET'] < 18.5) | (TOD_obs['HW_ET'] >= 27)),
           'HW_ET'] = st.johnsonsu.rvs(size= len(TOD_obs.loc[(TOD_obs['TOD'] == 4) & 
                                                             ((TOD_obs['HW_ET'] < 18.5) | (TOD_obs['HW_ET'] >= 27))]), 
                                       a=-0.71, b=1.00, loc=7.12, scale=1.31)

    TOD_obs.loc[ (TOD_obs['HW_ET'] > 24), 'HW_ET'] = TOD_obs['HW_ET'] - 24

    
    while len(TOD_obs.loc[(TOD_obs['dwell_work'] == 1) & (TOD_obs['dwell_exact'] >= 4.5)]) > 0:
        TOD_obs.loc[(TOD_obs['dwell_work'] == 1) & (TOD_obs['dwell_exact'] >= 4.5),
           'dwell_exact'] = st.johnsonsu.rvs(size= len(TOD_obs.loc[(TOD_obs['dwell_work'] == 1) & 
                                                                   (TOD_obs['dwell_exact'] >= 4.5)]), 
                                             a=0.49, b=0.94,loc=9.29, scale=1.26)

    while len(TOD_obs.loc[(TOD_obs['dwell_work'] == 2) & ((TOD_obs['dwell_exact'] < 4.5) | (TOD_obs['dwell_exact'] >= 7.75))]) > 0:
        TOD_obs.loc[(TOD_obs['dwell_work'] == 2) & ((TOD_obs['dwell_exact'] < 4.5) | (TOD_obs['dwell_exact'] >= 7.75)),
           'dwell_exact'] = st.johnsonsu.rvs(size= len(TOD_obs.loc[(TOD_obs['dwell_work'] == 2) & 
                                                               ((TOD_obs['dwell_exact'] < 4.5) | (TOD_obs['dwell_exact'] >= 7.75))]),
                                             a=0.49, b=0.94, loc=9.29, scale=1.26)

    while len(TOD_obs.loc[(TOD_obs['dwell_work'] == 3) & ((TOD_obs['dwell_exact'] < 7.75) | (TOD_obs['dwell_exact'] >= 9.0))]) > 0:
        TOD_obs.loc[(TOD_obs['dwell_work'] == 3) & ((TOD_obs['dwell_exact'] < 7.75) | (TOD_obs['dwell_exact'] >= 9.0)),
           'dwell_exact'] = st.johnsonsu.rvs(size= len(TOD_obs.loc[(TOD_obs['dwell_work'] == 3) & 
                                                               ((TOD_obs['dwell_exact'] < 7.75) | (TOD_obs['dwell_exact'] >= 9.0))]), 
                                             a=0.49, b=0.94,loc=9.29, scale=1.26)

    while len(TOD_obs.loc[(TOD_obs['dwell_work'] == 4) & ((TOD_obs['dwell_exact'] < 9.0) | (TOD_obs['dwell_exact'] >= 10.5))]) > 0:
        TOD_obs.loc[(TOD_obs['dwell_work'] == 4) & ((TOD_obs['dwell_exact'] < 9.0) | (TOD_obs['dwell_exact'] >= 10.5)),
           'dwell_exact'] = st.johnsonsu.rvs(size= len(TOD_obs.loc[(TOD_obs['dwell_work'] == 4) & 
                                                               ((TOD_obs['dwell_exact'] < 9.0) | (TOD_obs['dwell_exact'] >= 10.5))]), 
                                             a=0.49, b=0.94, loc=9.29, scale=1.26)

    while len(TOD_obs.loc[(TOD_obs['dwell_work'] == 5) & ((TOD_obs['dwell_exact'] < 10.5) | (TOD_obs['dwell_exact'] >= 24))]) > 0:
        TOD_obs.loc[(TOD_obs['dwell_work'] == 5) & ((TOD_obs['dwell_exact'] < 10.5) | (TOD_obs['dwell_exact'] >= 24)),
           'dwell_exact'] = st.johnsonsu.rvs(size= len(TOD_obs.loc[(TOD_obs['dwell_work'] == 5) & 
                                                               ((TOD_obs['dwell_exact'] < 10.5) | (TOD_obs['dwell_exact'] >= 24))]), 
                                             a=0.49, b=0.94, loc=9.29, scale=1.26)
    
    TOD_obs['WH_ST'] = TOD_obs['HW_ET'] + TOD_obs['dwell_exact']

    TOD_obs.loc[ (TOD_obs['WH_ST'] > 24), 'WH_ST'] = TOD_obs['WH_ST'] - 24
    
    persons = pd.merge(
        persons, TOD_obs[['HW_ET', 'WH_ST']], how='left',
        left_index=True, right_index=True)
    orca.add_table('persons', persons)


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
