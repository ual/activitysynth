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
            df = impute_missing_skims(mtc_skims, raw_skims)
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

    m = mm.get_step('WLCM_gen_tt')

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

    trips = orca.get_table("trips").to_frame
    n_by_TOD = (trips.groupby("TOD")
                     .size()
                     .reset_index(name='ntrips')
                   )

    n_by_TOD = n_by_TOD.join(ST_kde, on="TOD")

    n_by_TOD["ST"] = n_by_TOD.apply(lambda row: row['kde'].resample(row['ntrips']), axis=1)

    n_by_TOD["HW_trip_ST"] = n_by_TOD["ST"].apply(lambda x: x[0, ])
    n_by_TOD["WH_trip_ST"] = n_by_TOD["ST"].apply(lambda x: x[1, ])
    #ST_kde.kde.apply(lambda x: x.resample(2))

    samples = pd.DataFrame({'TOD':n_by_TOD.TOD.repeat(n_by_TOD.HW_trip_ST.str.len()),
                  'HW_trip_ST':np.concatenate(n_by_TOD.HW_trip_ST.values),
                  'WH_trip_ST':np.concatenate(n_by_TOD.WH_trip_ST.values),
                 })
    samples = samples.reset_index()
    #print(samples.index)

    #trips_sim = trips_sim.set_index("TOD")
    #n_by_TOD.ST.values[0, ][0, ].shape
    #n_by_TOD["HW_trip_ST"].apply(lambda x: x.shape)

    # handle 24+ hour
    samples.loc[samples["TOD"].isin([14.0, 24.0]) & (samples.WH_trip_ST < 12), 'WH_trip_ST'] -= 24

    samples_w_bounds = samples.join(ST_kde, how="left", on="TOD")
    samples_w_bounds.reset_index()

    samples_good = (samples_w_bounds.query('(HW_trip_ST_min <= HW_trip_ST <= HW_trip_ST_max) and ' +
                                           '(WH_trip_ST_min <= WH_trip_ST <= WH_trip_ST_max )')
                                    .loc[:, ["TOD", "HW_trip_ST", "WH_trip_ST"]]
                   )

    _n_by_TOD = (samples.loc[~samples.index.isin(samples_good.index)]
                        .groupby("TOD")
                        .size()
                        .reset_index(name='ntrips'))

    #print(samples.index)
    #print(samples_good.index)
    print("There are {} samples with out of bounds ST".format(_n_by_TOD.ntrips.sum()))
    if _n_by_TOD.shape[0] > 0:
        samples_extra = simulate(_n_by_TOD, ST_kde)
        samples = pd.concat([samples_good, samples_extra], sort=False)

    persons = pd.concat(persons.sort_values("TOD"),
                samples.sort_values("TOD"),
                axis=1)

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
