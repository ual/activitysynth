import orca
import pandana as pdna
import pandas as pd

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
    
    # new variables
    print('compute additional aggregation variables')
    nodessmall['pop_jobs_ratio_10000'] = (nodessmall['pop_10000'] / (nodessmall['jobs_10000'])).fillna(0)
    nodessmall['pop_jobs_ratio_25000'] = (nodessmall['pop_25000'] / (nodessmall['jobs_25000'])).fillna(0)
    # end of addition
    
    print(nodessmall.describe())
    orca.add_table('nodessmall', nodessmall)


@orca.step()
def network_aggregations_walk(netwalk):
    """
    This will be turned into a network aggregation template.

    """

    nodeswalk = networks.from_yaml(netwalk, 'network_aggregations_walk.yaml')
    nodeswalk = nodeswalk.fillna(0)
    
    # new variables
    print('compute additional aggregation variables')
    nodeswalk['prop_children_500_walk'] = ((nodeswalk['children_500_walk'] > 0).astype(int) / nodeswalk['hh_500_walk']).fillna(0)
    nodeswalk['prop_singles_500_walk'] = (nodeswalk['singles_500_walk'] / nodeswalk['hh_500_walk']).fillna(0)
    nodeswalk['prop_elderly_500_walk'] = (nodeswalk['elderly_hh_500_walk'] / nodeswalk['hh_500_walk']).fillna(0)
    nodeswalk['prop_black_500_walk'] = (nodeswalk['pop_black_500_walk'] / nodeswalk['pop_500_walk']).fillna(0)
    nodeswalk['prop_white_500_walk'] = (nodeswalk['pop_white_500_walk'] / nodeswalk['pop_500_walk']).fillna(0)
    nodeswalk['prop_asian_500_walk'] = (nodeswalk['pop_asian_500_walk'] / nodeswalk['pop_500_walk']).fillna(0)
    nodeswalk['prop_hisp_500_walk'] = (nodeswalk['pop_hisp_500_walk'] / nodeswalk['pop_500_walk']).fillna(0)
    nodeswalk['prop_rich_500_walk'] = (nodeswalk['rich_500_walk'] / nodeswalk['pop_500_walk']).fillna(0)
    nodeswalk['prop_poor_500_walk'] = (nodeswalk['poor_500_walk'] / nodeswalk['pop_500_walk']).fillna(0)

    nodeswalk['prop_children_1500_walk'] = ((nodeswalk['children_1500_walk'] > 0).astype(int)/nodeswalk['hh_1500_walk']).fillna(0)
    nodeswalk['prop_singles_1500_walk'] = (nodeswalk['singles_1500_walk'] / nodeswalk['hh_1500_walk']).fillna(0)
    nodeswalk['prop_elderly_1500_walk'] = (nodeswalk['elderly_hh_1500_walk'] / nodeswalk['hh_1500_walk']).fillna(0)
    nodeswalk['prop_black_1500_walk'] = (nodeswalk['pop_black_1500_walk'] / nodeswalk['pop_1500_walk']).fillna(0)
    nodeswalk['prop_white_1500_walk'] = (nodeswalk['pop_white_1500_walk'] / nodeswalk['pop_1500_walk']).fillna(0)
    nodeswalk['prop_asian_1500_walk'] = (nodeswalk['pop_asian_1500_walk'] / nodeswalk['pop_1500_walk']).fillna(0)
    nodeswalk['prop_hisp_1500_walk'] = (nodeswalk['pop_hisp_1500_walk'] / nodeswalk['pop_1500_walk']).fillna(0)
    nodeswalk['prop_rich_1500_walk'] = (nodeswalk['rich_1500_walk'] / nodeswalk['pop_1500_walk']).fillna(0)
    nodeswalk['prop_poor_1500_walk'] = (nodeswalk['poor_1500_walk'] / nodeswalk['pop_1500_walk']).fillna(0)

    nodeswalk['pop_jobs_ratio_1500_walk'] = (nodeswalk['pop_1500_walk'] / (nodeswalk['jobs_500_walk'])).fillna(0)
    nodeswalk['avg_hhs_500_walk'] = (nodeswalk['pop_500_walk'] / (nodeswalk['hh_500_walk'])).fillna(0)
    nodeswalk['avg_hhs_1500_walk'] = (nodeswalk['pop_1500_walk'] / (nodeswalk['hh_1500_walk'])).fillna(0)
    # end of addition
    
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
def auto_ownership_simulate(households):
    """
    Generate auto ownership choices for the synthetic pop households. The categories are:
    - 0: no vehicle
    - 1: one vehicle
    - 2: two vehicles
    - 3: three or more vehicles
    """
    
    
    # income bin dummies
    income_bins = pd.cut(orca.get_table('households').to_frame().income,
      bins = [0,20000,40000,60000,80000,100000,120000,np.inf],
      labels = ['2','4','6','8','10','12','12p'],include_lowest = True)

    income_bin_dummies = pd.get_dummies(income_bins,prefix = 'income')

    for i in income_bin_dummies.columns:
        orca.add_column('households',i,income_bin_dummies[i])
    
    
    # load UrbanAccess transit accessibility variables
    parcels = orca.get_table('parcels').to_frame()
    am_acc = pd.read_csv('./data/urbanaccess_transit/access_indicators_ampeak.csv',dtype = {'block_id':str})
    am_acc.block_id = am_acc.block_id.str.zfill(15)
    parcels_with_acc = parcels.merge(am_acc, how='left', on='block_id').reindex(index = parcels.index) # reorder to align with parcels table
    
    for acc_col in set(parcels_with_acc.columns) - set(parcels):
        # fill NA with median value
        orca.add_column('parcels',acc_col,
         parcels_with_acc[acc_col].fillna(parcels_with_acc[acc_col].median())
                   )
    
    @orca.table(cache=False)
    def hh_merged():
        df = orca.merge_tables(target = 'households',tables = ['households','units','buildings','parcels'
                                                          ,'nodessmall','nodeswalk'])
        return df
    
    m = mm.get_step('auto_ownership')
    
    # remove filters, specify out table, out column
    
    m.filters = None
    m.out_table = 'households'
    m.out_column = 'cars_alt'
    
    m.run()
    
    
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
    @orca.table()
    def persons_CHTS_format():
    # use persons with jobs for persons
        persons_with_job = pd.read_csv('./data/persons_w_jobs.csv')
        persons_with_TOD = orca.get_table('persons').to_frame().reset_index()[['person_id','TOD']] # The TOD column does not yet exist, and will depend on Emma's TOD choice model

        hh_df = orca.get_table('households').to_frame().reset_index()[['household_id','cars','tenure','income','persons','building_id']]
        jobs_df = orca.get_table('jobs').to_frame().reset_index()[['job_id','building_id']]
        buildings_df = orca.get_table('buildings').to_frame().reset_index()[['building_id','parcel_id']]
        parcels_df = orca.get_table('parcels').to_frame().reset_index()[['primary_id','zone_id']]
        parcels_df.rename(columns = {'primary_id':'parcel_id'}, inplace = True)

        persons_with_job = persons_with_job[['person_id','sex','age','race_id','worker','edu','household_id','job_id']]

        # merge time of day
        persons_with_job= persons_with_job.merge(persons_with_TOD, how = 'left', on = 'person_id')

        # rename columns/change values to match CHTS
        persons_with_job.columns = ['person_id','GEND','AGE','RACE1','JOBS','EDUCA','household_id','job_id','TOD']
        persons_with_job.RACE1 = persons_with_job.RACE1.map({1:1,2:2,3:3,4:3,5:3,6:4,7:5,8:97,9:97})
        persons_with_job.EDUCA = persons_with_job.EDUCA.map({0:1,1:1,2:1,3:1,4:1,5:1,6:1,7:1,8:1,9:1,
                                                            10:1,11:1,12:1,13:1,14:1,15:1,16:2,17:2,18:3,19:3,
                                                            20:4,21:5,22:6,23:6,24:6})
        persons_with_job.TOD = persons_with_job.TOD.map({2:'EA',3:'EA',12:'AM',14:'AM',22:'MD',23:'MD',24:'MD'})

        # read skim
        skim = pd.read_csv('/home/emma/ual_model_workspace/fall-2018-models/skims_110118.csv',index_col = 0)
        
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

        persons_with_job = persons_with_job.merge(hh_df, how = 'left', on = 'household_id')
        persons_with_job.drop(['building_id','parcel_id'],axis = 1,inplace = True)

        persons_with_job = persons_with_job.merge(jobs_df, how = 'inner',on = 'job_id')
        persons_with_job.drop(['building_id','parcel_id'],axis = 1,inplace = True)


        persons_with_job = persons_with_job.merge(MTC_acc, how = 'left',left_on = 'orig', right_on = 'taz1454')
        persons_with_job[MTC_acc.columns] = persons_with_job[MTC_acc.columns].fillna(0)

        persons_with_job = persons_with_job.merge(skim_combined, how = 'left', on = ['orig','dest','TOD'])

        
        # rename the remaning attributes
        persons_with_job['OWN'] = (persons_with_job['tenure']==1).astype(int)
        persons_with_job.rename(columns = {'cars':'HHVEH','income':'INCOM','persons':'HHSIZ'},inplace = True)
        return persons_with_job
    
    
    m = mm.get_step('primary_mode_choice')
    
    # remove filters, specify out table, out column
    
    m.filters = None
    m.tables = ['persons_CHTS_format']
    m.out_table = 'persons_CHTS_format'
    m.out_column = 'primary_commute_mode'
    
    m.run()
    
    
    
    
    