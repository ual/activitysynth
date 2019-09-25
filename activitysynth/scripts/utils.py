import orca
import pandas as pd
import numpy as np
import math
from urbansim.utils import misc


def register_skim_access_variable(
        column_name, variable_to_summarize, impedance_measure,
        distance, skims_table, agg=np.sum, log=False):
    """
    Register skim-based accessibility variable with orca.
    Parameters
    ----------
    column_name : str
        Name of the orca column to register this variable as.
    impedance_measure : str
        Name of the skims column to use to measure inter-zone impedance.
    variable_to_summarize : str
        Name of the zonal variable to summarize.
    distance : int
        Distance to query in the skims (e.g. 30 minutes travel time).
    mode_name: str
        Name of the mode to query in the skims.
    period: str
        Period (AM, PM, OffPeak) to query in the skims.

    Returns
    -------
    column_func : function
    """
    @orca.column('zones', column_name, cache=True, cache_scope='iteration')
    def column_func(zones):
        df = skims_table.to_frame()
        results = misc.compute_range(
            df, zones.get_column(variable_to_summarize),
            impedance_measure, distance, agg=agg)

        if len(results) < len(zones):
            results = results.reindex(zones.index).fillna(0)

        # add vars from orig zone, typically not included in skims
        results = results + zones[variable_to_summarize]

        if log:
            results = results.apply(eval('np.log1p'))

        return results

    return


def impute_missing_skims(mtc_skims, beam_skims_raw):
    df = beam_skims_raw.to_frame()

    # seconds to minutes
    df['gen_tt'] = df['generalizedTimeInS'] / 60

    mtc = mtc_skims.to_frame(columns=['orig', 'dest', 'da_distance_AM'])
    mtc.rename(
        columns={'orig': 'from_zone_id', 'dest': 'to_zone_id'},
        inplace=True)
    mtc.set_index(['from_zone_id', 'to_zone_id'], inplace=True)

    # miles to meters
    mtc['dist'] = mtc['da_distance_AM'] * 1609.34

    # create morning peak lookup
    df['gen_time_per_m'] = df['gen_tt'] / df['distanceInM']
    df['gen_cost_per_m'] = df['gen_cost'] / df['distanceInM']
    df.loc[df['hour'].isin([7, 8, 9]), 'period'] = 'AM'
    df_am = df[df['period'] == 'AM']
    df_am = df_am.replace([np.inf, -np.inf], np.nan)
    am_lookup = df_am[[
        'mode', 'gen_time_per_m', 'gen_cost_per_m']].dropna().groupby(
            ['mode']).mean().reset_index()

    # morning averages
    df_am_avg = df_am[[
        'from_zone_id', 'to_zone_id', 'mode', 'gen_tt',
        'gen_cost']].groupby(
        ['from_zone_id', 'to_zone_id', 'mode']).mean().reset_index()

    # long to wide
    df_am_pivot = df_am_avg.pivot_table(
        index=['from_zone_id', 'to_zone_id'], columns='mode')
    df_am_pivot.columns = ['_'.join(col) for col in df_am_pivot.columns.values]

    # combine with mtc-based dists
    merged = pd.merge(
        mtc[['dist']], df_am_pivot, left_index=True, right_index=True,
        how='left')

    # impute
    for mode in am_lookup['mode'].values:
        for impedance in ['gen_tt', 'gen_cost']:
            if impedance == 'gen_tt':
                lookup_col = 'gen_time_per_m'
            elif impedance == 'gen_cost':
                lookup_col = 'gen_cost_per_m'
            colname = impedance + '_' + mode
            lookup_val = am_lookup.loc[
                am_lookup['mode'] == mode, lookup_col].values[0]
            merged.loc[pd.isnull(merged[colname]), colname] = merged.loc[
                pd.isnull(merged[colname]), 'dist'] * lookup_val

    assert len(merged) == 2114116

    return merged


@orca.injectable( cache=True)
def age_filter(SL_long_format, schools, students):
    
    #Converting orca tables to dataframes
    df = SL_long_format.to_frame()
    sc = schools.to_frame()    
    st = students.to_frame()    
    
    # School availability matrix 
    sa_matrix = df.merge(sc.loc[:,sc.columns.str.startswith("grade_")], 
                         how = 'left', left_on = 'school_id', right_index=True)
    
    sa_matrix = np.array(sa_matrix.loc[:,sa_matrix.columns.str.startswith("grade_")])

    # Age matrix 
    age = st.age.replace(18, 17)   
    
    df = np.array(pd.get_dummies(age))
    index = pd.Series(np.nonzero(df)[1] - 1).replace(-1,0)
    df[np.arange(0,len(index)),index] = 1
    
    
#     sa_matrix = orca.get_injectable('sa_matrix')
#     age_matrix = orca.get_injectable('age_matrix')
    
    col = (sa_matrix.reshape(1109985,1,150,13) * df.reshape(1109985, 1, 1,13)).sum(-1)
    filter_1 = pd.Series(col.flatten()).replace(2,1).astype(bool)
    return filter_1

@orca.injectable(cache = True)
def school_mode_choice_table():
    
    #Define bradcasting
    orca.broadcast(cast = 'household', onto = 'persons', cast_index = True, onto_on= 'household_id')
    orca.broadcast(cast = 'zones', onto = 'persons', cast_index= True, onto_on = 'zone_id_home')
    
    #Merging tables 
    df = orca.merge_tables(target = 'persons', tables = ['households', 'zones', 'persons'])
    
    df = pd.merge(df, orca.get_table('beam_skims_imputed').to_frame().reset_index(), how = 'left', 
              left_on = ['zone_id_home', 'zone_id_school'], 
              right_on = ['from_zone_id', 'to_zone_id'])
    
    list_var = ['sex_SMC', 'tenure_SMC', 'recent_mover', 'hispanic_head_SMC', 'income_1',
       'race_2+races', 'race_african_american', 'race_asian',
       'race_hawaii/pacific', 'race_indian/alaska', 'race_other', 'race_white',
       'race_head_2+races', 'race_head_african_american', 'race_head_asian',
       'race_head_hawaii/pacific', 'race_head_indian/alaska',
       'race_head_other', 'race_head_white', 'age', 'persons', 'cars',
       'workers', 'children', 'age_of_head', 'HS_ET',
       'sum_residential_units_gen_tt_WALK_TRANSIT_15',
       'total_jobs_gen_tt_WALK_TRANSIT_45',
       'avg_income_gen_tt_WALK_TRANSIT_30', 'sum_residential_units',
       'sum_persons_gen_tt_WALK_TRANSIT_15',
       'sum_income_gen_tt_WALK_TRANSIT_15', 'sum_income_gen_tt_CAR_45',
       'sum_persons_gen_tt_CAR_15', 'avg_income', 'total_jobs', 'sum_income',
       'sum_persons_gen_tt_CAR_45', 'sum_income_gen_tt_WALK_TRANSIT_45',
       'sum_residential_units_gen_tt_WALK_TRANSIT_45',
       'avg_income_gen_tt_CAR_30', 'sum_residential_units_gen_tt_CAR_15',
       'sum_persons_gen_tt_WALK_TRANSIT_45', 'total_jobs_gen_tt_CAR_45',
       'total_jobs_gen_tt_CAR_15', 'sum_persons',
       'sum_residential_units_gen_tt_CAR_45', 'sum_income_gen_tt_CAR_15',
       'total_jobs_gen_tt_WALK_TRANSIT_15', 'gen_tt_CAR',
       'gen_tt_DRIVE_TRANSIT', 'gen_cost_WALK_TRANSIT',
       'gen_tt_RIDE_HAIL_TRANSIT', 'gen_cost_RIDE_HAIL_TRANSIT',
       'gen_cost_CAR', 'gen_cost_DRIVE_TRANSIT', 'gen_cost_WALK', 'dist',
       'gen_cost_RIDE_HAIL', 'gen_tt_RIDE_HAIL', 'gen_cost_BIKE',
       'gen_tt_WALK', 'gen_cost_RIDE_HAIL_POOLED', 'gen_tt_WALK_TRANSIT',
       'gen_tt_BIKE', 'gen_tt_RIDE_HAIL_POOLED'] #List of variables in the same order as in model estimation
    
    students = df[(df.age> 4) & (df.age <=18) & (df.student == 1)][list_var]
    
    return students
