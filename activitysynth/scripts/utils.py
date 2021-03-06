import orca
import pandas as pd
import numpy as np
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

    # impute mtc zone-to-zone distances where zero-valued in beam skims
    if len(df.loc[df['distanceInM'] == 0, 'distanceInM']) > 0:
        df.loc[df['distanceInM'] == 0, 'distanceInM'] = mtc.loc[
            pd.MultiIndex.from_frame(df.loc[df['distanceInM'] == 0, [
                'from_zone_id', 'to_zone_id']]), 'dist'].values

    # use MTC dists for all intra-taz distances
    intra_taz_mask = df['from_zone_id'] == df['to_zone_id']
    df.loc[intra_taz_mask, 'distanceInM'] = mtc.loc[pd.MultiIndex.from_frame(
        df.loc[intra_taz_mask, ['from_zone_id', 'to_zone_id']]), 'dist'].values


    # create morning peak lookup
    df['gen_time_per_m'] = df['gen_tt'] / df['distanceInM']
    df['gen_cost_per_m'] = df['gen_cost'] / df['distanceInM']
    df.loc[df['hour'].isin([7, 8, 9]), 'period'] = 'AM'
    df_am = df[df['period'] == 'AM']
    df_am = df_am.replace([np.inf, -np.inf], np.nan)
    df_am = df_am.loc[df_am.index.repeat(df_am.numObservations)]  # weighted
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
