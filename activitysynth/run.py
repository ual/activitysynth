import orca
import pandas as pd
import warnings

import urbansim_templates
from activitysynth.scripts import models, datasources, variables

warnings.simplefilter('ignore')
accessibilities_mode = 'compute'
year = 2010
output_tables = [
    'parcels', 'buildings', 'jobs', 'persons', 'households',
    'establishments', 'rentals', 'units']
output_bucket = 'urbansim-outputs'
beam_bucket = 'urbansim-beam'
asynth_data_dir = './data/'
fname_walk = 'walk_net_vars.csv'
fname_drive = 'drive_net_vars.csv'


if __name__ == "__main__":

    # initialize network
    model_steps = [
        'initialize_network_small',
        'initialize_network_walk'
    ]

    orca.run(model_steps)

    if accessibilities_mode == 'compute':
        model_steps = [
            'network_aggregations_small', 'network_aggregations_walk']
        orca.run(model_steps)
        orca.get_table('nodeswalk').to_frame().to_csv(
            asynth_data_dir + fname_walk)
        orca.get_table('nodessmall').to_frame().to_csv(
            asynth_data_dir + fname_drive)

    elif accessibilities_mode == 'precomputed':
        walk_net_vars = pd.read_csv(
            asynth_data_dir + fname_walk, index_col='osmid')
        drive_net_vars = pd.read_csv(
            asynth_data_dir + fname_drive, index_col='osmid')
        orca.add_table('nodeswalk', walk_net_vars)
        orca.add_table('nodessmall', drive_net_vars)

    model_steps = [
        'wlcm_simulate', 'TOD_choice_simulate',
        'TOD_distribution_simulate',
        'auto_ownership_simulate', 'primary_mode_choice_simulate',
        'generate_activity_plans']

    orca.run(model_steps)

    # save tables to parquet on s3
    # for table_name in output_tables:
    #     df = orca.get_table(table_name).to_frame()
    #     s3_url = 's3://{0}/{1}/{2}.parquet.gz'.format(
    #         output_bucket, year, table_name)
    #     df.to_parquet(s3_url, compression='gzip', engine='pyarrow')

    # # save plans to parquet s3
    # plans = orca.get_table('plans').to_frame()
    # s3_url = 's3://{0}/{1}/{2}.parquet.gz'.format(
    #     beam_bucket, year, 'activity_plans')
    # plans.to_parquet(s3_url, compression='gzip')
