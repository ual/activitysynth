import orca
import pandas as pd
import warnings
import urbansim_templates
import argparse
import boto3

from activitysynth.scripts import models, datasources, variables


warnings.simplefilter('ignore')
accessibilities_mode = 'compute'
year = 2010
data_mode = 'csv'
output_tables = [
    'parcels', 'buildings', 'jobs', 'persons', 'households',
    'establishments', 'rentals', 'units']
output_bucket = 'urbansim-outputs'
beam_bucket = 'urbansim-beam'
local_data_dir = './data/'
fname_walk = 'walk_net_vars.csv'
fname_drive = 'drive_net_vars.csv'


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Run ActivitySynth models.')
    parser.add_argument(
        "--data-mode", "-d", dest='data_mode', action="store",
        help="options: h5 (local), csv (local), s3 (parquet)")
    parser.add_argument(
        "--year", "-y", help="what simulation year is it?", action="store")
    parser.add_argument(
        "--access-vars-mode", "-a", help="option: compute, stored",
        action="store", dest='accessibilities_mode')

    options = parser.parse_args()

    if options.year:
        year = options.year
        print("The year is {0}.".format(year))

    if options.data_mode:
        data_mode = options.data_mode

    if options.accessibilities_mode:
        accessibilities_mode = options.accessibilities_mode

    orca.add_injectable('data_mode', data_mode)

    if data_mode == 'h5':
        @orca.injectable('store', cache=True)
        def hdfstore():
            return pd.HDFStore(
                (local_data_dir + "model_data.h5"), mode='r')
        orca.add_injectable('s3_input_data_url', None)
        orca.add_injectable('local_data_dir', None)

    elif data_mode == 's3':
        orca.add_injectable('store', None)
        orca.add_injectable(
            's3_input_data_url',
            's3://urbansim-baseyear-inputs/{0}.parquet.gz')
        orca.add_injectable('local_data_dir', None)

    elif data_mode == 'csv':
        orca.add_injectable('store', None)
        orca.add_injectable('s3_input_data_url', None)
        orca.add_injectable('local_data_dir', local_data_dir)

    else:
        raise ValueError(
            'Must specifiy a valid data mode. Valid options '
            'include "csv", "h5", and "s3".')

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
            local_data_dir + fname_walk)
        orca.get_table('nodessmall').to_frame().to_csv(
            local_data_dir + fname_drive)

    elif accessibilities_mode == 'stored':
        walk_net_vars = pd.read_csv(
            local_data_dir + fname_walk, index_col='osmid')
        drive_net_vars = pd.read_csv(
            local_data_dir + fname_drive, index_col='osmid')
        orca.add_table('nodeswalk', walk_net_vars)
        orca.add_table('nodessmall', drive_net_vars)

    model_steps = [
        'wlcm_simulate', 'TOD_choice_simulate',
        'TOD_distribution_simulate',
        'auto_ownership_simulate', 'primary_mode_choice_simulate',
        'generate_activity_plans']

    orca.run(model_steps)

    if data_mode == 's3':

        bucket_name = 's3://{0}/{1}'.format(output_bucket, year)
        s3 = boto3.resource('s3')

        # save tables to parquet on s3
        for table_name in output_tables:
            df = orca.get_table(table_name).to_frame()
            s3_url = 's3://{0}/{1}/{2}.parquet.gz'.format(
                output_bucket, year, table_name)

            df.to_parquet(s3_url, compression='gzip', engine='pyarrow')

        # save plans to parquet s3
        plans = orca.get_table('plans').to_frame()
        s3_url = 's3://{0}/{1}/{2}.parquet.gz'.format(
            beam_bucket, year, 'activity_plans')
        plans.to_parquet(s3_url, compression='gzip')
