import orca
import pandas as pd
import warnings
import urbansim_templates
import argparse
import s3fs
from datetime import datetime

from activitysynth.scripts import models, datasources, variables


warnings.simplefilter('ignore')


# default runtime args
year = 2010
scenario = None
data_mode = 'csv'
accessibilities_mode = 'compute'

output_tables = [
    'parcels', 'buildings', 'jobs', 'persons', 'households',
    'establishments', 'rentals', 'units']
io_bucket = 'urbansim-outputs'
beam_bucket = 'urbansim-beam'
write_out = False
output_format = 'csv'
csv_fnames = {
    'parcels': 'parcel_attr.csv',
    'buildings': 'buildings_v2.csv',
    'jobs': 'jobs_v2.csv',
    'establishments': 'establishments_v2.csv',
    'households': 'households_v2.csv',
    'persons': 'persons_v3.csv',
    'rentals': 'MTC_craigslist_listings_7-10-18.csv',
    'units': 'units_v2.csv',
    'skims': 'skims_110118.csv',
    'beam_drive_skims': 'sfbay-smart-base__2019-03-28_14-22-12/' +
            'ITERS/it.30/30.skimsExcerpt.csv',
    'beam_skims': 'sfbay-smart-base__2019-03-28_14-22-12/' +
            'ITERS/it.30/30.skims.csv',
    'drive_nodes': 'bay_area_tertiary_strongly_nodes.csv',
    'drive_edges': 'bay_area_tertiary_strongly_edges.csv',
    'drive_access_vars': 'drive_net_vars.csv',
    'walk_nodes': 'bayarea_walk_nodes.csv',
    'walk_edges': 'bayarea_walk_edges.csv',
    'walk_access_vars': 'walk_net_vars.csv',
    'zones': 'zones.csv',
    'zone_access_vars': 'zones_w_access_vars.csv'
}


def send_output_to_s3(
        output_tables, io_bucket, beam_bucket, year, scenario=None,
        output_format='csv'):

    today_str = datetime.now().strftime("%d%B%Y")

    # save tables to s3
    if output_format == 'csv':
        s3 = s3fs.S3FileSystem(anon=False)

    for table_name in output_tables:
        print(table_name)
        table = orca.get_table(table_name)
        df = table.to_frame(table.local_columns)
        if scenario:
            table_name = scenario + '/' + table_name
        s3_url = 's3://{0}/{1}/{2}/{3}.{4}'.format(
            io_bucket, today_str, year, table_name, output_format)
        if output_format == 'parquet':
            df.to_parquet(s3_url, engine='pyarrow')
        elif output_format == 'csv':
            with s3.open(s3_url, 'w') as f:
                df.to_csv(f)

    # save plans to s3
    table_name = 'activity_plans'
    print(table_name)
    plans = orca.get_table('plans').to_frame()
    if scenario:
        table_name = scenario + '/' + table_name
    s3_url = 's3://{0}/{1}/{2}/{3}.{4}'.format(
        beam_bucket, today_str, year, table_name, output_format)
    if output_format == 'parquet':
        plans.to_parquet(s3_url)
    elif output_format == 'csv':
        with s3.open(s3_url, 'w') as f:
            plans.to_csv(f)


if __name__ == "__main__":

    # parse runtime arguments
    parser = argparse.ArgumentParser(description='Run ActivitySynth models.')
    parser.add_argument(
        "--data-mode", "-d", dest='data_mode', action="store",
        help="options: h5 (local), csv (local), s3 (parquet)")
    parser.add_argument(
        "--year", "-y", help="what simulation year is it?", action="store")
    parser.add_argument(
        '--scenario', '-s', action='store', dest='scenario',
        help='specify which scenario to run')
    parser.add_argument(
        "--access-vars-mode", "-a", help="option: compute, stored",
        action="store", dest='accessibilities_mode')

    options = parser.parse_args()

    if options.year:
        year = options.year
    print("The year is {0}.".format(year))

    local_data_dir = '/home/data/spring_2019/{0}/'.format(str(year))

    if options.scenario:
        scenario = options.scenario
        local_data_dir += '{0}/'.format(scenario)

    if options.data_mode:
        data_mode = options.data_mode

    if options.accessibilities_mode:
        accessibilities_mode = options.accessibilities_mode

    # add orca injectables
    orca.add_injectable('data_mode', data_mode)
    orca.add_injectable('csv_fnames', csv_fnames)

    if data_mode == 'h5':
        @orca.injectable('store', cache=True)
        def hdfstore():
            return pd.HDFStore(
                (local_data_dir + "model_data_output.h5"), mode='r')
        orca.add_injectable('s3_input_data_url', None)
        orca.add_injectable('local_data_dir', None)

    elif data_mode == 's3':
        orca.add_injectable('store', None)
        orca.add_injectable(
            's3_input_data_url',
            's3://{0}/{1}/'.format(io_bucket, year) + '{0}.parquet.gz')
        orca.add_injectable('local_data_dir', None)

    elif data_mode == 'csv':
        orca.add_injectable('store', None)
        orca.add_injectable('s3_input_data_url', None)
        orca.add_injectable('local_data_dir', local_data_dir)

    else:
        raise ValueError(
            'Must specifiy a valid data mode. Valid options '
            'include "csv", "h5", and "s3".')

    # initialize networks
    model_steps = [
        'initialize_network_small',
        'initialize_network_walk',
    ]

    orca.run(model_steps)

    if accessibilities_mode == 'compute':
        model_steps = [
            'network_aggregations_small',
            'network_aggregations_walk',
            'impute_missing_skims',
            'skims_aggregations_drive',
            'skims_aggregations_other']
        orca.run(model_steps)
        orca.get_table('nodeswalk').to_frame().to_csv(
            local_data_dir + csv_fnames['walk_access_vars'])
        orca.get_table('nodessmall').to_frame().to_csv(
            local_data_dir + csv_fnames['drive_access_vars'])
        orca.get_table('zones').to_frame().to_csv(
            local_data_dir + csv_fnames['zone_access_vars'])

    elif accessibilities_mode == 'stored':
        walk_net_vars = pd.read_csv(
            local_data_dir + csv_fnames['walk_access_vars'],
            index_col='osmid')
        drive_net_vars = pd.read_csv(
            local_data_dir + csv_fnames['drive_access_vars'],
            index_col='osmid')
        zones = pd.read_csv(
            local_data_dir + csv_fnames['zone_access_vars'],
            index_col='zone_id', dtype={'zone_id': int})
        orca.add_table('nodeswalk', walk_net_vars)
        orca.add_table('nodessmall', drive_net_vars)
        orca.add_table('zones', zones)

    model_steps = [
        'wlcm_simulate', 'TOD_choice_simulate',
        'TOD_distribution_simulate',
        'auto_ownership_simulate', 'primary_mode_choice_simulate',
        'generate_activity_plans']

    orca.run(model_steps)

    if write_out:
        if data_mode == 's3' or data_mode == 'csv':
            send_output_to_s3(
                output_tables, io_bucket, beam_bucket, year, scenario,
                output_format)
