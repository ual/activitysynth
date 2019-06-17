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
accessibilities_mode = 'compute'
data_mode = 'local'
io_bucket = 'urbansim-outputs'
beam_bucket = 'urbansim-beam'
write_to_s3 = False
output_file_format = 'csv'
input_file_format = 'csv'
formattable_fname_dict = {
    'parcels': 'parcels.{0}',
    'buildings': 'buildings.{0}',
    'jobs': 'jobs.{0}',
    'establishments': 'establishments.{0}',
    'households': 'households.{0}',
    'persons': 'persons.{0}',
    'rentals': 'craigslist.{0}',
    'units': 'units.{0}',
    'skims': 'skims.{0}',
    'beam_skims': '30.skims-smart-23April2019-baseline.csv.gz',
    'drive_nodes': 'bay_area_tertiary_strongly_nodes.{0}',
    'drive_edges': 'bay_area_tertiary_strongly_edges.{0}',
    'drive_access_vars': 'drive_net_vars.{0}',
    'walk_nodes': 'bayarea_walk_nodes.{0}',
    'walk_edges': 'bayarea_walk_edges.{0}',
    'walk_access_vars': 'walk_net_vars.{0}',
    'zones': 'zones.{0}',
    'zone_access_vars': 'zones_w_access_vars.{0}',
}
output_tables = [
    'parcels', 'buildings', 'jobs', 'persons', 'households',
    'establishments', 'rentals', 'units']
temp_data_dir = './data/'
local_data_dir = '/home/data/spring_2019/'


def format_fname_dict(formattable_fname_dict, format='csv'):
    formatted_dict = {
        k: v.format('csv')
        for k, v in formattable_fname_dict.items()}
    return formatted_dict


def format_s3_url(
        bucket_name, today_str, year, table_name, output_file_format):
    s3_url = 's3://{0}/{1}/{2}/{3}.{4}'.format(
        bucket_name, today_str, year, table_name, output_file_format)
    return s3_url


def send_output_to_s3(
        output_tables, io_bucket, beam_bucket, year, scenario,
        output_file_format):

    today_str = datetime.now().strftime("%d%B%Y")

    # save urbansim tables to s3
    s3 = s3fs.S3FileSystem(anon=False)

    for table_name in output_tables:
        print(table_name)
        table = orca.get_table(table_name)
        df = table.to_frame(table.local_columns)

        if scenario:
            table_name = scenario + '/' + table_name

        s3_url = format_s3_url(
            io_bucket, today_str, year, table_name, output_file_format)

        with s3.open(s3_url, 'w') as f:
            if output_file_format == 'parquet':
                df.to_parquet(f, engine='pyarrow')
            elif output_file_format == 'csv':
                df.to_csv(f)

    # save activity plans to s3
    table_name = 'activity_plans'
    print(table_name)
    plans = orca.get_table('plans').to_frame()

    if scenario:
        table_name = scenario + '/' + table_name

    s3_url = format_s3_url(
        beam_bucket, today_str, year, table_name, output_file_format)

    with s3.open(s3_url, 'w') as f:
        if output_file_format == 'parquet':
            plans.to_parquet(f)
        elif output_file_format == 'csv':
            plans.to_csv(f)


if __name__ == "__main__":

    # parse runtime arguments
    parser = argparse.ArgumentParser(description='Run ActivitySynth models.')
    parser.add_argument(
        "--data-mode", "-d", dest='data_mode', action="store",
        help="options: local, remote")
    parser.add_argument(
        "--input-file-format", "-i", dest='input_file_format', action="store",
        help="options: h5, csv, parquet")
    parser.add_argument(
        "--ouput-file-format", "-o", dest='output_file_format', action="store",
        help="options: h5, csv, parquet")
    parser.add_argument(
        "--write-to-s3", "-w", dest='write_to_s3', action="store_true",
        help="options: h5, csv, parquet")
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

    if options.scenario:
        scenario = options.scenario

    if options.data_mode:
        data_mode = options.data_mode

    if options.input_file_format:
        input_file_format = options.input_file_format
    orca.add_injectable('input_file_format', input_file_format)

    if options.output_file_format:
        output_file_format = options.output_file_format

    if options.write_to_s3:
        write_to_s3 = options.write_to_s3

    if options.accessibilities_mode:
        accessibilities_mode = options.accessibilities_mode

    if data_mode == 'local':
        input_data_dir = local_data_dir
    elif data_mode == 's3':
        input_data_dir = 's3://{0}/'.format(io_bucket)

    input_data_dir = input_data_dir + '{0}/'.format(str(year))

    if scenario:
        input_data_dir += '{0}/'.format(scenario)

    orca.add_injectable('input_data_dir', input_data_dir)

    # h5 inputs not currently operational
    if input_file_format == 'h5':
        @orca.injectable('store', cache=True)
        def hdfstore():
            return pd.HDFStore(
                input_data_dir + "model_data_output.h5", mode='r')
        orca.add_injectable('input_fnames', None)  # h5 mode has no fnames

    elif input_file_format == 'parquet' or input_file_format == 'csv':
        orca.add_injectable('store', None)  # these modes have no data store
        input_fnames = format_fname_dict(
            formattable_fname_dict, input_file_format)
        orca.add_injectable('input_fnames', input_fnames)

    else:
        raise ValueError(
            'Must specifiy a valid input file format. Valid options '
            'include "csv", "h5", and "parquet".')

    # initialize networks
    model_steps = [
        'initialize_network_small',
        'initialize_network_walk',
        'impute_missing_skims'
    ]

    orca.run(model_steps)

    if accessibilities_mode == 'compute':
        model_steps = [
            'network_aggregations_small',
            'network_aggregations_walk',
            'skims_aggregations']
        orca.run(model_steps)

        # save computed access vars to temp data dir
        orca.get_table('nodeswalk').to_frame().to_csv(
            temp_data_dir + input_fnames['walk_access_vars'])
        orca.get_table('nodessmall').to_frame().to_csv(
            temp_data_dir + input_fnames['drive_access_vars'])
        orca.get_table('zones').to_frame().to_csv(
            temp_data_dir + input_fnames['zone_access_vars'])

    elif accessibilities_mode == 'stored':
        walk_net_vars = pd.read_csv(
            temp_data_dir + input_fnames['walk_access_vars'],
            index_col='osmid')
        drive_net_vars = pd.read_csv(
            temp_data_dir + input_fnames['drive_access_vars'],
            index_col='osmid')
        zones = pd.read_csv(
            temp_data_dir + input_fnames['zone_access_vars'],
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

    if write_to_s3:
        send_output_to_s3(
            output_tables, io_bucket, beam_bucket, year, scenario,
            output_file_format)
