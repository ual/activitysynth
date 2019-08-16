import orca
import pandas as pd
import warnings
import urbansim_templates
import argparse
import s3fs
from datetime import datetime
import os

from activitysynth.scripts import models, datasources, variables


warnings.simplefilter('ignore')


# default runtime args
scenario = None
accessibilities_mode = 'compute'
data_out = '/home/data/spring_2019/base/'
output_store = False
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
    'mtc_skims': 'mtc_skims.{0}',
    'beam_skims_raw': '30.skims-smart-23April2019-baseline.csv.gz',
    'beam_skims_imputed': 'beam_skims_imputed.{0}',
    # the following nodes and edges .csv's will be phased out and
    # replaced by travel model skims entirely
    'drive_nodes': 'drive_nodes.{0}',
    'drive_edges': 'drive_edges.{0}',
    'drive_access_vars': 'drive_net_vars.{0}',
    'walk_nodes': 'walk_nodes.{0}',
    'walk_edges': 'walk_edges.{0}',
    'walk_access_vars': 'walk_net_vars.{0}',
    'zones': 'zones.{0}',
    'zone_access_vars': 'zones_w_access_vars.{0}',
}
output_tables = [
    'parcels', 'buildings', 'jobs', 'persons', 'households',
    'establishments', 'rentals', 'units', 'zones', 'beam_skims_imputed',
    'mtc_skims', 'plans', 'walk_edges', 'walk_nodes', 'drive_edges',
    'drive_nodes']

# default input data dir
input_data_dir = './data/'


def format_fname_dict(formattable_fname_dict, format='csv'):
    formatted_dict = {
        k: v.format('csv')
        for k, v in formattable_fname_dict.items()}
    return formatted_dict


if __name__ == "__main__":

    # parse runtime arguments
    parser = argparse.ArgumentParser(description='Run ActivitySynth models.')
    # parser.add_argument(
    #     "--data-mode", "-d", dest='data_mode', action="store",
    #     help="options: local, remote")
    parser.add_argument(
        '--input-data-dir', '-i', action='store', dest='input_data_dir',
        help='full (pandas-compatible) path to input data directory')

    parser.add_argument(
        "--input-file-format", "-f", dest='input_file_format', action="store",
        help="options: h5, csv, parquet")

    parser.add_argument(
        '-o', action='store_true', dest='output_store',
        help='write output data tables to h5 data store')

    parser.add_argument(
        '-d', action='store_true', dest='data_out',
        help='full filepath for output data')

    parser.add_argument(
        "--access-vars-mode", "-a", help="option: compute, stored",
        action="store", dest='accessibilities_mode')

    options = parser.parse_args()

    if options.input_file_format:
        input_file_format = options.input_file_format
    orca.add_injectable('input_file_format', input_file_format)

    if options.output_store:
        if options.data_out:
            data_out = options.data_out
        if os.path.exists(data_out):
            os.remove(data_out)
    else:
        data_out = None

    if options.accessibilities_mode:
        accessibilities_mode = options.accessibilities_mode

    if options.input_data_dir:
        input_data_dir = options.input_data_dir

    input_data_dir = os.path.abspath(input_data_dir)
    orca.add_injectable('input_data_dir', input_data_dir)
    print('Reading input data from {0}'.format(input_data_dir))

    # h5 inputs not currently operational
    if input_file_format == 'h5':
        @orca.injectable('store', cache=True)
        def hdfstore():
            return pd.HDFStore(
                input_data_dir + "model_data_output.h5", mode='r')
        orca.add_injectable('input_fnames', None)  # h5 mode has no fnames

    # data modes that store data as individual files
    elif input_file_format == 'parquet' or input_file_format == 'csv':
        orca.add_injectable('store', None)
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
        'initialize_imputed_skims'
    ]

    orca.run(model_steps)

    # create and save access vars if not run before
    if accessibilities_mode == 'compute':
        model_steps = [
            'network_aggregations_small',
            'network_aggregations_walk',
            'skims_aggregations']
        orca.run(model_steps)

        orca.get_table('nodeswalk').to_frame().to_csv(
            input_data_dir + input_fnames['walk_access_vars'])
        orca.get_table('nodessmall').to_frame().to_csv(
            input_data_dir + input_fnames['drive_access_vars'])

        # skims aggregations step writes straight to the zones
        # table but we store the updated zones table separately
        orca.get_table('zones').to_frame().to_csv(
            input_data_dir + input_fnames['zone_access_vars'])

    elif accessibilities_mode == 'stored':
        walk_net_vars = pd.read_csv(
            input_data_dir + input_fnames['walk_access_vars'],
            index_col='osmid')
        drive_net_vars = pd.read_csv(
            input_data_dir + input_fnames['drive_access_vars'],
            index_col='osmid')
        zones = pd.read_csv(
            input_data_dir + input_fnames['zone_access_vars'],
            index_col='zone_id', dtype={'zone_id': int})
        orca.add_table('nodeswalk', walk_net_vars)
        orca.add_table('nodessmall', drive_net_vars)

        # if stored replace the existing zones table with the
        # one containing zone-level accessibility variables
        orca.add_table('zones', zones)

    model_steps = [
        'wlcm_simulate', 'TOD_choice_simulate',
        'TOD_distribution_simulate',
        'auto_ownership_simulate', 'primary_mode_choice_simulate',
        'generate_activity_plans', 'SLCM_simulate', 
        'TOD_school_arrival_simulate','TOD_school_departure_simulate',
        'TOD_school_distribution_simulate']

    orca.run(
        model_steps,
        data_out=data_out,
        out_base_tables=[],
        out_base_local=True,
        out_run_tables=output_tables,
        out_run_local=True)
