import orca
import pandas as pd
import numpy as np
import os
# data documentation: https://berkeley.app.box.com/notes/282712547032


@orca.table('parcels', cache=True)
def parcels(input_file_format, input_data_dir, store, input_fnames):
    if input_file_format == 'parquet':
        df = pd.read_parquet(
            os.path.join(input_data_dir, input_fnames['parcels']))
    elif input_file_format == 'h5':
        df = store['parcels']
    elif input_file_format == 'csv':
        try:
            df = pd.read_csv(
                os.path.join(input_data_dir, input_fnames['parcels']),
                index_col='parcel_id', dtype={
                    'parcel_id': int, 'block_id': str, 'apn': str})
        except ValueError:
            df = pd.read_csv(
                os.path.join(input_data_dir, input_fnames['parcels']),
                index_col='primary_id', dtype={
                    'primary_id': int, 'block_id': str, 'apn': str})
    return df


@orca.table('buildings', cache=True)
def buildings(input_file_format, input_data_dir, store, input_fnames):
    if input_file_format == 'parquet':
        df = pd.read_parquet(
            os.path.join(input_data_dir, input_fnames['buildings']))
    elif input_file_format == 'h5':
        df = store['buildings']
    elif input_file_format == 'csv':
        df = pd.read_csv(
            os.path.join(input_data_dir, input_fnames['buildings']),
            index_col='building_id', dtype={
                'building_id': int, 'parcel_id': int})
    df['res_sqft_per_unit'] = df[
        'residential_sqft'] / df['residential_units']
    df['res_sqft_per_unit'][df['res_sqft_per_unit'] == np.inf] = 0
    return df


@orca.table('jobs', cache=True)
def jobs(input_file_format, input_data_dir, store, input_fnames):
    if input_file_format == 'parquet':
        df = pd.read_parquet(
            os.path.join(input_data_dir, input_fnames['jobs']))
    elif input_file_format == 'h5':
        df = store['jobs']
    elif input_file_format == 'csv':
        try:
            df = pd.read_csv(
                os.path.join(input_data_dir, input_fnames['jobs']),
                index_col='job_id', dtype={'job_id': int, 'building_id': int})
        except ValueError:
            df = pd.read_csv(
                os.path.join(input_data_dir, input_fnames['jobs']),
                index_col=0, dtype={'job_id': int, 'building_id': int})
            df.index.name = 'job_id'
    return df


@orca.table('establishments', cache=True)
def establishments(input_file_format, input_data_dir, store, input_fnames):
    if input_file_format == 'parquet':
        df = pd.read_parquet(
            os.path.join(input_data_dir, input_fnames['establishments']))
    elif input_file_format == 'h5':
        df = store['establishments']
    elif input_file_format == 'csv':
        df = pd.read_csv(
            os.path.join(input_data_dir, input_fnames['establishments']),
            index_col='establishment_id', dtype={
                'establishment_id': int, 'building_id': int,
                'primary_id': int})
    return df


@orca.table('households', cache=True)
def households(input_file_format, input_data_dir, store, input_fnames):
    if input_file_format == 'parquet':
        df = pd.read_parquet(
            os.path.join(input_data_dir, input_fnames['households']))
    elif input_file_format == 'h5':
        df = store['households']
    elif input_file_format == 'csv':
        try:
            df = pd.read_csv(
                os.path.join(input_data_dir, input_fnames['households']),
                index_col='household_id', dtype={
                    'household_id': int, 'block_group_id': str, 'state': str,
                    'county': str, 'tract': str, 'block_group': str,
                    'building_id': int, 'unit_id': int, 'persons': float})
        except ValueError:
            df = pd.read_csv(
                os.path.join(input_data_dir, input_fnames['households']),
                index_col=0, dtype={
                    'household_id': int, 'block_group_id': str, 'state': str,
                    'county': str, 'tract': str, 'block_group': str,
                    'building_id': int, 'unit_id': int, 'persons': float})
            df.index.name = 'household_id'
    return df


@orca.table('persons', cache=True)
def persons(input_file_format, input_data_dir, store, input_fnames):
    if input_file_format == 'parquet':
        df = pd.read_parquet(
            os.path.join(input_data_dir, input_fnames['persons']))
    elif input_file_format == 'h5':
        df = store['persons']
    elif input_file_format == 'csv':
        try:
            df = pd.read_csv(
                os.path.join(input_data_dir, input_fnames['persons']),
                index_col='person_id', dtype={
                    'person_id': int, 'household_id': int})
        except ValueError:
            df = pd.read_csv(
                os.path.join(input_data_dir, input_fnames['persons']),
                index_col=0, dtype={'person_id': int, 'household_id': int})
            df.index.name = 'person_id'
    return df


@orca.table('rentals', cache=True)
def rentals(input_file_format, input_data_dir, store, input_fnames):
    if input_file_format == 'parquet':
        df = pd.read_parquet(
            os.path.join(input_data_dir, input_fnames['rentals']))
    elif input_file_format == 'h5':
        df = store['craigslist']
    elif input_file_format == 'csv':
        try:
            df = pd.read_csv(
                os.path.join(input_data_dir, input_fnames['rentals']),
                index_col='pid', dtype={
                    'pid': int, 'date': str, 'region': str,
                    'neighborhood': str, 'rent': float, 'sqft': float,
                    'rent_sqft': float, 'longitude': float,
                    'latitude': float, 'county': str, 'fips_block': str,
                    'state': str, 'bathrooms': str})
        except ValueError:
            df = pd.read_csv(
                os.path.join(input_data_dir, input_fnames['rentals']),
                index_col=0, dtype={
                    'date': str, 'region': str,
                    'neighborhood': str, 'rent': float, 'sqft': float,
                    'rent_sqft': float, 'longitude': float,
                    'latitude': float, 'county': str, 'fips_block': str,
                    'state': str, 'bathrooms': str})
            df.index.name = 'pid'
    df.rent[df.rent < 100] = 100.0
    df.rent[df.rent > 10000] = 10000.0
    df.rent_sqft[df.rent_sqft < .2] = .2
    df.rent_sqft[df.rent_sqft > 50] = 50.0
    return df


@orca.table('units', cache=True)
def units(input_file_format, input_data_dir, store, input_fnames):
    if input_file_format == 'parquet':
        df = pd.read_parquet(
            os.path.join(input_data_dir, input_fnames['units']))
    elif input_file_format == 'h5':
        df = store['units']
    elif input_file_format == 'csv':
        df = pd.read_csv(
            os.path.join(input_data_dir, input_fnames['units']),
            index_col='unit_id', dtype={'unit_id': int, 'building_id': int})
    df.index.name = 'unit_id'
    return df


@orca.table('zones', cache=True)
def zones(input_file_format, input_data_dir, store, input_fnames):
    if input_file_format == 'parquet':
        df = pd.read_parquet(
            os.path.join(input_data_dir, input_fnames['zones']))
    elif input_file_format == 'h5':
        df = store['zones']
    elif input_file_format == 'csv':
        df = pd.read_csv(
            os.path.join(input_data_dir, input_fnames['zones']),
            index_col='zone_id', dtype={'zone_id': int})
        if 'tract' in df.columns:
            df.drop('tract', axis=1, inplace=True)
    return df


# Tables from Jayne Chang
# Append AM peak UrbanAccess transit accessibility variables to parcels table
@orca.table('access_indicators_ampeak', cache=True)
def access_indicators_ampeak():
    # this filepath is hardcoded because it lives in the repo
    am_acc = pd.read_csv(
        './data/access_indicators_ampeak.csv', dtype={'block_id': str})
    am_acc.block_id = am_acc.block_id.str.zfill(15)
    am_acc.set_index('block_id', inplace=True)
    am_acc = am_acc.fillna(am_acc.median())
    return am_acc


# Tables from Emma
@orca.table('mtc_skims', cache=True)
def mtc_skims(input_file_format, input_data_dir, store, input_fnames):
    if input_file_format == 'parquet':
        df = pd.read_parquet(
            os.path.join(input_data_dir, input_fnames['mtc_skims']))
    elif input_file_format == 'h5':
        df = store['mtc_skims']
    elif input_file_format == 'csv':
        df = pd.read_csv(
            os.path.join(input_data_dir, input_fnames['mtc_skims']),
            index_col=0)
    return df


@orca.table(cache=True)
def beam_skims_raw(input_file_format, input_data_dir, store, input_fnames):
    """
    Load full BEAM skims, convert travel time to minutes
    """
    if input_file_format == 'parquet':
        df = pd.read_parquet(
            os.path.join(input_data_dir, input_fnames['beam_skims_raw']))
    elif input_file_format == 'h5':
        df = store['beam_skims_raw']
    elif input_file_format == 'csv':
        df = pd.read_csv(
            os.path.join(input_data_dir, input_fnames['beam_skims_raw']))

    df.rename(columns={
        'generalizedCost': 'gen_cost', 'origTaz': 'from_zone_id',
        'destTaz': 'to_zone_id'}, inplace=True)
    return df


@orca.table(cache=True)
def beam_skims_imputed(input_file_format, input_data_dir, store, input_fnames):
    """
    Load imputed BEAM skims
    """
    if input_file_format == 'parquet':
        df = pd.read_parquet(
            os.path.join(input_data_dir, input_fnames['beam_skims_imputed']))
    elif input_file_format == 'h5':
        df = store['beam_skims_imputed']
    elif input_file_format == 'csv':
        df = pd.read_csv(
            os.path.join(input_data_dir, input_fnames['beam_skims_imputed']))
    df.set_index(['from_zone_id', 'to_zone_id'], inplace=True)
    return df


# Broadcasts, a.k.a. merge relationships
orca.broadcast(
    'parcels', 'buildings', cast_index=True, onto_on='parcel_id')
orca.broadcast(
    'buildings', 'units', cast_index=True, onto_on='building_id')
orca.broadcast(
    'units', 'households', cast_index=True, onto_on='unit_id')
orca.broadcast(
    'households', 'persons', cast_index=True, onto_on='household_id')
orca.broadcast(
    'buildings', 'jobs', cast_index=True, onto_on='building_id')
orca.broadcast(
    'buildings', 'establishments', cast_index=True, onto_on='building_id')
orca.broadcast(
    'nodeswalk', 'parcels', cast_index=True, onto_on='node_id_walk')
orca.broadcast(
    'nodeswalk', 'rentals', cast_index=True, onto_on='node_id_walk')
orca.broadcast(
    'nodessmall', 'rentals', cast_index=True, onto_on='node_id_small')
orca.broadcast(
    'nodessmall', 'parcels', cast_index=True, onto_on='node_id_small')
orca.broadcast(
    'zones', 'parcels', cast_index=True, onto_on='zone_id')
