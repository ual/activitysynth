import orca
import pandas as pd

# data documentation: https://berkeley.app.box.com/notes/282712547032

orca.add_injectable(
    's3_input_data_url', 's3://urbansim-baseyear-inputs/{0}.parquet.gz')


@orca.table('parcels', cache=True)
def parcels(s3_input_data_url):
    df = pd.read_parquet(s3_input_data_url.format('parcels'))
    df.index.name = 'primary_id'
    return df


@orca.table('buildings', cache=True)
def buildings(s3_input_data_url):
    df = pd.read_parquet(s3_input_data_url.format('buildings'))
    return df


@orca.table('jobs', cache=True)
def jobs(s3_input_data_url):
    df = pd.read_parquet(s3_input_data_url.format('jobs'))
    return df


@orca.table('establishments', cache=True)
def establishments(s3_input_data_url):
    df = pd.read_parquet(s3_input_data_url.format('establishments'))
    return df


@orca.table('households', cache=True)
def households(s3_input_data_url):
    df = pd.read_parquet(s3_input_data_url.format('households'))
    return df


@orca.table('persons', cache=True)
def persons(s3_input_data_url):
    df = pd.read_parquet(s3_input_data_url.format('persons'))
    return df


@orca.table('rentals', cache=True)
def rentals(s3_input_data_url):
    df = pd.read_parquet(s3_input_data_url.format('rentals'))
    df['rent'] = df['rent'].astype(float)
    df.rent[df.rent < 100] = 100.0
    df.rent[df.rent > 10000] = 10000.0
    df.rent_sqft[df.rent_sqft < .2] = .2
    df.rent_sqft[df.rent_sqft > 50] = 50.0
    return df


@orca.table('units', cache=True)
def units(s3_input_data_url):
    df = pd.read_parquet(s3_input_data_url.format('units'))
    df.index.name = 'unit_id'
    return df


# Tables from Jayne Chang

# Append AM peak UrbanAccess transit accessibility variables to parcels table

parcels = orca.get_table('parcels').to_frame()
am_acc = pd.read_csv(
    './data/access_indicators_ampeak.csv', dtype={'block_id': str})
am_acc.block_id = am_acc.block_id.str.zfill(15)
parcels_with_acc = parcels.merge(
    am_acc, how='left', on='block_id').reindex(
    index = parcels.index) # reorder to align with parcels table

for acc_col in set(parcels_with_acc.columns) - set(parcels):
    # fill NA with median value
    orca.add_column('parcels',acc_col,
        parcels_with_acc[acc_col].fillna(parcels_with_acc[acc_col].median()))


# Tables from Emma
@orca.table('skims', cache=True)
def skims(s3_input_data_url):
    df = pd.read_parquet(s3_input_data_url.format('skims'))
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
    'nodesbeam', 'parcels', cast_index=True, onto_on='node_id_beam')
orca.broadcast(
    'nodesbeam', 'rentals', cast_index=True, onto_on='node_id_beam')
