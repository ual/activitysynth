# import orca
# import numpy as np
# import pandas as pd


# # data documentation: https://berkeley.app.box.com/notes/282712547032


# # Set data directory

# d = '/home/data/fall_2018/'

# if 'data_directory' in orca.list_injectables():
#     d = orca.get_injectable('data_directory')


# ############################################################

# # Tables from MTC Bay Area UrbanSim

# @orca.table(cache=True)
# def parcels():
#     df = pd.read_csv(
#         d + 'parcel_attr.csv',
#         index_col='primary_id',
#         dtype={'primary_id': int, 'block_id': str})
#     return df


# @orca.table(cache=True)
# def buildings():
#     df = pd.read_csv(
#         d + 'buildings_v2.csv',
#         index_col='building_id', dtype={'building_id': int, 'parcel_id': int})
#     df['res_sqft_per_unit'] = df['residential_sqft'] / df['residential_units']
#     df['res_sqft_per_unit'][df['res_sqft_per_unit'] == np.inf] = 0
#     return df


# ############################################################

# # Table of Rental Data from  Craigslist, bayarea_urbansim added by Arezoo

# @orca.table(cache=True)
# def craigslist():
#     df = pd.read_csv(
#         d + 'MTC_craigslist_listings_7-10-18.csv',
#         index_col='pid', dtype={'pid': int})
#     return df


# @orca.table(cache=True)
# def rentals():
#     df = pd.read_csv(
#         d + 'rentals_with_nodes.csv',
#         index_col='pid', dtype={'pid': int, 'rent': float})
#     return df


# ############################################################

# # Tables synthesized by Max Gardner

# @orca.table(cache=True)
# def units():
#     df = pd.read_csv(
#         d + 'units_v2.csv',
#         index_col='unit_id', dtype={'unit_id': int, 'building_id': int})
#     return df


# @orca.table(cache=True)
# def households():
#     df = pd.read_csv(
#         d + 'households_v2.csv',
#         index_col='household_id', dtype={
#             'household_id': int, 'block_group_id': str, 'state': str,
#             'county': str, 'tract': str, 'block_group': str,
#             'building_id': int, 'unit_id': int, 'persons': float})
#     return df


# @orca.table(cache=True)
# def persons():
#     df = pd.read_csv(
#         d + 'persons_v3.csv',
#         index_col='person_id', dtype={'person_id': int, 'household_id': int})
#     return df


# @orca.table(cache=True)
# def jobs():
#     df = pd.read_csv(
#         d + 'jobs_v2.csv',
#         index_col='job_id', dtype={'job_id': int, 'building_id': int})
#     return df


# ############################################################

# # Tables from Sam Blanchard
# @orca.table(cache=True)
# def establishments():
#     df = pd.read_csv(
#         d + 'establishments_v2.csv',
#         index_col='establishment_id', dtype={
#             'establishment_id': int, 'building_id': int, 'primary_id': int})
#     return df

# ############################################################

# # Broadcasts, a.k.a. merge relationships


# orca.broadcast(
#     'parcels', 'buildings', cast_index=True, onto_on='parcel_id')
# orca.broadcast(
#     'buildings', 'units', cast_index=True, onto_on='building_id')
# orca.broadcast(
#     'units', 'households', cast_index=True, onto_on='unit_id')
# orca.broadcast(
#     'households', 'persons', cast_index=True, onto_on='household_id')
# orca.broadcast(
#     'buildings', 'jobs', cast_index=True, onto_on='building_id')
# orca.broadcast(
#     'buildings', 'establishments', cast_index=True, onto_on='building_id')
# orca.broadcast(
#     'nodeswalk', 'parcels', cast_index=True, onto_on='node_id_walk')
# orca.broadcast(
#     'nodeswalk', 'rentals', cast_index=True, onto_on='node_id_walk')
# orca.broadcast(
#     'nodessmall', 'rentals', cast_index=True, onto_on='node_id_small')
# orca.broadcast(
#     'nodessmall', 'parcels', cast_index=True, onto_on='node_id_small')
# # orca.broadcast(
# #     'nodesbeam', 'parcels', cast_index=True, onto_on='node_id_beam')

import orca
import pandas as pd


# data documentation: https://berkeley.app.box.com/notes/282712547032


@orca.injectable('store', cache=True)
def hdfstore(data_directory):
    return pd.HDFStore((data_directory + "model_data.h5"), mode='r')


@orca.table('parcels', cache=True)
def parcels(store):
    df = store['parcels']
    df.index.name = 'primary_id'
    return df


@orca.table('buildings', cache=True)
def buildings(store):
    df = store['buildings']
    return df


@orca.table('jobs', cache=True)
def jobs(store):
    df = store['jobs']
    return df


@orca.table('establishments', cache=True)
def establishments(store):
    df = store['establishments']
    return df


@orca.table('households', cache=True)
def households(store):
    df = store['households']
    return df


@orca.table('persons', cache=True)
def persons(store):
    df = store['persons']
    return df


@orca.table('rentals', cache=True)
def rentals(store):
    craigslist = store['craigslist']
    craigslist['rent'] = craigslist['rent'].astype(float)
    craigslist.rent[craigslist.rent < 100] = 100.0
    craigslist.rent[craigslist.rent > 10000] = 10000.0

    craigslist.rent_sqft[craigslist.rent_sqft < .2] = .2
    craigslist.rent_sqft[craigslist.rent_sqft > 50] = 50.0
    return craigslist


@orca.table('units', cache=True)
def units(store):
    df = store['units']
    df.index.name = 'unit_id'
    return df


# @orca.table('nodessmall', cache=True)
# def nodessmall(store):
#     df = store['nodessmall']
#     return df


# @orca.table('edgessmall', cache=True)
# def edgessmall(store):
#     df = store['edgessmall']
#     return df


# @orca.table('nodeswalk', cache=True)
# def nodeswalk(store):
#     df = store['nodeswalk']
#     return df


# @orca.table('edgeswalk', cache=True)
# def edgeswalk(store):
#     df = store['edgeswalk']
#     return df


# @orca.table('nodesbeam', cache=True)
# def nodesbeam(store):
#     df = store['nodesbeam']
#     return df


# @orca.table('edgesbeam', cache=True)
# def edgesbeam(store):
#     df = store['edgesbeam']
#     return df


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
