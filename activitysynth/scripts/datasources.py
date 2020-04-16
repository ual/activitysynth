import orca
import pandas as pd
import numpy as np
import os
import random
import pylogit as pl
import s3fs
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
    am_acc = pd.read_csv('s3://baus-data/spring_2019/urbanaccess_transit/access_indicators_ampeak.csv', dtype={'block_id': str})
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


@orca.table(cache=True)
def drive_nodes(input_file_format, input_data_dir, store, input_fnames):
    if input_file_format == 'parquet':
        nodes = pd.read_parquet(
            os.path.join(input_data_dir, input_fnames['drive_nodes']))
    elif input_file_format == 'h5':
        nodes = store['drive_nodes']
    elif input_file_format == 'csv':
        nodes = pd.read_csv(os.path.join(
            input_data_dir,
            input_fnames['drive_nodes'])).set_index('osmid')
    return nodes


@orca.table(cache=True)
def drive_edges(input_file_format, input_data_dir, store, input_fnames):
    if input_file_format == 'parquet':
        edges = pd.read_parquet(
            os.path.join(input_data_dir, input_fnames['drive_edges']))
    elif input_file_format == 'h5':
        edges = store['drive_edges']
    elif input_file_format == 'csv':
        edges = pd.read_csv(os.path.join(
            input_data_dir,
            input_fnames['drive_edges'])).set_index('uniqueid')
    return edges


@orca.table(cache=True)
def walk_nodes(input_file_format, input_data_dir, store, input_fnames):
    if input_file_format == 'parquet':
        nodes = pd.read_parquet(
            os.path.join(input_data_dir, input_fnames['walk_nodes']))
    elif input_file_format == 'h5':
        nodes = store['walk_nodes']
    elif input_file_format == 'csv':
        nodes = pd.read_csv(os.path.join(
            input_data_dir,
            input_fnames['walk_nodes'])).set_index('osmid')
    return nodes


@orca.table(cache=True)
def walk_edges(input_file_format, input_data_dir, store, input_fnames):
    if input_file_format == 'parquet':
        edges = pd.read_parquet(
            os.path.join(input_data_dir, input_fnames['walk_edges']))
    elif input_file_format == 'h5':
        edges = store['walk_edges']
    elif input_file_format == 'csv':
        edges = pd.read_csv(os.path.join(
            input_data_dir,
            input_fnames['walk_edges'])).set_index('uniqueid')
    return edges

#Tables Juan
@orca.table(cache = True)
def schools():
    #Hardcoded 
    list_1 = [1459, 1685, 1746, 1749, 1750, 2006, 2063, 2327, 2662, 2679, 3378, 3379, 3381, 3432, 3454]
    df = pd.read_csv('s3://baus-data/spring_2019/schools_bay_area.csv', index_col = 'school_id')
    df = df[~df.index.isin(list_1)]
    return df 


@orca.table(cache = True) 
def public_schools_50(netsmall, schools):
    #50 closest public schools for each node id
    s = schools.to_frame()
    netsmall.set_pois('public_school', 500000, 10000, 
                  s[s.type == 'public'].Longitude, 
                  s[s.type == 'public'].Latitude, )
    
    df = netsmall.nearest_pois(200000,
                               'public_school',
                               num_pois=50,
                               include_poi_ids=True)
    
    df.columns = df.columns.astype(str)
    rank = schools.to_frame(columns='rank')
    
    # Get the score of the 50th closest scools to node ID
    for col in range(1,51):
        col_name = 'poi' + str(col)
        df = df.merge(rank, how = 'left', left_on = col_name, right_index = True)
        new_col_name = 'rank' + str(col)
        df.rename(columns = {'rank': new_col_name}, inplace = True)
        
    # Calculate average of the 50th closest scools to node ID
    for col in range(102,151):
        col_name = 'mean' + str(col-100)
        df[col_name]= df.iloc[:,100:col].mean(axis = 1)
    
    return df    

@orca.table(cache = True) 
def private_schools_100(netsmall, schools):
    #100 closest private schools for each node id
    s = schools.to_frame()
    netsmall.set_pois('private_school', 500000, 10000, 
                  s[s.type == 'private'].Longitude, 
                  s[s.type == 'private'].Latitude, )
    
    df = netsmall.nearest_pois(200000,
                               'private_school',
                               num_pois=100,
                               include_poi_ids=True)
    return df

@orca.table(cache = True)
def students(persons, households):
    df = orca.merge_tables(target='persons', 
                           tables=[persons, households], 
                           columns = ['income','member_id', 'age', 
                                      'household_id','node_id_small', 
                                      'student','zone_id_home', 'age_0_5',
                                      'age_5_12', 'age_12_15', 'age_15_18', 
                                      'female', 'minority','race_id','persons', 
                                      'black', 'asian', 'hh_size_over_4'])
    
    df['hh_inc_under_25k'] = ((df.income > 10) & (df.income <= 25000)).astype(int)
    df['hh_inc_25_to_75k'] = ((df.income > 25000) & (df.income <= 75000)).astype(int)
    df['hh_inc_75_to_200k'] = ((df.income > 75000) & (df.income <= 200000)).astype(int)
    
    df = df[(df.age> 4) & (df.age <=18) & (df.student == 1)]
    df.dropna(subset=['node_id_small'], inplace = True)
    
    #This needs to change when I merge persons and households. 
    df['home_city'] = 'San Francisco'
    return df


@orca.table(cache=True)
def closest_schools(students, public_schools_50, private_schools_100, schools):
    #Converting orca tables to dataframes. 
    st = students.to_frame()
    sc = schools.to_frame()
    pu_50 = public_schools_50.to_frame()
    pr_100 = private_schools_100.to_frame()
    
    #Creating long format table for school choice (1 row per each school-student pair)
    df = st.merge(pu_50.iloc[:,50:], 
                  how = 'left', 
                  left_on = 'node_id_small', 
                  right_index = True).merge(pr_100.iloc[:,100:],
                                            how = 'left', 
                                            left_on = 'node_id_small',
                                            right_index = True)
    df = df.loc[:,df.columns.str.startswith("poi")].unstack()
    df = df.reset_index().sort_values(by = 'person_id').rename(columns = {0: 'school_choice_set', 'person_id': 'obs_id'})
   
    # School availability matrix 
    sa_matrix = df.merge(sc.loc[:,sc.columns.str.startswith("grade_")], 
                         how = 'left', left_on = 'school_choice_set', right_index=True)
    sa_matrix = np.array(sa_matrix.loc[:,sa_matrix.columns.str.startswith("grade_")])

    # Age matrix 
    age = st.age.replace(18, 17)   
    age_matrix = np.array(pd.get_dummies(age))
    index = pd.Series(np.nonzero(age_matrix)[1] - 1).replace(-1,0)
    age_matrix[np.arange(0,len(index)),index] = 1

    # Creating age filter 
    col = (sa_matrix.reshape(1109985,1,150,13) * age_matrix.reshape(1109985, 1, 1,13)).sum(-1)
    filter_1 = pd.Series(col.flatten()).replace(2,1).astype(bool)
    
    return df[filter_1]

@orca.table(cache = True)
def long_format(closest_schools, students, schools,beam_skims_imputed ):
    cols = ['obs_id', 'zone_id_home', 'hh_inc_under_25k', 
            'hh_inc_25_to_75k', 'hh_inc_75_to_200k','school_choice_set', 
            'school_zone_id', 'rank', 'rank_1_4', 'rank_5_7',
            'rank_8_9', 'City', 'home_city']
    
    df = orca.merge_tables(target='closest_schools', tables=['students', 'schools', 'closest_schools'], columns = cols)
    
    beam_skims = orca.get_table('beam_skims_imputed').to_frame(columns = ['dist', 'gen_tt_CAR']).reset_index()
    
    df_1 = df.merge(beam_skims, how = 'left', 
             left_on = ['zone_id_home','school_zone_id'], 
             right_on = ['from_zone_id', 'to_zone_id'])
    
    return df_1 

@orca.table(cache= True)
def TOD_school_data_preparation(students):
    """
    Transforms students data from wide to long format for pylogit prediction
    """
    students = orca.get_table('students').to_frame()
    
    #Creating a fake alternative specific varible and its corresponding availability variable. 
    for x in [1,2,3,4,5]:
        name = 'tt_'+str(x)
        av_name = 'av_'+str(x)
        students[name] = [random.randint(1,20) for x in range(len(students))]
        students[av_name] = 1
        students['choice'] = [random.randint(1,5) for x in range(len(students))]
    students.reset_index(inplace = True)
    
    #Preparing for wide to long format transformation
    ind_variables = ['age_0_5', 'age_5_12','age_12_15','age_15_18', 
                     'minority', 'asian', 'black','hh_inc_under_25k',
                     'hh_inc_25_to_75k', 'hh_inc_75_to_200k', 
                     'female', 'hh_size_over_4']

    alt_varying_variables = {u'travel_time': dict([(1, 'tt_1'),
                                               (2, 'tt_2'),
                                               (3, 'tt_3'),
                                               (4, 'tt_4'),
                                               (5, 'tt_5')])}

    availability_variables = dict(zip(range(1, 6), ['av_1','av_2','av_3','av_4','av_5']))
    
    # Perform the desired conversion
    df = pl.convert_wide_to_long(wide_data= students,
                                       ind_vars = ind_variables,
                                       alt_specific_vars = alt_varying_variables,
                                       availability_vars = availability_variables, 
                                       obs_id_col = 'person_id',
                                       choice_col= 'choice')  
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

orca.broadcast(cast='students', onto='closest_schools', cast_index = True, onto_on='obs_id')
orca.broadcast(cast='schools', onto='closest_schools', cast_index = True, onto_on='school_choice_set')
orca.broadcast(cast='parcels', onto='schools', cast_index = 'zone_id', onto_on='school_parcel_id')
orca.broadcast(cast = 'zones', onto = 'persons', cast_index= True, onto_on = 'zone_id_home')
orca.broadcast(cast = 'public_schools_50', onto = 'nodessmall', cast_index= True, onto_index = True)
orca.broadcast(cast = 'nodessmall', onto = 'persons', cast_index= True, onto_on = 'node_id_small')
