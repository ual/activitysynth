import orca
import pandas as pd
import numpy as np

# data documentation: https://berkeley.app.box.com/notes/282712547032


@orca.table('parcels', cache=True)
def parcels(data_mode, store, s3_input_data_url, local_data_dir):
    if data_mode == 's3':
        df = pd.read_parquet(s3_input_data_url.format('parcels'))
    elif data_mode == 'h5':
        df = store['parcels']
    elif data_mode == 'csv':
        df = pd.read_csv(
            local_data_dir + 'parcel_attr.csv', index_col='primary_id',
            dtype={'primary_id': int, 'block_id': str})
    return df


@orca.table('buildings', cache=True)
def buildings(data_mode, store, s3_input_data_url, local_data_dir):
    if data_mode == 's3':
        df = pd.read_parquet(s3_input_data_url.format('buildings'))
    elif data_mode == 'h5':
        df = store['buildings']
    elif data_mode == 'csv':
        df = pd.read_csv(
            local_data_dir + 'buildings_v2.csv', index_col='building_id',
            dtype={'building_id': int, 'parcel_id': int})
        df['res_sqft_per_unit'] = df[
            'residential_sqft'] / df['residential_units']
        df['res_sqft_per_unit'][df['res_sqft_per_unit'] == np.inf] = 0
    return df


@orca.table('jobs', cache=True)
def jobs(data_mode, store, s3_input_data_url, local_data_dir):
    if data_mode == 's3':
        df = pd.read_parquet(s3_input_data_url.format('jobs'))
    elif data_mode == 'h5':
        df = store['jobs']
    elif data_mode == 'csv':
        df = pd.read_csv(
            local_data_dir + 'jobs_v2.csv', index_col='job_id',
            dtype={'job_id': int, 'building_id': int})
    return df


@orca.table('establishments', cache=True)
def establishments(data_mode, store, s3_input_data_url, local_data_dir):
    if data_mode == 's3':
        df = pd.read_parquet(s3_input_data_url.format('establishments'))
    elif data_mode == 'h5':
        df = store['establishments']
    elif data_mode == 'csv':
        df = pd.read_csv(
            local_data_dir + 'establishments_v2.csv',
            index_col='establishment_id', dtype={
                'establishment_id': int, 'building_id': int,
                'primary_id': int})
    return df

def load_chts_households(local_data_dir):
    #Use the CHTS persons processed with 
    # https://raw.githubusercontent.com/ual/ual_model_workspace/master/fall-2018-models/notebooks-max/WLCM_pre-processing.ipynb
    df = pd.read_csv(
        d + 'CHTS_csv_format/data/Deliv_HH.csv',
        #dtype={'SAMPN': 'S', 'PERNO': 'S'},
        #index_col = ["SAMPN", "PERNO"],
        usecols = ['SAMPN','HHVEH','HHBIC','OWN','INCOM','HHSIZ']
    )
    # rename/process columns to match those in complete households table:
    # household_id,serialno,persons,building_type,cars,income,race_of_head,
    # hispanic_head,age_of_head,workers,state,county,tract,block_group,
    # children,tenure,recent_mover,block_group_id,single_family,unit_id,building_id
    
    df = ( df.rename(index=str,
                     columns= {"SAMPN": "household_id", 
                               # ?    : "building_type", (should be an computed/derived variable)
                               # ?    : "race_of_head", (should be an computed/derived variable)
                               # ?    : "hispanic_head", (should be an computed/derived variable)
                               # ?    : "age_of_head", (should be an computed/derived variable)
                               # ?    : "workers", (should be an computed/derived variable)
                               # -    : "state", "county", "tract", "block_group", "block_group_id"
                               # ?    : "children", (should be an computed/derived variable)
                               # ?    : "recent_mover"?
                               # ?    : "single_family", (should be an computed/derived variable)
                               # ?    : "building_id", "unit_id"
                               "HHVEH": "cars",
                               "INCOM": "income", # category -> $
                               "HHSIZ": "persons", # (should be an computed/derived variable)
                               #"OWN": "tenure", #TODO:coding
                               "HHBIC": "bikes", #cannot be used as it is missing from the complete households
                                })
             .assign(tenure = lambda x: np.where(x['OWN'] == 1, 1, 2),
                    )
             .drop(columns=["OWN"])
         )
    return df    

@orca.table('households', cache=True)
def households(data_mode, store, s3_input_data_url, local_data_dir, append_chts=True):
    if data_mode == 's3':
        df = pd.read_parquet(s3_input_data_url.format('households'))
    elif data_mode == 'h5':
        df = store['households']
    elif data_mode == 'csv':
        df = pd.read_csv(
            local_data_dir + 'households_v2.csv',
            index_col='household_id', dtype={
                'household_id': int, 'block_group_id': str, 'state': str,
                'county': str, 'tract': str, 'block_group': str,
                'building_id': int, 'unit_id': int, 'persons': float})
    if append_chts:
        df_chts = load_chts_households(local_data_dir)
        df = pd.concat(df, df_chts)
        
    return df

def load_chts_persons(local_data_dir):
    """ load CHTS persons table
    """
    #Use the CHTS persons processed with 
    # https://raw.githubusercontent.com/ual/ual_model_workspace/master/fall-2018-models/notebooks-max/WLCM_pre-processing.ipynb

    df = pd.read_csv(
        local_data_dir + 'CHTS_csv_format/data/Deliv_PER.csv',
        #dtype={'SAMPN': 'S', 'PERNO': 'S'},
        #index_col = ["SAMPN", "PERNO"],
        usecols = ["SAMPN", "PERNO", "GEND", "RELAT", "AGE", "HISP",
                   "EMPLY", "WMODE", "EDUCA", "HOURS", "STUDE",
                   "RACE1"
                  ]
    )
    
    # CHTS EDUCA
    # 1 NOT A HIGH SCHOOL GRADUATE, 12 GRADE OR LESS (THIS INCLUDES VERY
    # YOUNG CHILDREN TOO)
    # 2 HIGH SCHOOL GRADUATE (HIGH SCHOOL DIPLOMA OR GED)
    # 3 SOME COLLEGE CREDIT BUT NO DEGREE
    # 4 ASSOCIATE OR TECHNICAL SCHOOL DEGREE
    # 5 BACHELOR’S OR UNDERGRADUATE DEGREE
    # 6 GRADUATE DEGREE (INCLUDES PROFESSIONAL DEGREE LIKE MD, DDS, JD) 

    # CHTS RACE
    #01 White
    #02 Black or African American
    #03 American Indian or Alaska Native
    #04 Asian (Asian Indian, Japanese, Chinese, Korean, Filipino, Vietnamese)
    #05 Native Hawaiian or Pacific Islander (Guamanian, Samoan, Fijian)
    #97 Other (please specify) (O_RACE)
    #98 I do not know
    #99 I prefer not to answer 
    
    # CHTS WMODE
    # 01 WALK
    # 02 BIKE
    # 03 WHEELCHAIR / MOBILITY SCOOTER
    # 04 OTHER NON-MOTORIZED (Skateboard, etc.)
    # PRIVATE VEHICLE:
    # 05 AUTO / VAN / TRUCK DRIVER
    # 06 AUTO / VAN / TRUCK PASSENGER
    # 07 CARPOOL / VANPOOL
    # 08 MOTORCYCLE / SCOOTER / MOPED
    # PRIVATE TRANSIT:
    # 09 TAXI / HIRED CAR / LIMO
    # 10 RENTAL CAR / VEHICLE
    # 11 PRIVATE SHUTTLE (SuperShuttle, employer, hotel, etc.)
    # 12 GREYHOUND BUS
    # 13 AIRPLANE
    # 14 OTHER PRIVATE TRANSIT
    # PUBLIC TRANSIT:
    # BUS:
    # 15 LOCAL BUS / RAPID BUS
    # 16 EXPRESS BUS / COMMUTER BUS (AC Transbay, Golden Gate Transit, etc.)
    # 17 PREMIUM BUS (Metro Orange / Silver Line)
    # 18 SCHOOL BUS
    # 19 PUBLIC TRANSIT SHUTTLE (DASH, Emery Go-Round, etc.)
    # 20 AIRBART / LAX FLYAWAY
    # 21 DIAL-A-RIDE / ParaTransit (Access Services, etc.)
    # 22 AMTRAK BUS
    # 23 OTHER BUS RAIL/SUBWAY:
    # 24 BART, METRO RED / PURPLE LINE
    # 25 ACE, AMTRAK, CALTRAIN, COASTER, METROLINK
    # 26 METRO BLUE / GREEN / GOLD LINE, MUNI METRO, SACRAMENTO LIGHT
    # RAIL, SAN DIEGO SPRINTER / TROLLEY / ORANGE/BLUE/GREEN, VTA
    # LIGHT RAIL
    # 27 STREET CAR / CABLE CAR
    # 28 OTHER RAIL
    # FERRY:
    # 29 FERRY / BOAT 

    # rename/process columns to match those in complete persons table:
    #person_id,member_id,age,primary_commute_mode,relate,edu,sex,
    #hours,hispanic,race_id,student,worker,household_id,
    #earning, work_at_home, node_id_small,node_id_walk,job_id

    df = ( df.rename(index=str,
                     columns= {"SAMPN": "household_id", 
                                 "PERNO": "member_id",
                                 "GEND": "sex",
                                 "RELAT": "relate", #TODO: check coding
                                 "AGE": "age",
                                 "HISP": "hispanic", #TODO:fix coding
                                 #  ?  : "earning",
                                 #  ?  : "work_at_home",
                                 #  ?  : "job_id",
                                 "WMODE": "primary_commute_mode", #TODO: fix coding
                                 "EDUCA": "edu", #TODO: fix coding
                                 "HOURS": "hours",
                                 "RACE1": "race_id", #TODO: fix coding
                                })
             .assign(student = lambda x: np.where(x['STUDE'].isin([1, 2]), 1, 0), # full or part time student
                     sex = lambda x: np.where(x['sex'] == 9, np.NaN, x['sex']),   # handle refused
                     worker = lambda x: np.where(x['EMPLY'] == 1, 1, 0),
                    )
             .drop(columns=["EMPLY", "STUDE"])
         )
    return df

@orca.table('persons', cache=True)
def persons(data_mode, store, s3_input_data_url, local_data_dir, append_chts=True):
    if data_mode == 's3':
        df = pd.read_parquet(s3_input_data_url.format('persons'))
    elif data_mode == 'h5':
        df = store['persons']
    elif data_mode == 'csv':
        df = pd.read_csv(
            local_data_dir + 'persons_v3.csv', index_col='person_id',
            dtype={'person_id': int, 'household_id': int})
    if append_chts:
        df_chts = load_chts_persons(local_data_dir)
        df = pd.concat(df, df_chts)    
    return df

@orca.table('activities', cache=True)
def activities(data_mode, store, s3_input_data_url, local_data_dir):
    """ load CHTS activities table
    Since activities table for the synthesized population does not exisit and will have to synthesized,
    the CHTS activities table is loaded as the "activities" table and used for estimation
    """
    
    place_df = pd.read_csv(
        local_data_dir + 'CHTS_csv_format/data/Deliv_PLACE.csv',
        dtype={'SAMPN': 'S', 'PERNO': 'S'},
        #index_col = ["SAMPN", "PERNO"],
        usecols = ["SAMPN", "PERNO", "GEND", "RELAT", "AGE", "HISP",
                   "EMPLY", "WMODE", "EDUCA", "HOURS", "STUDE",
                   "RACE1"
                  ]
    )

    act_df = pd.read_csv(
        local_data_dir + 'CHTS_csv_format/data/Deliv_ACTIVITY.csv',
        dtype={'SAMPN': 'S', 'PERNO': 'S'},
        #index_col = ["SAMPN", "PERNO"],
        usecols = ["SAMPN", "PERNO", "GEND", "RELAT", "AGE", "HISP",
                   "EMPLY", "WMODE", "EDUCA", "HOURS", "STUDE",
                   "RACE1"
                  ]
    )

        
    return df

@orca.table('rentals', cache=True)
def rentals(data_mode, store, s3_input_data_url, local_data_dir):
    if data_mode == 's3':
        df = pd.read_parquet(s3_input_data_url.format('rentals'))
    elif data_mode == 'h5':
        df = store['craigslist']
    elif data_mode == 'csv':
        df = pd.read_csv(
            local_data_dir + 'MTC_craigslist_listings_7-10-18.csv',
            index_col='pid', dtype={
                'pid': int, 'date': str, 'region': str, 'neighborhood': str,
                'rent': float, 'sqft': float, 'rent_sqft': float,
                'longitude': float, 'latitude': float, 'county': str,
                'fips_block': str, 'state': str, 'bathrooms': str})
    df.rent[df.rent < 100] = 100.0
    df.rent[df.rent > 10000] = 10000.0
    df.rent_sqft[df.rent_sqft < .2] = .2
    df.rent_sqft[df.rent_sqft > 50] = 50.0
    return df


@orca.table('units', cache=True)
def units(data_mode, store, s3_input_data_url, local_data_dir):
    if data_mode == 's3':
        df = pd.read_parquet(s3_input_data_url.format('units'))
    elif data_mode == 'h5':
        df = store['units']
    elif data_mode == 'csv':
        df = pd.read_csv(
            local_data_dir + 'units_v2.csv', index_col='unit_id',
            dtype={'unit_id': int, 'building_id': int})
    df.index.name = 'unit_id'
    return df


# Tables from Jayne Chang

# Append AM peak UrbanAccess transit accessibility variables to parcels table

@orca.table('access_indicators_ampeak', cache=True)
def access_indicators_ampeak():
    am_acc = pd.read_csv(
        './data/access_indicators_ampeak.csv', dtype={'block_id': str})
    am_acc.block_id = am_acc.block_id.str.zfill(15)
    am_acc.set_index('block_id', inplace=True)
    am_acc = am_acc.fillna(am_acc.median())
    return am_acc


# Tables from Emma
@orca.table('skims', cache=True)
def skims(data_mode, store, s3_input_data_url, local_data_dir):
    if data_mode == 's3':
        df = pd.read_parquet(s3_input_data_url.format('skims'))
    elif data_mode == 'h5':
        df = store['skims']
    elif data_mode == 'csv':
        df = pd.read_csv(local_data_dir + 'skims_110118.csv', index_col=0)
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
orca.broadcast(
    'nodesbeam', 'parcels', cast_index=True, onto_on='node_id_beam')
orca.broadcast(
    'nodesbeam', 'rentals', cast_index=True, onto_on='node_id_beam')
orca.broadcast(
    'nodesbeam', 'rentals', cast_index=True, onto_on='node_id_beam')
orca.broadcast(
    'jobs', 'persons', cast_index=True, onto_on='job_id')
