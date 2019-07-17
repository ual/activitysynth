import numpy as np
import pandas as pd
import orca
# os.chdir('../')
# import os; os.chdir('/home/amelia/activitysynth/activitysynth/')
# import os; os.chdir('/home/amelia/ual_model_workspace/spring-2019-models')
# print("Current working directory: {0}".format(os.getcwd()))
import sys; sys.path.insert(0, '/home/amelia/ual_model_workspace/spring-2019-models')

import warnings; warnings.simplefilter('ignore')
from urbansim.utils import misc
import pandana as pdna

from urbansim_templates import modelmanager as mm
from urbansim_templates.models import LargeMultinomialLogitStep
import warnings; warnings.simplefilter('ignore')
from matplotlib import pyplot as plt
import matplotlib.animation as animation
import seaborn as sns
import time

import scripts
print(scripts.__path__)
from scripts import datasources, models, variables, utils

start = time.time()

orca.run(['initialize_network_small', 'initialize_network_walk','impute_missing_skims']) 


script_start = time.time()
# Small netwrok 
nodessmall = pd.read_csv('/home/data/fall_2018/bay_area_tertiary_strongly_nodes.csv').set_index('osmid')
edgessmall = pd.read_csv('/home/data/fall_2018/bay_area_tertiary_strongly_edges.csv')
netsmall = pdna.Network(nodessmall.x, nodessmall.y, edgessmall.u,
                                edgessmall.v, edgessmall[['length']],
                                twoway=False)
netsmall.precompute(25000)

#Loading Data 
beam_skims = orca.get_table('beam_skims').to_frame()
reset_beam_skims = beam_skims.reset_index()

students = pd.read_csv('/home/juan/ual_model_workspace/spring-2019-models/notebooks-juan/students_with_school_id.csv')
schools = pd.read_csv('/home/juan/ual_model_workspace/spring-2019-models/notebooks-juan/schools.csv').rename({"parcel_id": "school_parcel_id"}, axis=1)
parcels = orca.get_table('parcels').to_frame()
hh = orca.get_table("households").to_frame().reset_index()
persons = orca.get_table("persons").to_frame().reset_index()
# print("hh len", len(hh))


#Preprocessing
students = students[students.AGE <= 18]


# Adding list of grades offered by each school
list_grades = []
for index, row in schools.loc[:,schools.columns.str.startswith("grade_")].iterrows():
    x = np.array(row)
    list_grades.append(x)
    
schools['list_grades'] = list_grades

def node_id_small(x, y, netsmall):
    """ Return the node ID given a pair of coordinates"""
    idssmall = netsmall.get_node_ids(x, y)
    return idssmall

#Merging students and schools 
students_1 = students.merge(schools, how = 'left', on = 'school_id').drop(["parcel_id_work", "zone_id_work", "nodeID"], axis=1)
# Droping students with no assigned school
students_1 = students_1.dropna(subset=['school_id'])

#Define the node ID for each home and school location in students dataset
students_1['node_id_home'] = node_id_small(students_1.HXCORD, students_1.HYCORD, netsmall)
students_1['node_id_school'] = node_id_small(students_1.Longitude, students_1.Latitude, netsmall)

print("Creating a df for public shcools only")
df_public = students_1[students_1.school_id <= 2827].loc[:,['SAMPN', 'PERNO', 'school_id',
                                                        'AGE','HCITY','HYCORD','HXCORD',
                                                        'SNAME_lookup','SCITY_lookup', 
                                                        'node_id_home', 'node_id_school']]

# #Creating a df for private shcools only
df_private = students_1[students_1.school_id > 2827].loc[:,['SAMPN', 'PERNO', 'school_id',
                                                        'AGE','HCITY','HYCORD','HXCORD',
                                                        'SNAME_lookup','SCITY_lookup', 
                                                        'node_id_home', 'node_id_school']]

print("Setting public and private schools as POIs.") 
netsmall.set_pois('public_school', 500000, 10000, schools[schools.type == 'public']["Longitude"], 
                  schools[schools.type == 'public']["Latitude"])


netsmall.set_pois('private_school', 500000, 10000, 
                  schools[schools.type == 'private'].Longitude, 
                  schools[schools.type == 'private'].Latitude)




#Public schools. 95% of the time, the school is within the x's closest schools. Finding x
n =50

# n closest public schools per each node in netsmall
distance_matrix_public = netsmall.nearest_pois(200000, 
                                               'public_school', 
                                               num_pois=n,
                                               include_poi_ids=True)

# Selects POIS id's only 
public_nodes = distance_matrix_public.iloc[:,n:]

# Creates a list of n closest POIs for each node
list_values = []
for index, row in public_nodes.iterrows():
    x = np.array(row)
    x = x[~np.isnan(x)]
    list_values.append(x)

#Add created list to public nodes 
public_nodes['list_values'] = list_values


merge = df_public.merge(public_nodes.loc[:,['list_values']], how = 'left', left_on = 'node_id_home', right_index= True)

school_position = []
for index, row in merge.iterrows():
    school_position.append(np.isin(row['school_id']-1000,row['list_values'])==True)
    
merge['school_position'] = school_position

merge.school_position.mean()

#Private schools. 95% of the time, the school is within the x's closest schools. Finding x
n =100

distance_matrix_private = netsmall.nearest_pois(100000, 
                                                'private_school', 
                                                num_pois=n, 
                                                include_poi_ids=True)

private_nodes = distance_matrix_private.iloc[:,n:]

list_values = []
for index, row in private_nodes.iterrows():
    x = np.array(row)
    x = x[~np.isnan(x)]
    list_values.append(x)
    
private_nodes['list_values'] = list_values


merge = df_private.merge(private_nodes.loc[:,['list_values']], how = 'left', left_on = 'node_id_home', right_index= True)

school_position = []
for index, row in merge.iterrows():
    school_position.append(np.isin(row['school_id']-1000,row['list_values'])==True)
    
merge['school_position'] = school_position

merge.school_position.mean()


def school_available(age, grades_offered):
    """ Checks if a school offers a grade according to the age range
    Input: Age: int in range (5-18)
           grades_offered: 13 element array
    Output: True if grade if offered acoording to age, false otherwise. 
     """
    index = age - 6
    
    if index < 0:
        index = 0

    if index < 12:
        result = (grades_offered[index] == 1) | (grades_offered[index + 1] == 1)
        
    elif index == 12:
        result = (grades_offered[index] == 1)
    
    return result

def school_choice_set(house_node_id, kid_age):
    """ Determines the school choice set given home node id and age of the student (4-18)
    Output: Pandas series with available school IDs"""
    
    public_id = distance_matrix_public.iloc[:,50:].loc[house_node_id] + 1000
    private_id = distance_matrix_private.iloc[:,100:].loc[house_node_id] + 1000

    schools_filter = pd.concat([public_id, private_id])
    schools_set = schools[schools.school_id.isin(schools_filter)]
    school_availability = [school_available(kid_age, x) for x in schools_set.list_grades]
    try:
        schools_available = schools_set[school_availability].school_id
        return schools_available
    except Exception as e:
        print(e)
        print("empty dataframe?")
    
    return np.nan

def get_zone_id(parcel_id):
    '''gets the zone_id (TAZ) of school and home locations by the parcel id'''
    try: 
        zone_id = (parcels.iloc[parcel_id])["zone_id"]

    except Exception as e:
#         print(e)
        zone_id = np.nan
    return zone_id
 

def get_parcel_id(school_id):
    try: 
#         parcel_id = (schools.iloc[school_id])["school_parcel_id"]
        parcel_id = schools[schools["school_id"] == school_id]["school_parcel_id"].values[0]
        
    except Exception as e:
        print(e)
        parcel_id = np.nan
#     print(parcel_id)
    return parcel_id
 

#merge persons and hh tables
per_hh = pd.merge(hh, persons, on='household_id')
pums_students = per_hh[(per_hh["age"] >= 5) & (per_hh["age"] <= 18) & (per_hh["student"] ==1)]


stu_pums = pums_students[["household_id", 'node_id_small_x', "age", "zone_id_home", 'hh_inc_under_25k', 'hh_inc_25_to_75k', 'hh_inc_75_to_200k']] 
print("stu pums length: ", len(stu_pums))
test = stu_pums.sample(1000)
test2 = stu_pums.sample(10000)
test3 = stu_pums.sample(30000)


def table_cols(record):
    age = int(record["age"])
    node_id_home = int(record["node_id_small_x"])
    zone_id_home = int(record["zone_id_home"])
    hh_id = int(record["household_id"])
    hh_inc_under_25k = record["hh_inc_under_25k"]
    hh_inc_25_to_75k = record["hh_inc_25_to_75k"]
    hh_inc_75_to_200k = record["hh_inc_75_to_200k"]
    
    school_choices = school_choice_set(node_id_home, age)
    
    d = {"node_id_home": node_id_home, "zone_id_home": zone_id_home, 
                        "age": age, "household_id": hh_id,
                       "school_choice_option": school_choices, "hh_inc_under_25k": hh_inc_under_25k, 
                       "hh_inc_25_to_75k": hh_inc_25_to_75k, "hh_inc_75_to_200k": hh_inc_75_to_200k}
    df = pd.DataFrame(d)
    df.columns = ["node_id_home", "zone_id_home", "age", "household_id",
                  "school_choice_option", "hh_inc_under_25k",  "hh_inc_25_to_75k",  "hh_inc_75_to_200k"]
    return df






def construct(df):
    print(".apply ")
    df1 = df.apply(table_cols, axis=1)
    
    print("concatenating")
    df2 = pd.concat(df1.values)
    print("vector opertions parcel and zone")
    df2["school_parcel_id"] = df2["school_choice_option"].apply(get_parcel_id)
    df2["school_zone_id"] = df2["school_parcel_id"].apply(get_zone_id)
    
    print("merge with beam skims")
    df3 = pd.merge(df2, beam_skims, left_on=['school_zone_id','zone_id_home'], right_on =['to_zone_id','from_zone_id'])
        
    return df3
 
    
 
# pre_iter_end = time.time()   
    
iter_start = time.time()
table = construct(stu_pums)

# print("table columns: ", table.columns)

print("sample size: ", len(stu_pums))
print("table len: ", len(table))
end = time.time()
print("total time elapsed: ", end-start)
print("pre-iter time elapsed: ", iter_start-start)
print(".apply elapsed: ", end-iter_start)

print("writing to .csv")
table.to_csv("/home/amelia/activitysynth/activitysynth/output/SLCM_validation.csv")
print("wrote to .csv")




