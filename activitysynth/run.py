import orca
import pandas as pd
import warnings
import os
import boto3

import urbansim_templates

from activitysynth.scripts import models, datasources, variables

warnings.simplefilter('ignore')

accessibilities_mode = 'recompute'
orca.add_injectable('data_directory', '/home/data/spring_2019/')


if __name__ == "__main__":

    # model_steps = [
    #     'initialize_network_small',
    #     'initialize_network_walk'
    # ]

    # orca.run(model_steps)
    year = 2010

    # if accessibilities_mode == 'recompute':
    #     model_steps = [
    #         'network_aggregations_small', 'network_aggregations_walk']
    #     orca.run(model_steps)

    # elif accessibilities_mode == 'precomputed':
    #     access_vars_dir = '../ual_model_workspace/spring-2019-models/data/'
    #     fname_walk = 'walk_net_vars.csv'
    #     fname_drive = 'drive_net_vars.csv'

    #     walk_net_vars = pd.read_csv(
    #         access_vars_dir + fname_walk, index_col='osmid')
    #     drive_net_vars = pd.read_csv(
    #         access_vars_dir + fname_drive, index_col='osmid')
    #     orca.add_table('nodeswalk', walk_net_vars)
    #     orca.add_table('nodessmall', drive_net_vars)

    # model_steps = [
    #     'wlcm_simulate', 'TOD_choice_simulate',
    #     'auto_ownership_simulate', 'primary_mode_choice_simulate',
    #     'TOD_distribution_simulate', 'generate_activity_plans']

    # orca.run(model_steps)

    s3 = boto3.resource('s3')
    data = open('./data/urbansim_beam_plans.csv', 'rb')
    s3.Bucket('urbansim-beam').put_object(
        Key='activity_plans_{0}.csv'.format(year), Body=data)
