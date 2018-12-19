import orca
import warnings

import urbansim_templates

from scripts import models, datasources, variables

warnings.simplefilter('ignore')

if __name__ == "__main__":

    model_steps = [
        'initialize_network_small', 'initialize_network_walk',
        'network_aggregations_small', 'network_aggregations_walk',
        'wlcm_simulate', 'TOD_choice_simulate',
        'auto_ownership_simulate', 'primary_mode_choice_simulate',
        'TOD_distribution_simulate', 'generate_activity_plans']

    orca.run(model_steps)
