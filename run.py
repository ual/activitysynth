import orca
import warnings

from urbansim_templates import modelmanager as mm
import urbansim_templates

from scripts import models, datasources, variables

warnings.simplefilter('ignore')

model_steps = [
    'initialize_network_small', 'initialize_network_walk',
    'network_aggregations_small', 'network_aggregations_walk',
    'WLCM']

orca.run(model_steps)
