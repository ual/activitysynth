import orca
from urbansim_templates import modelmanager as mm
from urbansim_templates.models import LargeMultinomialLogitStep

mm.initialize()

model_steps = [
    'initialize_network_small', 'initialize_network_walk',
    'network_aggregations_small', 'network_aggregations_walk',
    'WLCM', 'TOD_choice_simulate','TOD_distribution_simulate']

orca.run(model_steps)
