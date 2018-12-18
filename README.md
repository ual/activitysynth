# activitysynth
library for lightweight generation of activity plans

## What is it?
This repository was designed in partial satisfaction of the FY19 Q1 deliverable for the U.S. Department of Energy SMART Mobility Urban Science Pillar task 2.2.2: Coupling Land Use Models and Network Flow Models, led by PI Paul Waddell at UC Berkeley.

activitysynth can be thought of as a pared down version of [UrbanSim](https://github.com/UDST/urbansim) comprised of only those models required to generate activity plans for use in travel modeling. activitysynth currently only handles the synthesis of mandatory trips (i.e. commutes to and from work), but will eventually expand in scope to support the generation of full day's worth of activities for an entire synthetic population, including mandatory and discretionary trips for workers and non-workers alike. 

As it currently stands, activitysynth ingests a synthetic population (e.g. [SynthPop](https://github.com/UDST/synthpop)) and performs the following operations upon it:

- Workplace location choice
- Auto ownership
- Primary commute mode choice
- Time of departure (H --> W and W --> H)
- Activity plan file generation

Active development is under way to add the following steps to the activitysynth workflow:

- School location choice 
- Discretionary trips
- Intra-household coordinated activity planning

## Who should use it?
The primary beneficiary of this suite of models is the travel modeling community, specifically those who need to generate the raw materials for their models in a quick and efficient but realistic manner. The activity plan generation step is currently configured per the specification required for integration with the [BEAM](https://github.com/LBNL-UCB-STI/beam) agent-based travel modeling platform, but a [POLARIS](https://github.com/anl-polaris/polaris) specification is on-deck. Our goal is to eventually make activitysynth infinitely compatible with any travel model specified by the end user.

## How to get set up:
activitysynth makes heavy use of both the [Orca](https://github.com/UDST/orca) data pipelining tool and the [urbansim_templates](https://github.com/UDST/urbansim_templates) model design paradigm. All dependencies should be satisfied after running the following installation steps:
```
pip install git+git://github.com/udst/choicemodels.git
pip install git+git://github.com/udst/urbansim_templates.git
```
The demo notebook [here](https://github.com/ual/activitysynth/blob/master/notebooks/run_all_demo.ipynb) provides a step-by-step walkthrough of what a typical activitysynth implementation should look like.

In practice, once the required data inputs and model configurations are in place, a full simulation can be executed in the following two lines of code:
```
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
        'TOD_distribution_simulate']

    orca.run(model_steps)
```
