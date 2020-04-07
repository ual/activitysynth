# activitysynth
library for lightweight generation of daily activity plans for a synthetic population

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

## Installation
`git clone https://github.com/ual/activitysynth`

`cd activitysynth`

`python setup.py install`


## Execution
The `run.py` script in the main activitysynth directory will run activitysynth from start to finish. It accepts the following runtime arguments:
- simulation year
  - flag: "--year" or "-y"
  - optional, default: 2010
- data input mode
  - flag: "--data-mode"
  - optional, acceptable values: "csv", "s3", "h5"
    - csv (default): this mode will read all required input data from .csv files in a local data directory specified at the top of the run.py file
    - s3: s3 mode will read/write data from an s3 bucket specified at the top of the run.py file. To use this mode, the user must have their AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables set prior to execution.
    - h5: this mode will read all the required input data from a local .h5 datastore specified at the top of the run.py file
- accessibility variables mode
  - flag: "--access-vars-mode" or "-a"
  - optional, acceptable values: "compute", "stored"
    - compute (default): compute the accessibility variables on the fly, but store them in a local data directory so that they don't need to be re-computed the next time the script is run.
    - stored: if the accessibility variables have been computed on a previous run, use the stored values.

This [demo notebook](https://github.com/ual/activitysynth/blob/master/notebooks/run_all_demo.ipynb) provides an annotated walkthrough of what a typical activitysynth implementation should look like one step at a time.

______________________________________
## Work plan
 - Replace all network aggregation vars with skim-based counterparts (we are currently using both)
 - WLCM
   - [ ] refactor for work-at-home-aware synthetic population
   - [ ] distance-based weighted sampling of alternatives
 - Mode choice
   - [ ] fully template-ized implementation (requires support for interaction terms in small MNL template)
 - TOD choice
   - [ ] fully template-ized implementation (requires support for interaction terms in small MNL template)
 - SLCM
   - [ ] initial implementation
 - Discretionary trips
   - [ ] initial implementation
