import sys

import yaml

from src import pipereg

all_params = yaml.safe_load(open("params.yaml"))

pipereg.get_step(sys.argv[1]).run(all_params)
