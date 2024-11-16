#!/bin/bash

# Path to the virtual environment in the shared 'apps' folder
VENV_PATH="/opt/spark/work-dir/myenv"

# Activate the virtual environment
source "$VENV_PATH/bin/activate"

# Start JupyterLab with specified options
jupyter lab --ip=0.0.0.0 --no-browser