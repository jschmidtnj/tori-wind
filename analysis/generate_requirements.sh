#!/bin/bash

# abort on errors
set -e

env_name="wind"

env_file_name="environment.yml"

source $(conda info --base)/etc/profile.d/conda.sh

conda activate "$env_name"

# save current environment to file
conda env export --no-builds | grep -v "^prefix: " > "$env_file_name"

conda deactivate

echo "done with generating requirements for $env_name"

cd - > /dev/null

exit 0
