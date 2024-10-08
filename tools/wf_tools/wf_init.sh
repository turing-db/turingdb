#!/bin/bash
set -eou pipefail

MODULE=$1
TURING_REPO=$2

echo -e "Initiating new module folder named ${MODULE} in $(basename ${TURING_REPO}). "
# let's create the folder
module_base="${TURING_REPO}/src/bioinfo/notebooks/api-notebooks/${MODULE}/baseline_notebooks"
mkdir -p "${module_base}"
# let's create the first notebook
touch "${module_base}/notebook1.ipynb"
# let's crate the template parameter file
yq -n '{"notebook1":{ "param_1": null, "param_2": null }, "notebook2":{"param_1": null, "param_2": null}}' > "${module_base}/default_parameters.json"
# let's create the template for a requirement file
yq -n '{"comment": "remove this file or fill it and rename it"}' > "${module_base}/requirements.json_tmp"
echo -e "[DONE].\n"

echo "Congrats! here is the new module structure:"
cd "${TURING_REPO}/src/bioinfo/notebooks/api-notebooks"
tree "${MODULE}"
