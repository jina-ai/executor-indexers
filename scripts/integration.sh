#!/bin/bash
set -ex

# always run integration tests
pip install -r jinahub/indexers/tests/requirements.txt
export PYTHONPATH=.
pytest -s -v jinahub/indexers/tests/integration
local_exit_code=$?
if [[ ! $local_exit_code == 0 ]]; then
  EXIT_CODE=$local_exit_code
  echo integration test failed
fi
