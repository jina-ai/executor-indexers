#!/bin/bash
# find all the examples with changed code
# run the tests in that directory
changed_folders=()

for changed_file in $CHANGED_FILES; do
  echo changed $changed_file
  file_base_dir=$(dirname $changed_file)
  if [[ ! " ${changed_folders[@]} " =~ " ${file_base_dir} " ]]; then
    echo adding $file_base_dir
    changed_folders+=(${file_base_dir})
  fi
done

echo will run tests on ${changed_folders[@]}

sudo apt-get update && sudo apt-get install python3-venv

EXIT_CODE=0

root_dir=$(pwd)

for test_dir in ${changed_folders[@]}; do
  cd $test_dir
  if [[ -d "tests/" ]]; then
    if test -f "Dockerfile"; then
      docker build -f Dockerfile . -t test_image
      container_name=`docker run -d test_image:latest`
      sleep 2
      if [ $(docker inspect -f '{{.State.Running}}' $container_name) = "true" ]; then
        echo container for $test_dir started successfully
      else
        echo docker container did not start in $test_dir
        local_exit_code=1
      fi
      local_exit_code=$?
      docker stop $container_name
      docker image rm test_image:latest --force
    else
      python -m venv .venv
      pip install pytest pytest-mock
      pip install -r requirements.txt
      pytest -s -v tests/
      local_exit_code=$?
      deactivate
    fi
    if [[ ! $local_exit_code == 0 ]]; then
      EXIT_CODE=$local_exit_code
      echo $test_dir failed. local_exit_code = $local_exit_code, exit = $EXIT_CODE
    fi
  else
    echo 'no tests/ folder here. skipping...'
  fi
  cd $root_dir
  done

# always run integration tests
pip install -r jinahub/indexers/tests/requirements.txt
export PYTHONPATH=.
pytest -s -v jinahub/indexers/tests/integration
local_exit_code=$?
if [[ ! $local_exit_code == 0 ]]; then
  EXIT_CODE=$local_exit_code
  echo integration test failed
fi

echo final exit code = $EXIT_CODE
exit $EXIT_CODE
