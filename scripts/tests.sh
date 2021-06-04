sudo apt-get update && sudo apt-get install python3-venv

test_dirs=`find . -name "tests*" -type d`
echo $test_dirs

EXIT_CODE=0

root_dir=$(pwd)

for test_dir in ${test_dirs[@]}; do
  echo 'checking' $test_dir
  executor_dir="$(dirname "$test_dir")"
  echo 'changing to dir' $executor_dir
  cd $executor_dir
  python -m venv .venv
  pip install pytest pytest-mock
  pip install -r requirements.txt
  pytest -s -v tests/
  local_exit_code=$?
  deactivate
  if [[ ! $local_exit_code == 0 ]]; then
    EXIT_CODE=$local_exit_code
    echo this one failed. local_exit_code = $local_exit_code, exit = $EXIT_CODE
  fi
  cd $root_dir
  done

echo final exit code = $EXIT_CODE
exit $EXIT_CODE
