test_dirs=`find . -name "tests*" -type d`

EXIT_CODE=0

for test_dir in ${tests_dirs[@]}; do
  executor_dir="$(dirname "test_dir")"
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
  done

echo final exit code = $EXIT_CODE
exit $EXIT_CODE
