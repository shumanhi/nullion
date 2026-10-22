#!/usr/bin/env bash
set -euo pipefail
# Both source mounts are read-only. All installs, runtime data and test output
# live in the container; the contributor checkout and host services stay intact.
mkdir -p /tmp/nullion-src
cp -a /inputs/app/. /tmp/nullion-src/
rm -rf /tmp/nullion-src/.git
mkdir -p /tmp/nullion-src/{tests,scripts,docker}
cp -a /inputs/suite/tests/. /tmp/nullion-src/tests/
cp -a /inputs/suite/scripts/. /tmp/nullion-src/scripts/
cp -a /inputs/suite/docker/. /tmp/nullion-src/docker/
cp /inputs/suite/conftest.py /inputs/suite/docker-compose.e2e.yml /tmp/nullion-src/
cd /tmp/nullion-src
rm -rf .e2e-artifacts
export_results() {
  if [[ -d /outputs ]]; then
    [[ ! -d .e2e-artifacts ]] || cp -a .e2e-artifacts/. /outputs/
    [[ ! -f /tmp/nullion-e2e-coverage.json ]] || cp /tmp/nullion-e2e-coverage.json /outputs/
  fi
}
trap export_results EXIT
export NULLION_APP_REPO="$PWD"
python -m pip install --disable-pip-version-check --no-build-isolation '.[test]'
# pytest arguments are deliberately whitespace-separated, never evaluated as shell code.
read -r -a pytest_args <<< "${NULLION_E2E_PYTEST_ARGS:-tests/e2e}"
if [[ "${NULLION_TEST_COVERAGE:-0}" == 1 ]]; then
  python -m coverage run --source=src/nullion -m pytest -q "${pytest_args[@]}"
  python -m coverage report --fail-under=70
else
  python -m pytest -q "${pytest_args[@]}"
fi
