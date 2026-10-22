# Testing

Tests live alongside the implementation in `tests/`, with real product workflow
coverage in `tests/e2e/`. A fresh clone contains the complete released suite.
No second checkout, private-repository key, or live-provider credentials are
required for ordinary pull requests, including contributions from forks.

## Local commands

```bash
python -m pip install -e '.[test]'
./scripts/test-docker.sh
./scripts/e2e-docker.sh
```

Unit tests require at least 70% overall coverage. Both Docker commands mount the
checkout read-only, copy it into the container, and use temporary homes and
databases. E2E tests refuse to run on the host. Screenshots and workflow receipts
are exported to the ignored `.e2e-artifacts/` directory.

Pass pytest arguments to either runner for focused work:

```bash
./scripts/test-docker.sh tests/test_messaging_adapters_workflow.py
./scripts/e2e-docker.sh tests/e2e/test_web_surface_contracts.py
```

Focused unit runs still enforce overall coverage; use `python -m pytest` directly
with a temporary HOME for quick unit-only feedback. Browser, installer and E2E
checks must run in containers or CI runners. Live-stage probes are opt-in and
require separately authorized services and credentials.

The default E2E image is built from `docker/e2e.Dockerfile`. To reuse an already
built image, set `NULLION_E2E_IMAGE` and `NULLION_E2E_SKIP_BUILD=1`; this only skips
the image build and still installs and runs the current checkout.

## CI

The PR workflow checks out the requested commit once. Eight disjoint unit shards
run with temporary homes, then combine coverage at the same 70% threshold.
A separate Docker job runs the complete E2E directory. Linux, Windows and macOS
installer jobs use the installer harnesses from this same commit.
Every workflow has read-only repository permissions and ordinary fork PRs do not
receive secrets. Runtime/provider failures are simulated at external boundaries;
Nullion routes, state stores, rendered UI, artifacts and delivery remain real.

```bash
./scripts/installer-docker-linux.sh
# Windows CI runner:
# .\scripts\installer-docker-windows.ps1
# macOS CI runner:
# ./scripts/installer-macos.sh
```

Do not run installer or managed-service operations against an existing personal
installation. CI installer checks create disposable homes and work directories.
Do not include private logs, runtime databases, credentials, or personal output
in fixtures. Use fictional identities and example paths.

## Local operations dashboard

The dashboard, test runners, and app all come from this repository. A single
work-branch pin selects code and tests together; there is no second repository,
private access key, or paired test branch. E2E jobs execute the selected branch's
own `scripts/e2e-docker.sh` and `tests/`, rather than mixing it with another checkout.

Run `python3 scripts/local_ops_dashboard.py` to serve the dashboard on loopback
port 2020. On macOS, `python3 scripts/local_ops_dashboard.py --install-launchagent`
installs its login service from the current checkout, backs up the previous
LaunchAgent, and restores it if loading fails. This changes only the dashboard
service. Keep its source checkout available; branch cleanup protects it.

Instance configuration, databases, and logs remain in their runtime homes;
these are data directories, not separate Git repositories. Installing the
dashboard does not install, restart, or change the app's prod/stage/dev services.
The former standalone production-switching test launcher is retired; use the
explicit lane controls in this dashboard for runtime operations.
