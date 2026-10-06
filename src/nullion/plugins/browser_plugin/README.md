# Visual browser tasks

The optional `browser_run_task` tool uses Browser Use 0.13.10 with Nullion's
configured model. It is selected through ordinary structured tool scope; idle
chat never imports Browser Use or starts a browser. Existing low-level browser
tools remain available.

New installations prepare the worker automatically. Existing installations can
run the plugin setup with their application Python:

```sh
python -m nullion.plugins.browser_plugin.browser_use_setup --home ~/.nullion
```

The active lane automatically discovers `browser-use-venv`; an explicit
`NULLION_BROWSER_USE_PYTHON` overrides it. Setup installs Chromium too and checks
worker dependencies without changing the app environment.

For manual setup, install the dependency in a separate virtual environment using
`requirements-browser-use.txt`, then set `NULLION_BROWSER_USE_PYTHON` to that
environment's Python executable. Browser Use pins some SDK/Pillow versions that
conflict with Nullion's main environment, so install it separately. The worker
uses its own dependencies first and falls back to app-only packages from the
parent environment. Model calls use an authenticated ephemeral loopback bridge
to the parent's configured client. No model credentials are passed to the worker.

Each job launches an isolated Chromium profile. Browsing is visible on desktop
systems and headless on Linux when no display is available. Install
Chromium with the worker environment's `python -m playwright install chromium`.
`NULLION_BROWSER_HEADLESS=true` selects headless browsing, which some sites reject.
`NULLION_BROWSER_USE_EXECUTABLE_PATH` may select a compatible local Chromium
binary. The sandbox defaults to enabled; containers which require an unsandboxed
browser must explicitly set `NULLION_BROWSER_USE_SANDBOX=false`.

`NULLION_BROWSER_USE_CDP_URL` can attach to a dedicated browser owned by this
adapter, including a headed browser inside a container. Do not point it at a
personal browser: jobs navigate its active tab. The adapter detaches from an
explicit CDP endpoint and kills only browsers it launched itself. Access to a shared CDP endpoint is serialized between Browser Use jobs across Web and messaging workers. Reserve that endpoint for this adapter; other browser tools should use a separate browser.
A concurrent job reports a busy resource instead of mixing its pages with another request. Browser Use jobs are bounded to 600 seconds and
at most 30 steps; the default is 20 steps.

A task supplies a URL and objective as structured tool arguments. Navigation
and redirects are limited to that target host. Browser policy is checked before
startup; private IPs and blocked domains remain restricted. File upload, raw
JavaScript, filesystem, PDF generation and separate extraction-model actions
are excluded. This tool is for reading/comparing pages, not purchases, sign-in,
or external messages.

The tool captures screenshots automatically and retains only evidence used by
the report plus the last page/blocker capture. Exact quote/value matching grounds
observations against captured DOM and rendered page text. Unknown requirements and partial findings
remain visible; the library's success boolean is not proof of task completion.
The outer chat model summarizes the observations and PNG evidence accompanies
that answer. Intermediate profiles, logs and model transcripts are not delivered.
