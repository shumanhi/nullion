# Live stage test categories and overlap audit

Generated from reviewed metadata in `tests/e2e/live_stage_catalog.json` and the current test source.

This is a coverage catalogue, not an execution-status record. Redundant entry points were removed; their unique assertions remain in the canonical tests.

## Counts

The complete source inventory contains **295 potential parameterized entries** across 47 files and 148 test functions, with all 75 API scenarios explicitly selected.

The earlier 276-case combined selection was a particular run selection, not the whole repository inventory. An empty API scenario selection is a skipped placeholder, not a real workflow. A pytest case can contain several requests or scenarios; counts are not model-call counts.

| Execution type | Entries | Meaning |
| --- | ---: | --- |
| `live_workflow` | 272 | A running stage workflow that can consume model or tool quota. One case may send multiple chat requests. |
| `saved_replay` | 14 | Stage UI/history/byte checks of saved evidence; no new test-initiated model or source work. |
| `local_harness` | 9 | Deterministic local fixture/control checks; no real stage/provider workflow. |
| `aggregate_entry_point` | 0 | An alternative multi-scenario entry point; do not combine it with individual scenario cases. |

## Workflow categories

| Category | Entries | Coverage |
| --- | ---: | --- |
| `latency` — Plain chat and latency | 14 | First-response timing, model-call counts, startup and ordinary-turn fast paths. |
| `context` — Context, memory and retained evidence | 33 | Follow-ups, profile facts, history, numbered choices, compaction and exact retained references. |
| `research` — Research and source accuracy | 47 | Search coverage, bounded research, recommendations, prices, source links and missing facts. |
| `browser` — Browser lifecycle and recovery | 21 | Desktop attachment, navigation, screenshots, native delegation, owned tabs and recovery. |
| `artifacts` — Files and generated deliverables | 59 | Document creation, format fidelity, selected versions, downloadable bytes and artifact reuse. |
| `execution` — Code Mode and terminal execution | 17 | Programs, host modules, projections, reusable tools, unsupported imports and terminal outcomes. |
| `scheduler` — Scheduled jobs and live trackers | 42 | Owned job changes, cancellation, finite lifetimes, conditions, alert delivery and saved-run recovery. |
| `consent` — Account access and approvals | 32 | Source consent, read-only account boundaries, explicit choices and action approvals. |
| `presentation` — Progress, rendering and delivery | 21 | Shared status, compact reports, exact captions, Web history, sockets and reload behavior. |
| `harness` — Local test infrastructure | 9 | Deterministic transport, provider-stop controls and static catalogue invariants; no live model requests. |

## Duplicate and overlap findings

- **0 identical test bodies** after ignoring function names, source locations and docstrings. This is a structural check, not proof of semantic uniqueness.
- **48 API/Web scenario pairs** share a base scenario ID. Web sometimes adapts the prompt or requirements; transport, UI, history and artifact assertions also differ. These are overlap candidates, not interchangeable proof.
- **0 test-to-test wrappers remain.** Six redundant wrappers were removed; their extra primary-owner/history assertions now execute inside the canonical workflow. English, Spanish and Japanese consent variants remain collected.
- **0 aggregate pytest entries remain.** The redundant 54-scenario aggregate was removed. Individual API cases and the explicitly invoked standalone probe CLI remain available.
- Seven repeated pytest entries were removed (302 to 295). API/Web overlap still has distinct boundary assertions and is retained; select the affected surface for targeted runs.
- Shared simple/weather preambles are already cached for 30 minutes within one process. Restarting a runner or using different arguments causes new measurements; do not count a reused baseline as a new workflow.

### Retired selectors and canonical replacements

Historical timing records retain their original test IDs. These migrations do not rewrite or merge past measurements.

| Removed entry | Canonical replacement | Reason |
| --- | --- | --- |
| `tests/e2e/test_live_stage_web_producer_completion.py::test_live_web_primary_report_download_without_quality_grading` | `tests/e2e/test_live_stage_web_extended.py::test_live_web_compact_pdf_summary_keeps_heading_and_bullets_together` | Extra ownership/history assertions merged into the canonical workflow. |
| `tests/e2e/test_live_stage_web_producer_completion.py::test_live_web_primary_keeps_native_account_source_consent` | `tests/e2e/test_live_stage_web_extended.py::test_live_web_account_read_waits_for_explicit_source_choice[en]` | Extra ownership/history assertions merged into the canonical workflow. |
| `tests/e2e/test_live_stage_web_producer_generation.py::test_live_web_primary_docx_selects_only_final_version` | `tests/e2e/test_live_stage_web_extended.py::test_live_web_only_final_document_survives_delivery_and_reload` | Extra ownership/history assertions merged into the canonical workflow. |
| `tests/e2e/test_live_stage_web_producer_generation.py::test_live_web_primary_csv_survives_dashboard_polling` | `tests/e2e/test_live_stage_web_extended.py::test_live_web_dashboard_polling_during_generation_keeps_final_file_and_history` | Extra ownership/history assertions merged into the canonical workflow. |
| `tests/e2e/test_live_stage_web_producer_generation.py::test_live_web_primary_file_lookup_followup_does_not_regenerate` | `tests/e2e/test_live_stage_web_extended.py::test_live_web_existing_filename_lookup_does_not_recreate_or_attach_input` | Extra ownership/history assertions merged into the canonical workflow. |
| `tests/e2e/test_live_stage_web_producer_generation.py::test_live_web_primary_plain_chat_stays_fast_in_three_languages` | `tests/e2e/test_live_stage_web_followups.py::test_live_web_plain_turn_latency_across_languages` | Extra ownership/history assertions merged into the canonical workflow. |
| `tests/e2e/test_live_stage_chat_contracts.py::test_live_stage_chat_tool_scope_and_latency_contracts` | `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario` | Use explicit individual API scenarios; standalone probe CLI remains available. |

### Related families needing selective coverage

- **plain-fast-path**: Keep startup phase timing and the one-model-call assertion; run one representative for routine smoke and the others only for changed latency boundaries.
- **final-document-only**: DOCX canonical workflow now includes the primary-owner/history checks. Retain the API draft-manifest case for its separate native boundary.
- **saved-file-reuse**: File-lookup workflow includes primary-owner/history checks once. PDF byte identity remains separate coverage.
- **missing-public-fact**: API and Web check related incomplete-source behavior. Prefer the affected surface; retain source proof and exactly-once rendering assertions.
- **capture-language-variants**: Shared runner and setup, distinct English/Japanese/Arabic prompts. Retain multilingual coverage; choose one for routine capture smoke.
- **archive-transform-and-retry**: Shared archive fixture runner, different workbook columns and forbidden-retry assertions. Not duplicates; retain both for the affected paths.

## Saved selection policy

- **Smoke:** the eight reviewed `smoke` selectors below cover latency, weather, scheduler inventory, generated bytes, history, native source links, worker cleanup and source consent. This is a small acceptance sample, not release-wide proof.
- **Affected category:** inspect selectors for the changed workflow, then include adjacent permissions/delivery/latency checks as needed.
- **Extended:** run remaining distinct live workflows only when broader acceptance is authorized. Choose API or Web representatives for overlapping scenarios; preserve the affected real surface as final proof.
- **Saved replays:** validate captured content, links, bytes and rendering without regenerating expensive results. They require their recorded fixtures and are not fresh production-workflow proof.
- **Local harness:** run in Docker with deterministic local checks, separately from paid acceptance.
- **State and consent:** shared desktop/browser profiles, preferences, memories and named jobs prevent blanket parallelism. Do not infer concurrency safety from a category. Isolate resources before parallel runs. Cases requiring real sends or existing-job execution still require their explicit authorization and prerequisites.

The consolidation changes collected test entry points, not runtime routing, stage configuration or production state.

## Read-only commands

```sh
python3 scripts/live_stage_test_catalog.py
python3 scripts/live_stage_test_catalog.py --category browser --selectors
python3 scripts/live_stage_test_catalog.py --kind saved_replay --selectors
python3 scripts/live_stage_test_catalog.py --set smoke --selectors
python3 scripts/live_stage_test_catalog.py --json
```

These commands only inspect source and print metadata/selectors. They never import tests, launch pytest, contact stage or run provider/tool calls. Selectors follow [pytest collection argument syntax](https://docs.pytest.org/en/stable/how-to/usage.html#specifying-which-tests-to-run).

API scenario selectors also require the matching explicit `NULLION_LIVE_SCENARIOS` selection in the existing live wrapper. An empty selection intentionally runs no API scenario; this utility does not enable provider access or supply authorization.

When adding/removing tests or scenarios, review the JSON assignments and regenerate this document with `python3 scripts/live_stage_test_catalog.py --write-doc docs/testing/live-stage-test-catalog.md`.

### Smoke selectors

- `tests/e2e/test_live_stage_web_startup_latency.py::test_live_stage_plain_requests_preserve_fast_path_and_report_startup_timings`
- `tests/e2e/test_live_stage_web_producer_generation.py::test_live_web_primary_weather_delivers_current_tool_result`
- `tests/e2e/test_live_stage_web_producer_generation.py::test_live_web_primary_inventory_does_not_start_scheduled_work`
- `tests/e2e/test_live_stage_web_reasoning.py::test_live_stage_web_generated_text_delivers_exact_bytes_once`
- `tests/e2e/test_live_stage_web_followups.py::test_live_web_archived_history_recall_from_fresh_conversation`
- `tests/e2e/test_live_stage_web_batched_research.py::test_live_text_reader_preserves_exact_links`
- `tests/e2e/test_live_stage_code_mode.py::test_live_stage_visual_worker_closes_its_tabs`
- `tests/e2e/test_live_stage_web_extended.py::test_live_web_account_read_waits_for_explicit_source_choice[en]`

## Complete category membership

Skip flags below come from declared test decorators. They are not a complete dependency or authorization list; source fixtures, recorded files, accounts and desktop profiles may also be required.

### Plain chat and latency

| Selector | Variants | Execution type | Declared skip flags |
| --- | ---: | --- | --- |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[simple]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[startup_warm_first_chat]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[plain_chat_attachment_planner_fast_path]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_chat_contracts.py::test_live_stage_conceptual_answer_avoids_shell_completion_detour` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_code_mode.py::test_live_stage_plain_turns_keep_the_fast_path` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_batched_research.py::test_live_compact_plain_turns_keep_one_model_call_and_fast_feedback` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_followups.py::test_live_web_plain_turn_latency_across_languages` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_followups.py::test_live_web_conceptual_question_does_not_end_with_shell_receipt` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[simple]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[startup_warm_first_chat]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[plain_chat_attachment_planner_fast_path]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_reasoning.py::test_live_stage_web_plain_requests_retain_fast_path` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_reasoning.py::test_live_stage_web_read_workflow_latency_uses_multiple_samples` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_startup_latency.py::test_live_stage_plain_requests_preserve_fast_path_and_report_startup_timings` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |

### Context, memory and retained evidence

| Selector | Variants | Execution type | Declared skip flags |
| --- | ---: | --- | --- |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[active_followup_context_guard]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[active_related_followup_context_guard]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[reply_anchor_context_guard]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[profile_weather]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[browser_followup_account_scope_guard]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[single_numbered_choice_list_contract]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[chat_history_memory_recall_artifact]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[recent_context_artifact_reuse_chain]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[ambiguous_report_reference]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[artifact_context_followup]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_chat_contracts.py::test_live_stage_flight_correction_keeps_parent_and_delivers_final_pdf` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_chat_contracts.py::test_live_stage_independent_memory_and_history_recall_contract` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_code_mode.py::test_live_same_pdf_keeps_its_bytes_and_thread_identity` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_code_mode.py::test_live_stage_recent_production_run_regressions` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_code_mode.py::test_live_stage_large_transfer_after_numeric_choice` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_code_mode.py::test_live_stage_multi_source_facts_survive_compaction` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_code_mode.py::test_live_stage_local_request_keeps_profile_after_separate_trip` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_compaction_integrity.py::test_large_uploaded_source_preserves_middle_record_in_delivered_report` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_dispatch_handoff.py::test_live_vague_followup_does_not_cancel_generated_file` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_profile_shopping_contract.py::test_live_stage_shopping_and_saved_profile_context` | 1 | `live_workflow` | NULLION_LIVE_PROFILE_CITY_NAMES, NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_user_time_policy.py::test_live_stage_web_uses_saved_time_policy_in_foreign_browser` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_batched_research.py::test_live_history_lookup_updates_the_shared_card_without_fresh_source_work` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_followups.py::test_live_web_archived_history_recall_from_fresh_conversation` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[single_numbered_choice_list_contract]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[profile_weather]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[browser_followup_account_scope_guard]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[chat_history_memory_recall_artifact]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[recent_context_artifact_reuse_chain]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[artifact_context_followup]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[ambiguous_report_reference]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_producer_generation.py::test_live_web_primary_image_bytes_and_followup_keep_selected_subject` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_reasoning.py::test_live_stage_web_checks_contradicted_premise_before_actions` | 2 | `live_workflow` | NULLION_LIVE_STAGE_E2E |

### Research and source accuracy

| Selector | Variants | Execution type | Declared skip flags |
| --- | ---: | --- | --- |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[browser_price_stock_no_files]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[weather]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[browser_not_found_catalog_guard]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[browser_filter_followup]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[browser_rental_live_records]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[browser_flight_live_records]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[market_quote_live_records]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_bounded_comparison.py::test_live_scoped_recommendation_resolves_real_contenders_without_exhaustive_listing_reads` | 1 | `live_workflow` | NULLION_LIVE_BOUNDED_COMPARISON_SOURCE, NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_chat_contracts.py::test_live_stage_source_comparison_retains_verified_quotes` | 1 | `live_workflow` | NULLION_LIVE_SOURCE_COMPARISON_PROMPT, NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_chat_contracts.py::test_live_stage_research_discloses_missing_information_once` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_code_mode.py::test_live_stage_reservation_composes_reads` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_code_mode.py::test_live_stage_recent_recommendations_open_underlying_sources` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_code_mode.py::test_live_stage_quote_keeps_billing_period_separate_from_elapsed_duration` | 2 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_search_coverage.py::test_live_web_follows_discovered_alternate_after_primary_negative` | 1 | `live_workflow` | NULLION_LIVE_COVERAGE_CASE, NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_batched_research.py::test_live_comparison_uses_visible_quantities_without_an_extra_tool_round` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_batched_research.py::test_live_availability_reconsiders_sources_before_giving_up` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_batched_research.py::test_live_source_audit_uses_retained_native_composite_reads` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_batched_research.py::test_live_research_uses_short_reads_and_keeps_plain_and_weather_turns_fast` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_batched_research.py::test_live_text_reader_preserves_exact_links` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_batched_research.py::test_live_readable_news_uses_shared_native_read_policy` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_extended.py::test_live_web_missing_public_fact_is_explained_once_without_losing_findings` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_followups.py::test_live_web_browser_report_synthesis_preserves_helmet_answer` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[weather]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[browser_price_stock_no_files]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[browser_not_found_catalog_guard]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[browser_flight_live_records]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[browser_rental_live_records]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[market_quote_live_records]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[browser_filter_followup]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_event_entities_in_three_languages` | 3 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_producer_generation.py::test_live_web_primary_weather_delivers_current_tool_result` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_research_efficiency.py::test_live_stage_research_releases_only_its_desktop_tabs` | 1 | `live_workflow` | NULLION_LIVE_OBSERVATION_CDP_URL, NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_research_efficiency.py::test_live_stage_bounded_research_delivers_without_unnecessary_expansion` | 2 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_research_efficiency.py::test_live_stage_readonly_deadline_summary_keeps_account_findings` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_research_efficiency.py::test_live_stage_research_then_artifact_preserves_source_and_delivers_file` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_response_proof.py::test_live_web_forecast_and_composed_question_use_different_proof` | 2 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_shopping.py::test_live_web_amazon_powder_comparison_uses_profile_marketplace` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_source_groups.py::test_live_browser_groups_subject_fields_and_delivers_only_specific_limitations` | 2 | `live_workflow` | NULLION_LIVE_BOUNDED_COMPARISON_SOURCE, NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_source_incident.py::test_live_event_artist_research_keeps_original_question` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_source_incident.py::test_live_image_question_followed_by_event_link` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_source_incident.py::test_live_structured_source_record_comparison_keeps_relationships` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |

### Browser lifecycle and recovery

| Selector | Variants | Execution type | Declared skip flags |
| --- | ---: | --- | --- |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[browser_continuation_registry_guard]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[browser_screenshot_artifact_delivery]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[browser_context_capture_recovery]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[browser_context_capture_recovery_variant]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[browser_page_screenshot_artifact_delivery]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_chat_contracts.py::test_live_stage_text_lookup_keeps_browser_evidence_internal` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_code_mode.py::test_live_stage_read_only_code_mode` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_code_mode.py::test_live_stage_inline_read_recovery_stays_in_one_program` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_code_mode.py::test_live_stage_recovers_capture_in_one_code_session` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_code_mode.py::test_live_stage_visual_worker_closes_its_tabs` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_code_mode.py::test_live_stage_flight_form_reaches_interactive_worker` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_code_mode.py::test_live_stage_delegation_runs_native_preflight_without_repair_program` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_failure_attribution.py::test_live_fallback_keeps_confirmed_findings_and_current_uncertainty` | 2 | `live_workflow` | NULLION_LIVE_FALLBACK_EXPECTED_TEXT, NULLION_LIVE_FALLBACK_SOURCE_URL, NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_repair_budget_contract.py::test_live_stage_keeps_answer_after_two_failed_waits` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_batched_research.py::test_live_visual_page_and_text_use_shared_production_and_generic_preparation` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_batched_research.py::test_live_screenshot_followup_keeps_the_current_page_context` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_followups.py::test_live_web_stop_releases_owned_browser_and_next_lookup_completes` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_followups.py::test_live_web_single_page_sdk_lookup_returns_verified_values` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[browser_continuation_registry_guard]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[browser_page_screenshot_artifact_delivery]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |

### Files and generated deliverables

| Selector | Variants | Execution type | Declared skip flags |
| --- | ---: | --- | --- |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[artifact_preflight_scope_fast_path]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[file_search_listing_no_attachment]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[requested_workspace_file_delivery]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[svg_artifact_file_write]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[image]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[email_image]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E; explicit email-send authorization |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[complex_artifact_pack]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[mixed_embedded_media_artifact_package]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[html_research_real_images]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[archive_download_extract_manifest]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[archive_transform_workbook]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[planner_complex_artifact_workbook]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[planner_downloaded_data_workbook]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[artifact_twenty_row_xlsx_delivery]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[random_website_artifact_bundle]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[planner_large_web_dataset_handoff]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[artifact_followup_retry_guard]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[archive_tool_fallback_followup_chain]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[formatted_pdf_flyer_pack]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[kids_playbook_artifact_pack]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[spreadsheet_visual_workbook_pack]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[executive_personal_briefing_pack]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[research_artifact_email_followup_pack]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E; explicit email-send authorization |
| `tests/e2e/test_live_stage_chat_contracts.py::test_live_stage_draft_artifact_is_never_delivered_or_restored` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_code_mode.py::test_live_stage_code_mode_writes_and_delivers_html` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_code_mode.py::test_live_stage_calendar_trip_infographic_delivers_real_image` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_code_mode.py::test_live_stage_calendar_trip_pdf_efficiency` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_code_mode.py::test_live_stage_mixed_pdf_package_has_no_unused_fallback` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_retained_artifact_validation.py::test_real_delivered_report_retains_its_ui_files_bytes_and_original_contract` | 1 | `saved_replay` | NULLION_LIVE_REPORT_REPLAY_FILE, NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_batched_research.py::test_live_research_preserves_schedule_and_delivers_generated_page` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_extended.py::test_live_web_only_final_document_survives_delivery_and_reload` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_extended.py::test_live_web_compact_pdf_summary_keeps_heading_and_bullets_together` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_extended.py::test_live_web_dashboard_polling_during_generation_keeps_final_file_and_history` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_extended.py::test_live_web_existing_filename_lookup_does_not_recreate_or_attach_input` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_file_lookup.py::test_live_web_missing_filename_finishes_without_creating_replacement` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_followups.py::test_live_web_command_named_file_uses_shared_attachment_delivery` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_followups.py::test_live_web_generated_csv_has_one_delivery_and_reload_keeps_it` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_followups.py::test_live_web_archive_extract_transformation_returns_only_final_file` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_followups.py::test_live_web_browser_facts_can_become_a_generated_visual` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_followups.py::test_live_web_browser_facts_do_not_block_raster_image_generation` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_followups.py::test_live_web_rejected_image_provider_does_not_publish_a_placeholder` | 1 | `live_workflow` | NULLION_LIVE_IMAGE_REJECTION_STATUS, NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[svg_artifact_file_write]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[artifact_preflight_scope_fast_path]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[artifact_twenty_row_xlsx_delivery]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[html_research_real_images]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[mixed_embedded_media_artifact_package]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[random_website_artifact_bundle]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[formatted_pdf_flyer_pack]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[spreadsheet_visual_workbook_pack]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[kids_playbook_artifact_pack]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[executive_personal_briefing_pack]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[complex_artifact_pack]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[image]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[archive_download_extract_manifest]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[archive_tool_fallback_followup_chain]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_output_continuation.py::test_live_web_negotiated_output_keeps_goal_and_delivers_generated_png` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_producer_generation.py::test_live_web_primary_xlsx_has_all_twenty_rows` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_reasoning.py::test_live_stage_web_generated_text_delivers_exact_bytes_once` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_trip_completion.py::test_trip_research_completes_with_source_progress_and_verified_pdf` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E, NULLION_LIVE_TRIP_REPLAY_PROMPT |

### Code Mode and terminal execution

| Selector | Variants | Execution type | Declared skip flags |
| --- | ---: | --- | --- |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[structured_tool_id_boundary_guards]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[local_system_terminal_fallback]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[local_system_followup_terminal_fallback]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[local_system_update_followup_terminal_outcome]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_chat_contracts.py::test_live_stage_core_exec_is_discoverable_and_preserves_approval` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_chat_contracts.py::test_live_stage_reusable_tool_creation_and_independent_reuse_contract` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_chat_contracts.py::test_live_stage_existing_source_path_remains_read_only_then_edits_explicitly` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_code_mode.py::test_live_stage_output_projection_preserves_native_findings` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_code_mode.py::test_live_stage_assignment_only_program_returns_page_evidence` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_code_mode.py::test_live_stage_local_renderer_delivers_png_without_image_provider` | 2 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_code_mode.py::test_live_stage_host_modules_create_and_deliver_html` | 2 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_code_mode.py::test_live_stage_terminal_timeout_retains_partial_output_without_rescan` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[local_system_terminal_fallback]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[local_system_followup_terminal_fallback]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[local_system_update_followup_terminal_outcome]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |

### Scheduled jobs and live trackers

| Selector | Variants | Execution type | Declared skip flags |
| --- | ---: | --- | --- |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[scheduler_manual_run]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[scheduler_manual_run_short_followup]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[scheduler_inventory_read_only]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[scheduler_disable_not_delete]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[scheduler_bulk_enable_recovery]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[scheduler_fresh_followup_after_bulk_enable]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[agent_turn_limit_doctor_card]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_chat_contracts.py::test_live_stage_targeted_scheduler_answer_and_core_tools` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_cron_execution_contract.py::test_live_stage_saved_cron_has_valid_scope_and_visible_terminal_outcome` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_live_tracking.py::test_live_stage_web_any_finite_window_and_same_tracker_amendments` | 1 | `live_workflow` | NULLION_LIVE_NEWS_SOURCE, NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_live_tracking.py::test_live_stage_web_temporary_tracking_and_numbered_clarification` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_live_tracking.py::test_live_stage_cnn_developing_section_delivers_one_grounded_match` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_live_tracking.py::test_live_stage_web_expiry_followup_renews_saved_window` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_live_tracking.py::test_live_stage_web_cadence_clarification_uses_actual_tracker_contract` | 2 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_live_tracking.py::test_live_stage_web_numeric_lifetime_and_cadence_creates_article_tracker` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_live_tracking.py::test_live_stage_web_real_semantic_tracker_alerts_only_for_a_new_url` | 1 | `live_workflow` | NULLION_LIVE_NEWS_SOURCE, NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_live_tracking.py::test_live_stage_two_source_tracker_does_not_substitute_the_last_page` | 1 | `live_workflow` | NULLION_LIVE_NEWS_SOURCE, NULLION_LIVE_OBSERVATION_CDP_URL, NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_live_tracking.py::test_live_stage_web_repairs_changed_observation_region` | 1 | `live_workflow` | NULLION_LIVE_NEWS_SOURCE, NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_live_tracking.py::test_live_stage_shared_presentation_and_filter_amendment` | 1 | `live_workflow` | NULLION_LIVE_NEWS_SOURCE, NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_receipt_continuity.py::test_live_receipt_facts_survive_management_only_amendment` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_scheduler_cancellation.py::test_live_confirmation_questions_offer_numeric_choices` | 3 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_scheduler_cancellation.py::test_live_stage_completed_scheduler_followup_and_adjacent_edit` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_scheduler_followups.py::test_live_status_followup_after_enable_does_not_start_the_job` | 3 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_small_mutations.py::test_live_reminder_changes_commit_quickly_and_clean_up_the_owned_record` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_small_mutations.py::test_live_status_update_does_not_invent_a_personal_task` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_snapshot_tracker.py::test_live_stage_web_snapshot_tracker_ignores_clock_and_alerts_on_changed_total` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_snapshot_tracker.py::test_live_stage_web_snapshot_tracker_checks_the_public_incident_source` | 1 | `live_workflow` | NULLION_LIVE_SNAPSHOT_PUBLIC_SOURCE, NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_cron_recovery.py::test_live_saved_cron_finishes_remaining_work_and_persists_evidence` | 4 | `live_workflow` | NULLION_LIVE_CRON_REPLAY_FILE, NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_followups.py::test_live_web_owned_scheduler_rename_and_cancel` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_followups.py::test_live_web_owned_cron_delivers_report_with_actual_data` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[scheduler_inventory_read_only]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_producer_generation.py::test_live_web_primary_inventory_does_not_start_scheduled_work` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_tracker_updates.py::test_live_web_amended_filter_continues_for_new_matches_without_duplicate_alerts` | 1 | `live_workflow` | NULLION_LIVE_NEWS_SOURCE, NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_tracker_updates.py::test_live_web_saved_filter_replay_finishes_alert_delivery_without_recreating` | 1 | `live_workflow` | NULLION_LIVE_SNAPSHOT_REPLAY_NAME, NULLION_LIVE_STAGE_E2E |

### Account access and approvals

| Selector | Variants | Execution type | Declared skip flags |
| --- | ---: | --- | --- |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[account_answer_fulfillment]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[calendar]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[calendar_tomorrow_live_access]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[calendar_weekend_live_access]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[calendar_answer_not_raw_listing]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[github_prs]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[email_send_approval_payload_live]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E; explicit email-send authorization |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[email_read_followup]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[email_urgent_read_recovery]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[email_simple_live_access]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[email_request_overrides_pr_context]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[email_request_overrides_security_history]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[complex_html_email_with_attachments]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E; explicit email-send authorization |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[vague_account_source_selection]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_chat_contracts.py::test_live_stage_email_to_web_answer_preserves_status_and_tool_evidence` | 1 | `live_workflow` | NULLION_LIVE_EMAIL_STATUS_PROMPT, NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_code_mode.py::test_live_stage_pdf_email_shows_native_approval_without_sending` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_code_mode.py::test_live_stage_email_uses_code_session` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_extended.py::test_live_web_account_read_waits_for_explicit_source_choice[en]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_extended.py::test_live_web_account_read_waits_for_explicit_source_choice[es]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_extended.py::test_live_web_account_read_waits_for_explicit_source_choice[ja]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_followups.py::test_live_web_email_pagination_follows_exact_continuation_without_query_partitioning` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[vague_account_source_selection]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[calendar]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[calendar_tomorrow_live_access]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[calendar_weekend_live_access]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[calendar_answer_not_raw_listing]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[email_simple_live_access]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[email_urgent_read_recovery]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[email_read_followup]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[email_request_overrides_pr_context]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[email_request_overrides_security_history]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_matrix.py::test_live_stage_web_catalogue[github_prs]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |

### Progress, rendering and delivery

| Selector | Variants | Execution type | Declared skip flags |
| --- | ---: | --- | --- |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[stage_runtime_patch_regressions]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_api_scenarios.py::test_live_stage_api_scenario[platform_code_fence_formatting]` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_code_mode.py::test_live_stage_progress_is_compact_and_visibly_smaller` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_live_tracking.py::test_live_stage_existing_condition_amendment_render` | 1 | `saved_replay` | NULLION_LIVE_SNAPSHOT_REPLAY_NAME, NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_snapshot_tracker.py::test_live_stage_web_snapshot_alert_survives_reload` | 1 | `saved_replay` | NULLION_LIVE_SNAPSHOT_REPLAY_NAME, NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_batched_research.py::test_live_native_reads_show_meaningful_shared_progress` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_compact_report.py::test_live_compact_scheduled_report_preserves_required_findings` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_followups.py::test_live_web_local_read_does_not_append_activity_when_verbose_is_off` | 1 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_native_report.py::test_native_corrected_report_has_one_title_and_preserves_required_content` | 1 | `saved_replay` | NULLION_LIVE_REVIEW_LAYOUT_REPLAY, NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_report_density.py::test_native_report_is_compact_and_keeps_decision_facts` | 1 | `saved_replay` | NULLION_LIVE_REVIEW_LAYOUT_REPLAY, NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_review_layout.py::test_live_replayed_amazon_report_is_compact_and_readable_in_web_and_saved_history` | 1 | `saved_replay` | NULLION_LIVE_REVIEW_LAYOUT_REPLAY, NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_review_links.py::test_reviewed_recommendation_preserves_actionable_links_after_reload` | 1 | `saved_replay` | NULLION_LIVE_REVIEW_LAYOUT_REPLAY, NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_review_outage.py::test_review_outage_notice_and_report_render_once_without_losing_attachment` | 1 | `saved_replay` | NULLION_LIVE_REVIEW_LAYOUT_REPLAY, NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_saved_report_rendering.py::test_live_saved_report_preserves_facts_in_history_and_rendered_web` | 1 | `saved_replay` | NULLION_LIVE_REPORT_REPLAY_FILE, NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_saved_report_rendering.py::test_live_completed_report_receipt_matches_caption_and_download_bytes` | 1 | `saved_replay` | NULLION_LIVE_REPORT_REPLAY_FILE, NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_saved_report_rendering.py::test_live_captured_package_downloads_and_renders_without_regeneration` | 1 | `saved_replay` | NULLION_LIVE_REPORT_REPLAY_FILE, NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_saved_report_rendering.py::test_live_public_followup_retained_source_and_ui_after_optional_review_outage` | 1 | `saved_replay` | NULLION_LIVE_REPORT_REPLAY_FILE, NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_socket_delivery.py::test_live_socket_returns_a_matching_terminal_result` | 2 | `live_workflow` | NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_source_groups.py::test_live_completed_product_reply_keeps_fields_on_reload_without_another_model_call` | 1 | `saved_replay` | NULLION_LIVE_BOUNDED_COMPARISON_SOURCE, NULLION_LIVE_RECEIPT_REPLAY_ID, NULLION_LIVE_STAGE_E2E |
| `tests/e2e/test_live_stage_web_source_recovery.py::test_reviewed_long_source_answer_survives_web_publication_and_reload` | 1 | `saved_replay` | NULLION_LIVE_REVIEW_LAYOUT_REPLAY, NULLION_LIVE_STAGE_E2E |

### Local test infrastructure

| Selector | Variants | Execution type | Declared skip flags |
| --- | ---: | --- | --- |
| `tests/e2e/test_live_stage_chat_contracts.py::test_live_stage_default_matrix_covers_core_tool_families` | 1 | `local_harness` | See fixture/tool prerequisites |
| `tests/e2e/test_live_stage_provider_guard_boundary.py::test_paid_live_session_stops_after_first_terminal_provider_failure` | 6 | `local_harness` | NULLION_RUN_E2E |
| `tests/e2e/test_live_stage_transport.py::test_stage_transport_waits_before_forwarding_and_never_resends_bytes` | 1 | `local_harness` | See fixture/tool prerequisites |
| `tests/e2e/test_live_stage_transport.py::test_stage_transport_closes_when_connection_deadline_expires` | 1 | `local_harness` | See fixture/tool prerequisites |
