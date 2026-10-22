#!/usr/bin/env python3
"""Probe a running Nullion stage instance through the real chat API.

This is intentionally opt-in: it can spend live provider/tool quota. Use it before
marking tool-scope or delivery fixes ready when the bug was only visible in a
running chat surface.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import functools
import io
import json
import os
import re
import sqlite3
import struct
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from dataclasses import dataclass, replace
from html.parser import HTMLParser
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from uuid import uuid4
import zlib


@dataclass(frozen=True)
class Scenario:
    name: str
    prompt: str
    max_average_ms: int | None
    max_single_ms: int | None
    max_model_start_ms: int | None = None
    required_text_any: tuple[str, ...] = ()
    required_text_all: tuple[str, ...] = ()
    forbidden_text: tuple[str, ...] = ()
    required_tool_any: tuple[str, ...] = ()
    required_tool_all: tuple[str, ...] = ()
    required_tool_or_structured_unavailable: tuple[str, ...] = ()
    forbidden_tool_any: tuple[str, ...] = ()
    required_tool_output_text_any: tuple[str, ...] = ()
    forbidden_tool_output_text: tuple[str, ...] = ()
    followups: tuple[str, ...] = ()
    required_artifact_extensions: tuple[str, ...] = ()
    forbidden_artifact_extensions: tuple[str, ...] = ()
    required_artifact_names: tuple[str, ...] = ()
    require_image_artifact: bool = False
    require_loaded_browser_capture: bool = False
    require_nonuniform_image: bool = False
    required_embedded_media_extensions: tuple[str, ...] = ()
    min_embedded_media_count: int = 0
    require_xlsx_formula: bool = False
    require_xlsx_chart: bool = False
    require_xlsx_conditional_formatting: bool = False
    required_xlsx_sheet_names: tuple[str, ...] = ()
    required_artifact_text_all: tuple[str, ...] = ()
    required_artifact_text_each: tuple[str, ...] = ()
    required_artifact_text_if_tool_unavailable: tuple[tuple[str, str], ...] = ()
    require_visual_assets: bool = False
    require_raster_html_images: bool = False
    min_artifact_count: int = 0
    max_artifact_count: int = 0
    require_no_artifacts: bool = False
    min_xlsx_rows: int = 0
    require_no_tools: bool = False
    allow_approval_required: bool = False
    auto_approve: bool = False
    requires_email_send_flag: bool = False
    runner: str = "http"
    max_numbered_blocks_per_response: int | None = None
    forbidden_after_artifact_tool_any: tuple[str, ...] = ()


_LOCAL_SYSTEM_FORBIDDEN_TOOL_OUTPUT = (
    "ln -sfn",
    "/usr/local/bin/python3",
    "permission denied",
    "filesystem_access",
    "filesystem_mutation_approval_required",
)


_LIVE_ACCESS_UNAVAILABLE_FORBIDDEN = (
    "can't see your live",
    "cannot see your live",
    "don't have live",
    "do not have live",
    "not exposed",
    "not available",
    "tool isn't exposed",
    "tool is not exposed",
    "connector tool",
    "send me either",
    "shared calendar link",
    "pasted agenda",
    "screenshot of your calendar",
    "don't want to guess",
)


_BROWSER_VERIFIED_RESULT_FORBIDDEN = (
    "Source-backed points",
    "Results I found",
    "Top matches I found",
    "Search leads I found",
    "Relevant source points",
    "source leads from search results",
    "still need page-level verification",
    "exact availability, pricing",
    "can't reliably read the live results",
    "cannot reliably read the live results",
    "opened the page, but",
    "vehicleSortBy",
)


SCENARIOS: dict[str, Scenario] = {
    "browser_price_stock_no_files": Scenario(
        name="browser_price_stock_no_files",
        prompt=("Open https://books.toscrape.com/catalogue/a-light-in-the-attic_1000/index.html "
                "and read the displayed price and number of copies in stock. Return those values and the "
                "product link. Do not purchase or attach files."),
        max_average_ms=45000, max_single_ms=60000,
        required_text_all=("£51.77", "22"),
        required_tool_any=("browser_extract_detail", "browser_assert_page_state"),
        required_tool_output_text_any=("In stock (22 available)",),
        forbidden_tool_any=("file_write", "pdf_create", "browser_screenshot"),
        forbidden_text=("not every requested part", "task is still open"),
        require_no_artifacts=True,
    ),
    "active_followup_context_guard": Scenario(
        name="active_followup_context_guard",
        prompt=(
            "Create a stage QA artifact package about fictional transit maintenance. Make a PDF summary, "
            "a CSV with 8 rows, and a self-contained HTML page. Keep it test-only and include "
            "STAGE ACTIVE FOLLOWUP QA."
        ),
        max_average_ms=240000,
        max_single_ms=180000,
        required_text_any=(),
        forbidden_text=(
            "active task",
            "use that after the active task",
            "attach that to the active task",
            "STAGE ACTIVE FOLLOWUP QA",
        ),
        required_artifact_extensions=(".pdf", ".csv", ".html"),
        required_artifact_text_all=("STAGE ACTIVE FOLLOWUP QA",),
        require_visual_assets=True,
        min_artifact_count=3,
        runner="active_followup_context_guard",
    ),
    "active_related_followup_context_guard": Scenario(
        name="active_related_followup_context_guard",
        prompt=(
            "Create a self-contained HTML file named stage_active_related_followup.html about synthetic "
            "station signage QA. Include the phrase STAGE ACTIVE RELATED FOLLOWUP QA, a title, a small "
            "table, and one CSS block. Attach only the HTML file."
        ),
        followups=(
            "And regenerate the HTML with a section named Related follow-up revision before you attach it.",
        ),
        max_average_ms=240000,
        max_single_ms=240000,
        required_text_any=("html", "attached", "stage_active_related_followup"),
        forbidden_text=("which html", "what html", "which file", "which report", "saved chat target"),
        required_artifact_extensions=(".html",),
        required_artifact_names=("stage_active_related_followup.html",),
        required_artifact_text_all=("STAGE ACTIVE RELATED FOLLOWUP QA", "Related follow-up revision"),
        min_artifact_count=1,
        runner="active_related_followup_context_guard",
    ),
    "simple": Scenario(
        name="simple",
        prompt="Hi",
        max_average_ms=9000,
        max_single_ms=15000,
        forbidden_text=("tools available", "cannot help", "cannot directly"),
    ),
    "startup_warm_first_chat": Scenario(
        name="startup_warm_first_chat",
        prompt="Hi",
        max_average_ms=5000,
        max_single_ms=8000,
        max_model_start_ms=1500,
        forbidden_text=("tools available", "cannot help", "cannot directly"),
        require_no_artifacts=True,
        require_no_tools=True,
    ),
    "plain_chat_attachment_planner_fast_path": Scenario(
        name="plain_chat_attachment_planner_fast_path",
        prompt="How are you?",
        max_average_ms=5000,
        max_single_ms=8000,
        forbidden_text=("tools available", "cannot help", "cannot directly"),
        require_no_artifacts=True,
        require_no_tools=True,
    ),
    "structured_tool_id_boundary_guards": Scenario(
        name="structured_tool_id_boundary_guards",
        prompt="Run live stage structured tool id boundary guard checks.",
        max_average_ms=15000,
        max_single_ms=15000,
        runner="structured_tool_id_boundary_guards",
    ),
    "artifact_preflight_scope_fast_path": Scenario(
        name="artifact_preflight_scope_fast_path",
        prompt=(
            "Create a tiny test-only artifact pack named stage_latency_probe with a one-page PDF note, "
            "a CSV containing two rows, and a self-contained HTML page. Use concise synthetic content."
        ),
        max_average_ms=120000,
        max_single_ms=120000,
        max_model_start_ms=2500,
        required_text_any=(),
        forbidden_text=("couldn't attach", "task is still open"),
        required_artifact_extensions=(".pdf", ".csv", ".html"),
    ),
    "stage_runtime_patch_regressions": Scenario(
        name="stage_runtime_patch_regressions",
        prompt="Run live stage runtime patch regression checks.",
        max_average_ms=15000,
        max_single_ms=15000,
        runner="stage_runtime_patch_regressions",
    ),
    "file_search_listing_no_attachment": Scenario(
        name="file_search_listing_no_attachment",
        prompt="Run live stage file-search listing delivery guard checks.",
        max_average_ms=60000,
        max_single_ms=75000,
        required_text_any=("pdf", "stage_file_search_delivery_guard"),
        forbidden_text=("MEDIA:", "attached the requested"),
        required_tool_any=("file_search",),
        require_no_artifacts=True,
        runner="file_search_listing_no_attachment",
    ),
    "requested_workspace_file_delivery": Scenario(
        name="requested_workspace_file_delivery",
        prompt="Run live stage requested workspace file delivery checks.",
        max_average_ms=90000,
        max_single_ms=120000,
        forbidden_text=("I found 1 file", "I found files", "MEDIA:", "artifact is not available"),
        forbidden_tool_any=("spreadsheet_create", "file_read", "archive_extract"),
        required_artifact_extensions=(".xlsx",),
        runner="requested_workspace_file_delivery",
    ),
    "local_system_terminal_fallback": Scenario(
        name="local_system_terminal_fallback",
        prompt=(
            "Inspect the local Python runtime version on this computer. Do not update anything. "
            "Use local shell diagnostics if needed and give a short answer."
        ),
        max_average_ms=45000,
        max_single_ms=60000,
        required_text_any=("python", "version"),
        forbidden_text=("I found 2 files", "I found files"),
        required_tool_any=("terminal_exec",),
        forbidden_tool_any=("file_search",),
        forbidden_tool_output_text=_LOCAL_SYSTEM_FORBIDDEN_TOOL_OUTPUT,
        require_no_artifacts=True,
        allow_approval_required=True,
        auto_approve=True,
    ),
    "local_system_followup_terminal_fallback": Scenario(
        name="local_system_followup_terminal_fallback",
        prompt=(
            "I want to verify the Python runtime installed on this computer. Do not change anything yet."
        ),
        followups=(
            "Can you check whether it is version 3.11 using local shell diagnostics? Do not update anything.",
        ),
        max_average_ms=70000,
        max_single_ms=90000,
        required_text_any=("python", "3.11", "version"),
        forbidden_text=("I found 4 files", "I found files", "site-packages"),
        required_tool_any=("terminal_exec",),
        forbidden_tool_any=("file_search",),
        forbidden_tool_output_text=_LOCAL_SYSTEM_FORBIDDEN_TOOL_OUTPUT,
        require_no_artifacts=True,
        allow_approval_required=True,
        auto_approve=True,
    ),
    "local_system_update_followup_terminal_outcome": Scenario(
        name="local_system_update_followup_terminal_outcome",
        prompt=(
            "Check the latest version of Python on my computer. Use local shell diagnostics and do not change anything."
        ),
        followups=(
            "Can u update it to version 3.11? Do not actually install or modify anything in this live test; "
            "use local shell diagnostics and tell me whether Python 3.11 is installed or what local command would be needed.",
        ),
        max_average_ms=70000,
        max_single_ms=90000,
        required_text_any=("python", "3.11", "installed", "command", "would"),
        forbidden_text=("I found 1 file", "I found files", "site-packages\n", "bin\n"),
        required_tool_any=("terminal_exec",),
        forbidden_tool_any=("file_search",),
        forbidden_tool_output_text=_LOCAL_SYSTEM_FORBIDDEN_TOOL_OUTPUT,
        require_no_artifacts=True,
        allow_approval_required=True,
        auto_approve=True,
    ),
    "platform_code_fence_formatting": Scenario(
        name="platform_code_fence_formatting",
        prompt="Run live stage platform code-fence formatting checks.",
        max_average_ms=15000,
        max_single_ms=15000,
        runner="platform_code_fence_formatting",
    ),
    "svg_artifact_file_write": Scenario(
        name="svg_artifact_file_write",
        prompt=(
            "Create a downloadable SVG file named stage_svg_live_contract_{run_id}.svg. "
            "It should be a valid standalone SVG with a blue star and the text STAGE SVG QA. "
            "Attach only the SVG artifact; do not paste the raw SVG code in chat."
        ),
        max_average_ms=90000,
        max_single_ms=120000,
        required_text_any=(),
        forbidden_text=("'''svg", "```svg", "<svg"),
        required_tool_any=("file_write",),
        required_artifact_extensions=(".svg",),
    ),
    "account_answer_fulfillment": Scenario(
        name="account_answer_fulfillment",
        prompt="Run live stage account-answer fulfillment checks.",
        max_average_ms=15000,
        max_single_ms=15000,
        runner="account_answer_fulfillment",
    ),
    "reply_anchor_context_guard": Scenario(
        name="reply_anchor_context_guard",
        prompt="Run live stage reply-anchor context guard checks.",
        max_average_ms=15000,
        max_single_ms=15000,
        runner="reply_anchor_context_guard",
    ),
    "weather": Scenario(
        name="weather",
        prompt="What's the weather like in 10024, New York?",
        max_average_ms=30000,
        max_single_ms=45000,
        required_text_any=("°", "F", "forecast", "weather", "temperature"),
        forbidden_text=("could not resolve", "send me a", "ZIP code"),
        required_tool_any=("weather_forecast",),
    ),
    "profile_weather": Scenario(
        name="profile_weather",
        prompt="What's the weather like",
        max_average_ms=30000,
        max_single_ms=45000,
        required_text_any=("°", "F", "forecast", "weather", "temperature"),
        forbidden_text=("need your location", "send a city", "postal code", "zip code", "could not resolve"),
        required_tool_any=("weather_forecast",),
    ),
    "calendar": Scenario(
        name="calendar",
        prompt="Check my calendar",
        max_average_ms=45000,
        max_single_ms=60000,
        required_text_any=("calendar", "event", "schedule", "upcoming", "today"),
        forbidden_text=(
            "404",
            "route i tried returned",
            "different calendar source",
            "which one you want",
            "weather in",
            "temperature",
            "°f",
            *_LIVE_ACCESS_UNAVAILABLE_FORBIDDEN,
        ),
        required_tool_any=("calendar_list", "connector_request"),
        forbidden_tool_any=("weather_forecast",),
        required_tool_output_text_any=("calendar", "event"),
    ),
    "calendar_tomorrow_live_access": Scenario(
        name="calendar_tomorrow_live_access",
        prompt="What's in my calendar for tomorrow?",
        max_average_ms=60000,
        max_single_ms=90000,
        required_text_any=("calendar", "event", "events", "schedule", "tomorrow", "nothing", "no events"),
        forbidden_text=(
            "wrong schedule",
            "weather in",
            "temperature",
            "°f",
            *_LIVE_ACCESS_UNAVAILABLE_FORBIDDEN,
        ),
        required_tool_any=("calendar_list", "connector_request"),
        forbidden_tool_any=("weather_forecast",),
        required_tool_output_text_any=("calendar", "event"),
        require_no_artifacts=True,
    ),
    "calendar_weekend_live_access": Scenario(
        name="calendar_weekend_live_access",
        prompt="Whats in my calendar for this weekend?",
        max_average_ms=60000,
        max_single_ms=90000,
        required_text_any=("calendar", "event", "events", "schedule", "weekend", "saturday", "sunday", "nothing", "no events"),
        forbidden_text=(
            "wrong weekend schedule",
            "weather in",
            "temperature",
            "°f",
            *_LIVE_ACCESS_UNAVAILABLE_FORBIDDEN,
        ),
        required_tool_any=("calendar_list", "connector_request"),
        forbidden_tool_any=("weather_forecast",),
        required_tool_output_text_any=("calendar", "event"),
        require_no_artifacts=True,
    ),
    "calendar_answer_not_raw_listing": Scenario(
        name="calendar_answer_not_raw_listing",
        prompt=(
            "Can you check whether the event titled Wrecked is on my calendar this weekend? "
            "Answer yes or no first; do not dump unrelated calendar rows."
        ),
        max_average_ms=45000,
        max_single_ms=60000,
        required_text_any=("calendar", "Wrecked", "event"),
        forbidden_text=(
            "I checked your calendar. Found",
            "📅 I checked your calendar. Found",
            "weather in",
            "temperature",
            "°f",
            *_LIVE_ACCESS_UNAVAILABLE_FORBIDDEN,
        ),
        required_tool_any=("calendar_list", "connector_request"),
        forbidden_tool_any=("weather_forecast",),
        required_tool_output_text_any=("calendar", "event"),
    ),
    "scheduler_manual_run": Scenario(
        name="scheduler_manual_run",
        prompt=(
            "Choose exactly one enabled scheduled task from my current cron list and start a manual run now. "
            "Use the scheduler tools; do not just list the crons."
        ),
        max_average_ms=45000,
        max_single_ms=60000,
        required_text_any=("manual scheduled task run", "started", "delivered"),
        forbidden_text=("not exposed", "not available", "couldn't actually start", "just list", "STDOUT", "EXIT CODE"),
        required_tool_all=("list_crons", "run_cron"),
        runner="scheduler_manual_run",
    ),
    "scheduler_manual_run_short_followup": Scenario(
        name="scheduler_manual_run_short_followup",
        prompt="Run live stage scheduler short follow-up manual run contract checks.",
        max_average_ms=90000,
        max_single_ms=120000,
        required_text_any=("manual scheduled task run", "started", "delivered"),
        forbidden_text=("not exposed", "not available", "couldn't actually start", "just list", "STDOUT", "EXIT CODE"),
        required_tool_any=("run_cron",),
        runner="scheduler_manual_run_short_followup",
    ),
    "scheduler_inventory_read_only": Scenario(
        name="scheduler_inventory_read_only",
        prompt=(
            "Show my scheduled-task inventory only. Do not start, rerun, or trigger any scheduled task. "
            "Reply with a concise numbered inventory."
        ),
        max_average_ms=60000,
        max_single_ms=60000,
        required_text_any=("scheduled", "task", "cron", "inventory"),
        forbidden_text=("Which one should I use", "Reply with the number", "Manual scheduled task run", "started a manual run"),
        required_tool_any=("list_crons",),
        forbidden_tool_any=("run_cron",),
        require_no_artifacts=True,
    ),
    "scheduler_disable_not_delete": Scenario(
        name="scheduler_disable_not_delete",
        prompt="Disable a disposable stage cron fixture without deleting it.",
        max_average_ms=90000,
        max_single_ms=120000,
        required_text_any=("disabled", "cron", "scheduled"),
        forbidden_text=("deleted", "removed", "I found 1 file", ".noop", "not exposed", "not available"),
        required_tool_any=("toggle_cron", "update_cron"),
        required_tool_all=("list_crons",),
        forbidden_tool_any=("file_search",),
        require_no_artifacts=True,
        runner="scheduler_disable_not_delete",
    ),
    "scheduler_bulk_enable_recovery": Scenario(
        name="scheduler_bulk_enable_recovery",
        prompt="Enable all of my crons",
        max_average_ms=90000,
        max_single_ms=120000,
        required_text_any=("enabled", "cron", "scheduled"),
        forbidden_text=("I found 1 file", ".noop", "file:", "not exposed", "not available"),
        required_tool_any=("toggle_cron", "update_cron"),
        forbidden_tool_any=("file_search",),
        require_no_artifacts=True,
        runner="scheduler_bulk_enable_recovery",
    ),
    "scheduler_fresh_followup_after_bulk_enable": Scenario(
        name="scheduler_fresh_followup_after_bulk_enable",
        prompt="Enable all of my crons",
        max_average_ms=120000,
        max_single_ms=150000,
        required_text_any=("enabled", "cron", "scheduled"),
        forbidden_text=("I found 1 file", ".noop", "file:", "not exposed", "not available"),
        required_tool_any=("toggle_cron", "update_cron"),
        forbidden_tool_any=("file_search", "run_cron"),
        require_no_artifacts=True,
        runner="scheduler_fresh_followup_after_bulk_enable",
    ),
    "image": Scenario(
        name="image",
        prompt="Generate an image of a monkey going to a lake house",
        max_average_ms=120000,
        max_single_ms=150000,
        forbidden_text=("cannot generate", "tools available", "image prompt you can use"),
        required_tool_any=("image_generate",),
        require_image_artifact=True,
    ),
    "github_prs": Scenario(
        name="github_prs",
        prompt="Check open pull requests for https://github.com/shumanhi/nullion",
        max_average_ms=45000,
        max_single_ms=60000,
        required_text_any=("shumanhi/nullion", "pull request", "pulls", "open"),
        forbidden_text=("send one of these", "repository url", "github account connection", "need the repository"),
        required_tool_any=("browser_navigate", "browser_extract_text", "web_fetch", "terminal_exec", "connector_request"),
    ),
    "browser_not_found_catalog_guard": Scenario(
        name="browser_not_found_catalog_guard",
        prompt=(
            "Any tickets for Broadway shows tonight? Give a concise list with show names and starting prices "
            "if the browser page has ticket options."
        ),
        max_average_ms=90000,
        max_single_ms=120000,
        required_text_any=("$", "from", "ticket", "show"),
        forbidden_text=(
            "sorry, but the page you were looking for was not found",
            "skip to main content",
            "back to top",
            "privacy policy",
            "website terms",
        ),
        required_tool_any=("browser_navigate", "browser_extract_text", "web_fetch"),
        require_no_artifacts=True,
    ),
    "browser_filter_followup": Scenario(
        name="browser_filter_followup",
        prompt="Find the final price for a sedan rental next Saturday, pickup 8 am near Upper West Side in Manhattan, driver age 39, return around 9 pm.",
        followups=("Compare a second nearby pickup location too.",),
        max_average_ms=90000, max_single_ms=120000,
        forbidden_text=("task is still open", "enough verified evidence", "couldn't complete the requested operation"),
        required_tool_any=("browser_navigate", "browser_extract_text", "browser_extract_items", "web_search"),
        require_no_artifacts=True,
    ),
    "browser_rental_live_records": Scenario(
        name="browser_rental_live_records",
        prompt=(
            "Can you find a rental car on Enterprise from downtown Providence, RI from Jun 29 pickup "
            "after 12pm and return on Jul 5? Give actual available options with prices, or name the "
            "specific live-search blocker if no verified result records can be produced."
        ),
        max_average_ms=240000,
        max_single_ms=240000,
        required_text_any=("$", "price", "total", "blocker", "unable to complete"),
        forbidden_text=_BROWSER_VERIFIED_RESULT_FORBIDDEN,
        required_tool_any=("browser_navigate", "browser_extract_text", "browser_extract_items", "browser_run_js", "web_fetch"),
        require_no_artifacts=True,
        auto_approve=True,
    ),
    "browser_flight_live_records": Scenario(
        name="browser_flight_live_records",
        prompt=(
            "Can you find a flight from JFK to Chicago for this weekend? Give actual flight options "
            "with prices and carriers, or name the specific live-search blocker if no verified result "
            "records can be produced."
        ),
        max_average_ms=240000,
        max_single_ms=240000,
        required_text_any=("$", "price", "airline", "flight", "blocker", "unable to complete"),
        forbidden_text=_BROWSER_VERIFIED_RESULT_FORBIDDEN,
        required_tool_any=("browser_navigate", "browser_extract_text", "browser_extract_items", "browser_run_js", "web_fetch"),
        require_no_artifacts=True,
        auto_approve=True,
    ),
    "market_quote_live_records": Scenario(
        name="market_quote_live_records",
        prompt=(
            "Give me current market prices for CRML, MSFT, OKLO, and TMC. Use live market quote data "
            "when available, but do not give search-result leads; return a concise table-like answer "
            "with the ticker, company, current price, and source basis."
        ),
        max_average_ms=120000,
        max_single_ms=180000,
        required_text_any=("CRML", "MSFT", "OKLO", "TMC", "$"),
        forbidden_text=(
            *_BROWSER_VERIFIED_RESULT_FORBIDDEN,
            "Advertisement",
            "Robinhood",
            "Yahoo",
            "Trailing total returns",
            "link:",
        ),
        required_tool_any=("market_quote",),
        require_no_artifacts=True,
    ),
    "browser_continuation_registry_guard": Scenario(
        name="browser_continuation_registry_guard",
        prompt=(
            "Can you research NYC Citi Bike e-bike helmet requirements for adults? "
            "Give a concise answer with the source basis."
        ),
        max_average_ms=240000,
        max_single_ms=240000,
        required_text_any=("helmet", "adult", "citi bike", "citibike", "e-bike", "ebike"),
        forbidden_text=(
            *_BROWSER_VERIFIED_RESULT_FORBIDDEN,
            "unknown tool",
            "unavailable/unknown tool",
            "not actually callable",
            "resend the task",
            "enable/request the browser tool scope",
        ),
        required_tool_any=("browser_navigate", "browser_extract_text", "browser_extract_items", "web_fetch"),
        forbidden_tool_output_text=("unknown tool: browser_snapshot", "unavailable/unknown tool"),
        require_no_artifacts=True,
    ),
    "browser_followup_account_scope_guard": Scenario(
        name="browser_followup_account_scope_guard",
        prompt=(
            "Research a distinctive sculptural table lamp using public web sources. "
            "Give the strongest current match and source link."
        ),
        followups=("Try another public web search source and continue the same research.",),
        max_average_ms=240000,
        max_single_ms=240000,
        required_text_any=("lamp", "source", "http"),
        forbidden_text=("Source-backed points", "checked your email", "matching messages", "email thread"),
        required_tool_any=("browser_navigate", "browser_extract_text", "browser_extract_items", "web_search"),
        forbidden_tool_any=("email_search", "email_read", "calendar_list"),
        require_no_artifacts=True,
    ),
    "browser_screenshot_artifact_delivery": Scenario(
        name="browser_screenshot_artifact_delivery",
        prompt="Send me a screenshot of the cart",
        max_average_ms=180000,
        max_single_ms=180000,
        forbidden_text=("Results I found", "Top matches I found", "Search leads I found"),
        required_tool_any=("browser_screenshot",),
        required_artifact_extensions=(".png",),
        require_image_artifact=True,
        min_artifact_count=1,
        runner="browser_screenshot_artifact_delivery",
    ),
    "browser_context_capture_recovery": Scenario(
        name="browser_context_capture_recovery",
        prompt="表示中のページのスクリーンショットを送ってください。",
        max_average_ms=35000, max_single_ms=60000,
        required_tool_any=("browser_screenshot",), required_artifact_extensions=(".png",),
        require_image_artifact=True, require_loaded_browser_capture=True,
        require_nonuniform_image=True, min_artifact_count=1,
        runner="browser_screenshot_artifact_delivery",
    ),
    "browser_context_capture_recovery_variant": Scenario(
        name="browser_context_capture_recovery_variant",
        prompt="أرسل صورة الصفحة المفتوحة الآن.",
        max_average_ms=35000, max_single_ms=60000,
        required_tool_any=("browser_screenshot",), required_artifact_extensions=(".png",),
        require_image_artifact=True, require_loaded_browser_capture=True,
        require_nonuniform_image=True, min_artifact_count=1,
        runner="browser_screenshot_artifact_delivery",
    ),
    "browser_page_screenshot_artifact_delivery": Scenario(
        name="browser_page_screenshot_artifact_delivery",
        prompt="Go to https://example.com and send me a screenshot of the page.",
        max_average_ms=120000,
        max_single_ms=180000,
        forbidden_text=("Results I found", "Top matches I found", "Search leads I found", "I found", "source points"),
        required_tool_any=("browser_screenshot",),
        required_artifact_extensions=(".png",),
        require_image_artifact=True,
        require_loaded_browser_capture=True,
        require_nonuniform_image=True,
        min_artifact_count=1,
    ),
    "agent_turn_limit_doctor_card": Scenario(
        name="agent_turn_limit_doctor_card",
        prompt="Run live stage agent turn limit Doctor approval card contract checks.",
        max_average_ms=15000,
        max_single_ms=15000,
        runner="agent_turn_limit_doctor_card",
    ),
    "email_image": Scenario(
        name="email_image",
        prompt='Send an email to recipient@example.com with subject "Hi" saying "Hi" and include an image of a dog.',
        max_average_ms=120000,
        max_single_ms=150000,
        required_text_any=("email sent", "recipient@example.com"),
        forbidden_text=("cannot send", "cannot generate", "delivery_confirmation", "json confirmation"),
        required_tool_any=("email_send",),
        require_image_artifact=True,
        allow_approval_required=True,
        auto_approve=True,
        requires_email_send_flag=True,
    ),
    "email_send_approval_payload_live": Scenario(
        name="email_send_approval_payload_live",
        prompt=(
            "Send a short test-only email to recipient@example.com. The subject must start with "
            "Nullion stage approval payload. The body must say this is a live stage approval payload test."
        ),
        max_average_ms=120000,
        max_single_ms=150000,
        required_text_any=("email", "sent", "recipient@example.com"),
        forbidden_text=(
            "couldn't create a usable approval request",
            "no changes were made",
            "cannot send",
            "tool isn't exposed",
            "tool is not exposed",
        ),
        required_tool_any=("email_send",),
        allow_approval_required=True,
        auto_approve=True,
        requires_email_send_flag=True,
        require_no_artifacts=True,
        runner="email_send_approval_payload_live",
    ),
    "email_read_followup": Scenario(
        name="email_read_followup",
        prompt="Check my unread inbox emails from the last day",
        followups=("Open the second email from that list and summarize the full body.",),
        max_average_ms=150000,
        max_single_ms=180000,
        required_text_any=("summary", "from", "subject"),
        forbidden_text=("only have access to the search preview", "full email-read tool is available", "tool wasn't available"),
        required_tool_any=("email_read",),
        forbidden_tool_any=("browser_navigate", "browser_extract_text", "browser_extract_items", "web_fetch"),
    ),
    "email_urgent_read_recovery": Scenario(
        name="email_urgent_read_recovery",
        prompt="Anything urgent in my email overnight",
        max_average_ms=90000,
        max_single_ms=120000,
        required_text_any=("email", "mail", "urgent", "inbox", "message", "connection"),
        forbidden_text=(
            "same email read step failed",
            "i stopped because the same email read step failed",
            "no valid email message id was provided",
            "please send the email message id",
            "latest saved context",
            "saved chat target",
        ),
        required_tool_any=("email_search", "email_read"),
    ),
    "email_simple_live_access": Scenario(
        name="email_simple_live_access",
        prompt="Check my email",
        max_average_ms=90000,
        max_single_ms=120000,
        required_text_any=("email", "mail", "inbox", "message", "unread", "no new"),
        forbidden_text=(
            "don't have live email access",
            "do not have live email access",
            "can't access email",
            "cannot access email",
            "email isn't connected",
            "email is not connected",
            "tool isn't exposed",
            "tool is not exposed",
            "this turn right now",
            "send me",
            "paste",
        ),
        required_tool_any=("email_search", "email_read", "connector_request"),
        forbidden_tool_any=(
            "email_attachment_read",
            "browser_navigate",
            "browser_extract_text",
            "browser_extract_items",
            "browser_run_js",
            "web_fetch",
            "terminal_exec",
            "file_read",
            "file_search",
        ),
        required_tool_output_text_any=("email", "mail", "gmail", "message"),
        require_no_artifacts=True,
    ),
    "email_request_overrides_pr_context": Scenario(
        name="email_request_overrides_pr_context",
        prompt="Check open pull requests for https://github.com/shumanhi/nullion",
        followups=("Check my email",),
        max_average_ms=90000,
        max_single_ms=120000,
        required_text_any=("email", "mail", "inbox", "message", "unread", "connection"),
        required_tool_any=("email_search", "email_read"),
        forbidden_text=(
            "saved chat target",
            "latest saved context",
            "github.com/shumanhi/nullion/pull",
            "pull request #",
            "i believe you mean pr",
            "last known status",
        ),
    ),
    "email_request_overrides_security_history": Scenario(
        name="email_request_overrides_security_history",
        prompt=(
            "Record this reference for later: https://myaccount.google.com/notifications is the Google security "
            "notifications page. Do not check email for this setup turn."
        ),
        followups=("Check my emails",),
        max_average_ms=150000,
        max_single_ms=180000,
        required_text_any=("email", "mail", "inbox", "message", "unread", "connection"),
        required_tool_any=("email_search", "email_read"),
        forbidden_tool_any=("browser_navigate", "browser_extract_text", "browser_extract_items", "web_fetch"),
        forbidden_text=(
            "saved chat target",
            "latest saved context",
            "myaccount.google.com/notifications",
            "google security notifications",
            "connected access",
            "i did not get a fresh live check",
        ),
    ),
    "complex_artifact_pack": Scenario(
        name="complex_artifact_pack",
        prompt=(
            "Use public museum websites to research three current exhibits. Create a multi-format deliverable pack: "
            "a DOCX report, PPTX briefing deck, XLSX comparison workbook with images, CSV data export, PDF visual "
            "summary, self-contained HTML page, and linked HTML page with sibling image assets. Include source URLs, "
            "use embedded/source images where appropriate, and do not attach internal notes or placeholder text files."
        ),
        max_average_ms=240000,
        max_single_ms=300000,
        required_text_any=(),
        forbidden_text=("placeholder.txt", "couldn't attach", "task is still open"),
        required_tool_any=(
            "browser_navigate",
            "browser_extract_text",
            "browser_extract_items",
            "browser_image_collect",
            "document_create",
            "presentation_create",
            "spreadsheet_create",
            "pdf_create",
            "file_write",
        ),
        required_artifact_extensions=(".docx", ".pptx", ".xlsx", ".csv", ".pdf", ".html"),
        forbidden_artifact_extensions=(".txt", ".json"),
    ),
    "mixed_embedded_media_artifact_package": Scenario(
        name="mixed_embedded_media_artifact_package",
        prompt=(
            "Create a test-only mixed artifact package named stage_mixed_embedded_media_pack. Deliver exactly "
            "these final artifacts: DOCX report, XLSX workbook with an embedded raster image, CSV export, "
            "PDF summary, and self-contained HTML report. Include STAGE MIXED EMBEDDED QA in every deliverable. "
            "Attach all five final files and do not attach internal notes, JSON sidecars, TXT files, screenshots, "
            "or source image files."
        ),
        max_average_ms=240000,
        max_single_ms=300000,
        required_text_any=(),
        forbidden_text=("couldn't attach", "task is still open", "placeholder.txt"),
        required_tool_all=("spreadsheet_create", "document_create", "pdf_create"),
        required_artifact_extensions=(".docx", ".xlsx", ".csv", ".pdf", ".html"),
        required_embedded_media_extensions=(".xlsx",),
        min_embedded_media_count=1,
        required_artifact_text_all=("STAGE MIXED EMBEDDED QA",),
        forbidden_artifact_extensions=(".txt", ".json", ".png", ".jpg", ".jpeg"),
        min_artifact_count=5,
        max_artifact_count=5,
    ),
    "html_research_real_images": Scenario(
        name="html_research_real_images",
        prompt=(
            "Use public travel and parks sources to research three train-accessible outdoor day trips. "
            "Create one self-contained HTML report with local/source raster images embedded in the HTML, "
            "source URLs, and a short practical itinerary. Do not use SVG placeholders."
        ),
        max_average_ms=180000,
        max_single_ms=240000,
        required_text_any=(),
        forbidden_text=("placeholder", "couldn't attach", "task is still open"),
        required_tool_any=("browser_image_collect",),
        required_artifact_extensions=(".html",),
        required_artifact_text_all=("source", "itinerary"),
        require_visual_assets=True,
        require_raster_html_images=True,
        forbidden_artifact_extensions=(".txt", ".json", ".svg"),
    ),
    "archive_download_extract_manifest": Scenario(
        name="archive_download_extract_manifest",
        prompt=(
            "TGQA live archive workflow: download this ZIP file, extract it with the archive tool, inspect the "
            "extracted file list, and create a new XLSX workbook named stage_archive_manifest_rebuilt.xlsx listing "
            "every real file inside the archive. Exclude __MACOSX metadata sidecars from the workbook. "
            "Attach only the final XLSX workbook; do not attach the downloaded ZIP, CSV sidecar, JSON sidecar, "
            "or extracted source files. URL: "
            "https://www.dropbox.com/scl/fi/v4jdn94gs14lk0id9249v/Archive.zip?"
            "rlkey=n6kq26mcm7kn7jhbhvut30fgs&st=whdna52x&dl=1"
        ),
        max_average_ms=180000,
        max_single_ms=240000,
        required_text_any=(),
        forbidden_text=("couldn't attach", "task is still open", "File is not readable as UTF-8 text"),
        required_tool_all=("file_download", "archive_extract"),
        required_artifact_extensions=(".xlsx",),
        forbidden_artifact_extensions=(".zip", ".csv", ".json"),
        required_artifact_names=("stage_archive_manifest_rebuilt.xlsx",),
        required_artifact_text_all=(
            "pre_market_report_2026-06-04.html",
            "nullion_github_metrics_history.csv",
            "tgqa_image_bytes_delivery_2.pdf",
        ),
        min_artifact_count=1,
    ),
    "archive_transform_workbook": Scenario(
        name="archive_transform_workbook",
        prompt=(
            "TGQA live archive transform workbook: extract this existing ZIP file with archive_extract, "
            "and create a new XLSX workbook named {output_name}. "
            "The workbook must have one row per PNG file and exactly these columns: Original filename, "
            "Uppercase extension, Reversed stem, Stem characters, and Image. For Reversed stem, reverse the "
            "filename stem characters, for example alpha-sun.png becomes nus-ahpla. Embed the local PNG file "
            "inside the Image cell for its row. Attach only {output_name}; do not "
            "attach the ZIP, extracted PNG files, screenshots, raw archive manifests, CSV sidecars, JSON sidecars, "
            "or source files. ZIP path: {archive_path}"
        ),
        max_average_ms=240000,
        max_single_ms=300000,
        required_text_any=(),
        forbidden_text=("couldn't attach", "task is still open", "File is not readable as UTF-8 text"),
        required_tool_all=("archive_extract", "spreadsheet_create"),
        required_artifact_extensions=(".xlsx",),
        required_embedded_media_extensions=(".xlsx",),
        min_embedded_media_count=3,
        required_artifact_names=("stage_archive_transform_field_notes.xlsx",),
        required_artifact_text_all=("alpha-sun.png", "nus-ahpla", "thgilnoom-ateb", "revir-ammag"),
        forbidden_artifact_extensions=(".zip", ".csv", ".json", ".png", ".jpg", ".jpeg"),
        require_visual_assets=True,
        min_artifact_count=1,
        runner="stage_workspace_archive_artifact_retry_guard",
    ),
    "planner_complex_artifact_workbook": Scenario(
        name="planner_complex_artifact_workbook",
        prompt=(
            "/planner TGQA live planner complex artifact workbook: extract this existing ZIP file with "
            "archive_extract, inspect every PNG inside it, and create a new XLSX workbook named "
            "stage_planner_complex_media_workbook.xlsx. The workbook must include one row per PNG file and "
            "these columns: Original filename, Slug, Stem length, Palette note, and Image. For Slug, lowercase "
            "the filename stem and replace punctuation with hyphens. Embed each local PNG file inside the Image "
            "cell for its row. Attach only stage_planner_complex_media_workbook.xlsx; do not attach ZIP files, "
            "raw manifests, sidecars, extracted PNG files, screenshots, or source files. ZIP path: {archive_path}"
        ),
        max_average_ms=300000,
        max_single_ms=360000,
        required_text_any=(),
        forbidden_text=("couldn't attach", "task is still open", "File is not readable as UTF-8 text"),
        required_tool_all=("archive_extract", "spreadsheet_create"),
        required_artifact_extensions=(".xlsx",),
        required_embedded_media_extensions=(".xlsx",),
        min_embedded_media_count=3,
        required_artifact_names=("stage_planner_complex_media_workbook.xlsx",),
        required_artifact_text_all=("alpha-sun.png", "beta-moonlight.png", "gamma-river.png"),
        forbidden_artifact_extensions=(".zip", ".csv", ".json", ".png", ".jpg", ".jpeg"),
        require_visual_assets=True,
        min_artifact_count=1,
        runner="planner_complex_artifact_workbook",
    ),
    "planner_downloaded_data_workbook": Scenario(
        name="planner_downloaded_data_workbook",
        prompt=(
            "/planner TGQA live planner downloaded data workbook: download this public CSV file with "
            "file_download, inspect the saved local file with file_read, and create a new XLSX workbook named "
            "stage_planner_downloaded_data_workbook.xlsx. The workbook must include at least the first five "
            "data rows from the source and columns Rank, State, Postal, Population, and Population bucket. "
            "For Population bucket, derive a compact value from each row's population instead of copying a "
            "source column. Attach only stage_planner_downloaded_data_workbook.xlsx; do not attach the source "
            "CSV, screenshots, JSON sidecars, or raw downloaded files. URL: "
            "https://raw.githubusercontent.com/plotly/datasets/master/2014_usa_states.csv"
        ),
        max_average_ms=240000,
        max_single_ms=300000,
        required_text_any=(),
        forbidden_text=("couldn't attach", "task is still open", "File is not readable as UTF-8 text"),
        required_tool_all=("file_download", "file_read", "spreadsheet_create"),
        required_artifact_extensions=(".xlsx",),
        required_artifact_names=("stage_planner_downloaded_data_workbook.xlsx",),
        required_artifact_text_all=("Alabama", "Alaska", "Population bucket"),
        forbidden_artifact_extensions=(".csv", ".json", ".png", ".jpg", ".jpeg"),
        min_artifact_count=1,
        runner="planner_downloaded_data_workbook",
    ),
    "artifact_twenty_row_xlsx_delivery": Scenario(
        name="artifact_twenty_row_xlsx_delivery",
        prompt=(
            "Create an XLSX workbook named stage_twenty_row_inventory.xlsx with exactly 20 product rows. "
            "Columns: Product number, SKU, Short name, Category, Unit price, and QA note. "
            "Use synthetic test-only product data. Attach only the XLSX workbook."
        ),
        max_average_ms=180000,
        max_single_ms=240000,
        required_text_any=(),
        forbidden_text=("couldn't attach", "task is still open", "File is not readable as UTF-8 text"),
        required_artifact_extensions=(".xlsx",),
        required_artifact_names=("stage_twenty_row_inventory.xlsx",),
        required_artifact_text_all=("QA note",),
        forbidden_artifact_extensions=(".csv", ".json", ".txt"),
        min_artifact_count=1,
        min_xlsx_rows=21,
    ),
    "random_website_artifact_bundle": Scenario(
        name="random_website_artifact_bundle",
        prompt=(
            "Create a random website artifact package for live stage QA about a fictional media archive. "
            "Deliver exactly these final artifacts: a CSV dataset, a linked HTML report, a self-contained "
            "HTML report with embedded image assets, and a PDF media report. Include the phrase "
            "STAGE RANDOM WEBSITE QA in every deliverable. Do not attach JSON sidecars, screenshots, "
            "raw source files, or only a PDF."
        ),
        max_average_ms=240000,
        max_single_ms=300000,
        required_text_any=(),
        forbidden_text=("couldn't attach", "task is still open", "only generated", "only attached"),
        required_tool_all=("file_write", "pdf_create"),
        required_artifact_extensions=(".csv", ".html", ".pdf"),
        required_artifact_text_all=("STAGE RANDOM WEBSITE QA",),
        forbidden_artifact_extensions=(".json", ".txt", ".png", ".jpg", ".jpeg", ".svg", ".webp"),
        require_visual_assets=True,
        min_artifact_count=4,
    ),
    "planner_large_web_dataset_handoff": Scenario(
        name="planner_large_web_dataset_handoff",
        prompt=(
            "/planner TGQA live planner large web dataset handoff: open this public JSON feed, collect "
            "the first 100 records, and create an XLSX workbook named "
            "stage_planner_large_web_dataset_handoff.xlsx. The workbook must have columns Record, Category, "
            "Detail URL, Image URL, Thumbnail URL, and Summary. For Record, write values like Photo 001 through "
            "Photo 100. Persist any large collected source dataset through an available workspace file/tool "
            "handoff before creating the workbook; do not ask me to paste CSV or JSON. Attach only "
            "stage_planner_large_web_dataset_handoff.xlsx. URL: https://jsonplaceholder.typicode.com/photos"
        ),
        max_average_ms=300000,
        max_single_ms=360000,
        required_text_any=(),
        forbidden_text=("Waiting for user input", "please provide the dataset", "paste", "large_tool_results"),
        required_tool_all=("browser_run_js", "file_write", "spreadsheet_create"),
        required_artifact_extensions=(".xlsx",),
        required_artifact_names=("stage_planner_large_web_dataset_handoff.xlsx",),
        required_artifact_text_all=("Photo 001", "Photo 100", "Thumbnail URL"),
        forbidden_artifact_extensions=(".csv", ".json", ".txt"),
        min_artifact_count=1,
        runner="planner_large_web_dataset_handoff",
    ),
    "artifact_followup_retry_guard": Scenario(
        name="artifact_followup_retry_guard",
        prompt=(
            "TGQA live artifact follow-up retry guard: extract this existing ZIP file with archive_extract, "
            "and create a new XLSX workbook named {output_name}. "
            "The workbook must have one row per PNG file and exactly these columns: Original filename, "
            "Filename stem, Extension, Byte count, and Image. Embed the local PNG file inside the Image cell "
            "for its row. Attach only {output_name}; do not attach the ZIP, extracted "
            "PNG files, screenshots, raw archive manifests, CSV sidecars, JSON sidecars, or source files. "
            "ZIP path: {archive_path}"
        ),
        max_average_ms=240000,
        max_single_ms=300000,
        required_text_any=(),
        forbidden_text=("couldn't attach", "task is still open", "File is not readable as UTF-8 text"),
        required_tool_all=("archive_extract", "spreadsheet_create"),
        required_artifact_extensions=(".xlsx",),
        required_embedded_media_extensions=(".xlsx",),
        min_embedded_media_count=3,
        required_artifact_names=(),
        required_artifact_text_all=("alpha-sun.png", "beta-moonlight.png", "gamma-river.png"),
        forbidden_artifact_extensions=(".zip", ".csv", ".json", ".png", ".jpg", ".jpeg"),
        forbidden_after_artifact_tool_any=(
            "browser_click_element",
            "browser_wait_for",
            "browser_assert_page_state",
        ),
        require_visual_assets=True,
        min_artifact_count=1,
        runner="stage_workspace_archive_artifact_retry_guard",
    ),
    "single_numbered_choice_list_contract": Scenario(
        name="single_numbered_choice_list_contract",
        prompt=(
            "For this test-only stage alert, do not use tools. Reply with exactly one numbered selection list: "
            "1. Mark the alert in progress 2. Mark the alert resolved. Keep it concise."
        ),
        followups=("1",),
        max_average_ms=45000,
        max_single_ms=60000,
        required_text_any=(),
        forbidden_text=("multiple numbered option groups", "which group", "which list", "matching email", "email thread"),
        forbidden_tool_any=("email_search", "email_read", "calendar_list", "browser_navigate"),
        require_no_tools=True,
        max_numbered_blocks_per_response=1,
    ),
    "chat_history_memory_recall_artifact": Scenario(
        name="chat_history_memory_recall_artifact",
        prompt=(
            "For this live QA conversation only, keep this checkpoint in context: ORCHID-7442-LUMEN. "
            "The paired phrase is clean ferry. Do not save a durable preference, use tools, or create a file yet. "
            "Reply with one short acknowledgement."
        ),
        followups=(
            "Change topics for one turn: give two brief project hygiene tips. Do not mention the checkpoint.",
            "Now recall the checkpoint and paired phrase from earlier in this same chat. Create a CSV named "
            "stage_chat_memory_recall.csv with columns Field and Value. Attach only that CSV.",
        ),
        max_average_ms=150000,
        max_single_ms=180000,
        required_text_any=(),
        forbidden_text=("which checkpoint", "which phrase", "saved chat target", "latest saved context"),
        required_tool_any=("file_write", "spreadsheet_create"),
        forbidden_tool_any=("email_search", "email_read", "calendar_list", "browser_navigate"),
        required_artifact_extensions=(".csv",),
        required_artifact_names=("stage_chat_memory_recall.csv",),
        required_artifact_text_all=("ORCHID-7442-LUMEN", "clean ferry"),
    ),
    "recent_context_artifact_reuse_chain": Scenario(
        name="recent_context_artifact_reuse_chain",
        prompt=(
            "Create a CSV named stage_followup_source_values.csv with four rows: "
            "North 12, South 19, East 7, and West 15. Attach only the CSV."
        ),
        followups=(
            "Using the CSV you just created in this same chat, create an XLSX workbook named "
            "stage_followup_reuse_values.xlsx with the same four rows and a Summary sheet containing the total. "
            "Attach only the XLSX.",
            "Using the workbook you just created, create a PDF named stage_followup_reuse_summary.pdf that states "
            "the total and the top region. Attach only the PDF.",
        ),
        max_average_ms=240000,
        max_single_ms=300000,
        forbidden_text=("which csv", "which workbook", "saved chat target", "latest saved context", "couldn't attach"),
        required_tool_all=("pdf_create",),
        required_tool_any=("file_write", "spreadsheet_create", "terminal_exec"),
        forbidden_tool_any=("email_search", "email_read", "calendar_list", "browser_navigate"),
        required_artifact_extensions=(".csv", ".xlsx", ".pdf"),
        required_artifact_names=(
            "stage_followup_source_values.csv",
            "stage_followup_reuse_values.xlsx",
            "stage_followup_reuse_summary.pdf",
        ),
        required_xlsx_sheet_names=("Values", "Summary"),
        required_artifact_text_all=("North", "South", "53"),
    ),
    "archive_tool_fallback_followup_chain": Scenario(
        name="archive_tool_fallback_followup_chain",
        prompt=(
            "TGQA live archive fallback chain: download this ZIP file with the file download tool, extract it with "
            "the archive tool, inspect the extracted file list, and create a new XLSX workbook named "
            "stage_archive_fallback_chain.xlsx listing every real file inside the archive. Exclude __MACOSX metadata "
            "sidecars from the workbook. Attach only the final XLSX workbook. URL: "
            "https://www.dropbox.com/scl/fi/v4jdn94gs14lk0id9249v/Archive.zip?"
            "rlkey=n6kq26mcm7kn7jhbhvut30fgs&st=whdna52x&dl=1"
        ),
        followups=(
            "Using the extracted archive context from this same chat, create a CSV named "
            "stage_archive_extension_summary.csv listing file extension and count for real files only. "
            "Attach only the CSV; do not attach the source archive or sidecars.",
        ),
        max_average_ms=300000,
        max_single_ms=360000,
        required_text_any=(),
        forbidden_text=("File is not readable as UTF-8 text", "couldn't attach", "task is still open"),
        required_tool_all=("file_download", "archive_extract"),
        required_artifact_extensions=(".xlsx", ".csv"),
        forbidden_artifact_extensions=(".zip", ".json"),
        required_artifact_names=("stage_archive_fallback_chain.xlsx", "stage_archive_extension_summary.csv"),
        required_artifact_text_all=(
            "pre_market_report_2026-06-04.html",
            "nullion_github_metrics_history.csv",
            "tgqa_image_bytes_delivery_2.pdf",
        ),
        min_artifact_count=2,
    ),
    "complex_html_email_with_attachments": Scenario(
        name="complex_html_email_with_attachments",
        prompt=(
            "Create a stage QA email package and send it to recipient@example.com. Subject must start with "
            "\"Nullion stage deep QA\". The email body should be complex HTML with a table, headings, bullet list, "
            "inline styling, and a clear timestamp. Attach a generated CSV, XLSX workbook, PDF summary, and one HTML "
            "file. Keep all content test-only and include the phrase STAGE QA TEST."
        ),
        max_average_ms=240000,
        max_single_ms=300000,
        required_text_any=("recipient@example.com", "email", "sent"),
        forbidden_text=("cannot send", "couldn't attach", "task is still open"),
        required_tool_any=("file_write", "html_create"),
        required_tool_all=("email_send", "spreadsheet_create", "pdf_create"),
        required_artifact_extensions=(".csv", ".xlsx", ".pdf", ".html"),
        required_artifact_text_all=("STAGE QA TEST",),
        allow_approval_required=True,
        auto_approve=True,
        requires_email_send_flag=True,
    ),
    "ambiguous_report_reference": Scenario(
        name="ambiguous_report_reference",
        prompt="U built a report i think",
        max_average_ms=30000,
        max_single_ms=45000,
        required_text_any=("report", "which", "what", "clarify", "send"),
        forbidden_text=("github.com/", "pull request", "saved chat target", "latest saved context"),
        forbidden_tool_any=("email_search", "email_read", "calendar_list", "browser_navigate"),
    ),
    "vague_account_source_selection": Scenario(
        name="vague_account_source_selection",
        prompt=(
            "Can you check whether the vague external account thing happened? Discover authorized sources if "
            "needed, but ask me which source to check before reading any account data."
        ),
        max_average_ms=45000,
        max_single_ms=60000,
        required_text_any=("choose", "which source", "source", "1."),
        forbidden_text=("i checked your email", "found 1 matching message", "found matching message"),
        required_tool_any=("request_tool_scope",),
        forbidden_tool_any=("email_search", "email_read", "calendar_list"),
        max_numbered_blocks_per_response=1,
    ),
    "artifact_context_followup": Scenario(
        name="artifact_context_followup",
        prompt=(
            "Create a compact stage context QA CSV report with exactly three rows comparing Apollo, Artemis, and Voyager. "
            "Save it as a downloadable CSV artifact. Include columns Program, Era, Primary focus, and QA note."
        ),
        followups=(
            "Using that report from this conversation, create a PDF executive summary with a short title, one table, "
            "and two bullet recommendations.",
        ),
        max_average_ms=180000,
        max_single_ms=220000,
        required_text_any=(),
        forbidden_text=("which report", "what report", "github.com/", "pull request", "saved chat target"),
        required_tool_any=("file_write", "spreadsheet_create", "pdf_create"),
        required_artifact_extensions=(".csv", ".pdf"),
    ),
    "formatted_pdf_flyer_pack": Scenario(
        name="formatted_pdf_flyer_pack",
        prompt=(
            "Create a polished test-only visual design package about a fictional community night market. "
            "Deliver three distinctly formatted files: "
            "1. community_night_market_poster_STAGE_FORMAT_QA.pdf, a one-page poster/flyer PDF with a large title, "
            "event details, a generated or sourced visual, and no chopped text; "
            "2. community_night_market_memo_STAGE_FORMAT_QA.pdf, a two-page formal PDF memo with a table and "
            "restrained business styling; "
            "3. community_night_market_poster_STAGE_FORMAT_QA.html, a self-contained HTML version of the poster. "
            "Use different layout styles for the poster and memo. Do not use emojis. Do not attach placeholders, "
            "internal notes, or sidecar text files. Include the phrase STAGE FORMAT QA."
        ),
        max_average_ms=240000,
        max_single_ms=300000,
        required_text_any=(),
        forbidden_text=("placeholder.txt", "couldn't attach", "task is still open"),
        required_tool_all=("pdf_create", "file_write"),
        required_artifact_extensions=(".pdf", ".html"),
        required_artifact_text_all=("STAGE FORMAT QA",),
        required_artifact_names=(
            "community_night_market_poster_STAGE_FORMAT_QA.pdf",
            "community_night_market_memo_STAGE_FORMAT_QA.pdf",
            "community_night_market_poster_STAGE_FORMAT_QA.html",
        ),
        require_visual_assets=True,
        forbidden_artifact_extensions=(".txt", ".json"),
        min_artifact_count=3,
    ),
    "kids_playbook_artifact_pack": Scenario(
        name="kids_playbook_artifact_pack",
        prompt=(
            "Create a test-only children's activity playbook artifact pack about space gardeners. "
            "Include a DOCX booklet, a PDF booklet, a PPTX read-aloud deck, and a self-contained HTML edition. "
            "The design should use kid-friendly colors, large readable headings, generated illustrations if the "
            "image tool is available; if not, use browser/image collection tools to collect at least one relevant public NASA or museum image as a local raster PNG/JPG asset "
            "and embed that compatible current-turn image path inside both the DOCX and PPTX. Do not use file_search or old workspace images as substitute art. "
            "Include page numbers, at least one activity page, and a simple glossary. "
            "No emojis, no placeholder files, no unlabeled empty bordered panels, and no chopped or overlapping text. "
            "Drawing or writing boxes are okay only when clearly labeled as activity areas. Include the phrase STAGE KIDS QA."
        ),
        max_average_ms=420000,
        max_single_ms=360000,
        required_text_any=(),
        forbidden_text=("placeholder.txt", "couldn't attach", "task is still open"),
        required_tool_all=("document_create", "presentation_create", "pdf_create", "file_write"),
        required_artifact_extensions=(".docx", ".pdf", ".pptx", ".html"),
        required_embedded_media_extensions=(".docx", ".pptx"),
        required_artifact_text_all=("STAGE KIDS QA",),
        require_visual_assets=True,
        forbidden_artifact_extensions=(".txt", ".json"),
        min_artifact_count=4,
    ),
    "spreadsheet_visual_workbook_pack": Scenario(
        name="spreadsheet_visual_workbook_pack",
        prompt=(
            "Create a complex visual spreadsheet package comparing four fictional cafe pop-up locations. "
            "Deliver an XLSX workbook with multiple sheets, real Excel formula cells that begin with '=' "
            "(set formulas_required=true when calling the spreadsheet tool), "
            "conditional formatting, a chart, and embedded "
            "images or generated visual markers; also deliver a CSV export and a PDF executive summary. "
            "Use rows where setup cost, projected revenue, and operating cost are numeric columns; add a Gross Profit "
            "formula column with formulas exactly like =D2-E2, =D3-E3, =D4-E4, and =D5-E5, and a Payback Days "
            "formula column with formulas exactly like =ROUND(C2/F2,2), =ROUND(C3/F3,2), =ROUND(C4/F4,2), and =ROUND(C5/F5,2). "
            "Include source/assumption notes inside the files, use professional formatting, and avoid emojis, "
            "placeholder files, and internal sidecars. Include the phrase STAGE SHEET QA."
        ),
        max_average_ms=240000,
        max_single_ms=300000,
        required_text_any=(),
        forbidden_text=("placeholder.txt", "couldn't attach", "task is still open"),
        required_tool_any=("image_generate", "browser_image_collect", "file_download", "file_write"),
        required_tool_all=("spreadsheet_create", "pdf_create"),
        required_artifact_extensions=(".xlsx", ".csv", ".pdf"),
        required_embedded_media_extensions=(".xlsx",),
        require_xlsx_formula=True,
        require_xlsx_chart=True,
        require_xlsx_conditional_formatting=True,
        required_artifact_text_all=("STAGE SHEET QA",),
        require_visual_assets=True,
        forbidden_artifact_extensions=(".txt", ".json"),
        min_artifact_count=3,
    ),
    "executive_personal_briefing_pack": Scenario(
        name="executive_personal_briefing_pack",
        prompt=(
            "Create a test-only executive personal briefing for the next 24 hours using real current data. "
            "Use my live calendar, the weather for Providence, Rhode Island, current quotes for SPY, QQQ, "
            "and DIA, and one current official public museum or science-center web page as a source for a "
            "recommended optional activity. Clearly distinguish facts, empty calendar results, and unavailable "
            "data; include source URLs and an as-of timestamp. If no compatible calendar source is configured, "
            "write exactly 'Calendar status: unavailable' in each file. Deliver exactly three polished files named "
            "executive_personal_briefing.xlsx, executive_personal_briefing.pdf, and "
            "executive_personal_briefing.html. The workbook must contain Briefing, Markets, and Schedule sheets; "
            "use real Excel formula cells for market change calculations, conditional formatting, and a chart. "
            "The PDF and self-contained HTML must be readable executive summaries, not raw tool output. "
            "Include the phrase NULLION EXECUTIVE BRIEFING QA in all three files. Do not send email, create "
            "calendar events, or attach JSON, TXT, logs, sidecars, or any other files."
        ),
        max_average_ms=420000,
        max_single_ms=420000,
        required_text_any=("executive_personal_briefing", "briefing", "attached"),
        forbidden_text=(
            "placeholder.txt",
            "task is still open",
            "source-backed points",
            "send me your calendar",
            "paste your calendar",
        ),
        required_tool_any=("browser_navigate", "web_fetch"),
        required_tool_all=(
            "weather_forecast",
            "market_quote",
            "spreadsheet_create",
            "pdf_create",
            "file_write",
        ),
        required_tool_or_structured_unavailable=("calendar_list",),
        forbidden_tool_any=("email_send", "calendar_create", "calendar_update", "calendar_delete"),
        required_artifact_extensions=(".xlsx", ".pdf", ".html"),
        forbidden_artifact_extensions=(".json", ".txt", ".log", ".md", ".csv"),
        required_artifact_names=(
            "executive_personal_briefing.xlsx",
            "executive_personal_briefing.pdf",
            "executive_personal_briefing.html",
        ),
        require_xlsx_formula=True,
        require_xlsx_chart=True,
        require_xlsx_conditional_formatting=True,
        required_xlsx_sheet_names=("Briefing", "Markets", "Schedule"),
        required_artifact_text_all=("NULLION EXECUTIVE BRIEFING QA",),
        required_artifact_text_each=("NULLION EXECUTIVE BRIEFING QA",),
        required_artifact_text_if_tool_unavailable=(("calendar_list", "Calendar status: unavailable"),),
        forbidden_tool_output_text=(
            "preserved_existing_artifact",
            "preserved_existing_embedded_media_artifact",
        ),
        require_visual_assets=True,
        min_artifact_count=3,
        max_artifact_count=3,
    ),
    "research_artifact_email_followup_pack": Scenario(
        name="research_artifact_email_followup_pack",
        prompt=(
            "Use one public parks or museum web page as a source for a compact test-only culture itinerary benchmark. "
            "Create an XLSX comparison workbook with an embedded real image or generated visual marker, plus a PDF "
            "executive summary. Include source URLs, a 3-row scoring table, and the phrase STAGE MULTI TOOL QA. "
            "Do not use emojis or placeholder files."
        ),
        followups=(
            "Using the artifacts from this same conversation, send recipient@example.com a complex HTML email with "
            "the subject starting Nullion stage multi tool QA. Attach only the PDF executive summary and XLSX workbook. "
            "Mention that this is a test-only email.",
        ),
        max_average_ms=420000,
        max_single_ms=480000,
        required_text_any=("recipient@example.com", "email", "sent"),
        forbidden_text=("which artifacts", "couldn't attach", "placeholder.txt", "task is still open"),
        required_tool_any=("image_generate", "browser_image_collect", "browser_navigate", "web_fetch"),
        required_tool_all=("spreadsheet_create", "pdf_create", "email_send"),
        required_artifact_extensions=(".xlsx", ".pdf"),
        required_embedded_media_extensions=(".xlsx",),
        required_artifact_text_all=("STAGE MULTI TOOL QA",),
        require_visual_assets=True,
        forbidden_artifact_extensions=(".txt", ".json"),
        min_artifact_count=2,
        allow_approval_required=True,
        auto_approve=True,
        requires_email_send_flag=True,
    ),
}


DEFAULT_LIVE_STAGE_SCENARIOS: tuple[str, ...] = (
    "active_followup_context_guard",
    "active_related_followup_context_guard",
    "simple",
    "startup_warm_first_chat",
    "plain_chat_attachment_planner_fast_path",
    "structured_tool_id_boundary_guards",
    "stage_runtime_patch_regressions",
    "file_search_listing_no_attachment",
    "requested_workspace_file_delivery",
    "scheduler_manual_run",
    "scheduler_manual_run_short_followup",
    "scheduler_inventory_read_only",
    "scheduler_disable_not_delete",
    "scheduler_bulk_enable_recovery",
    "scheduler_fresh_followup_after_bulk_enable",
    "local_system_terminal_fallback",
    "local_system_followup_terminal_fallback",
    "local_system_update_followup_terminal_outcome",
    "platform_code_fence_formatting",
    "svg_artifact_file_write",
    "browser_not_found_catalog_guard",
    "browser_rental_live_records",
    "browser_flight_live_records",
    "market_quote_live_records",
    "browser_continuation_registry_guard",
    "browser_followup_account_scope_guard",
    "browser_screenshot_artifact_delivery",
    "browser_context_capture_recovery",
    "browser_context_capture_recovery_variant",
    "browser_page_screenshot_artifact_delivery",
    "html_research_real_images",
    "agent_turn_limit_doctor_card",
    "email_simple_live_access",
    "email_urgent_read_recovery",
    "scheduler_manual_run",
    "scheduler_inventory_read_only",
    "scheduler_disable_not_delete",
    "scheduler_bulk_enable_recovery",
    "scheduler_fresh_followup_after_bulk_enable",
    "archive_download_extract_manifest",
    "archive_transform_workbook",
    "planner_complex_artifact_workbook",
    "planner_downloaded_data_workbook",
    "artifact_twenty_row_xlsx_delivery",
    "mixed_embedded_media_artifact_package",
    "random_website_artifact_bundle",
    "artifact_followup_retry_guard",
    "single_numbered_choice_list_contract",
    "chat_history_memory_recall_artifact",
    "recent_context_artifact_reuse_chain",
    "weather",
    "profile_weather",
    "calendar",
    "calendar_tomorrow_live_access",
    "calendar_weekend_live_access",
    "calendar_answer_not_raw_listing",
    "image",
    "github_prs",
    "vague_account_source_selection",
)


DEEP_LIVE_STAGE_SCENARIOS: tuple[str, ...] = (
    "email_read_followup",
    "email_request_overrides_pr_context",
    "email_request_overrides_security_history",
    "complex_html_email_with_attachments",
    "artifact_preflight_scope_fast_path",
    "complex_artifact_pack",
    "archive_tool_fallback_followup_chain",
    "formatted_pdf_flyer_pack",
    "kids_playbook_artifact_pack",
    "spreadsheet_visual_workbook_pack",
    "executive_personal_briefing_pack",
    "research_artifact_email_followup_pack",
)


def _live_request_headers(headers: dict[str, str]) -> dict[str, str]:
    """Optional canonical authority when a container uses a host-network alias."""
    authority = os.environ.get("NULLION_LIVE_HOST_HEADER", "").strip()
    return {**headers, **({"Host": authority} if authority else {})}


def _post_json(url: str, payload: dict[str, Any], timeout: int) -> tuple[dict[str, Any], float]:
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers=_live_request_headers({"Content-Type": "application/json", "Accept": "application/json"}),
        method="POST",
    )
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise AssertionError(f"HTTP {exc.code} from {url}: {raw[:1000]}") from exc
    elapsed_ms = (time.perf_counter() - started) * 1000
    try:
        parsed = json.loads(raw.decode("utf-8"))
    except Exception as exc:  # pragma: no cover - diagnostic guard
        raise AssertionError(f"Non-JSON response from {url}: {raw[:1000]!r}") from exc
    return parsed, elapsed_ms


def _get_json(url: str, timeout: int) -> dict[str, Any]:
    request = urllib.request.Request(url, headers=_live_request_headers({"Accept": "application/json"}), method="GET")
    with urllib.request.urlopen(request, timeout=timeout) as response:
        raw = response.read()
    return json.loads(raw.decode("utf-8"))


def _delete_json(url: str, timeout: int) -> dict[str, Any]:
    request = urllib.request.Request(url, headers=_live_request_headers({"Accept": "application/json"}), method="DELETE")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise AssertionError(f"HTTP {exc.code} from {url}: {raw[:1000]}") from exc
    return json.loads(raw.decode("utf-8"))


def _patch_json(url: str, payload: dict[str, Any], timeout: int) -> dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers=_live_request_headers({"Content-Type": "application/json", "Accept": "application/json"}),
        method="PATCH",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise AssertionError(f"HTTP {exc.code} from {url}: {raw[:1000]}") from exc
    return json.loads(raw.decode("utf-8"))


def _approve_once(base_url: str, approval_id: str, timeout: int, *, mode: str = "once") -> dict[str, Any]:
    url = f"{base_url}/api/approve/{urllib.parse.quote(approval_id)}"
    try:
        response, _elapsed_ms = _post_json(
            url,
            {"mode": mode},
            timeout=timeout,
        )
    except AssertionError as exc:
        message = str(exc)
        if mode == "once" and "Choose 2x, 5x, or 10x" in message:
            response, _elapsed_ms = _post_json(
                url,
                {"mode": "limit_2x"},
                timeout=timeout,
            )
        else:
            raise
    assert response.get("ok") is True, f"approval {approval_id} failed: {response!r}"
    resume = response.get("resume")
    return resume if isinstance(resume, dict) else response


def _event_tool_names(
    runtime_db: Path | None,
    conversation_id: str,
    *,
    since_created_at: str | None = None,
) -> set[str]:
    names: set[str] = set()
    for event in [
        *_conversation_event_payloads(runtime_db, conversation_id, row_limit=1000),
        *_runtime_event_payloads(runtime_db, conversation_id, row_limit=1000),
    ]:
        normalized = _normalize_event_payload(event)
        created_at = str(normalized.get("created_at") or "").strip()
        if since_created_at and created_at and created_at < since_created_at:
            continue
        if normalized.get("tool_name"):
            names.add(str(normalized["tool_name"]))
        payload = event.get("payload")
        if isinstance(payload, dict):
            payload_tool_name = payload.get("tool_name")
            if isinstance(payload_tool_name, str) and payload_tool_name:
                names.add(payload_tool_name)
        for result in event.get("tool_results") or ():
            if isinstance(result, dict) and result.get("tool_name"):
                names.add(str(result["tool_name"]))
    names.update(_mini_agent_tool_names_for_conversation(runtime_db, conversation_id))
    return names


def _event_tool_completed(runtime_db: Path | None, conversation_id: str, tool_name: str) -> bool:
    for event in [
        *_conversation_event_payloads(runtime_db, conversation_id, row_limit=1000),
        *_runtime_event_payloads(runtime_db, conversation_id, row_limit=1000),
    ]:
        normalized = _normalize_event_payload(event)
        event_tool_name = str(normalized.get("tool_name") or "").strip()
        event_status = str(normalized.get("status") or normalized.get("tool_status") or "").strip().lower()
        event_type = str(normalized.get("event_type") or "").strip()
        if event_tool_name == tool_name and (event_status == "completed" or event_type == "tool.invocation.completed"):
            return True
        for result in event.get("tool_results") or ():
            if not isinstance(result, dict):
                continue
            if str(result.get("tool_name") or "").strip() != tool_name:
                continue
            if str(result.get("status") or "").strip().lower() == "completed":
                return True
    return False


def _normalize_event_payload(event: Any) -> dict[str, Any]:
    if not isinstance(event, dict):
        return {}
    nested = event.get("payload")
    if isinstance(nested, dict) and isinstance(event.get("event_type"), str):
        normalized = dict(nested)
        normalized.setdefault("actor", event.get("actor"))
        normalized.setdefault("created_at", event.get("created_at"))
        normalized.setdefault("event_id", event.get("event_id"))
        normalized.setdefault("event_type", event.get("event_type"))
        return normalized
    return event


def _event_tool_names_with_poll(
    runtime_db: Path | None,
    conversation_id: str,
    *,
    required_tool_any: tuple[str, ...] = (),
    required_tool_all: tuple[str, ...] = (),
    since_created_at: str | None = None,
    timeout_seconds: float = 30.0,
) -> set[str]:
    deadline = time.perf_counter() + timeout_seconds
    last_seen: set[str] = set()
    while True:
        last_seen = _event_tool_names(
            runtime_db,
            conversation_id,
            since_created_at=since_created_at,
        )
        any_satisfied = not required_tool_any or bool(last_seen.intersection(required_tool_any))
        all_satisfied = set(required_tool_all).issubset(last_seen)
        if any_satisfied and all_satisfied:
            return last_seen
        if time.perf_counter() >= deadline:
            return last_seen
        time.sleep(0.2)


def _event_tool_output_texts(runtime_db: Path | None, conversation_id: str) -> list[str]:
    texts: list[str] = []
    for payload in [
        *_conversation_event_payloads(runtime_db, conversation_id),
        *_runtime_event_payloads(runtime_db, conversation_id),
    ]:
        event_payload = payload.get("payload")
        if isinstance(event_payload, dict) and isinstance(event_payload.get("tool_name"), str):
            texts.append(json.dumps(event_payload, ensure_ascii=False, sort_keys=True))
        for result in payload.get("tool_results") or ():
            if isinstance(result, dict):
                texts.append(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return texts


def _event_tool_output_texts_with_poll(
    runtime_db: Path | None,
    conversation_id: str,
    *,
    required_output_tools: tuple[str, ...] = (),
    timeout_seconds: float = 30.0,
) -> list[str]:
    """Wait for full receipts, which can persist after tool timing events."""
    required = set(required_output_tools)
    deadline = time.perf_counter() + max(0.0, timeout_seconds)
    while True:
        texts = _event_tool_output_texts(runtime_db, conversation_id)
        if not required:
            return texts
        completed = set()
        for raw in texts:
            try:
                payload = json.loads(raw)
            except (TypeError, ValueError):
                continue
            for record in _walk_json_mappings(payload):
                if record.get("status") == "completed" and isinstance(record.get("output"), dict):
                    completed.add(record.get("tool_name"))
        if required.issubset(completed) or time.perf_counter() >= deadline:
            return texts
        time.sleep(min(0.1, max(0.0, deadline - time.perf_counter())))


def _walk_json_mappings(value: object) -> Iterable[dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        for nested in value.values():
            yield from _walk_json_mappings(nested)
    elif isinstance(value, list):
        for nested in value:
            yield from _walk_json_mappings(nested)


def _structured_unavailable_required_tools(tool_output_texts: Iterable[str]) -> set[str]:
    unavailable: set[str] = set()
    for raw_text in tool_output_texts:
        try:
            payload = json.loads(raw_text)
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        for record in _walk_json_mappings(payload):
            if str(record.get("tool_name") or "") != "request_tool_scope":
                continue
            status = str(record.get("status") or "").strip().lower()
            event_type = str(record.get("event_type") or "").strip().lower()
            if status != "completed" and event_type != "tool.invocation.completed":
                continue
            output = record.get("output")
            if not isinstance(output, dict):
                continue
            if output.get("missing_connector_app_scope") or output.get("source_selection_required"):
                continue
            for key in ("unavailable_required_tool_names", "unavailable_tools"):
                raw_names = output.get(key)
                if not isinstance(raw_names, list):
                    continue
                unavailable.update(
                    str(tool_name or "").strip()
                    for tool_name in raw_names
                    if str(tool_name or "").strip()
                )
    return unavailable


def _structured_completed_tool_names(tool_output_texts: Iterable[str]) -> tuple[set[str], bool]:
    completed: set[str] = set()
    saw_status = False
    for raw_text in tool_output_texts:
        try:
            payload = json.loads(raw_text)
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        for record in _walk_json_mappings(payload):
            tool_name = str(record.get("tool_name") or "").strip()
            if not tool_name:
                continue
            status = str(record.get("status") or record.get("tool_status") or "").strip().lower()
            event_type = str(record.get("event_type") or "").strip().lower()
            if status:
                saw_status = True
            if status == "completed" or event_type == "tool.invocation.completed":
                completed.add(tool_name)
    return completed, saw_status


def _latest_latency_mark_ms(
    runtime_db: Path | None,
    conversation_id: str,
    mark_name: str,
) -> float | None:
    for payload in reversed(
        _conversation_event_payloads(
            runtime_db,
            conversation_id,
            event_type="conversation.latency_timing",
        )
    ):
        marks = payload.get("marks")
        if not isinstance(marks, list):
            continue
        for mark in marks:
            if not isinstance(mark, dict):
                continue
            if mark.get("name") != mark_name:
                continue
            try:
                return float(mark.get("ms") or 0.0)
            except (TypeError, ValueError):
                return None
    return None


def _latest_turn_latency_total_ms(
    runtime_db: Path | None,
    conversation_id: str,
    turn_id: str,
) -> float | None:
    for payload in reversed(
        _conversation_event_payloads(
            runtime_db,
            conversation_id,
            event_type="conversation.latency_timing",
        )
    ):
        if payload.get("turn_id") != turn_id:
            continue
        try:
            total_ms = payload.get("total_ms")
            if total_ms is not None:
                return float(total_ms)
        except (TypeError, ValueError):
            pass
        marks = payload.get("marks")
        if not isinstance(marks, list):
            return None
        values: list[float] = []
        for mark in marks:
            if not isinstance(mark, dict):
                continue
            try:
                values.append(float(mark.get("ms") or 0.0))
            except (TypeError, ValueError):
                continue
        return max(values) if values else None
    return None


def _latest_turn_latency_total_ms_with_poll(
    runtime_db: Path | None,
    conversation_id: str,
    turn_id: str,
    *,
    timeout_seconds: float = 10.0,
) -> float | None:
    deadline = time.perf_counter() + timeout_seconds
    while True:
        total_ms = _latest_turn_latency_total_ms(runtime_db, conversation_id, turn_id)
        if total_ms is not None or time.perf_counter() >= deadline:
            return total_ms
        time.sleep(0.2)


def _latest_latency_mark_ms_with_poll(
    runtime_db: Path | None,
    conversation_id: str,
    mark_name: str,
    *,
    timeout_seconds: float = 10.0,
) -> float | None:
    deadline = time.perf_counter() + timeout_seconds
    while True:
        mark_ms = _latest_latency_mark_ms(runtime_db, conversation_id, mark_name)
        if mark_ms is not None or time.perf_counter() >= deadline:
            return mark_ms
        time.sleep(0.2)


def _latest_status_latency_mark_ms(
    base_url: str,
    timeout: int,
    conversation_id: str,
    mark_name: str,
) -> float | None:
    try:
        status = _get_json(f"{base_url}/api/status?full=1", timeout)
    except Exception:
        return None
    for payload in status.get("latency_timings") or ():
        if not isinstance(payload, dict):
            continue
        if str(payload.get("conversation_id") or "") != conversation_id:
            continue
        marks = payload.get("marks")
        if not isinstance(marks, list):
            continue
        for mark in marks:
            if not isinstance(mark, dict):
                continue
            if mark.get("name") != mark_name:
                continue
            try:
                return float(mark.get("ms") or 0.0)
            except (TypeError, ValueError):
                return None
    return None


def _conversation_event_payloads(
    runtime_db: Path | None,
    conversation_id: str,
    *,
    event_type: str | None = None,
    row_limit: int = 1000,
) -> list[dict[str, Any]]:
    return _conversation_collection_payloads(
        runtime_db,
        collection="conversation_events",
        conversation_id=conversation_id,
        event_type=event_type,
        row_limit=row_limit,
    )


def _runtime_event_payloads(
    runtime_db: Path | None,
    conversation_id: str,
    *,
    row_limit: int = 1000,
) -> list[dict[str, Any]]:
    if runtime_db is None or not runtime_db.exists():
        return []
    conn = sqlite3.connect(str(runtime_db))
    try:
        table_names = {
            str(row[0])
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        if "runtime_events" not in table_names:
            return []
        rows = conn.execute(
            "SELECT payload FROM runtime_events "
            "WHERE json_extract(payload, '$.actor') = ? "
            "OR json_extract(payload, '$.payload.principal_id') = ? "
            "OR json_extract(payload, '$.payload.conversation_id') = ? "
            "ORDER BY updated_at DESC LIMIT ?",
            (conversation_id, conversation_id, conversation_id, row_limit),
        ).fetchall()
    finally:
        conn.close()
    payloads: list[dict[str, Any]] = []
    for (raw_payload,) in rows:
        try:
            payload = json.loads(raw_payload)
        except Exception:
            continue
        if isinstance(payload, dict):
            payloads.append(payload)
    return payloads


def _conversation_collection_payloads(
    runtime_db: Path | None,
    *,
    collection: str,
    conversation_id: str,
    event_type: str | None = None,
    row_limit: int = 1000,
) -> list[dict[str, Any]]:
    if runtime_db is None or not runtime_db.exists():
        return []
    conn = sqlite3.connect(str(runtime_db))
    try:
        table_names = {
            str(row[0])
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        params: tuple[Any, ...]
        if collection == "conversation_events" and "conversation_events" in table_names:
            # The live stage DB can contain tens of thousands of event rows.
            # Prefer the dedicated event table and its conversation/created
            # expression index even when the generic runtime_items table also
            # exists in the same store.
            query = (
                "SELECT payload FROM conversation_events "
                "WHERE json_extract(payload, '$.conversation_id') = ?"
            )
            params = (conversation_id,)
            order_by = " ORDER BY json_extract(payload, '$.created_at') DESC LIMIT ?"
        elif "runtime_items" in table_names:
            query = (
                "SELECT payload FROM runtime_items WHERE collection = ? AND json_extract(payload, '$.conversation_id') = ?"
            )
            params = (collection, conversation_id)
            order_by = " ORDER BY updated_at DESC LIMIT ?"
        elif "items" in table_names:
            query = (
                "SELECT payload FROM items WHERE collection = ? AND json_extract(payload, '$.conversation_id') = ?"
            )
            params = (collection, conversation_id)
            order_by = " ORDER BY updated_at DESC LIMIT ?"
        elif "conversation_events" in table_names:
            query = (
                "SELECT payload FROM conversation_events WHERE collection = ? AND json_extract(payload, '$.conversation_id') = ?"
            )
            params = (collection, conversation_id)
            order_by = " ORDER BY updated_at DESC LIMIT ?"
        elif collection == "conversation_events" and "runtime_events" in table_names:
            query = "SELECT payload FROM runtime_events WHERE json_extract(payload, '$.payload.conversation_id') = ?"
            params = (conversation_id,)
            order_by = " ORDER BY updated_at DESC LIMIT ?"
        else:
            return []
        if event_type:
            query += " AND json_extract(payload, '$.event_type') = ?"
            params = (*params, event_type)
        rows = conn.execute(query + order_by, (*params, row_limit)).fetchall()
    finally:
        conn.close()
    payloads: list[dict[str, Any]] = []
    for (raw_payload,) in reversed(rows):
        try:
            payload = json.loads(raw_payload)
        except Exception:
            continue
        normalized = _normalize_event_payload(payload)
        if normalized:
            payloads.append(normalized)
    return payloads


def _turn_payload(
    runtime_db: Path | None,
    conversation_id: str,
    turn_id: str,
) -> dict[str, Any] | None:
    for payload in _conversation_collection_payloads(
        runtime_db,
        collection="conversation_turns",
        conversation_id=conversation_id,
    ):
        if payload.get("turn_id") == turn_id and payload.get("status") == "accepted":
            return payload
    for payload in _conversation_event_payloads(runtime_db, conversation_id):
        if payload.get("turn_id") == turn_id and payload.get("status") == "accepted":
            return payload
        if payload.get("turn_id") == turn_id and payload.get("event_type") == "conversation.turn_recorded":
            return payload
    return None


def _turn_payload_with_poll(
    runtime_db: Path | None,
    conversation_id: str,
    turn_id: str,
    *,
    timeout_seconds: float = 20.0,
) -> dict[str, Any] | None:
    deadline = time.perf_counter() + timeout_seconds
    while True:
        payload = _turn_payload(runtime_db, conversation_id, turn_id)
        if payload is not None or time.perf_counter() >= deadline:
            return payload
        time.sleep(0.2)


def _chat_turn_for_user_message(
    runtime_db: Path | None,
    conversation_id: str,
    user_message: str,
) -> dict[str, Any] | None:
    matches = [
        payload
        for payload in _conversation_event_payloads(runtime_db, conversation_id, event_type="conversation.chat_turn")
        if payload.get("user_message") == user_message
    ]
    return matches[-1] if matches else None


def _message_received_for_user_message(
    runtime_db: Path | None,
    conversation_id: str,
    user_message: str,
) -> dict[str, Any] | None:
    matches = [
        payload
        for payload in _conversation_event_payloads(runtime_db, conversation_id, event_type="conversation.message_received")
        if payload.get("user_message") == user_message
    ]
    return matches[-1] if matches else None


def _tool_names_for_user_message_with_poll(
    runtime_db: Path | None,
    conversation_id: str,
    user_message: str,
    *,
    required_tool_any: tuple[str, ...] = (),
    required_tool_all: tuple[str, ...] = (),
    timeout_seconds: float = 180.0,
) -> set[str]:
    deadline = time.perf_counter() + timeout_seconds
    last_seen: set[str] = set()
    while True:
        chat_turn = _chat_turn_for_user_message(runtime_db, conversation_id, user_message)
        last_seen = _tool_names_from_chat_turn(chat_turn)
        received = _message_received_for_user_message(runtime_db, conversation_id, user_message)
        received_at = str((received or {}).get("created_at") or "").strip()
        if received_at:
            last_seen.update(
                _event_tool_names(
                    runtime_db,
                    conversation_id,
                    since_created_at=received_at,
                )
            )
        any_satisfied = not required_tool_any or bool(last_seen.intersection(required_tool_any))
        all_satisfied = set(required_tool_all).issubset(last_seen)
        if any_satisfied and all_satisfied:
            return last_seen
        if time.perf_counter() >= deadline:
            return last_seen
        time.sleep(0.2)


def _materialized_prompt(prompt: str, *, scenario_name: str, index: int) -> str:
    run_id = f"{scenario_name}_{index}_{uuid4().hex[:8]}".replace("-", "_")
    return str(prompt).replace("{run_id}", run_id)


def _chat_turn_for_user_message_with_poll(
    runtime_db: Path | None,
    conversation_id: str,
    user_message: str,
    *,
    timeout_seconds: float = 20.0,
) -> dict[str, Any] | None:
    deadline = time.perf_counter() + timeout_seconds
    while True:
        payload = _chat_turn_for_user_message(runtime_db, conversation_id, user_message)
        if payload is not None or time.perf_counter() >= deadline:
            return payload
        time.sleep(0.2)


def _media_artifacts_from_conversation(
    runtime_db: Path | None,
    conversation_id: str,
    *,
    required_extensions: tuple[str, ...],
) -> list[dict[str, Any]]:
    required = {
        extension if extension.startswith(".") else f".{extension}"
        for extension in (str(item or "").strip().lower() for item in required_extensions)
        if extension
    }
    artifacts: list[dict[str, Any]] = []
    seen: set[str] = set()
    for payload in _conversation_event_payloads(runtime_db, conversation_id, event_type="conversation.chat_turn"):
        text = str(payload.get("assistant_reply") or "")
        for match in re.finditer(r"MEDIA:\s*([^\n\r]+)", text):
            candidate = match.group(1).strip().strip("`'\"")
            path = Path(candidate).expanduser()
            suffix = path.suffix.lower()
            if required and suffix not in required:
                continue
            if not path.exists() or str(path) in seen:
                continue
            seen.add(str(path))
            artifacts.append({"name": path.name, "path": str(path)})
        for result in payload.get("tool_results") or ():
            if not isinstance(result, dict) or result.get("status") != "completed":
                continue
            output = result.get("output")
            if not isinstance(output, dict):
                continue
            candidates: list[str] = []
            descriptors = output.get("artifact_descriptors")
            if isinstance(descriptors, list):
                for descriptor in descriptors:
                    if not isinstance(descriptor, dict):
                        continue
                    role = str(descriptor.get("role") or "").strip().lower()
                    path = str(descriptor.get("path") or "").strip()
                    extension = str(descriptor.get("extension") or Path(path).suffix).strip().lower()
                    if path and extension in required and role in {"", "deliverable"}:
                        candidates.append(path)
            for key in ("artifact_path", "path"):
                path = str(output.get(key) or "").strip()
                if path and (not required or Path(path).suffix.lower() in required):
                    candidates.append(path)
            for paths_key in ("artifact_paths", "attachment_paths"):
                raw_paths = output.get(paths_key)
                if not isinstance(raw_paths, list):
                    continue
                for raw_path in raw_paths:
                    path = str(raw_path or "").strip()
                    if path and (not required or Path(path).suffix.lower() in required):
                        candidates.append(path)
            for candidate in candidates:
                path = Path(candidate).expanduser()
                suffix = path.suffix.lower()
                if required and suffix not in required:
                    continue
                if not path.exists() or str(path) in seen:
                    continue
                seen.add(str(path))
                artifacts.append({"name": path.name, "path": str(path)})
    return artifacts


def _planner_group_ids_from_conversation(runtime_db: Path | None, conversation_id: str) -> set[str]:
    group_ids: set[str] = set()
    for payload in _conversation_event_payloads(runtime_db, conversation_id, event_type="conversation.chat_turn"):
        for result in payload.get("tool_results") or ():
            if not isinstance(result, dict):
                continue
            output = result.get("output")
            if isinstance(output, dict):
                group_id = str(output.get("task_group_id") or "").strip()
                if group_id:
                    group_ids.add(group_id)
    if runtime_db is None or not runtime_db.exists():
        return group_ids
    conn = sqlite3.connect(str(runtime_db))
    try:
        table_names = {
            str(row[0])
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        if "mini_agent_runs" not in table_names:
            return group_ids
        rows = conn.execute(
            """
            SELECT payload
            FROM mini_agent_runs
            WHERE json_extract(payload, '$.metadata.conversation_id') = ?
            ORDER BY updated_at ASC
            """,
            (conversation_id,),
        ).fetchall()
    finally:
        conn.close()
    for (raw_payload,) in rows:
        try:
            payload = json.loads(raw_payload)
        except Exception:
            continue
        group_id = str(payload.get("capsule_id") or "").strip()
        if group_id:
            group_ids.add(group_id)
    return group_ids


def _mini_agent_run_rows_for_groups(runtime_db: Path | None, group_ids: set[str]) -> list[dict[str, Any]]:
    if runtime_db is None or not runtime_db.exists() or not group_ids:
        return []
    conn = sqlite3.connect(str(runtime_db))
    try:
        table_names = {
            str(row[0])
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        if "mini_agent_runs" not in table_names:
            return []
        rows = conn.execute(
            """
            SELECT payload
            FROM mini_agent_runs
            WHERE json_extract(payload, '$.capsule_id') IN ({})
            ORDER BY updated_at ASC
            """.format(",".join("?" for _ in group_ids)),
            tuple(sorted(group_ids)),
        ).fetchall()
    finally:
        conn.close()
    parsed: list[dict[str, Any]] = []
    for (payload_text,) in rows:
        try:
            payload = json.loads(payload_text)
        except Exception:
            continue
        if isinstance(payload, dict):
            parsed.append(payload)
    return parsed


def _mini_agent_tool_names_for_conversation(runtime_db: Path | None, conversation_id: str) -> set[str]:
    group_ids = _planner_group_ids_from_conversation(runtime_db, conversation_id)
    names: set[str] = set()
    for run in _mini_agent_run_rows_for_groups(runtime_db, group_ids):
        metadata = run.get("metadata")
        if not isinstance(metadata, dict):
            continue
        for raw_name in metadata.get("allowed_tools") or ():
            name = str(raw_name or "").strip()
            if name:
                names.add(name)
    return names


def _wait_for_conversation_media_artifact(
    runtime_db: Path | None,
    conversation_id: str,
    *,
    required_extensions: tuple[str, ...],
    timeout_seconds: float,
) -> list[dict[str, Any]]:
    deadline = time.perf_counter() + timeout_seconds
    artifacts: list[dict[str, Any]] = []
    while True:
        artifacts = _media_artifacts_from_conversation(
            runtime_db,
            conversation_id,
            required_extensions=required_extensions,
        )
        if artifacts or time.perf_counter() >= deadline:
            return artifacts
        time.sleep(1.0)


def _tool_result_completed_requested_artifact(result: dict[str, Any], scenario: Scenario) -> bool:
    if result.get("status") != "completed":
        return False
    required_extensions = {
        extension if extension.startswith(".") else f".{extension}"
        for extension in (str(item or "").strip().lower() for item in scenario.required_artifact_extensions)
        if extension
    }
    if not required_extensions:
        return False
    output = result.get("output")
    if not isinstance(output, dict):
        return False
    descriptors = output.get("artifact_descriptors")
    if isinstance(descriptors, list):
        for descriptor in descriptors:
            if not isinstance(descriptor, dict):
                continue
            extension = str(descriptor.get("extension") or Path(str(descriptor.get("path") or "")).suffix).lower()
            role = str(descriptor.get("role") or "").lower()
            if extension in required_extensions and role in {"", "deliverable"}:
                return True
    for key in ("artifact_path", "path"):
        path = str(output.get(key) or "").strip()
        if path and Path(path).suffix.lower() in required_extensions:
            return True
    for paths_key in ("artifact_paths", "attachment_paths"):
        paths = output.get(paths_key)
        if isinstance(paths, list) and any(Path(str(path or "")).suffix.lower() in required_extensions for path in paths):
            return True
    return False


def _assert_no_forbidden_tools_after_requested_artifact(
    scenario: Scenario,
    chat_turn: dict[str, Any] | None,
) -> None:
    forbidden = set(scenario.forbidden_after_artifact_tool_any)
    if not forbidden or chat_turn is None:
        return
    tool_results = [item for item in (chat_turn.get("tool_results") or []) if isinstance(item, dict)]
    artifact_seen = False
    offenders: list[str] = []
    for result in tool_results:
        tool_name = str(result.get("tool_name") or "")
        if artifact_seen and tool_name in forbidden:
            offenders.append(tool_name)
        if _tool_result_completed_requested_artifact(result, scenario):
            artifact_seen = True
    assert artifact_seen, (
        f"{scenario.name}: no completed requested artifact found in persisted tool results; "
        f"tools={[item.get('tool_name') for item in tool_results]!r}"
    )
    assert not offenders, (
        f"{scenario.name}: forbidden tools ran after the requested artifact completed: {offenders!r}; "
        f"tools={[item.get('tool_name') for item in tool_results]!r}"
    )


def _websocket_url(base_url: str) -> str:
    parsed = urllib.parse.urlparse(base_url)
    scheme = "wss" if parsed.scheme == "https" else "ws"
    netloc = parsed.netloc or parsed.path
    return urllib.parse.urlunparse((scheme, netloc, "/ws/chat", "", "", ""))


def _png_chunk(kind: bytes, data: bytes) -> bytes:
    return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data) & 0xFFFFFFFF)


def _solid_png_bytes(width: int, height: int, rgb: tuple[int, int, int]) -> bytes:
    rows = b"".join(b"\x00" + bytes(rgb) * width for _ in range(height))
    return b"".join(
        [
            b"\x89PNG\r\n\x1a\n",
            _png_chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)),
            _png_chunk(b"IDAT", zlib.compress(rows)),
            _png_chunk(b"IEND", b""),
        ]
    )


def _write_archive_transform_fixture(directory: Path) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    archive_path = directory / "field-notes-images.zip"
    images = {
        "alpha-sun.png": (210, 72, 64),
        "beta-moonlight.png": (74, 112, 205),
        "gamma-river.png": (42, 148, 128),
    }
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as package:
        for filename, color in images.items():
            package.writestr(f"observations/{filename}", _solid_png_bytes(180, 120, color))
        package.writestr("notes/readme.md", "Synthetic archive fixture for live artifact QA.")
    return archive_path


class _QuietSimpleHTTPRequestHandler(SimpleHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        return


@contextlib.contextmanager
def _serve_directory(directory: Path):
    handler = functools.partial(_QuietSimpleHTTPRequestHandler, directory=str(directory))
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        yield f"http://{host}:{port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _run_browser_screenshot_artifact_delivery(
    *,
    base_url: str,
    runtime_db: Path | None,
    scenario: Scenario,
    timeout: int,
    index: int,
    runtime_db_available: bool,
    download_dir: Path | None = None,
) -> dict[str, Any]:
    cart_url = "https://example.com/?nullion_stage_cart=1"
    conversation_id = f"web:live-stage-contract:{scenario.name}:{uuid4().hex[:10]}:{index}"
    setup_prompt = f"Open this public test cart page and keep it open: {cart_url}"
    _setup_response, setup_elapsed_ms = _post_json(
        f"{base_url}/api/chat",
        {"text": setup_prompt, "conversation_id": conversation_id},
        timeout=timeout,
    )
    response, screenshot_elapsed_ms = _post_json(
        f"{base_url}/api/chat",
        {"text": scenario.prompt, "conversation_id": conversation_id},
        timeout=timeout,
    )
    elapsed_ms = screenshot_elapsed_ms
    artifacts = list(response.get("artifacts") or [])
    if not artifacts:
        artifacts = _wait_for_conversation_media_artifact(
            runtime_db,
            conversation_id,
            required_extensions=scenario.required_artifact_extensions,
            timeout_seconds=20.0,
        )
    final_response = dict(response)
    final_response["artifacts"] = artifacts
    tool_names = _event_tool_names_with_poll(
        runtime_db,
        conversation_id,
        required_tool_any=scenario.required_tool_any,
        required_tool_all=scenario.required_tool_all,
    )
    _assert_contract(
        scenario,
        response=final_response,
        elapsed_ms=elapsed_ms,
        tool_names=tool_names,
        runtime_db_available=runtime_db_available,
        tool_output_texts=_event_tool_output_texts_with_poll(
            runtime_db, conversation_id,
            required_output_tools=("browser_screenshot",) if scenario.require_loaded_browser_capture else (),
        ),
    )
    scenario_download_dir = (
        download_dir / scenario.name / f"run-{index}"
        if download_dir is not None
        else None
    )
    downloaded = _download_and_check_artifacts(
        base_url,
        artifacts,
        scenario=scenario,
        timeout=timeout,
        download_dir=scenario_download_dir,
    )
    return {
        "scenario": scenario.name,
        "conversation_id": conversation_id,
        "elapsed_ms": round(elapsed_ms, 1),
        "setup_elapsed_ms": round(setup_elapsed_ms, 1),
        "screenshot_elapsed_ms": round(screenshot_elapsed_ms, 1),
        "fixture_url": cart_url,
        "tools": sorted(tool_names),
        "artifact_count": len(artifacts),
        "artifact_extensions": sorted(_artifact_extensions(artifacts)),
        "downloaded_artifacts": downloaded,
    }


def _run_email_send_approval_payload_live(
    *,
    base_url: str,
    runtime_db: Path | None,
    scenario: Scenario,
    timeout: int,
    index: int,
    runtime_db_available: bool,
) -> dict[str, Any]:
    subject = f"Nullion stage approval payload {uuid4().hex[:8]}"
    body = "This is a live stage approval payload test."
    conversation_id = f"web:live-stage-contract:{scenario.name}:{uuid4().hex[:10]}:{index}"
    prompt = (
        f"Send a short test-only email to recipient@example.com with subject {subject!r} "
        f"and body {body!r}."
    )
    response, elapsed_ms = _post_json(
        f"{base_url}/api/chat",
        {"text": prompt, "conversation_id": conversation_id},
        timeout=timeout,
    )
    assert response.get("type") == "approval_required", (
        f"{scenario.name}: expected approval_required before sending email, got {response!r}"
    )
    approval_id = str(response.get("approval_id") or "").strip()
    assert approval_id, f"{scenario.name}: approval payload missing approval_id: {response!r}"
    approval_blob = json.dumps(response, ensure_ascii=False, sort_keys=True)
    lowered_blob = approval_blob.lower()
    for fragment in ("recipient@example.com", subject.lower(), body.lower()):
        assert fragment in lowered_blob, (
            f"{scenario.name}: approval payload did not preserve {fragment!r}: {approval_blob[:1000]!r}"
        )
    assert str(response.get("tool_name") or "").strip(), (
        f"{scenario.name}: approval payload missing display tool_name: {response!r}"
    )
    assert str(response.get("tool_detail") or "").strip(), (
        f"{scenario.name}: approval payload missing display tool_detail: {response!r}"
    )

    approval_result, approval_elapsed_ms = _post_json(
        f"{base_url}/api/approve/{urllib.parse.quote(approval_id)}",
        {"mode": "once"},
        timeout=timeout,
    )
    assert approval_result.get("ok") is True, (
        f"{scenario.name}: approval endpoint did not accept once approval: {approval_result!r}"
    )
    confirmation_text = str(approval_result.get("confirmation_text") or "")
    lowered_confirmation = confirmation_text.lower()
    for fragment in ("recipient@example.com", subject.lower(), body.lower()):
        assert fragment in lowered_confirmation, (
            f"{scenario.name}: approval confirmation lost {fragment!r}: {confirmation_text[:1000]!r}"
        )
    resume = approval_result.get("resume")
    assert isinstance(resume, dict), f"{scenario.name}: approval resume missing response dict: {approval_result!r}"
    elapsed_total_ms = elapsed_ms + approval_elapsed_ms
    tool_names = _event_tool_names_with_poll(
        runtime_db,
        conversation_id,
        required_tool_any=scenario.required_tool_any,
        timeout_seconds=20.0,
    )
    assert _event_tool_completed(runtime_db, conversation_id, "email_send"), (
        f"{scenario.name}: email_send appeared but did not complete after approval"
    )
    _assert_contract(
        scenario,
        response=resume,
        elapsed_ms=elapsed_total_ms,
        tool_names=tool_names,
        runtime_db_available=runtime_db_available,
        tool_output_texts=_event_tool_output_texts(runtime_db, conversation_id),
    )
    return {
        "scenario": scenario.name,
        "conversation_id": conversation_id,
        "elapsed_ms": round(elapsed_total_ms, 1),
        "approval_id": approval_id,
        "tools": sorted(tool_names),
        "artifact_count": len(resume.get("artifacts") or []),
        "checked": [
            "email_send_requires_user_approval",
            "approval_payload_preserves_recipient_subject_body",
            "approval_confirmation_preserves_recipient_subject_body",
            "approved_turn_sends_email",
        ],
    }


async def _send_ws_turn_and_wait_done(ws: Any, payload: dict[str, Any], *, timeout: int) -> list[dict[str, Any]]:
    await ws.send(json.dumps(payload))
    deadline = time.perf_counter() + timeout
    events: list[dict[str, Any]] = []
    while time.perf_counter() < deadline:
        raw = await asyncio.wait_for(ws.recv(), timeout=max(1.0, deadline - time.perf_counter()))
        event = json.loads(raw)
        if isinstance(event, dict):
            events.append(event)
            if event.get("type") == "done" and event.get("turn_id") == payload.get("turn_id"):
                return events
    raise AssertionError(f"Timed out waiting for done event for {payload.get('turn_id')!r}")


async def _run_active_followup_context_guard_async(
    *,
    base_url: str,
    runtime_db: Path | None,
    scenario: Scenario,
    timeout: int,
    index: int,
    runtime_db_available: bool,
) -> dict[str, Any]:
    try:
        import websockets
    except Exception as exc:  # pragma: no cover - depends on live probe env
        raise AssertionError("active_followup_context_guard requires the websockets package") from exc

    conversation_id = f"web:live-stage-contract:{scenario.name}:{uuid4().hex[:10]}:{index}"
    weather_prompt = (
        "What's the weather like tomorrow in New York City? Give a short answer and then ask whether I want "
        "an hour-by-hour breakdown."
    )
    followup_prompt = "Sure"
    weather_turn_id = "turn-live-weather"
    artifact_turn_id = "turn-live-artifact"
    followup_turn_id = "turn-live-sure"
    started = time.perf_counter()
    artifacts: list[Any] = []
    ws_url = _websocket_url(base_url)
    async with websockets.connect(ws_url, origin=base_url) as ws:
        await _send_ws_turn_and_wait_done(
            ws,
            {"text": weather_prompt, "conversation_id": conversation_id, "turn_id": weather_turn_id},
            timeout=timeout,
        )
        await ws.send(json.dumps({
            "text": scenario.prompt,
            "conversation_id": conversation_id,
            "turn_id": artifact_turn_id,
        }))
        await asyncio.sleep(0.25)
        await ws.send(json.dumps({
            "text": followup_prompt,
            "conversation_id": conversation_id,
            "turn_id": followup_turn_id,
        }))
        pending = {artifact_turn_id, followup_turn_id}
        deadline = time.perf_counter() + timeout
        while pending and time.perf_counter() < deadline:
            raw = await asyncio.wait_for(ws.recv(), timeout=max(1.0, deadline - time.perf_counter()))
            event = json.loads(raw)
            if not isinstance(event, dict):
                continue
            if event.get("type") == "done" and event.get("turn_id") in pending:
                pending.discard(str(event.get("turn_id")))
                artifacts.extend(event.get("artifacts") or [])
        if pending:
            raise AssertionError(f"Timed out waiting for live WebSocket turns: {sorted(pending)!r}")

    elapsed_ms = (time.perf_counter() - started) * 1000
    follow_turn = _turn_payload_with_poll(runtime_db, conversation_id, followup_turn_id)
    artifact_turn = _turn_payload_with_poll(runtime_db, conversation_id, artifact_turn_id)
    weather_turn = _turn_payload_with_poll(runtime_db, conversation_id, weather_turn_id)
    assert follow_turn is not None, f"{scenario.name}: missing persisted follow-up turn"
    assert artifact_turn is not None, f"{scenario.name}: missing persisted active artifact turn"
    assert weather_turn is not None, f"{scenario.name}: missing persisted weather setup turn"
    assert follow_turn.get("parent_turn_id") != artifact_turn_id, (
        f"{scenario.name}: vague follow-up was attached to active artifact turn: {follow_turn!r}"
    )
    sure_chat_turn = _chat_turn_for_user_message_with_poll(runtime_db, conversation_id, followup_prompt)
    assert sure_chat_turn is not None, f"{scenario.name}: missing final chat turn for vague follow-up"
    text = str(sure_chat_turn.get("assistant_reply") or "")
    assert text.strip(), f"{scenario.name}: vague follow-up produced an empty assistant reply"
    _assert_contract(
        scenario,
        response={"text": text, "artifacts": artifacts},
        elapsed_ms=elapsed_ms,
        tool_names=_event_tool_names(runtime_db, conversation_id),
        runtime_db_available=runtime_db_available,
    )
    return {
        "scenario": scenario.name,
        "conversation_id": conversation_id,
        "elapsed_ms": round(elapsed_ms, 1),
        "followup_parent_turn_id": follow_turn.get("parent_turn_id"),
        "artifact_turn_id": artifact_turn_id,
        "artifact_count": len(artifacts),
        "artifact_extensions": sorted(_artifact_extensions(artifacts)),
        "tools": sorted(_event_tool_names(runtime_db, conversation_id)),
    }


def _run_active_followup_context_guard(
    *,
    base_url: str,
    runtime_db: Path | None,
    scenario: Scenario,
    timeout: int,
    index: int,
    runtime_db_available: bool,
) -> dict[str, Any]:
    return asyncio.run(
        _run_active_followup_context_guard_async(
            base_url=base_url,
            runtime_db=runtime_db,
            scenario=scenario,
            timeout=timeout,
            index=index,
            runtime_db_available=runtime_db_available,
        )
    )


async def _run_active_related_followup_context_guard_async(
    *,
    base_url: str,
    runtime_db: Path | None,
    scenario: Scenario,
    timeout: int,
    index: int,
    runtime_db_available: bool,
    download_dir: Path | None = None,
) -> dict[str, Any]:
    try:
        import websockets
    except Exception as exc:  # pragma: no cover - depends on live probe env
        raise AssertionError("active_related_followup_context_guard requires the websockets package") from exc

    assert scenario.followups, f"{scenario.name}: scenario must define the related follow-up prompt"
    followup_prompt = scenario.followups[0]
    conversation_id = f"web:live-stage-contract:{scenario.name}:{uuid4().hex[:10]}:{index}"
    artifact_turn_id = "turn-live-related-artifact"
    followup_turn_id = "turn-live-related-followup"
    started = time.perf_counter()
    artifacts: list[Any] = []
    done_text_by_turn: dict[str, str] = {}
    ws_url = _websocket_url(base_url)
    async with websockets.connect(ws_url, origin=base_url) as ws:
        await ws.send(json.dumps({
            "text": scenario.prompt,
            "conversation_id": conversation_id,
            "turn_id": artifact_turn_id,
        }))
        await asyncio.sleep(0.05)
        await ws.send(json.dumps({
            "text": followup_prompt,
            "conversation_id": conversation_id,
            "turn_id": followup_turn_id,
        }))
        pending = {artifact_turn_id, followup_turn_id}
        deadline = time.perf_counter() + timeout
        while pending and time.perf_counter() < deadline:
            raw = await asyncio.wait_for(ws.recv(), timeout=max(1.0, deadline - time.perf_counter()))
            event = json.loads(raw)
            if not isinstance(event, dict):
                continue
            if event.get("type") == "done" and event.get("turn_id") in pending:
                turn_id = str(event.get("turn_id") or "")
                pending.discard(turn_id)
                artifacts.extend(event.get("artifacts") or [])
                done_text_by_turn[turn_id] = str(event.get("text") or "")
        if pending:
            raise AssertionError(f"Timed out waiting for live WebSocket turns: {sorted(pending)!r}")

    elapsed_ms = (time.perf_counter() - started) * 1000
    artifact_turn = _turn_payload_with_poll(runtime_db, conversation_id, artifact_turn_id)
    follow_turn = _turn_payload_with_poll(runtime_db, conversation_id, followup_turn_id)
    assert artifact_turn is not None, f"{scenario.name}: missing persisted active artifact turn"
    assert follow_turn is not None, f"{scenario.name}: missing persisted related follow-up turn"
    assert follow_turn.get("parent_turn_id") == artifact_turn_id, (
        f"{scenario.name}: related follow-up was not linked to active artifact turn: {follow_turn!r}"
    )
    followup_chat_turn = _chat_turn_for_user_message_with_poll(runtime_db, conversation_id, followup_prompt)
    assert followup_chat_turn is not None, f"{scenario.name}: missing related follow-up chat turn"
    if followup_chat_turn.get("parent_turn_id") is not None:
        assert followup_chat_turn.get("parent_turn_id") == artifact_turn_id, (
            f"{scenario.name}: follow-up chat turn parent mismatch: {followup_chat_turn!r}"
        )
    if not artifacts:
        artifacts = _wait_for_conversation_media_artifact(
            runtime_db,
            conversation_id,
            required_extensions=scenario.required_artifact_extensions,
            timeout_seconds=20.0,
        )
    final_text = (
        str(followup_chat_turn.get("assistant_reply") or "").strip()
        or done_text_by_turn.get(followup_turn_id, "").strip()
        or "Related follow-up artifact delivered."
    )
    tool_names = _event_tool_names_with_poll(
        runtime_db,
        conversation_id,
        required_tool_any=scenario.required_tool_any,
        required_tool_all=scenario.required_tool_all,
    )
    followup_elapsed_ms = _latest_turn_latency_total_ms_with_poll(
        runtime_db,
        conversation_id,
        followup_turn_id,
    )
    measured_followup_elapsed_ms = followup_elapsed_ms if followup_elapsed_ms is not None else elapsed_ms
    _assert_contract(
        scenario,
        response={"text": final_text, "artifacts": artifacts},
        elapsed_ms=measured_followup_elapsed_ms,
        tool_names=tool_names,
        runtime_db_available=runtime_db_available,
        tool_output_texts=_event_tool_output_texts(runtime_db, conversation_id),
    )
    scenario_download_dir = (
        download_dir / scenario.name / f"run-{index}"
        if download_dir is not None
        else None
    )
    downloaded = _download_and_check_artifacts(
        base_url,
        artifacts,
        scenario=scenario,
        timeout=timeout,
        download_dir=scenario_download_dir,
    )
    return {
        "scenario": scenario.name,
        "conversation_id": conversation_id,
        "elapsed_ms": round(measured_followup_elapsed_ms, 1),
        "session_elapsed_ms": round(elapsed_ms, 1),
        "followup_parent_turn_id": follow_turn.get("parent_turn_id"),
        "artifact_turn_id": artifact_turn_id,
        "tools": sorted(tool_names),
        "artifact_count": len(artifacts),
        "artifact_extensions": sorted(_artifact_extensions(artifacts)),
        "downloaded_artifacts": downloaded,
    }


def _run_active_related_followup_context_guard(
    *,
    base_url: str,
    runtime_db: Path | None,
    scenario: Scenario,
    timeout: int,
    index: int,
    runtime_db_available: bool,
    download_dir: Path | None = None,
) -> dict[str, Any]:
    return asyncio.run(
        _run_active_related_followup_context_guard_async(
            base_url=base_url,
            runtime_db=runtime_db,
            scenario=scenario,
            timeout=timeout,
            index=index,
            runtime_db_available=runtime_db_available,
            download_dir=download_dir,
        )
    )


def _run_file_search_listing_no_attachment(
    *,
    base_url: str,
    runtime_db: Path | None,
    scenario: Scenario,
    timeout: int,
    index: int,
    runtime_db_available: bool,
) -> dict[str, Any]:
    artifact_root = Path(
        os.environ.get(
            "NULLION_LIVE_STAGE_ARTIFACT_ROOT",
            str(Path.home() / ".nullion-stage" / "workspaces" / "workspace_admin" / "artifacts"),
        )
    )
    artifact_root.mkdir(parents=True, exist_ok=True)
    fixture = artifact_root / f"stage_file_search_delivery_guard_{uuid4().hex[:10]}.pdf"
    fixture.write_bytes(b"%PDF-1.4\n% live stage file-search delivery guard\n")
    conversation_id = f"web:live-stage-contract:{scenario.name}:{uuid4().hex[:10]}:{index}"
    prompt = (
        f"Find the pdf file named {fixture.name} in my workspace and reply with a concise filename-only listing. "
        "Do not attach, upload, or send the file."
    )
    response, elapsed_ms = _post_json(
        f"{base_url}/api/chat",
        {"text": prompt, "conversation_id": conversation_id},
        timeout=timeout,
    )
    tool_names = _event_tool_names_with_poll(
        runtime_db,
        conversation_id,
        required_tool_any=scenario.required_tool_any,
    )
    _assert_contract(
        scenario,
        response=response,
        elapsed_ms=elapsed_ms,
        tool_names=tool_names,
        runtime_db_available=runtime_db_available,
    )
    text = str(response.get("text") or response.get("reply") or "")
    assert fixture.name in text, f"{scenario.name}: fixture filename missing from response: {text[:1000]!r}"
    assert str(fixture) not in text, f"{scenario.name}: leaked absolute fixture path: {text[:1000]!r}"
    chat_turn = _chat_turn_for_user_message(runtime_db, conversation_id, prompt)
    if chat_turn is not None:
        assistant_reply = str(chat_turn.get("assistant_reply") or "")
        assert "MEDIA:" not in assistant_reply, (
            f"{scenario.name}: persisted reply still contains media directive: {assistant_reply[:1000]!r}"
        )
        latency = chat_turn.get("latency")
        if isinstance(latency, dict) and "artifact_count" in latency:
            assert latency.get("artifact_count") == 0, (
                f"{scenario.name}: persisted turn recorded artifacts: {latency!r}"
            )
    return {
        "scenario": scenario.name,
        "conversation_id": conversation_id,
        "elapsed_ms": round(elapsed_ms, 1),
        "fixture": fixture.name,
        "tools": sorted(tool_names),
        "artifact_count": len(response.get("artifacts") or []),
        "checked": [
            "file_search_listing_does_not_attach_existing_pdf",
            "persisted_reply_has_no_media_directive",
        ],
    }


def _write_minimal_xlsx(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as package:
        package.writestr(
            "[Content_Types].xml",
            """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/sharedStrings.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/>
</Types>""",
        )
        package.writestr(
            "_rels/.rels",
            """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>""",
        )
        package.writestr(
            "xl/workbook.xml",
            """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets><sheet name="QA" sheetId="1" r:id="rId1"/></sheets>
</workbook>""",
        )
        package.writestr(
            "xl/_rels/workbook.xml.rels",
            """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" Target="sharedStrings.xml"/>
</Relationships>""",
        )
        package.writestr(
            "xl/worksheets/sheet1.xml",
            """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetData>
    <row r="1"><c r="A1" t="s"><v>0</v></c><c r="B1" t="s"><v>1</v></c></row>
    <row r="2"><c r="A2" t="s"><v>2</v></c><c r="B2" t="s"><v>3</v></c></row>
  </sheetData>
</worksheet>""",
        )
        package.writestr(
            "xl/sharedStrings.xml",
            """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" count="4" uniqueCount="4">
  <si><t>Field</t></si><si><t>Value</t></si><si><t>Stage contract</t></si><si><t>Requested file delivery</t></si>
</sst>""",
        )


def _run_requested_workspace_file_delivery(
    *,
    base_url: str,
    runtime_db: Path | None,
    scenario: Scenario,
    timeout: int,
    index: int,
    runtime_db_available: bool,
    download_dir: Path | None = None,
) -> dict[str, Any]:
    artifact_root = Path(
        os.environ.get(
            "NULLION_LIVE_STAGE_ARTIFACT_ROOT",
            str(Path.home() / ".nullion-stage" / "workspaces" / "workspace_admin" / "artifacts"),
        )
    )
    fixture = artifact_root / f"stage_requested_file_delivery_{uuid4().hex[:10]}.xlsx"
    _write_minimal_xlsx(fixture)
    conversation_id = f"web:live-stage-contract:{scenario.name}:{uuid4().hex[:10]}:{index}"
    prompt = (
        f"Adjunta el archivo exacto llamado {fixture.name} como archivo descargable. "
        "No respondas con una lista de archivos ni con una ruta local."
    )
    response, elapsed_ms = _post_json(
        f"{base_url}/api/chat",
        {"text": prompt, "conversation_id": conversation_id},
        timeout=timeout,
    )
    tool_names = _event_tool_names_with_poll(
        runtime_db,
        conversation_id,
        required_tool_any=scenario.required_tool_any,
    )
    _assert_contract(
        scenario,
        response=response,
        elapsed_ms=elapsed_ms,
        tool_names=tool_names,
        runtime_db_available=runtime_db_available,
        tool_output_texts=_event_tool_output_texts(runtime_db, conversation_id),
    )
    artifacts = list(response.get("artifacts") or [])
    artifact_names = {Path(_artifact_name(artifact)).name for artifact in artifacts}
    expected_stem = fixture.stem
    delivered_requested_xlsx = any(
        name == fixture.name or (name.startswith(f"{expected_stem}-") and name.endswith(fixture.suffix))
        for name in artifact_names
    )
    assert delivered_requested_xlsx, (
        f"{scenario.name}: requested fixture was not delivered; expected {fixture.name!r} or a collision-safe copy, "
        f"got {sorted(artifact_names)!r}; response={response!r}"
    )
    text = str(response.get("text") or response.get("reply") or "")
    assert str(fixture) not in text, f"{scenario.name}: leaked absolute fixture path: {text[:1000]!r}"
    scenario_download_dir = download_dir / scenario.name / f"run-{index}" if download_dir is not None else None
    downloaded = _download_and_check_artifacts(
        base_url,
        artifacts,
        scenario=scenario,
        timeout=timeout,
        download_dir=scenario_download_dir,
    )
    return {
        "scenario": scenario.name,
        "conversation_id": conversation_id,
        "elapsed_ms": round(elapsed_ms, 1),
        "fixture": fixture.name,
        "tools": sorted(tool_names),
        "artifact_count": len(artifacts),
        "artifact_extensions": sorted(_artifact_extensions(artifacts)),
        "downloaded_artifacts": downloaded,
        "checked": [
            "requested_workspace_xlsx_delivered_as_artifact",
            "response_did_not_degrade_to_file_listing",
            "downloaded_xlsx_validated",
        ],
    }


def _run_scheduler_manual_run(
    *,
    base_url: str,
    runtime_db: Path | None,
    scenario: Scenario,
    timeout: int,
    index: int,
    runtime_db_available: bool,
) -> dict[str, Any]:
    fixture_name = f"Stage live scheduler manual run {uuid4().hex[:10]}"
    created_cron_id = ""
    conversation_id = f"web:live-stage-contract:{scenario.name}:{uuid4().hex[:10]}:{index}"
    try:
        created, _elapsed = _post_json(
            f"{base_url}/api/crons",
            {
                "name": fixture_name,
                "schedule": "*/30 * * * *",
                "task": f"Reply exactly: Manual scheduled task run started for {fixture_name}.",
                "enabled": True,
                "workspace_id": "workspace_admin",
                "delivery_channel": "web",
                "delivery_target": conversation_id,
            },
            timeout=timeout,
        )
        assert created.get("ok") is True, f"{scenario.name}: could not create fixture cron: {created!r}"
        job = created.get("job")
        assert isinstance(job, dict), f"{scenario.name}: fixture cron response missing job: {created!r}"
        created_cron_id = str(job.get("id") or "").strip()
        assert created_cron_id, f"{scenario.name}: fixture cron response missing id: {created!r}"

        prompt = (
            f"Find the enabled scheduled task named {fixture_name!r} in my current cron list and start "
            "one manual run now. Use the scheduler tools; do not just list the crons."
        )
        response, elapsed_ms = _post_json(
            f"{base_url}/api/chat",
            {"text": prompt, "conversation_id": conversation_id},
            timeout=timeout,
        )
        tool_names = _event_tool_names_with_poll(
            runtime_db,
            conversation_id,
            required_tool_any=scenario.required_tool_any,
            required_tool_all=scenario.required_tool_all,
        )
        _assert_contract(
            scenario,
            response=response,
            elapsed_ms=elapsed_ms,
            tool_names=tool_names,
            runtime_db_available=runtime_db_available,
        )
        artifacts = list(response.get("artifacts") or [])
        result = {
            "scenario": scenario.name,
            "conversation_id": conversation_id,
            "elapsed_ms": round(elapsed_ms, 1),
            "tools": sorted(tool_names),
            "artifact_count": len(artifacts),
            "artifact_extensions": sorted(_artifact_extensions(artifacts)),
            "downloaded_artifacts": [],
            "fixture_cron_id": created_cron_id,
            "fixture_cron_name": fixture_name,
        }
        chat_turn = _chat_turn_for_user_message(runtime_db, conversation_id, prompt)
        if chat_turn is not None:
            latency = chat_turn.get("latency")
            if isinstance(latency, dict):
                result["model_start_ms"] = latency.get("model_start_ms")
        return result
    finally:
        if created_cron_id:
            try:
                _delete_json(
                    f"{base_url}/api/crons/{urllib.parse.quote(created_cron_id)}",
                    timeout=timeout,
                )
            except Exception:
                pass


def _run_scheduler_manual_run_short_followup(
    *,
    base_url: str,
    runtime_db: Path | None,
    scenario: Scenario,
    timeout: int,
    index: int,
    runtime_db_available: bool,
) -> dict[str, Any]:
    fixture_name = f"Stage live scheduler short followup {uuid4().hex[:10]}"
    created_cron_id = ""
    conversation_id = f"web:live-stage-contract:{scenario.name}:{uuid4().hex[:10]}:{index}"
    followup_prompt = "Trigger this again"
    try:
        created, _elapsed = _post_json(
            f"{base_url}/api/crons",
            {
                "name": fixture_name,
                "schedule": "*/30 * * * *",
                "task": f"Reply exactly: Manual scheduled task run started for {fixture_name}.",
                "enabled": True,
                "workspace_id": "workspace_admin",
                "delivery_channel": "web",
                "delivery_target": conversation_id,
            },
            timeout=timeout,
        )
        assert created.get("ok") is True, f"{scenario.name}: could not create fixture cron: {created!r}"
        job = created.get("job")
        assert isinstance(job, dict), f"{scenario.name}: fixture cron response missing job: {created!r}"
        created_cron_id = str(job.get("id") or "").strip()
        assert created_cron_id, f"{scenario.name}: fixture cron response missing id: {created!r}"

        setup_prompt = (
            f"Find the enabled scheduled task named {fixture_name!r} in my current cron list. "
            "Do not run it yet; just identify the single matching cron."
        )
        _setup_response, setup_elapsed_ms = _post_json(
            f"{base_url}/api/chat",
            {"text": setup_prompt, "conversation_id": conversation_id},
            timeout=timeout,
        )
        response, followup_elapsed_ms = _post_json(
            f"{base_url}/api/chat",
            {"text": followup_prompt, "conversation_id": conversation_id},
            timeout=timeout,
        )
        followup_tools = _tool_names_for_user_message_with_poll(
            runtime_db,
            conversation_id,
            followup_prompt,
            required_tool_any=scenario.required_tool_any,
            timeout_seconds=30.0,
        )
        _assert_contract(
            scenario,
            response=response,
            elapsed_ms=followup_elapsed_ms,
            tool_names=followup_tools,
            runtime_db_available=runtime_db_available,
        )
        return {
            "scenario": scenario.name,
            "conversation_id": conversation_id,
            "setup_elapsed_ms": round(setup_elapsed_ms, 1),
            "followup_elapsed_ms": round(followup_elapsed_ms, 1),
            "elapsed_ms": round(setup_elapsed_ms + followup_elapsed_ms, 1),
            "tools": sorted(followup_tools),
            "artifact_count": len(response.get("artifacts") or []),
            "fixture_cron_id": created_cron_id,
            "fixture_cron_name": fixture_name,
        }
    finally:
        if created_cron_id:
            try:
                _delete_json(
                    f"{base_url}/api/crons/{urllib.parse.quote(created_cron_id)}",
                    timeout=timeout,
                )
            except Exception:
                pass


def _run_scheduler_bulk_enable_recovery(
    *,
    base_url: str,
    runtime_db: Path | None,
    scenario: Scenario,
    timeout: int,
    index: int,
    runtime_db_available: bool,
) -> dict[str, Any]:
    fixture_name = f"Stage live bulk enable recovery {uuid4().hex[:10]}"
    created_cron_id = ""
    conversation_id = f"web:live-stage-contract:{scenario.name}:{uuid4().hex[:10]}:{index}"
    initial_rows = _get_json(f"{base_url}/api/crons", timeout=timeout)
    initial_enabled_by_id = {
        str(row.get("id") or ""): bool(row.get("enabled"))
        for row in initial_rows
        if isinstance(row, dict) and str(row.get("id") or "").strip()
    }
    try:
        created, _elapsed = _post_json(
            f"{base_url}/api/crons",
            {
                "name": fixture_name,
                "schedule": "*/45 * * * *",
                "task": f"Reply exactly: stage bulk enable fixture {fixture_name}.",
                "enabled": False,
                "workspace_id": "workspace_admin",
                "delivery_channel": "web",
                "delivery_target": conversation_id,
            },
            timeout=timeout,
        )
        assert created.get("ok") is True, f"{scenario.name}: could not create fixture cron: {created!r}"
        job = created.get("job")
        assert isinstance(job, dict), f"{scenario.name}: fixture cron response missing job: {created!r}"
        created_cron_id = str(job.get("id") or "").strip()
        assert created_cron_id, f"{scenario.name}: fixture cron response missing id: {created!r}"

        response, elapsed_ms = _post_json(
            f"{base_url}/api/chat",
            {"text": scenario.prompt, "conversation_id": conversation_id},
            timeout=timeout,
        )
        tool_names = _event_tool_names_with_poll(
            runtime_db,
            conversation_id,
            required_tool_any=scenario.required_tool_any or scenario.required_tool_all,
        )
        _assert_contract(
            scenario,
            response=response,
            elapsed_ms=elapsed_ms,
            tool_names=tool_names,
            runtime_db_available=runtime_db_available,
        )
        rows_after = _get_json(f"{base_url}/api/crons", timeout=timeout)
        fixture_after = next(
            (
                row
                for row in rows_after
                if isinstance(row, dict) and str(row.get("id") or "") == created_cron_id
            ),
            None,
        )
        assert fixture_after is not None, f"{scenario.name}: fixture cron disappeared before cleanup"
        assert bool(fixture_after.get("enabled")) is True, (
            f"{scenario.name}: fixture cron was not enabled by chat turn: {fixture_after!r}"
        )
        text = str(response.get("text") or response.get("reply") or "")
        assert ".noop" not in text, f"{scenario.name}: response leaked file-search fallback: {text[:1000]!r}"
        return {
            "scenario": scenario.name,
            "conversation_id": conversation_id,
            "elapsed_ms": round(elapsed_ms, 1),
            "tools": sorted(tool_names),
            "artifact_count": len(response.get("artifacts") or []),
            "fixture_cron_id": created_cron_id,
            "fixture_cron_name": fixture_name,
            "checked": [
                "bulk_enable_uses_scheduler_tools",
                "file_search_not_used",
                "fixture_cron_enabled",
            ],
        }
    finally:
        for cron_id, enabled in initial_enabled_by_id.items():
            if not cron_id:
                continue
            try:
                _patch_json(
                    f"{base_url}/api/crons/{urllib.parse.quote(cron_id)}",
                    {"enabled": enabled},
                    timeout=timeout,
                )
            except Exception:
                pass
        if created_cron_id:
            try:
                _delete_json(
                    f"{base_url}/api/crons/{urllib.parse.quote(created_cron_id)}",
                    timeout=timeout,
                )
            except Exception:
                pass


def _run_scheduler_disable_not_delete(
    *,
    base_url: str,
    runtime_db: Path | None,
    scenario: Scenario,
    timeout: int,
    index: int,
    runtime_db_available: bool,
) -> dict[str, Any]:
    fixture_name = f"Stage live disable fixture {uuid4().hex[:10]}"
    created_cron_id = ""
    conversation_id = f"web:live-stage-contract:{scenario.name}:{uuid4().hex[:10]}:{index}"
    try:
        created, _elapsed = _post_json(
            f"{base_url}/api/crons",
            {
                "name": fixture_name,
                "schedule": "*/50 * * * *",
                "task": f"Reply exactly: stage disable fixture {fixture_name}.",
                "enabled": True,
                "workspace_id": "workspace_admin",
                "delivery_channel": "web",
                "delivery_target": conversation_id,
            },
            timeout=timeout,
        )
        assert created.get("ok") is True, f"{scenario.name}: could not create fixture cron: {created!r}"
        job = created.get("job")
        assert isinstance(job, dict), f"{scenario.name}: fixture cron response missing job: {created!r}"
        created_cron_id = str(job.get("id") or "").strip()
        assert created_cron_id, f"{scenario.name}: fixture cron response missing id: {created!r}"

        prompt = (
            f"Disable the scheduled task named {fixture_name!r}. Do not delete it. "
            "Use the scheduler tools and then tell me it is disabled."
        )
        response, elapsed_ms = _post_json(
            f"{base_url}/api/chat",
            {"text": prompt, "conversation_id": conversation_id},
            timeout=timeout,
        )
        tool_names = _event_tool_names_with_poll(
            runtime_db,
            conversation_id,
            required_tool_any=scenario.required_tool_any or scenario.required_tool_all,
            required_tool_all=scenario.required_tool_all,
        )
        _assert_contract(
            scenario,
            response=response,
            elapsed_ms=elapsed_ms,
            tool_names=tool_names,
            runtime_db_available=runtime_db_available,
            tool_output_texts=_event_tool_output_texts(runtime_db, conversation_id),
        )
        rows_after = _get_json(f"{base_url}/api/crons", timeout=timeout)
        fixture_after = next(
            (
                row
                for row in rows_after
                if isinstance(row, dict) and str(row.get("id") or "") == created_cron_id
            ),
            None,
        )
        assert fixture_after is not None, f"{scenario.name}: fixture cron was deleted instead of disabled"
        assert bool(fixture_after.get("enabled")) is False, (
            f"{scenario.name}: fixture cron still enabled after disable request: {fixture_after!r}"
        )
        return {
            "scenario": scenario.name,
            "conversation_id": conversation_id,
            "elapsed_ms": round(elapsed_ms, 1),
            "tools": sorted(tool_names),
            "artifact_count": len(response.get("artifacts") or []),
            "fixture_cron_id": created_cron_id,
            "fixture_cron_name": fixture_name,
            "checked": [
                "disable_uses_scheduler_tools",
                "fixture_cron_still_exists",
                "fixture_cron_disabled",
            ],
        }
    finally:
        if created_cron_id:
            try:
                _delete_json(
                    f"{base_url}/api/crons/{urllib.parse.quote(created_cron_id)}",
                    timeout=timeout,
                )
            except Exception:
                pass


def _tool_names_from_chat_turn(chat_turn: dict[str, Any] | None) -> set[str]:
    if not isinstance(chat_turn, dict):
        return set()
    names: set[str] = set()
    for result in chat_turn.get("tool_results") or ():
        if isinstance(result, dict):
            name = str(result.get("tool_name") or result.get("name") or "").strip()
            if name:
                names.add(name)
    return names


def _run_scheduler_fresh_followup_after_bulk_enable(
    *,
    base_url: str,
    runtime_db: Path | None,
    scenario: Scenario,
    timeout: int,
    index: int,
    runtime_db_available: bool,
) -> dict[str, Any]:
    fixture_name = f"Stage live fresh cron check {uuid4().hex[:10]}"
    created_cron_id = ""
    conversation_id = f"web:live-stage-contract:{scenario.name}:{uuid4().hex[:10]}:{index}"
    initial_rows = _get_json(f"{base_url}/api/crons", timeout=timeout)
    initial_enabled_by_id = {
        str(row.get("id") or ""): bool(row.get("enabled"))
        for row in initial_rows
        if isinstance(row, dict) and str(row.get("id") or "").strip()
    }
    followup_prompt = "Do a fresh check"
    try:
        created, _elapsed = _post_json(
            f"{base_url}/api/crons",
            {
                "name": fixture_name,
                "schedule": "*/45 * * * *",
                "task": f"Reply exactly: stage fresh cron fixture {fixture_name}.",
                "enabled": False,
                "workspace_id": "workspace_admin",
                "delivery_channel": "web",
                "delivery_target": conversation_id,
            },
            timeout=timeout,
        )
        assert created.get("ok") is True, f"{scenario.name}: could not create fixture cron: {created!r}"
        job = created.get("job")
        assert isinstance(job, dict), f"{scenario.name}: fixture cron response missing job: {created!r}"
        created_cron_id = str(job.get("id") or "").strip()
        assert created_cron_id, f"{scenario.name}: fixture cron response missing id: {created!r}"

        first_response, first_elapsed_ms = _post_json(
            f"{base_url}/api/chat",
            {"text": scenario.prompt, "conversation_id": conversation_id},
            timeout=timeout,
        )
        first_tool_names = _event_tool_names_with_poll(
            runtime_db,
            conversation_id,
            required_tool_any=scenario.required_tool_any,
            required_tool_all=scenario.required_tool_all,
        )
        _assert_contract(
            scenario,
            response=first_response,
            elapsed_ms=first_elapsed_ms,
            tool_names=first_tool_names,
            runtime_db_available=runtime_db_available,
        )

        followup_response, followup_elapsed_ms = _post_json(
            f"{base_url}/api/chat",
            {"text": followup_prompt, "conversation_id": conversation_id},
            timeout=timeout,
        )
        followup_text = str(followup_response.get("text") or followup_response.get("reply") or "")
        forbidden = (
            "not exposed",
            "not available",
            "latest successful action",
            "few minutes ago",
            "send \"check crons\"",
            "send “check crons”",
        )
        hits = [fragment for fragment in forbidden if fragment in followup_text.lower()]
        assert not hits, f"{scenario.name}: follow-up answered from stale/no-tool path {hits!r}: {followup_text[:1000]!r}"
        followup_tool_names = _tool_names_for_user_message_with_poll(
            runtime_db,
            conversation_id,
            followup_prompt,
            required_tool_any=("list_crons",),
            timeout_seconds=180.0,
        )
        if runtime_db_available:
            assert "list_crons" in followup_tool_names, (
                f"{scenario.name}: follow-up did not run live scheduler read; "
                f"turn tools={sorted(followup_tool_names)!r}; text={followup_text[:1000]!r}"
            )
            assert "file_search" not in followup_tool_names, (
                f"{scenario.name}: follow-up fell back to file search instead of scheduler: "
                f"{sorted(followup_tool_names)!r}"
            )
            assert "run_cron" not in followup_tool_names, (
                f"{scenario.name}: follow-up started a manual cron run instead of checking inventory: "
                f"{sorted(followup_tool_names)!r}"
            )
        return {
            "scenario": scenario.name,
            "conversation_id": conversation_id,
            "first_elapsed_ms": round(first_elapsed_ms, 1),
            "followup_elapsed_ms": round(followup_elapsed_ms, 1),
            "first_tools": sorted(first_tool_names),
            "followup_tools": sorted(followup_tool_names),
            "artifact_count": len(followup_response.get("artifacts") or []),
            "fixture_cron_id": created_cron_id,
            "fixture_cron_name": fixture_name,
            "checked": [
                "bulk_enable_uses_scheduler_tools",
                "followup_runs_list_crons",
                "followup_not_stale_history_answer",
            ],
        }
    finally:
        for cron_id, enabled in initial_enabled_by_id.items():
            if not cron_id:
                continue
            try:
                _patch_json(
                    f"{base_url}/api/crons/{urllib.parse.quote(cron_id)}",
                    {"enabled": enabled},
                    timeout=timeout,
                )
            except Exception:
                pass
        if created_cron_id:
            try:
                _delete_json(
                    f"{base_url}/api/crons/{urllib.parse.quote(created_cron_id)}",
                    timeout=timeout,
                )
            except Exception:
                pass


def _run_local_http_archive_transform_workbook(
    *,
    base_url: str,
    runtime_db: Path | None,
    scenario: Scenario,
    timeout: int,
    index: int,
    runtime_db_available: bool,
    download_dir: Path | None = None,
) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="nullion-live-archive-transform-") as temp_name:
        fixture_dir = Path(temp_name)
        archive_path = _write_archive_transform_fixture(fixture_dir)
        with _serve_directory(fixture_dir) as fixture_base_url:
            archive_url = f"{fixture_base_url}/{urllib.parse.quote(archive_path.name)}"
            prompt = scenario.prompt.format(archive_url=archive_url)
            conversation_id = f"web:live-stage-contract:{scenario.name}:{uuid4().hex[:10]}:{index}"
            response, elapsed_ms = _post_json(
                f"{base_url}/api/chat",
                {"text": prompt, "conversation_id": conversation_id},
                timeout=timeout,
            )
            tool_names = _event_tool_names_with_poll(
                runtime_db,
                conversation_id,
                required_tool_all=scenario.required_tool_all,
            )
            _assert_contract(
                scenario,
                response=response,
                elapsed_ms=elapsed_ms,
                tool_names=tool_names,
                runtime_db_available=runtime_db_available,
                tool_output_texts=_event_tool_output_texts(runtime_db, conversation_id),
            )
            chat_turn = _chat_turn_for_user_message(runtime_db, conversation_id, prompt)
            _assert_no_forbidden_tools_after_requested_artifact(scenario, chat_turn)
            artifacts = list(response.get("artifacts") or [])
            scenario_download_dir = (
                download_dir / scenario.name / f"run-{index}"
                if download_dir is not None
                else None
            )
            downloaded = _download_and_check_artifacts(
                base_url,
                artifacts,
                scenario=scenario,
                timeout=timeout,
                download_dir=scenario_download_dir,
            )
            return {
                "scenario": scenario.name,
                "conversation_id": conversation_id,
                "elapsed_ms": round(elapsed_ms, 1),
                "fixture": archive_path.name,
                "tools": sorted(tool_names),
                "artifact_count": len(artifacts),
                "artifact_extensions": sorted(_artifact_extensions(artifacts)),
                "downloaded_artifacts": downloaded,
                "post_artifact_forbidden_tools_checked": bool(scenario.forbidden_after_artifact_tool_any),
            }


def _run_stage_workspace_archive_artifact_retry_guard(
    *,
    base_url: str,
    runtime_db: Path | None,
    scenario: Scenario,
    timeout: int,
    index: int,
    runtime_db_available: bool,
    download_dir: Path | None = None,
) -> dict[str, Any]:
    upload_root = Path(
        os.environ.get(
            "NULLION_LIVE_STAGE_UPLOAD_ROOT",
            str(Path.home() / ".nullion-stage" / "workspaces" / "workspace_admin" / "uploads"),
        )
    )
    fixture_dir = upload_root / "live_stage_archive_fixtures" / f"{scenario.name}-{uuid4().hex[:10]}"
    archive_path = _write_archive_transform_fixture(fixture_dir)
    output_name = f"stage_artifact_followup_retry_guard_{uuid4().hex[:10]}.xlsx"
    scenario_for_run = replace(scenario, required_artifact_names=(output_name,))
    prompt = scenario.prompt.format(archive_path=archive_path, output_name=output_name)
    conversation_id = f"web:live-stage-contract:{scenario.name}:{uuid4().hex[:10]}:{index}"
    response, elapsed_ms = _post_json(
        f"{base_url}/api/chat",
        {"text": prompt, "conversation_id": conversation_id},
        timeout=timeout,
    )
    tool_names = _event_tool_names_with_poll(
        runtime_db,
        conversation_id,
        required_tool_all=scenario_for_run.required_tool_all,
    )
    _assert_contract(
        scenario_for_run,
        response=response,
        elapsed_ms=elapsed_ms,
        tool_names=tool_names,
        runtime_db_available=runtime_db_available,
        tool_output_texts=_event_tool_output_texts(runtime_db, conversation_id),
    )
    chat_turn = _chat_turn_for_user_message(runtime_db, conversation_id, prompt)
    _assert_no_forbidden_tools_after_requested_artifact(scenario_for_run, chat_turn)
    artifacts = list(response.get("artifacts") or [])
    scenario_download_dir = (
        download_dir / scenario.name / f"run-{index}"
        if download_dir is not None
        else None
    )
    downloaded = _download_and_check_artifacts(
        base_url,
        artifacts,
        scenario=scenario_for_run,
        timeout=timeout,
        download_dir=scenario_download_dir,
    )
    return {
        "scenario": scenario.name,
        "conversation_id": conversation_id,
        "elapsed_ms": round(elapsed_ms, 1),
        "fixture": str(archive_path),
        "output_name": output_name,
        "tools": sorted(tool_names),
        "artifact_count": len(artifacts),
        "artifact_extensions": sorted(_artifact_extensions(artifacts)),
        "downloaded_artifacts": downloaded,
        "post_artifact_forbidden_tools_checked": bool(scenario.forbidden_after_artifact_tool_any),
    }


def _run_planner_complex_artifact_workbook(
    *,
    base_url: str,
    runtime_db: Path | None,
    scenario: Scenario,
    timeout: int,
    index: int,
    runtime_db_available: bool,
    download_dir: Path | None = None,
) -> dict[str, Any]:
    upload_root = Path(
        os.environ.get(
            "NULLION_LIVE_STAGE_UPLOAD_ROOT",
            str(Path.home() / ".nullion-stage" / "workspaces" / "workspace_admin" / "uploads"),
        )
    )
    fixture_dir = upload_root / "live_stage_archive_fixtures" / f"{scenario.name}-{uuid4().hex[:10]}"
    archive_path = _write_archive_transform_fixture(fixture_dir)
    prompt = scenario.prompt.format(archive_path=archive_path)
    conversation_id = f"web:live-stage-contract:{scenario.name}:{uuid4().hex[:10]}:{index}"
    response, elapsed_ms = _post_json(
        f"{base_url}/api/chat",
        {"text": prompt, "conversation_id": conversation_id},
        timeout=timeout,
    )
    initial_text = str(response.get("text") or response.get("reply") or "")
    forbidden_hits = [fragment for fragment in scenario.forbidden_text if fragment.lower() in initial_text.lower()]
    assert not forbidden_hits, (
        f"{scenario.name}: initial planner response contained terminal failure text {forbidden_hits!r}: "
        f"{initial_text[:1000]!r}"
    )
    artifacts = list(response.get("artifacts") or [])
    if not artifacts:
        artifacts = _wait_for_conversation_media_artifact(
            runtime_db,
            conversation_id,
            required_extensions=scenario.required_artifact_extensions,
            timeout_seconds=max(10.0, float(timeout)),
        )
    tool_names = _event_tool_names_with_poll(
        runtime_db,
        conversation_id,
        required_tool_all=scenario.required_tool_all,
        timeout_seconds=15.0,
    )
    final_response = dict(response)
    delivered_text = "\n\n".join(
        str(payload.get("assistant_reply") or "")
        for payload in _conversation_event_payloads(runtime_db, conversation_id, event_type="conversation.chat_turn")
        if str(payload.get("assistant_reply") or "").strip()
    )
    final_response["text"] = delivered_text or initial_text or "Planner artifact delivered."
    final_response["artifacts"] = artifacts
    _assert_contract(
        scenario,
        response=final_response,
        elapsed_ms=elapsed_ms,
        tool_names=tool_names,
        runtime_db_available=runtime_db_available,
        tool_output_texts=_event_tool_output_texts(runtime_db, conversation_id),
    )
    chat_turn = _chat_turn_for_user_message(runtime_db, conversation_id, prompt)
    _assert_no_forbidden_tools_after_requested_artifact(scenario, chat_turn)
    group_ids = _planner_group_ids_from_conversation(runtime_db, conversation_id)
    mini_agent_runs = _mini_agent_run_rows_for_groups(runtime_db, group_ids)
    terminal_bad_runs = [
        run
        for run in mini_agent_runs
        if str(run.get("status") or "").lower() in {"failed", "cancelled"}
    ]
    assert not terminal_bad_runs, (
        f"{scenario.name}: planner mini-agent group had failed/cancelled rows: {terminal_bad_runs!r}"
    )
    scenario_download_dir = (
        download_dir / scenario.name / f"run-{index}"
        if download_dir is not None
        else None
    )
    downloaded = _download_and_check_artifacts(
        base_url,
        artifacts,
        scenario=scenario,
        timeout=timeout,
        download_dir=scenario_download_dir,
    )
    return {
        "scenario": scenario.name,
        "conversation_id": conversation_id,
        "elapsed_ms": round(elapsed_ms, 1),
        "fixture": str(archive_path),
        "planner_group_ids": sorted(group_ids),
        "mini_agent_statuses": sorted({str(run.get("status") or "") for run in mini_agent_runs}),
        "tools": sorted(tool_names),
        "artifact_count": len(artifacts),
        "artifact_extensions": sorted(_artifact_extensions(artifacts)),
        "downloaded_artifacts": downloaded,
    }


def _run_planner_downloaded_data_workbook(
    *,
    base_url: str,
    runtime_db: Path | None,
    scenario: Scenario,
    timeout: int,
    index: int,
    runtime_db_available: bool,
    download_dir: Path | None = None,
) -> dict[str, Any]:
    conversation_id = f"web:live-stage-contract:{scenario.name}:{uuid4().hex[:10]}:{index}"
    response, elapsed_ms = _post_json(
        f"{base_url}/api/chat",
        {"text": scenario.prompt, "conversation_id": conversation_id},
        timeout=timeout,
    )
    initial_text = str(response.get("text") or response.get("reply") or "")
    forbidden_hits = [fragment for fragment in scenario.forbidden_text if fragment.lower() in initial_text.lower()]
    assert not forbidden_hits, (
        f"{scenario.name}: initial planner response contained terminal failure text {forbidden_hits!r}: "
        f"{initial_text[:1000]!r}"
    )
    artifacts = list(response.get("artifacts") or [])
    if not artifacts:
        artifacts = _wait_for_conversation_media_artifact(
            runtime_db,
            conversation_id,
            required_extensions=scenario.required_artifact_extensions,
            timeout_seconds=max(10.0, float(timeout)),
        )
    tool_names = _event_tool_names_with_poll(
        runtime_db,
        conversation_id,
        required_tool_all=scenario.required_tool_all,
        timeout_seconds=15.0,
    )
    delivered_text = "\n\n".join(
        str(payload.get("assistant_reply") or "")
        for payload in _conversation_event_payloads(runtime_db, conversation_id, event_type="conversation.chat_turn")
        if str(payload.get("assistant_reply") or "").strip()
    )
    final_response = dict(response)
    final_response["text"] = delivered_text or initial_text or "Planner artifact delivered."
    final_response["artifacts"] = artifacts
    _assert_contract(
        scenario,
        response=final_response,
        elapsed_ms=elapsed_ms,
        tool_names=tool_names,
        runtime_db_available=runtime_db_available,
        tool_output_texts=_event_tool_output_texts(runtime_db, conversation_id),
    )
    group_ids = _planner_group_ids_from_conversation(runtime_db, conversation_id)
    mini_agent_runs = _mini_agent_run_rows_for_groups(runtime_db, group_ids)
    terminal_bad_runs = [
        run
        for run in mini_agent_runs
        if str(run.get("status") or "").lower() in {"failed", "cancelled"}
    ]
    assert not terminal_bad_runs, (
        f"{scenario.name}: planner mini-agent group had failed/cancelled rows: {terminal_bad_runs!r}"
    )
    scenario_download_dir = (
        download_dir / scenario.name / f"run-{index}"
        if download_dir is not None
        else None
    )
    downloaded = _download_and_check_artifacts(
        base_url,
        artifacts,
        scenario=scenario,
        timeout=timeout,
        download_dir=scenario_download_dir,
    )
    return {
        "scenario": scenario.name,
        "conversation_id": conversation_id,
        "elapsed_ms": round(elapsed_ms, 1),
        "planner_group_ids": sorted(group_ids),
        "mini_agent_statuses": sorted({str(run.get("status") or "") for run in mini_agent_runs}),
        "tools": sorted(tool_names),
        "artifact_count": len(artifacts),
        "artifact_extensions": sorted(_artifact_extensions(artifacts)),
        "downloaded_artifacts": downloaded,
    }


def _run_planner_large_web_dataset_handoff(
    *,
    base_url: str,
    runtime_db: Path | None,
    scenario: Scenario,
    timeout: int,
    index: int,
    runtime_db_available: bool,
    download_dir: Path | None = None,
) -> dict[str, Any]:
    conversation_id = f"web:live-stage-contract:{scenario.name}:{uuid4().hex[:10]}:{index}"
    response, elapsed_ms = _post_json(
        f"{base_url}/api/chat",
        {"text": scenario.prompt, "conversation_id": conversation_id},
        timeout=timeout,
    )
    initial_text = str(response.get("text") or response.get("reply") or "")
    forbidden_hits = [fragment for fragment in scenario.forbidden_text if fragment.lower() in initial_text.lower()]
    assert not forbidden_hits, (
        f"{scenario.name}: initial planner response contained terminal handoff failure text "
        f"{forbidden_hits!r}: {initial_text[:1000]!r}"
    )
    artifacts = list(response.get("artifacts") or [])
    if not artifacts:
        artifacts = _wait_for_conversation_media_artifact(
            runtime_db,
            conversation_id,
            required_extensions=scenario.required_artifact_extensions,
            timeout_seconds=max(10.0, float(timeout)),
        )
    tool_names = _event_tool_names_with_poll(
        runtime_db,
        conversation_id,
        required_tool_all=scenario.required_tool_all,
        timeout_seconds=20.0,
    )
    delivered_text = "\n\n".join(
        str(payload.get("assistant_reply") or "")
        for payload in _conversation_event_payloads(runtime_db, conversation_id, event_type="conversation.chat_turn")
        if str(payload.get("assistant_reply") or "").strip()
    )
    final_response = dict(response)
    final_response["text"] = delivered_text or initial_text or "Planner artifact delivered."
    final_response["artifacts"] = artifacts
    _assert_contract(
        scenario,
        response=final_response,
        elapsed_ms=elapsed_ms,
        tool_names=tool_names,
        runtime_db_available=runtime_db_available,
        tool_output_texts=_event_tool_output_texts(runtime_db, conversation_id),
    )
    group_ids = _planner_group_ids_from_conversation(runtime_db, conversation_id)
    mini_agent_runs = _mini_agent_run_rows_for_groups(runtime_db, group_ids)
    terminal_bad_runs = [
        run
        for run in mini_agent_runs
        if str(run.get("status") or "").lower() in {"failed", "cancelled", "waiting_input"}
    ]
    assert not terminal_bad_runs, (
        f"{scenario.name}: planner mini-agent group had terminal/pause rows: {terminal_bad_runs!r}"
    )
    scenario_download_dir = (
        download_dir / scenario.name / f"run-{index}"
        if download_dir is not None
        else None
    )
    downloaded = _download_and_check_artifacts(
        base_url,
        artifacts,
        scenario=scenario,
        timeout=timeout,
        download_dir=scenario_download_dir,
    )
    return {
        "scenario": scenario.name,
        "conversation_id": conversation_id,
        "elapsed_ms": round(elapsed_ms, 1),
        "planner_group_ids": sorted(group_ids),
        "mini_agent_statuses": sorted({str(run.get("status") or "") for run in mini_agent_runs}),
        "tools": sorted(tool_names),
        "artifact_count": len(artifacts),
        "artifact_extensions": sorted(_artifact_extensions(artifacts)),
        "downloaded_artifacts": downloaded,
    }


def _run_platform_code_fence_formatting(
    *,
    scenario: Scenario,
    timeout: int,
    index: int,
) -> dict[str, Any]:
    stage_python = Path(
        os.environ.get(
            "NULLION_LIVE_STAGE_PYTHON",
            str(Path.home() / ".nullion-stage" / "venv" / "bin" / "python"),
        )
    )
    assert stage_python.exists(), f"{scenario.name}: stage python not found: {stage_python}"
    probe = r"""
import asyncio
import json
from types import SimpleNamespace

from nullion.cron_delivery import scheduled_task_delivery_text
from nullion.telegram_formatting import format_telegram_text
from nullion.slack_app import _format_slack_reply
from nullion.discord_app import _send_discord_chunks_with_plain_fallback


class Channel:
    def __init__(self):
        self.sent = []

    async def send(self, text):
        self.sent.append(text)


async def main():
    raw = "'''svg\n<svg width=\"1\"></svg>\n'''"
    telegram_text, telegram_options = format_telegram_text(raw)
    slack_text = _format_slack_reply(raw)
    channel = Channel()
    await _send_discord_chunks_with_plain_fallback(channel, raw, limit=1900)
    scheduled = scheduled_task_delivery_text(
        SimpleNamespace(name="Stage formatting probe"),
        (
            "Daily synthetic update:\n\n"
            "Primary movement:\n\n"
            "Jun 17, 6:00pm — Rooftop venue, Metro City\n\n"
            "Vendor A is now from $203, down from prior $221.\n\n"
            "Link: https://example.com/very/long/link/path/that/will-be-hard-on-mobile\n\n"
            "New broader region listing:\n\n"
            "Jun 22, 8:00pm — The Hall, Harbor Town\n\n"
            "Vendor B has a newly surfaced listing from $313; table lowest $312."
        ),
    )
    scheduled_telegram_text, scheduled_telegram_options = format_telegram_text(scheduled)
    print(json.dumps({
        "telegram": {
            "text": telegram_text,
            "options": telegram_options,
        },
        "scheduled": {
            "text": scheduled,
            "telegram_text": scheduled_telegram_text,
            "telegram_options": scheduled_telegram_options,
        },
        "slack": slack_text,
        "discord": channel.sent,
    }, sort_keys=True))

asyncio.run(main())
"""
    started = time.perf_counter()
    completed = subprocess.run(
        [str(stage_python), "-c", probe],
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    elapsed_ms = (time.perf_counter() - started) * 1000
    assert completed.returncode == 0, (
        f"{scenario.name}: stage probe failed rc={completed.returncode} "
        f"stdout={completed.stdout!r} stderr={completed.stderr!r}"
    )
    payload = json.loads(completed.stdout)
    telegram_text = str(payload["telegram"]["text"])
    assert payload["telegram"]["options"] == {"parse_mode": "HTML"}
    assert "<pre><code class=\"language-svg\">" in telegram_text
    assert "&lt;svg width=&quot;1&quot;&gt;&lt;/svg&gt;" in telegram_text
    assert "'''svg" not in telegram_text
    assert payload["slack"] == "```svg\n<svg width=\"1\"></svg>\n```"
    assert payload["discord"] == ["```svg\n<svg width=\"1\"></svg>\n```"]
    scheduled = str(payload["scheduled"]["text"])
    assert "📌 **Primary movement**\n\n• Jun 17, 6:00pm" in scheduled
    assert "\n• Vendor A is now from $203, down from prior $221.\n" in scheduled
    assert "\n• Link: https://example.com/very/long/link/path/that/will-be-hard-on-mobile\n" in scheduled
    assert "\n\nVendor A is now" not in scheduled
    assert "\n\nLink: https://example.com" not in scheduled
    scheduled_telegram = str(payload["scheduled"]["telegram_text"])
    assert "📌 <b>Primary movement</b>\n\n• Jun 17, 6:00pm" in scheduled_telegram
    if scenario.max_single_ms is not None:
        assert elapsed_ms <= scenario.max_single_ms, (
            f"{scenario.name}: single probe latency {elapsed_ms:.1f}ms exceeded {scenario.max_single_ms}ms"
        )
    return {
        "scenario": scenario.name,
        "ok": True,
        "elapsed_ms": round(elapsed_ms, 1),
        "run": index,
        "checked": [
            "telegram_quote_fence_to_html_pre_code",
            "telegram_scheduled_heading_detail_compaction",
            "slack_quote_fence_to_code_block",
            "discord_quote_fence_to_code_block",
        ],
    }


def _run_agent_turn_limit_doctor_card(
    *,
    scenario: Scenario,
    timeout: int,
    index: int,
) -> dict[str, Any]:
    stage_python = Path(
        os.environ.get(
            "NULLION_LIVE_STAGE_PYTHON",
            str(Path.home() / ".nullion-stage" / "venv" / "bin" / "python"),
        )
    )
    assert stage_python.exists(), f"{scenario.name}: stage python not found: {stage_python}"
    with tempfile.TemporaryDirectory(prefix="nullion-live-limit-approval-") as temp_name:
        probe = r"""
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

from nullion import chat_operator, telegram_app, web_app
from nullion.agent_turn_limits import (
    AGENT_TURN_LIMIT_EXTENSION_REQUEST_KIND,
    max_iterations_from_resume_token,
)
from nullion.approval_display import approval_display_from_request
from nullion.runtime import PersistentRuntime
from nullion.runtime_store import RuntimeStore

runtime = PersistentRuntime(
    store=RuntimeStore(),
    checkpoint_path=Path(__import__("sys").argv[1]) / "runtime.db",
    started_at=datetime(2026, 6, 5, tzinfo=UTC),
)
approval_id = chat_operator._store_agent_turn_limit_extension_suspended_turn(
    runtime,
    conversation_id="telegram:stage-limit-live",
    chat_id="stage-limit-live",
    message="/chat build a large workbook",
    request_id="req-limit-live",
    message_id="msg-limit-live",
    messages_snapshot=[{"role": "user", "content": [{"type": "text", "text": "build a large workbook"}]}],
    current_max_iterations=12,
    tool_results=[],
    required_attachment_extensions=("xlsx",),
)
approval = runtime.store.get_approval_request(approval_id)
display = approval_display_from_request(approval)
actions = []
for row in telegram_app._approval_card_actions(approval):
    for button in row:
        if isinstance(button, (list, tuple)) and button:
            actions.append(button[-1])
        else:
            actions.append(button)
acknowledgement, reply = telegram_app._execute_decision_action(
    SimpleNamespace(runtime=runtime),
    kind="approval",
    action="extend_limit_5x",
    record_id=approval_id,
)
suspended = runtime.store.get_suspended_turn(approval_id)
payload = web_app._web_approval_required_payload(
    {
        "approval_id": approval_id,
        "tool_name": approval.context.get("tool_name"),
        "tool_detail": approval.context.get("tool_detail"),
        "request_kind": approval.request_kind,
    }
)
print(json.dumps({
    "approval_id": approval_id,
    "request_kind": approval.request_kind,
    "presented_by": approval.context.get("presented_by"),
    "title": display.title,
    "copy": display.copy,
    "actions": actions,
    "web_payload_request_kind": payload.get("request_kind"),
    "acknowledgement": acknowledgement,
    "resume_reply": reply,
    "resume_max_iterations": max_iterations_from_resume_token(suspended.resume_token),
}, sort_keys=True))
"""
        started = time.perf_counter()
        completed = subprocess.run(
            [str(stage_python), "-c", probe, temp_name],
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
    elapsed_ms = (time.perf_counter() - started) * 1000
    assert completed.returncode == 0, (
        f"{scenario.name}: stage probe failed rc={completed.returncode} "
        f"stdout={completed.stdout!r} stderr={completed.stderr!r}"
    )
    payload = json.loads(completed.stdout)
    assert payload["request_kind"] == "agent_turn_limit_extension"
    assert payload["web_payload_request_kind"] == "agent_turn_limit_extension"
    assert payload["presented_by"] == "doctor"
    assert "doctor" in str(payload["title"]).lower()
    assert {"extend_limit_2x", "extend_limit_5x", "extend_limit_10x", "deny"}.issubset(
        set(payload["actions"])
    )
    assert payload["acknowledgement"] == "Doctor approved 5x tool budget"
    assert payload["resume_max_iterations"] == 60
    if scenario.max_single_ms is not None:
        assert elapsed_ms <= scenario.max_single_ms, (
            f"{scenario.name}: single probe latency {elapsed_ms:.1f}ms exceeded {scenario.max_single_ms}ms"
        )
    return {
        "scenario": scenario.name,
        "ok": True,
        "elapsed_ms": round(elapsed_ms, 1),
        "run": index,
        "checked": [
            "doctor_presents_tool_budget_approval",
            "telegram_has_2x_5x_10x_actions",
            "web_payload_preserves_request_kind",
            "approved_multiplier_extends_resume_budget",
        ],
    }


def _run_structured_tool_id_boundary_guards(
    *,
    scenario: Scenario,
    timeout: int,
    index: int,
) -> dict[str, Any]:
    stage_python = Path(
        os.environ.get(
            "NULLION_LIVE_STAGE_PYTHON",
            str(Path.home() / ".nullion-stage" / "venv" / "bin" / "python"),
        )
    )
    assert stage_python.exists(), f"{scenario.name}: stage python not found: {stage_python}"
    probe = r'''
import json
from types import SimpleNamespace
from dataclasses import dataclass
from nullion import chat_operator, tools
from nullion.agent_orchestrator import _terminal_tool_failure_text
from nullion.runtime_store import RuntimeStore
from nullion.tools import ToolInvocation, ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec
from nullion.turn_context_policy import ScopedTurnToolRegistry, TurnToolEvidence, TurnToolScopeDecision

@dataclass
class RuntimeDouble:
    store: RuntimeStore

def spec(name, tags=()):
    return ToolSpec(
        name=name,
        description=name,
        risk_level=ToolRiskLevel.LOW,
        side_effect_class=ToolSideEffectClass.READ,
        requires_approval=False,
        timeout_seconds=5,
        capability_tags=tags,
    )

def inv(name, args):
    return ToolInvocation("live-" + name, name, "web:live-stage-tool-boundary", args)

registry = ToolRegistry(plugin_registration_allowed=True)
tools.register_email_plugin(
    registry,
    email_searcher=lambda query, limit: [{"id": "msg-1", "subject": query}],
    email_reader=lambda message_id: (_ for _ in ()).throw(AssertionError("email_reader_called")),
)
email = registry.invoke(inv("email_read", {"id": "?unknown"}))

tools.register_reminder_tools(registry, RuntimeDouble(RuntimeStore()), default_chat_id="web:live-stage-tool-boundary")
reminder = registry.invoke(inv("delete_reminder", {"task_id": "?unknown"}))

tools.register_cron_tools(registry, cron_runner=lambda job, invocation=None: "ok")
cron = registry.invoke(inv("run_cron", {"ids": ["cron-1", "?unknown"]}))

retry_registry = ToolRegistry()
for tool_spec in (
    spec("email_search", ("email", "connector", "account_read")),
    spec("email_read", ("email", "connector", "account_read")),
):
    retry_registry.register(
        tool_spec,
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
initial_read = SimpleNamespace(
    tool_results=[
        ToolResult(
            "read-1",
            "email_read",
            "failed",
            {"reason": "invalid_message_id", "required_source_tool": "email_search"},
            error="Invalid email message id.",
        )
    ],
    artifacts=[],
    suspended_for_approval=False,
    final_text="I need the exact email message ID.",
)
retry_visible_tools = []
def run_retry(_history, scoped_registry):
    retry_visible_tools.append(sorted(definition["name"] for definition in scoped_registry.list_tool_definitions()))
    return SimpleNamespace(
        tool_results=[ToolResult("search-1", "email_search", "completed", {"results": [{"id": "msg-1"}]})],
        artifacts=[],
        suspended_for_approval=False,
        final_text="Searched email.",
    )
retry_result, retry_tool_registry, retried_source_search = chat_operator._run_chat_turn_schema_followup_tool_retry(
    initial_result=initial_read,
    base_history=[],
    active_tool_registry=retry_registry,
    evidence=chat_operator.build_turn_tool_evidence(user_message="check more", conversation_result=None),
    run_turn=run_retry,
)

media_registry = ToolRegistry()
media_registry.register(spec("audio_transcribe", ("media",)), lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}))
media_registry.register(spec("image_extract_text", ("media",)), lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}))
plain_media_scope = ScopedTurnToolRegistry(
    media_registry,
    evidence=TurnToolEvidence(has_attachments=False, context_linked=False),
    tool_scope_decision=TurnToolScopeDecision(
        requested_tool_names=("audio_transcribe", "image_extract_text"),
        required_tool_names=("audio_transcribe", "image_extract_text"),
        confidence=1.0,
        valid=True,
    ),
)
attachment_media_scope = ScopedTurnToolRegistry(
    media_registry,
    evidence=TurnToolEvidence(has_attachments=True, context_linked=False),
    tool_scope_decision=TurnToolScopeDecision(
        requested_tool_names=("audio_transcribe",),
        required_tool_names=("audio_transcribe",),
        confidence=1.0,
        valid=True,
    ),
)

print(json.dumps({
    "tools_path": tools.__file__,
    "email": {"status": email.status, "reason": email.output.get("reason"), "terminal": _terminal_tool_failure_text(email)},
    "reminder": {"status": reminder.status, "reason": reminder.output.get("reason"), "terminal": _terminal_tool_failure_text(reminder)},
    "cron": {"status": cron.status, "reason": cron.output.get("reason"), "invalid_ids": cron.output.get("invalid_ids"), "terminal": _terminal_tool_failure_text(cron)},
    "email_read_recovery": {
        "retried": retried_source_search,
        "final_text": retry_result.final_text,
        "visible_tools": retry_visible_tools,
        "retry_registry_tools": sorted(definition["name"] for definition in retry_tool_registry.list_tool_definitions()),
    },
    "plain_media_tools": sorted(definition["name"] for definition in plain_media_scope.list_tool_definitions()),
    "attachment_media_tools": sorted(definition["name"] for definition in attachment_media_scope.list_tool_definitions()),
}, sort_keys=True))
'''
    started = time.perf_counter()
    completed = subprocess.run(
        [str(stage_python), "-c", probe],
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    elapsed_ms = (time.perf_counter() - started) * 1000
    assert completed.returncode == 0, (
        f"{scenario.name}: stage probe failed rc={completed.returncode} "
        f"stdout={completed.stdout!r} stderr={completed.stderr!r}"
    )
    payload = json.loads(completed.stdout)
    assert payload["email"]["status"] == "failed"
    assert payload["email"]["reason"] == "invalid_message_id"
    assert "search email" in payload["email"]["terminal"]
    assert payload["reminder"]["status"] == "failed"
    assert payload["reminder"]["reason"] == "invalid_reminder_task_id"
    assert "reminder id" in payload["reminder"]["terminal"]
    assert payload["cron"]["status"] == "failed"
    assert payload["cron"]["reason"] == "invalid_cron_id"
    assert payload["cron"]["invalid_ids"] == ["?unknown"]
    assert "scheduled-task id" in payload["cron"]["terminal"]
    assert payload["email_read_recovery"]["retried"] is True
    assert payload["email_read_recovery"]["visible_tools"] == [["email_search"]]
    assert payload["email_read_recovery"]["retry_registry_tools"] == ["email_search"]
    assert "audio_transcribe" not in payload["plain_media_tools"]
    assert "image_extract_text" not in payload["plain_media_tools"]
    assert "audio_transcribe" in payload["attachment_media_tools"]
    if scenario.max_single_ms is not None:
        assert elapsed_ms <= scenario.max_single_ms, (
            f"{scenario.name}: single probe latency {elapsed_ms:.1f}ms exceeded {scenario.max_single_ms}ms"
        )
    return {
        "scenario": scenario.name,
        "ok": True,
        "elapsed_ms": round(elapsed_ms, 1),
        "run": index,
        "tools_path": payload.get("tools_path"),
        "checked": [
            "email_read",
            "run_cron",
            "delete_reminder",
            "email_read_source_search_recovery",
            "plain_text_media_scope_guard",
        ],
    }


def _run_stage_runtime_patch_regressions(
    *,
    scenario: Scenario,
    timeout: int,
    index: int,
) -> dict[str, Any]:
    stage_python = Path(
        os.environ.get(
            "NULLION_LIVE_STAGE_PYTHON",
            str(Path.home() / ".nullion-stage" / "venv" / "bin" / "python"),
        )
    )
    assert stage_python.exists(), f"{scenario.name}: stage python not found: {stage_python}"
    probe = r'''
import json
import os
import tempfile

with tempfile.TemporaryDirectory() as home:
    os.environ["NULLION_HOME"] = home

    from nullion.agent_orchestrator import _repeated_tool_failure_message
    from nullion.builder_memory import _EXPLICIT_TURN_MEMORY_SYSTEM_PROMPT
    from nullion.preferences import Preferences, save_preferences
    from nullion.response_sanitizer import sanitize_user_visible_reply
    from nullion.tools import ToolResult

    save_preferences(Preferences(emoji_level="expressive"))

    body = (
        "Account notice. "
        "This sentence is raw email body text and should not be copied verbatim into chat. "
        * 12
    )
    email_result = ToolResult(
        "read-1",
        "email_read",
        "completed",
        {
            "message": {
                "subject": "Important update",
                "from": "Service <service@example.com>",
                "date": "2026-06-01",
                "snippet": "Compact account preview with the useful action.",
                "body": body,
            }
        },
    )
    cleaned = sanitize_user_visible_reply(
        user_message="Check my emails for anything urgent",
        reply="✉️ I checked your email.\n\n**Important update**\n" + body,
        tool_results=[email_result],
    )

    repeated = _repeated_tool_failure_message(
        result=ToolResult("tool-1", "email_read", "failed", {}, error="HTTP Error 400: Bad Request"),
        repeated_count=3,
    )

    print(json.dumps({
        "paths": {
            "response_sanitizer": sanitize_user_visible_reply.__module__,
            "builder_prompt_chars": len(_EXPLICIT_TURN_MEMORY_SYSTEM_PROMPT),
        },
        "email_cleaned": {
            "has_prefix": cleaned.startswith("✉️ "),
            "has_preview": "Preview: Compact account preview with the useful action." in cleaned,
            "contains_raw_body": body in cleaned,
        },
        "repeated_failure": {
            "starts_email_emoji": repeated.startswith("✉️ "),
            "mentions_fresh_search": "fresh email search" in repeated,
            "mentions_old_retry_text": "same email read step failed 3 times" in repeated,
        },
        "memory_prompt": {
            "mentions_style": "emoji use" in _EXPLICIT_TURN_MEMORY_SYSTEM_PROMPT,
            "mentions_layout": "scan-friendly layout" in _EXPLICIT_TURN_MEMORY_SYSTEM_PROMPT,
        },
    }, sort_keys=True))
'''
    started = time.perf_counter()
    completed = subprocess.run(
        [str(stage_python), "-c", probe],
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    elapsed_ms = (time.perf_counter() - started) * 1000
    assert completed.returncode == 0, (
        f"{scenario.name}: stage probe failed rc={completed.returncode} "
        f"stdout={completed.stdout!r} stderr={completed.stderr!r}"
    )
    payload = json.loads(completed.stdout)
    assert payload["email_cleaned"]["has_prefix"] is True
    assert payload["email_cleaned"]["has_preview"] is True
    assert payload["email_cleaned"]["contains_raw_body"] is False
    assert payload["repeated_failure"]["starts_email_emoji"] is True
    assert payload["repeated_failure"]["mentions_fresh_search"] is True
    assert payload["repeated_failure"]["mentions_old_retry_text"] is False
    assert payload["memory_prompt"]["mentions_style"] is True
    assert payload["memory_prompt"]["mentions_layout"] is True
    if scenario.max_single_ms is not None:
        assert elapsed_ms <= scenario.max_single_ms, (
            f"{scenario.name}: single probe latency {elapsed_ms:.1f}ms exceeded {scenario.max_single_ms}ms"
        )
    return {
        "scenario": scenario.name,
        "ok": True,
        "elapsed_ms": round(elapsed_ms, 1),
        "run": index,
        "checked": [
            "email_read_visible_summary",
            "repeated_email_failure_emoji_prefix",
            "explicit_style_memory_prompt",
        ],
    }


def _run_account_answer_fulfillment(
    *,
    scenario: Scenario,
    timeout: int,
    index: int,
) -> dict[str, Any]:
    stage_python = Path(
        os.environ.get(
            "NULLION_LIVE_STAGE_PYTHON",
            str(Path.home() / ".nullion-stage" / "venv" / "bin" / "python"),
        )
    )
    assert stage_python.exists(), f"{scenario.name}: stage python not found: {stage_python}"
    probe = r'''
import json
from nullion.response_sanitizer import sanitize_user_visible_reply
from nullion.tools import ToolResult

email_result = ToolResult(
    "search-1",
    "email_search",
    "completed",
    {
        "query": "recent account evidence",
        "results": [
            {
                "id": "msg-1",
                "subject": "Reminder: Initial consultation",
                "from": "Office <office@example.com>",
                "snippet": "You have an upcoming booking.",
            },
            {
                "id": "msg-2",
                "subject": "Confirmed: Initial consultation",
                "from": "Office <office@example.com>",
                "snippet": "Your appointment is confirmed.",
            },
        ],
    },
)
reply = sanitize_user_visible_reply(
    user_message="account evidence question",
    reply=(
        "I checked your email, but those records do not prove the requested answer. "
        "They only show appointment reminders and confirmations."
    ),
    tool_results=[email_result],
)
file_result = ToolResult(
    "file-search-1",
    "file_search",
    "completed",
    {"matches": ["/Users/example/workspace/report_alpha.txt", "/Users/example/workspace/report_beta.txt"]},
)
file_reply = sanitize_user_visible_reply(
    user_message="file-backed question",
    reply='{"matches": ["/Users/example/workspace/report_alpha.txt", "/Users/example/workspace/report_beta.txt"]}',
    tool_results=[file_result],
)
connector_result = ToolResult(
    "connector-1",
    "connector_request",
    "completed",
    {"items": [{"title": "Account record", "summary": "Structured connector row", "date": "2026-06-01"}]},
)
connector_reply = sanitize_user_visible_reply(
    user_message="connector-backed question",
    reply='{"items": [{"title": "Account record", "summary": "Structured connector row"}]}',
    tool_results=[connector_result],
)
print(json.dumps({
    "reply": reply,
    "file_reply": file_reply,
    "connector_reply": connector_reply,
    "module": sanitize_user_visible_reply.__module__,
}, sort_keys=True))
'''
    started = time.perf_counter()
    completed = subprocess.run(
        [str(stage_python), "-c", probe],
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    elapsed_ms = (time.perf_counter() - started) * 1000
    assert completed.returncode == 0, (
        f"{scenario.name}: stage probe failed rc={completed.returncode} "
        f"stdout={completed.stdout!r} stderr={completed.stderr!r}"
    )
    payload = json.loads(completed.stdout)
    reply = str(payload.get("reply") or "")
    file_reply = str(payload.get("file_reply") or "")
    connector_reply = str(payload.get("connector_reply") or "")
    assert "do not prove the requested answer" in reply
    assert "appointment reminders and confirmations" in reply
    assert "Found 2 matching messages" not in reply
    assert "not read the file contents yet" in file_reply
    assert "evidence, not the final answer" in file_reply
    assert "/Users/example" not in file_reply
    assert "do not by themselves prove the final answer" in connector_reply
    assert "Account record" in connector_reply
    if scenario.max_single_ms is not None:
        assert elapsed_ms <= scenario.max_single_ms, (
            f"{scenario.name}: single probe latency {elapsed_ms:.1f}ms exceeded {scenario.max_single_ms}ms"
        )
    return {
        "scenario": scenario.name,
        "ok": True,
        "elapsed_ms": round(elapsed_ms, 1),
        "run": index,
        "module": payload.get("module"),
        "checked": [
            "account_answer_preserved_over_generic_email_summary",
            "file_search_summary_marks_evidence_not_answer",
            "connector_summary_marks_evidence_not_answer",
        ],
    }


def _run_reply_anchor_context_guard(
    *,
    scenario: Scenario,
    timeout: int,
    index: int,
) -> dict[str, Any]:
    stage_python = Path(
        os.environ.get(
            "NULLION_LIVE_STAGE_PYTHON",
            str(Path.home() / ".nullion-stage" / "venv" / "bin" / "python"),
        )
    )
    assert stage_python.exists(), f"{scenario.name}: stage python not found: {stage_python}"
    probe = r'''
import json
import tempfile
from pathlib import Path
from types import SimpleNamespace

from nullion import chat_operator
from nullion.config import load_settings
from nullion.conversation_runtime import ConversationTurnDisposition
from nullion.runtime import bootstrap_persistent_runtime
from nullion.turn_dispatch_graph import TurnDispatchDecision, TurnDispatchPolicy


class Model:
    def create(self, **kwargs):
        system = str(kwargs.get("system") or "")
        if "relationship" in system and "target_index" in system:
            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps({
                        "relationship": "follow_up",
                        "effect": "continue",
                        "confidence": 0.97,
                        "target_index": 0,
                    }),
                }]
            }
        if "web_action" in system and "scheduler_action" in system:
            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps({
                        "web_action": "none",
                        "scheduler_action": "none",
                        "skill_pack_action": "none",
                        "requested_tool_names": [],
                        "required_tool_names": [],
                        "confidence": 0.9,
                    }),
                }]
            }
        return {"content": [{"type": "text", "text": "{}"}]}


class Orchestrator:
    def __init__(self, model_client, replies):
        self.model_client = model_client
        self.replies = replies
        self.calls = []

    def run_turn(self, **kwargs):
        self.calls.append(kwargs)
        index = min(len(self.calls) - 1, len(self.replies) - 1)
        return SimpleNamespace(
            suspended_for_approval=False,
            approval_id=None,
            tool_results=[],
            final_text=self.replies[index],
            artifacts=[],
            thinking_text="",
            reached_iteration_limit=False,
        )


def history_sequence(history):
    return [
        (str(item.get("role") or ""), str(item.get("content", [{}])[0].get("text") or ""))
        for item in history
        if item.get("role") in {"user", "assistant"}
    ]


chat_operator._telegram_screenshot_reply_if_requested = lambda *args, **kwargs: None
chat_operator.run_pre_chat_artifact_workflow = lambda *args, **kwargs: SimpleNamespace(kind="none", image_result=None)
chat_operator._chat_capability_inventory_prompt = lambda runtime, **kwargs: None
chat_operator._enabled_skill_pack_prompt = lambda settings: None
chat_operator._enabled_skill_pack_index_prompt = lambda settings: None
chat_operator._build_skill_hint = lambda runtime, user_message: None
chat_operator._broadcast_new_workspace_approvals = lambda *args, **kwargs: None
chat_operator._needs_required_attachment_repair = lambda *args, **kwargs: False
chat_operator._enforce_chat_response_fulfillment = lambda runtime, **kwargs: kwargs["reply"]
chat_operator._try_builder_reflection = lambda *args, **kwargs: None


runtime = bootstrap_persistent_runtime(Path(tempfile.mkdtemp()) / "reply-anchor-runtime.db")
conversation_id = chat_operator._conversation_id_for_chat("100001")
settings = load_settings(env={"NULLION_TELEGRAM_OPERATOR_CHAT_ID": "100001", "NULLION_TELEGRAM_CHAT_ENABLED": "true"})
model = Model()
email_reply = "I did not find payment evidence. If you want, I can search deeper."
weather_reply = "Tomorrow looks sunny. If you want, I can give an hour-by-hour breakdown."
orchestrator = Orchestrator(model, (
    email_reply,
    weather_reply,
    "I will run the deeper account search from the quoted email result.",
))
email = chat_operator._render_chat_turn(
    runtime,
    message="/chat Did I make a payment?",
    chat_id="100001",
    settings=settings,
    model_client=model,
    agent_orchestrator=orchestrator,
    append_activity_trace=False,
)
weather = chat_operator._render_chat_turn(
    runtime,
    message="/chat What's the weather tomorrow?",
    chat_id="100001",
    settings=settings,
    model_client=model,
    agent_orchestrator=orchestrator,
    append_activity_trace=False,
)
weather_turn_id = runtime.store.get_conversation_head(conversation_id)["active_turn_id"]

def forbidden_saved_history_lookup(*args, **kwargs):
    raise AssertionError("explicit Telegram reply anchors must not trigger saved-history lookup or retry")

chat_operator._automatic_saved_chat_history_prompt = forbidden_saved_history_lookup
reply = chat_operator._render_chat_turn(
    runtime,
    message="/chat Sí",
    chat_id="100001",
    settings=settings,
    model_client=model,
    agent_orchestrator=orchestrator,
    append_activity_trace=False,
    reply_context={
        "platform": "telegram",
        "reply_to_message_id": 17022,
        "reply_to_from_bot": True,
        "reply_to_text": email,
    },
    turn_dispatch_decision=TurnDispatchDecision(
        policy=TurnDispatchPolicy.PARALLEL,
        disposition=ConversationTurnDisposition.INDEPENDENT,
        reason="explicit_reply_context",
    ),
)
head = runtime.store.get_conversation_head(conversation_id)
turn = runtime.store.get_conversation_turn(head.get("active_turn_id") if isinstance(head, dict) else "")
sequence = history_sequence(orchestrator.calls[-1]["conversation_history"])
print(json.dumps({
    "email_reply": email,
    "weather_reply": weather,
    "reply": reply,
    "weather_turn_id": weather_turn_id,
    "head_turn_id": head.get("active_turn_id") if isinstance(head, dict) else None,
    "reply_parent_turn_id": turn.parent_turn_id if turn is not None else None,
    "reply_disposition": turn.disposition.value if turn is not None else None,
    "reply_disposition_reason": turn.disposition_reason if turn is not None else None,
    "history_sequence": sequence,
}, sort_keys=True))
'''
    started = time.perf_counter()
    completed = subprocess.run(
        [str(stage_python), "-c", probe],
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    elapsed_ms = (time.perf_counter() - started) * 1000
    assert completed.returncode == 0, (
        f"{scenario.name}: stage probe failed rc={completed.returncode} "
        f"stdout={completed.stdout!r} stderr={completed.stderr!r}"
    )
    payload = json.loads(completed.stdout)
    assert payload["email_reply"] == "I did not find payment evidence. If you want, I can search deeper."
    assert payload["weather_reply"] == "Tomorrow looks sunny. If you want, I can give an hour-by-hour breakdown."
    assert payload["reply"] == "I will run the deeper account search from the quoted email result."
    assert payload["reply_parent_turn_id"] is None
    assert payload["reply_disposition"] == "independent"
    assert payload["reply_disposition_reason"] == "dispatch:explicit_reply_context"
    assert ["assistant", payload["weather_reply"]] not in payload["history_sequence"]
    assert payload["history_sequence"][-1] == ["assistant", payload["email_reply"]]
    if scenario.max_single_ms is not None:
        assert elapsed_ms <= scenario.max_single_ms, (
            f"{scenario.name}: single probe latency {elapsed_ms:.1f}ms exceeded {scenario.max_single_ms}ms"
        )
    return {
        "scenario": scenario.name,
        "ok": True,
        "elapsed_ms": round(elapsed_ms, 1),
        "run": index,
        "checked": [
            "explicit_reply_context_does_not_continue_active_branch",
            "explicit_reply_context_excludes_unrelated_visible_turns",
            "explicit_reply_context_blocks_saved_history_retry",
        ],
        "weather_turn_id": payload["weather_turn_id"],
        "reply_parent_turn_id": payload["reply_parent_turn_id"],
    }


def _artifact_is_image(artifact: Any) -> bool:
    if not isinstance(artifact, dict):
        return False
    values = [
        artifact.get("mime_type"),
        artifact.get("media_type"),
        artifact.get("content_type"),
        artifact.get("name"),
        artifact.get("path"),
        artifact.get("url"),
    ]
    lowered = " ".join(str(value or "").lower() for value in values).strip()
    return "image/" in lowered or lowered.endswith((".png", ".jpg", ".jpeg", ".webp"))


def _artifact_name(artifact: Any) -> str:
    if not isinstance(artifact, dict):
        return ""
    return str(artifact.get("name") or artifact.get("path") or artifact.get("url") or "")


def _artifact_extensions(artifacts: list[Any]) -> set[str]:
    extensions: set[str] = set()
    for artifact in artifacts:
        name = _artifact_name(artifact).lower()
        suffix = Path(name).suffix.lower()
        if suffix:
            extensions.add(suffix)
    return extensions


def _xlsx_sheet_xml_has_formula(sheet_xml: bytes) -> bool:
    return b"<f" in sheet_xml or b":f" in sheet_xml


_NUMBERED_LINE_RE = re.compile(r"^\s*(?P<number>\d{1,2})\.\s+\S")


def _numbered_block_count(text: str) -> int:
    count = 0
    in_block = False
    for line in str(text or "").splitlines():
        match = _NUMBERED_LINE_RE.match(line)
        if not match:
            if not line.strip():
                in_block = False
            continue
        number = int(match.group("number"))
        if not in_block or number == 1:
            count += 1
            in_block = True
        else:
            in_block = True
    return count


def _assert_numbered_response_shape(scenario: Scenario, response: dict[str, Any]) -> None:
    if scenario.max_numbered_blocks_per_response is None:
        return
    text = str(response.get("text") or response.get("reply") or "")
    numbered_blocks = _numbered_block_count(text)
    assert numbered_blocks <= scenario.max_numbered_blocks_per_response, (
        f"{scenario.name}: expected at most {scenario.max_numbered_blocks_per_response} numbered block(s), "
        f"got {numbered_blocks}: {text[:1000]!r}"
    )


def _artifact_url(artifact: Any) -> str | None:
    if not isinstance(artifact, dict):
        return None
    url = artifact.get("url")
    if isinstance(url, str) and url.strip():
        return url.strip()
    return None


def _safe_artifact_filename(name: str, index: int) -> str:
    candidate = Path(name).name.strip() or f"artifact-{index}"
    cleaned = "".join(ch if ch.isalnum() or ch in {".", "-", "_"} else "_" for ch in candidate)
    return cleaned[:180] or f"artifact-{index}"


class _VisibleTextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hidden_depth = 0
        self.parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        del attrs
        if tag.lower() in {"script", "style", "template", "noscript"}:
            self.hidden_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in {"script", "style", "template", "noscript"} and self.hidden_depth:
            self.hidden_depth -= 1

    def handle_data(self, data: str) -> None:
        if not self.hidden_depth and data.strip():
            self.parts.append(data.strip())


def _normalized_visible_text(text: object) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _extract_html_visible_text(data: bytes) -> str:
    parser = _VisibleTextParser()
    parser.feed(data.decode("utf-8", errors="replace"))
    return _normalized_visible_text(" ".join(parser.parts))


def _zip_text_from_members(data: bytes, prefixes: tuple[str, ...]) -> str:
    parts: list[str] = []
    with zipfile.ZipFile(io.BytesIO(data)) as package:
        for member in package.namelist():
            if member.startswith(prefixes) and member.endswith(".xml"):
                raw = package.read(member).decode("utf-8", errors="replace")
                parts.extend(re.findall(r">([^<>]{2,})<", raw))
    return _normalized_visible_text(" ".join(parts))


def _validate_pdf_visual_content(name: str, data: bytes) -> tuple[str, int]:
    try:
        from pypdf import PdfReader
    except Exception as exc:  # pragma: no cover - live environment guard
        raise AssertionError(f"artifact {name!r} PDF validation requires pypdf") from exc
    reader = PdfReader(io.BytesIO(data))
    assert reader.pages, f"artifact {name!r} has no PDF pages"
    text_parts: list[str] = []
    image_count = 0
    content_bytes = 0
    for page in reader.pages:
        try:
            text_parts.append(page.extract_text() or "")
        except Exception:
            pass
        resources = page.get("/Resources") or {}
        xobjects = resources.get("/XObject") if hasattr(resources, "get") else None
        if xobjects:
            try:
                for xobject in xobjects.values():
                    obj = xobject.get_object()
                    if obj.get("/Subtype") == "/Image":
                        image_count += 1
            except Exception:
                pass
        try:
            contents = page.get_contents()
            if isinstance(contents, list):
                content_bytes += sum(len(item.get_data()) for item in contents)
            elif contents is not None:
                content_bytes += len(contents.get_data())
        except Exception:
            pass
    text = _normalized_visible_text(" ".join(text_parts))
    assert len(text) >= 20 or image_count > 0 or content_bytes >= 400, (
        f"artifact {name!r} has PDF pages but no meaningful visible content"
    )
    return text, image_count


def _validate_html_visual_content(name: str, data: bytes) -> tuple[str, int]:
    prefix = data[:5000].lower()
    assert b"<html" in prefix or b"<!doctype html" in prefix, f"artifact {name!r} does not look like HTML"
    raw = data.decode("utf-8", errors="replace")
    text = _extract_html_visible_text(data)
    assert len(text) >= 40, f"artifact {name!r} has too little visible HTML text"
    forbidden = ("placeholder", "lorem ipsum", "todo")
    visual_fragments = [
        *re.findall(r"<img\b[^>]*>", raw, flags=re.IGNORECASE | re.DOTALL),
        *re.findall(r"<svg\b.*?</svg>", raw, flags=re.IGNORECASE | re.DOTALL),
        *re.findall(r"background(?:-image)?\s*:[^;\"'}]+", raw, flags=re.IGNORECASE),
    ]
    embedded_image_payload = re.compile(
        r"(\b(?:src|href)\s*=\s*(['\"])data:image/[^;\"']+;base64,)[^\"']+(\2)",
        flags=re.IGNORECASE,
    )

    def strip_embedded_image_payload(fragment: str) -> str:
        return embedded_image_payload.sub(
            lambda match: f"{match.group(1)}<embedded>{match.group(3)}",
            fragment,
        )

    hits = sorted(
        {
            fragment
            for fragment in forbidden
            if any(fragment in strip_embedded_image_payload(visual).lower() for visual in visual_fragments)
        }
    )
    assert not hits, f"artifact {name!r} contains placeholder visual content: {hits!r}"
    image_refs = re.findall(r"<img\b[^>]*\bsrc\s*=", raw, flags=re.IGNORECASE)
    css_visuals = re.findall(r"background(?:-image)?\s*:", raw, flags=re.IGNORECASE)
    svg_visuals = re.findall(r"<svg\b", raw, flags=re.IGNORECASE)
    return text, len(image_refs) + len(css_visuals) + len(svg_visuals)


def _html_raster_data_image_count(data: bytes) -> int:
    raw = data.decode("utf-8", errors="replace")
    return len(
        re.findall(
            r"<img\b[^>]*\bsrc\s*=\s*['\"]data:image/(?:png|jpe?g|webp|gif);base64,",
            raw,
            flags=re.IGNORECASE,
        )
    )


def _office_media_count(data: bytes) -> int:
    with zipfile.ZipFile(io.BytesIO(data)) as package:
        return len(
            [
                member
                for member in package.namelist()
                if member.startswith(("word/media/", "ppt/media/", "xl/media/"))
            ]
        )


def _validate_docx_visual_content(name: str, data: bytes) -> tuple[str, int]:
    with zipfile.ZipFile(io.BytesIO(data)) as package:
        names = package.namelist()
        assert "word/document.xml" in names, f"artifact {name!r} is missing word/document.xml"
    text = _zip_text_from_members(data, ("word/document.xml", "word/header", "word/footer"))
    assert len(text) >= 40, f"artifact {name!r} has too little DOCX visible text"
    return text, _office_media_count(data)


def _validate_pptx_visual_content(name: str, data: bytes) -> tuple[str, int]:
    with zipfile.ZipFile(io.BytesIO(data)) as package:
        slide_members = [
            member
            for member in package.namelist()
            if member.startswith("ppt/slides/slide") and member.endswith(".xml")
        ]
    assert slide_members, f"artifact {name!r} has no PPTX slides"
    text = _zip_text_from_members(data, ("ppt/slides/slide", "ppt/notesSlides/notesSlide"))
    assert len(text) >= 30, f"artifact {name!r} has too little PPTX visible text"
    return text, _office_media_count(data)


def _xlsx_row_count(data: bytes) -> int:
    with zipfile.ZipFile(io.BytesIO(data)) as package:
        names = package.namelist()
        sheet_members = [
            member
            for member in names
            if member.startswith("xl/worksheets/sheet") and member.endswith(".xml")
        ]
        sheet_xml = b"".join(package.read(member) for member in sheet_members)
    return len(re.findall(rb"<(?:[A-Za-z_][\w.-]*:)?row\b", sheet_xml))


def _xlsx_sheet_names(data: bytes) -> tuple[str, ...]:
    with zipfile.ZipFile(io.BytesIO(data)) as package:
        workbook_xml = package.read("xl/workbook.xml").decode("utf-8", errors="replace")
    return tuple(
        dict.fromkeys(
            match.group(1)
            for match in re.finditer(r"<sheet\b[^>]*\bname=['\"]([^'\"]+)['\"]", workbook_xml)
        )
    )


def _validate_xlsx_visual_content(name: str, data: bytes) -> tuple[str, int]:
    with zipfile.ZipFile(io.BytesIO(data)) as package:
        names = package.namelist()
        sheet_members = [
            member
            for member in names
            if member.startswith("xl/worksheets/sheet") and member.endswith(".xml")
        ]
        assert sheet_members, f"artifact {name!r} has no XLSX worksheets"
        shared_text = package.read("xl/sharedStrings.xml").decode("utf-8", errors="replace") if "xl/sharedStrings.xml" in names else ""
    row_count = _xlsx_row_count(data)
    assert row_count >= 2, f"artifact {name!r} has too few worksheet rows"
    inline_text = _zip_text_from_members(data, ("xl/worksheets/sheet", "xl/sharedStrings.xml"))
    text = _normalized_visible_text(f"{shared_text} {inline_text}")
    assert len(text) >= 20 or row_count >= 4, f"artifact {name!r} has too little XLSX visible content"
    return text, _office_media_count(data)


def _artifact_bytes(
    base_url: str,
    artifact: Any,
    *,
    timeout: int,
) -> tuple[bytes, str]:
    url = _artifact_url(artifact)
    if url:
        absolute_url = urllib.parse.urljoin(base_url + "/", url)
        last_http_error: urllib.error.HTTPError | None = None
        for attempt in range(4):
            request = urllib.request.Request(absolute_url, headers={"Accept": "*/*"}, method="GET")
            try:
                with urllib.request.urlopen(request, timeout=timeout) as response:
                    return response.read(), response.headers.get("Content-Type", "")
            except urllib.error.HTTPError as exc:
                last_http_error = exc
                if exc.code != 404 or attempt == 3:
                    break
                time.sleep(0.35 * (attempt + 1))
        path = Path(str(artifact.get("path") or "")).expanduser() if isinstance(artifact, dict) else Path("")
        if path.exists():
            return path.read_bytes(), ""
        if last_http_error is not None:
            raise last_http_error
    path = Path(str(artifact.get("path") or "")).expanduser() if isinstance(artifact, dict) else Path("")
    assert path.exists(), f"artifact {_artifact_name(artifact)!r} has no URL and local path does not exist: {path}"
    return path.read_bytes(), ""


def _download_and_check_artifacts(
    base_url: str,
    artifacts: list[Any],
    *,
    scenario: Scenario,
    timeout: int,
    download_dir: Path | None = None,
    structured_unavailable_tools: Iterable[str] = (),
) -> list[dict[str, Any]]:
    downloaded: list[dict[str, Any]] = []
    downloaded_extensions: set[str] = set()
    artifact_text_parts: list[str] = []
    artifact_visible_texts: list[tuple[str, str]] = []
    visual_asset_count = 0
    if download_dir is not None:
        download_dir.mkdir(parents=True, exist_ok=True)
    for index, artifact in enumerate(artifacts, start=1):
        data, content_type = _artifact_bytes(base_url, artifact, timeout=timeout)
        name = _artifact_name(artifact)
        assert data, f"artifact {name!r} downloaded as empty bytes"
        suffix = Path(name).suffix.lower()
        if scenario.require_nonuniform_image and suffix == ".png":
            from PIL import Image
            with Image.open(io.BytesIO(data)) as image:
                assert any(low != high for low, high in image.convert("RGB").getextrema()), (
                    f"{scenario.name}: screenshot contains only a single color"
                )
        downloaded_extensions.add(suffix)
        visible_text = ""
        artifact_visual_asset_count = 0
        if suffix == ".pdf":
            assert data.startswith(b"%PDF-"), f"artifact {name!r} is not a PDF"
            visible_text, artifact_visual_asset_count = _validate_pdf_visual_content(name, data)
        if suffix in {".xlsx", ".docx", ".pptx"}:
            assert data.startswith(b"PK"), f"artifact {name!r} is not an Office zip"
            if suffix in scenario.required_embedded_media_extensions:
                with zipfile.ZipFile(io.BytesIO(data)) as package:
                    media_members = [
                        member
                        for member in package.namelist()
                        if member.startswith(("word/media/", "ppt/media/", "xl/media/"))
                    ]
                assert media_members, f"artifact {name!r} did not contain embedded Office media"
                if scenario.min_embedded_media_count > 0:
                    assert len(media_members) >= scenario.min_embedded_media_count, (
                        f"artifact {name!r} contained {len(media_members)} embedded Office media item(s), "
                        f"expected at least {scenario.min_embedded_media_count}"
                    )
            if suffix == ".xlsx" and (
                scenario.require_xlsx_formula
                or scenario.require_xlsx_chart
                or scenario.require_xlsx_conditional_formatting
            ):
                with zipfile.ZipFile(io.BytesIO(data)) as package:
                    names = package.namelist()
                    sheet_xml = b"".join(
                        package.read(member)
                        for member in names
                        if member.startswith("xl/worksheets/sheet") and member.endswith(".xml")
                    )
                    chart_members = [member for member in names if member.startswith("xl/charts/chart")]
                if scenario.require_xlsx_formula:
                    assert _xlsx_sheet_xml_has_formula(sheet_xml), (
                        f"artifact {name!r} did not contain real Excel formulas"
                    )
                if scenario.require_xlsx_chart:
                    assert chart_members, f"artifact {name!r} did not contain Excel charts"
                if scenario.require_xlsx_conditional_formatting:
                    assert b"conditionalFormatting" in sheet_xml, (
                        f"artifact {name!r} did not contain Excel conditional formatting"
                    )
            if suffix == ".docx":
                visible_text, artifact_visual_asset_count = _validate_docx_visual_content(name, data)
            elif suffix == ".pptx":
                visible_text, artifact_visual_asset_count = _validate_pptx_visual_content(name, data)
            elif suffix == ".xlsx":
                visible_text, artifact_visual_asset_count = _validate_xlsx_visual_content(name, data)
                if scenario.required_xlsx_sheet_names:
                    sheet_names = set(_xlsx_sheet_names(data))
                    missing_sheet_names = set(scenario.required_xlsx_sheet_names).difference(sheet_names)
                    assert not missing_sheet_names, (
                        f"artifact {name!r} is missing required worksheet names "
                        f"{sorted(missing_sheet_names)!r}; got {sorted(sheet_names)!r}"
                    )
                if scenario.min_xlsx_rows > 0:
                    row_count = _xlsx_row_count(data)
                    assert row_count >= scenario.min_xlsx_rows, (
                        f"artifact {name!r} contained {row_count} worksheet row(s), "
                        f"expected at least {scenario.min_xlsx_rows}"
                    )
        if suffix in {".html", ".htm"}:
            visible_text, artifact_visual_asset_count = _validate_html_visual_content(name, data)
            if scenario.require_raster_html_images:
                raster_count = _html_raster_data_image_count(data)
                assert raster_count > 0, (
                    f"artifact {name!r} did not embed any raster data-image assets; "
                    "SVG-only or CSS-only visuals are not enough for this scenario"
                )
        if suffix in {".csv", ".tsv"}:
            visible_text = _normalized_visible_text(data.decode("utf-8", errors="replace"))
            assert len(visible_text) >= 20 and ("," in visible_text or "\t" in visible_text), (
                f"artifact {name!r} has too little delimited visible content"
            )
        if suffix == ".svg":
            prefix = data[:5000].lower()
            assert b"<svg" in prefix and b"</svg" in data.lower(), (
                f"artifact {name!r} does not look like SVG"
            )
            visible_text = _normalized_visible_text(data.decode("utf-8", errors="replace"))
            artifact_visual_asset_count = 1
        if visible_text:
            artifact_text_parts.append(visible_text)
            artifact_visible_texts.append((name, visible_text))
        visual_asset_count += artifact_visual_asset_count
        saved_path = None
        if download_dir is not None:
            saved_path = download_dir / _safe_artifact_filename(name, index)
            saved_path.write_bytes(data)
        downloaded.append(
            {
                "name": name,
                "bytes": len(data),
                "content_type": content_type,
                "path": str(saved_path) if saved_path is not None else None,
                "visible_text_chars": len(visible_text),
                "visual_asset_count": artifact_visual_asset_count,
            }
        )
    missing_downloads = set(scenario.required_artifact_extensions).difference(downloaded_extensions)
    assert not missing_downloads, (
        f"{scenario.name}: required artifact extensions were present in metadata but not downloadable/readable: "
        f"{sorted(missing_downloads)!r}; downloaded={sorted(downloaded_extensions)!r}"
    )
    artifact_text = _normalized_visible_text(" ".join(artifact_text_parts))
    missing_text = [
        required
        for required in scenario.required_artifact_text_all
        if required.lower() not in artifact_text.lower()
    ]
    assert not missing_text, (
        f"{scenario.name}: missing required text inside downloaded artifacts {missing_text!r}; "
        f"artifact_text_preview={artifact_text[:1000]!r}"
    )
    missing_each = {
        name: [
            required
            for required in scenario.required_artifact_text_each
            if required.lower() not in visible_text.lower()
        ]
        for name, visible_text in artifact_visible_texts
    }
    missing_each = {name: missing for name, missing in missing_each.items() if missing}
    assert not missing_each, (
        f"{scenario.name}: required text was not present in every readable artifact: {missing_each!r}"
    )
    unavailable_set = {
        str(tool_name or "").strip()
        for tool_name in structured_unavailable_tools
        if str(tool_name or "").strip()
    }
    conditional_missing: dict[str, list[str]] = {}
    for tool_name, required_text in scenario.required_artifact_text_if_tool_unavailable:
        if tool_name not in unavailable_set:
            continue
        for name, visible_text in artifact_visible_texts:
            if required_text.lower() not in visible_text.lower():
                conditional_missing.setdefault(name, []).append(required_text)
    assert not conditional_missing, (
        f"{scenario.name}: structured unavailable source was not explicitly disclosed in every readable artifact: "
        f"{conditional_missing!r}"
    )
    if scenario.require_visual_assets:
        assert visual_asset_count > 0, (
            f"{scenario.name}: expected embedded images or visual assets in downloaded artifacts; "
            f"downloaded={downloaded!r}"
        )
    return downloaded


def _forbidden_tool_output_fragment_matches(text: str, fragment: str) -> bool:
    needle = fragment.lower()
    if not needle:
        return False
    if needle.startswith("/"):
        return re.search(rf"{re.escape(needle)}(?![A-Za-z0-9._/-])", text) is not None
    return needle in text


def _reply_artifact_extension_mentions(text: str, extensions: Iterable[str]) -> list[str]:
    hits: list[str] = []
    for raw_extension in extensions:
        extension = str(raw_extension or "").strip().lower()
        if not extension:
            continue
        if not extension.startswith("."):
            extension = f".{extension}"
        pattern = rf"(?:MEDIA:[^\s]+|[^\s/\\]+\w){re.escape(extension)}(?:\b|$)"
        if re.search(pattern, text, flags=re.IGNORECASE):
            hits.append(extension)
    return hits


def _assert_contract(
    scenario: Scenario,
    *,
    response: dict[str, Any],
    elapsed_ms: float,
    tool_names: set[str],
    runtime_db_available: bool,
    tool_output_texts: list[str] | None = None,
) -> None:
    if scenario.allow_approval_required and response.get("type") == "approval_required":
        approval_id = response.get("approval_id")
        assert isinstance(approval_id, str) and approval_id.strip(), (
            f"{scenario.name}: approval_required response missing approval_id: {response!r}"
        )
        if scenario.max_single_ms is not None:
            assert elapsed_ms <= scenario.max_single_ms, (
                f"{scenario.name}: single request latency {elapsed_ms:.1f}ms exceeded {scenario.max_single_ms}ms"
            )
        return
    text = str(response.get("text") or response.get("reply") or "")
    lowered = text.lower()
    completed_tool_names, has_structured_tool_status = _structured_completed_tool_names(tool_output_texts or ())
    verified_tool_names = completed_tool_names if has_structured_tool_status else tool_names
    if scenario.require_loaded_browser_capture:
        records = []
        for raw in tool_output_texts or ():
            try:
                records.extend(_walk_json_mappings(json.loads(raw)))
            except (TypeError, ValueError):
                continue
        captures = [r.get("output", {}) for r in records
                    if r.get("tool_name") == "browser_screenshot" and r.get("status") == "completed"
                    and isinstance(r.get("output"), dict)]
        navigation_sessions = {r["output"].get("session_id") for r in records
            if r.get("tool_name") in {"browser_navigate", "browser_open"}
            and r.get("status") == "completed" and isinstance(r.get("output"), dict)}
        assert captures and all(
            capture.get("session_id") in navigation_sessions
            and str(capture.get("page_url") or "").startswith(("http://", "https://"))
            for capture in captures
        ), f"{scenario.name}: missing loaded-page capture from the navigation session: {captures!r}"
    _assert_numbered_response_shape(scenario, response)
    if scenario.forbidden_text:
        hits = [fragment for fragment in scenario.forbidden_text if fragment.lower() in lowered]
        assert not hits, f"{scenario.name}: forbidden response text {hits!r}: {text[:1000]!r}"
    for fragment in scenario.required_text_all:
        assert fragment.lower() in lowered, (
            f"{scenario.name}: required value {fragment!r} missing from response: {text[:1000]!r}"
        )
    if scenario.required_text_any:
        assert any(fragment.lower() in lowered for fragment in scenario.required_text_any), (
            f"{scenario.name}: none of {scenario.required_text_any!r} appeared in response: {text[:1000]!r}"
        )
    if scenario.required_tool_any and runtime_db_available:
        assert verified_tool_names.intersection(scenario.required_tool_any), (
            f"{scenario.name}: expected a completed result from one of {scenario.required_tool_any!r}, "
            f"got completed={sorted(verified_tool_names)!r}, attempted={sorted(tool_names)!r}"
        )
    if scenario.required_tool_all and runtime_db_available:
        missing_tools = set(scenario.required_tool_all).difference(verified_tool_names)
        assert not missing_tools, (
            f"{scenario.name}: expected completed results from all tools {scenario.required_tool_all!r}, "
            f"missing {sorted(missing_tools)!r}; completed={sorted(verified_tool_names)!r}; "
            f"attempted={sorted(tool_names)!r}"
        )
    if scenario.required_tool_or_structured_unavailable and runtime_db_available:
        structured_unavailable = _structured_unavailable_required_tools(tool_output_texts or ())
        unresolved_tools = set(scenario.required_tool_or_structured_unavailable).difference(
            set(verified_tool_names) | structured_unavailable
        )
        assert not unresolved_tools, (
            f"{scenario.name}: required tools had neither completed runtime evidence nor a structured unavailable "
            f"scope result: {sorted(unresolved_tools)!r}; tools={sorted(tool_names)!r}; "
            f"structured_unavailable={sorted(structured_unavailable)!r}"
        )
    if scenario.forbidden_tool_any and runtime_db_available:
        forbidden_tools = set(scenario.forbidden_tool_any).intersection(tool_names)
        assert not forbidden_tools, (
            f"{scenario.name}: forbidden tools used {sorted(forbidden_tools)!r}; got {sorted(tool_names)!r}"
        )
    joined_tool_output = "\n".join(tool_output_texts or []).lower()
    if scenario.required_tool_output_text_any and runtime_db_available:
        assert any(fragment.lower() in joined_tool_output for fragment in scenario.required_tool_output_text_any), (
            f"{scenario.name}: none of {scenario.required_tool_output_text_any!r} appeared in persisted "
            f"tool evidence; tools={sorted(tool_names)!r}"
        )
    if scenario.forbidden_tool_output_text and runtime_db_available:
        hits = [
            fragment
            for fragment in scenario.forbidden_tool_output_text
            if _forbidden_tool_output_fragment_matches(joined_tool_output, fragment)
        ]
        assert not hits, (
            f"{scenario.name}: forbidden tool-output text {hits!r} found in persisted runtime evidence"
        )
    if scenario.require_no_tools and runtime_db_available:
        assert not tool_names, f"{scenario.name}: expected no tool use, got {sorted(tool_names)!r}"
    artifacts = response.get("artifacts") or []
    if scenario.require_no_artifacts:
        assert not artifacts, f"{scenario.name}: expected no artifacts, got {artifacts!r}"
    if scenario.require_image_artifact:
        assert any(_artifact_is_image(artifact) for artifact in artifacts), (
            f"{scenario.name}: expected image artifact in API response, got {artifacts!r}; text={text[:1000]!r}"
        )
    extensions = _artifact_extensions(list(artifacts))
    if scenario.min_artifact_count:
        assert len(artifacts) >= scenario.min_artifact_count, (
            f"{scenario.name}: expected at least {scenario.min_artifact_count} artifacts, got {len(artifacts)}; "
            f"artifacts={artifacts!r}"
        )
    if scenario.max_artifact_count:
        assert len(artifacts) <= scenario.max_artifact_count, (
            f"{scenario.name}: expected at most {scenario.max_artifact_count} artifacts, got {len(artifacts)}; "
            f"artifacts={artifacts!r}"
        )
    if scenario.required_artifact_extensions:
        missing = set(scenario.required_artifact_extensions).difference(extensions)
        assert not missing, (
            f"{scenario.name}: missing artifact extensions {sorted(missing)!r}; got {sorted(extensions)!r}; "
            f"artifacts={artifacts!r}"
        )
    if scenario.required_artifact_names:
        artifact_names = {Path(_artifact_name(artifact)).name for artifact in artifacts}
        missing_names = set(scenario.required_artifact_names).difference(artifact_names)
        assert not missing_names, (
            f"{scenario.name}: missing artifact names {sorted(missing_names)!r}; got {sorted(artifact_names)!r}; "
            f"artifacts={artifacts!r}"
        )
    if scenario.forbidden_artifact_extensions:
        forbidden = set(scenario.forbidden_artifact_extensions).intersection(extensions)
        assert not forbidden, (
            f"{scenario.name}: forbidden artifact extensions delivered {sorted(forbidden)!r}; artifacts={artifacts!r}"
        )
        mentioned_forbidden = _reply_artifact_extension_mentions(text, scenario.forbidden_artifact_extensions)
        assert not mentioned_forbidden, (
            f"{scenario.name}: forbidden artifact extensions mentioned as deliverables "
            f"{sorted(set(mentioned_forbidden))!r}; text={text[:1000]!r}"
        )
    if scenario.max_single_ms is not None:
        assert elapsed_ms <= scenario.max_single_ms, (
            f"{scenario.name}: single request latency {elapsed_ms:.1f}ms exceeded {scenario.max_single_ms}ms"
        )


def _scenario_for_config(scenario: Scenario, config: dict[str, Any]) -> Scenario:
    if scenario.name != "image":
        return scenario
    if config.get("image_generate_available") or config.get("image_generate_enabled"):
        return scenario
    return replace(
        scenario,
        required_text_any=("image generation is not configured", "configure"),
        forbidden_text=(*scenario.forbidden_text, "which item should i use", "file, title, link"),
        required_tool_any=(),
        require_image_artifact=False,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8753", help="Running Nullion base URL")
    parser.add_argument("--runtime-db", type=Path, default=Path.home() / ".nullion-stage" / "runtime.db")
    parser.add_argument("--scenario", action="append", choices=sorted(SCENARIOS), help="Scenario(s) to run")
    parser.add_argument("--repeat", type=int, default=1, help="Requests per scenario")
    parser.add_argument("--timeout", type=int, default=180, help="HTTP timeout seconds")
    parser.add_argument("--deep", action="store_true", help="Include long-running artifact and connector scenarios")
    parser.add_argument(
        "--allow-email-send",
        action="store_true",
        help="Allow scenarios that send real email to recipient@example.com after approval",
    )
    parser.add_argument("--skip-download-check", action="store_true", help="Skip artifact byte download checks")
    parser.add_argument("--download-dir", type=Path, help="Save downloaded artifacts for visual/manual QA")
    args = parser.parse_args(argv)

    selected = args.scenario or list(DEFAULT_LIVE_STAGE_SCENARIOS)
    if args.deep and args.scenario is None:
        selected.extend(DEEP_LIVE_STAGE_SCENARIOS)
        if args.allow_email_send:
            selected.extend([
                "email_send_approval_payload_live",
                "complex_html_email_with_attachments",
                "research_artifact_email_followup_pack",
            ])
    base_url = args.base_url.rstrip("/")
    runtime_db_available = args.runtime_db.exists()
    failures: list[str] = []
    summary: list[dict[str, Any]] = []

    try:
        status = _get_json(f"{base_url}/api/status?full=1", args.timeout)
        summary.append({"scenario": "status", "ok": bool(status.get("ok", True)), "keys": sorted(status)[:20]})
        config = _get_json(f"{base_url}/api/config", args.timeout)
        summary.append({"scenario": "config_read", "ok": True})
    except Exception as exc:
        config = {}
        failures.append(f"stage_preflight: {exc}")

    for scenario_name in selected:
        scenario = _scenario_for_config(SCENARIOS[scenario_name], config)
        if scenario.requires_email_send_flag and not args.allow_email_send:
            failures.append(f"{scenario.name}: requires --allow-email-send")
            continue
        samples: list[float] = []
        for index in range(args.repeat):
            conversation_id = f"web:live-stage-contract:{scenario.name}:{uuid4().hex[:10]}:{index}"
            print(
                json.dumps({"event": "scenario_start", "scenario": scenario.name, "index": index}),
                flush=True,
            )
            try:
                if scenario.runner == "active_followup_context_guard":
                    result = _run_active_followup_context_guard(
                        base_url=base_url,
                        runtime_db=args.runtime_db,
                        scenario=scenario,
                        timeout=args.timeout,
                        index=index,
                        runtime_db_available=runtime_db_available,
                    )
                    samples.append(float(result["elapsed_ms"]))
                    summary.append(result)
                    continue
                if scenario.runner == "active_related_followup_context_guard":
                    result = _run_active_related_followup_context_guard(
                        base_url=base_url,
                        runtime_db=args.runtime_db,
                        scenario=scenario,
                        timeout=args.timeout,
                        index=index,
                        runtime_db_available=runtime_db_available,
                        download_dir=args.download_dir,
                    )
                    samples.append(float(result["elapsed_ms"]))
                    summary.append(result)
                    continue
                if scenario.runner == "structured_tool_id_boundary_guards":
                    result = _run_structured_tool_id_boundary_guards(
                        scenario=scenario,
                        timeout=args.timeout,
                        index=index,
                    )
                    samples.append(float(result["elapsed_ms"]))
                    summary.append(result)
                    continue
                if scenario.runner == "stage_runtime_patch_regressions":
                    result = _run_stage_runtime_patch_regressions(
                        scenario=scenario,
                        timeout=args.timeout,
                        index=index,
                    )
                    samples.append(float(result["elapsed_ms"]))
                    summary.append(result)
                    continue
                if scenario.runner == "file_search_listing_no_attachment":
                    result = _run_file_search_listing_no_attachment(
                        base_url=base_url,
                        runtime_db=args.runtime_db,
                        scenario=scenario,
                        timeout=args.timeout,
                        index=index,
                        runtime_db_available=runtime_db_available,
                    )
                    samples.append(float(result["elapsed_ms"]))
                    summary.append(result)
                    continue
                if scenario.runner == "requested_workspace_file_delivery":
                    result = _run_requested_workspace_file_delivery(
                        base_url=base_url,
                        runtime_db=args.runtime_db,
                        scenario=scenario,
                        timeout=args.timeout,
                        index=index,
                        runtime_db_available=runtime_db_available,
                        download_dir=args.download_dir,
                    )
                    samples.append(float(result["elapsed_ms"]))
                    summary.append(result)
                    continue
                if scenario.runner == "scheduler_manual_run":
                    result = _run_scheduler_manual_run(
                        base_url=base_url,
                        runtime_db=args.runtime_db,
                        scenario=scenario,
                        timeout=args.timeout,
                        index=index,
                        runtime_db_available=runtime_db_available,
                    )
                    samples.append(float(result["elapsed_ms"]))
                    summary.append(result)
                    continue
                if scenario.runner == "scheduler_manual_run_short_followup":
                    result = _run_scheduler_manual_run_short_followup(
                        base_url=base_url,
                        runtime_db=args.runtime_db,
                        scenario=scenario,
                        timeout=args.timeout,
                        index=index,
                        runtime_db_available=runtime_db_available,
                    )
                    samples.append(float(result["followup_elapsed_ms"]))
                    summary.append(result)
                    continue
                if scenario.runner == "scheduler_bulk_enable_recovery":
                    result = _run_scheduler_bulk_enable_recovery(
                        base_url=base_url,
                        runtime_db=args.runtime_db,
                        scenario=scenario,
                        timeout=args.timeout,
                        index=index,
                        runtime_db_available=runtime_db_available,
                    )
                    samples.append(float(result["elapsed_ms"]))
                    summary.append(result)
                    continue
                if scenario.runner == "scheduler_disable_not_delete":
                    result = _run_scheduler_disable_not_delete(
                        base_url=base_url,
                        runtime_db=args.runtime_db,
                        scenario=scenario,
                        timeout=args.timeout,
                        index=index,
                        runtime_db_available=runtime_db_available,
                    )
                    samples.append(float(result["elapsed_ms"]))
                    summary.append(result)
                    continue
                if scenario.runner == "scheduler_fresh_followup_after_bulk_enable":
                    result = _run_scheduler_fresh_followup_after_bulk_enable(
                        base_url=base_url,
                        runtime_db=args.runtime_db,
                        scenario=scenario,
                        timeout=args.timeout,
                        index=index,
                        runtime_db_available=runtime_db_available,
                    )
                    samples.append(float(result["followup_elapsed_ms"]))
                    summary.append(result)
                    continue
                if scenario.runner == "local_http_archive_transform_workbook":
                    result = _run_local_http_archive_transform_workbook(
                        base_url=base_url,
                        runtime_db=args.runtime_db,
                        scenario=scenario,
                        timeout=args.timeout,
                        index=index,
                        runtime_db_available=runtime_db_available,
                        download_dir=args.download_dir,
                    )
                    samples.append(float(result["elapsed_ms"]))
                    summary.append(result)
                    continue
                if scenario.runner == "stage_workspace_archive_artifact_retry_guard":
                    result = _run_stage_workspace_archive_artifact_retry_guard(
                        base_url=base_url,
                        runtime_db=args.runtime_db,
                        scenario=scenario,
                        timeout=args.timeout,
                        index=index,
                        runtime_db_available=runtime_db_available,
                        download_dir=args.download_dir,
                    )
                    samples.append(float(result["elapsed_ms"]))
                    summary.append(result)
                    continue
                if scenario.runner == "planner_complex_artifact_workbook":
                    result = _run_planner_complex_artifact_workbook(
                        base_url=base_url,
                        runtime_db=args.runtime_db,
                        scenario=scenario,
                        timeout=args.timeout,
                        index=index,
                        runtime_db_available=runtime_db_available,
                        download_dir=args.download_dir,
                    )
                    samples.append(float(result["elapsed_ms"]))
                    summary.append(result)
                    continue
                if scenario.runner == "planner_downloaded_data_workbook":
                    result = _run_planner_downloaded_data_workbook(
                        base_url=base_url,
                        runtime_db=args.runtime_db,
                        scenario=scenario,
                        timeout=args.timeout,
                        index=index,
                        runtime_db_available=runtime_db_available,
                        download_dir=args.download_dir,
                    )
                    samples.append(float(result["elapsed_ms"]))
                    summary.append(result)
                    continue
                if scenario.runner == "planner_large_web_dataset_handoff":
                    result = _run_planner_large_web_dataset_handoff(
                        base_url=base_url,
                        runtime_db=args.runtime_db,
                        scenario=scenario,
                        timeout=args.timeout,
                        index=index,
                        runtime_db_available=runtime_db_available,
                        download_dir=args.download_dir,
                    )
                    samples.append(float(result["elapsed_ms"]))
                    summary.append(result)
                    continue
                if scenario.runner == "browser_screenshot_artifact_delivery":
                    result = _run_browser_screenshot_artifact_delivery(
                        base_url=base_url,
                        runtime_db=args.runtime_db,
                        scenario=scenario,
                        timeout=args.timeout,
                        index=index,
                        runtime_db_available=runtime_db_available,
                        download_dir=args.download_dir,
                    )
                    samples.append(float(result["elapsed_ms"]))
                    summary.append(result)
                    continue
                if scenario.runner == "email_send_approval_payload_live":
                    result = _run_email_send_approval_payload_live(
                        base_url=base_url,
                        runtime_db=args.runtime_db,
                        scenario=scenario,
                        timeout=args.timeout,
                        index=index,
                        runtime_db_available=runtime_db_available,
                    )
                    samples.append(float(result["elapsed_ms"]))
                    summary.append(result)
                    continue
                if scenario.runner == "platform_code_fence_formatting":
                    result = _run_platform_code_fence_formatting(
                        scenario=scenario,
                        timeout=args.timeout,
                        index=index,
                    )
                    samples.append(float(result["elapsed_ms"]))
                    summary.append(result)
                    continue
                if scenario.runner == "agent_turn_limit_doctor_card":
                    result = _run_agent_turn_limit_doctor_card(
                        scenario=scenario,
                        timeout=args.timeout,
                        index=index,
                    )
                    samples.append(float(result["elapsed_ms"]))
                    summary.append(result)
                    continue
                if scenario.runner == "account_answer_fulfillment":
                    result = _run_account_answer_fulfillment(
                        scenario=scenario,
                        timeout=args.timeout,
                        index=index,
                    )
                    samples.append(float(result["elapsed_ms"]))
                    summary.append(result)
                    continue
                if scenario.runner == "reply_anchor_context_guard":
                    result = _run_reply_anchor_context_guard(
                        scenario=scenario,
                        timeout=args.timeout,
                        index=index,
                    )
                    samples.append(float(result["elapsed_ms"]))
                    summary.append(result)
                    continue
                responses: list[dict[str, Any]] = []
                elapsed_total_ms = 0.0
                for turn_prompt in (scenario.prompt, *scenario.followups):
                    turn_prompt = _materialized_prompt(
                        turn_prompt,
                        scenario_name=scenario.name,
                        index=index,
                    )
                    response, elapsed_ms = _post_json(
                        f"{base_url}/api/chat",
                        {"text": turn_prompt, "conversation_id": conversation_id},
                        timeout=args.timeout,
                    )
                    elapsed_total_ms += elapsed_ms
                    if response.get("type") == "approval_required" and scenario.auto_approve:
                        approval_blob = json.dumps(response, ensure_ascii=False, sort_keys=True).lower()
                        approval_hits = [
                            fragment
                            for fragment in scenario.forbidden_tool_output_text
                            if _forbidden_tool_output_fragment_matches(approval_blob, fragment)
                        ]
                        assert not approval_hits, (
                            f"{scenario.name}: refusing to auto-approve forbidden boundary {approval_hits!r}: "
                            f"{approval_blob[:1000]!r}"
                        )
                        approval_id = str(response.get("approval_id") or "").strip()
                        assert approval_id, f"{scenario.name}: approval_required without approval_id: {response!r}"
                        approval_mode = (
                            "limit_2x"
                            if str(response.get("request_kind") or "").strip() == "agent_turn_limit_extension"
                            else "once"
                        )
                        response = _approve_once(base_url, approval_id, args.timeout, mode=approval_mode)
                    _assert_numbered_response_shape(scenario, response)
                    responses.append(response)
                response = responses[-1]
                samples.append(elapsed_total_ms)
                tool_names = _event_tool_names_with_poll(
                    args.runtime_db,
                    conversation_id,
                    required_tool_any=scenario.required_tool_any,
                    required_tool_all=scenario.required_tool_all,
                )
                all_artifacts = [
                    artifact
                    for item in responses
                    if isinstance(item, dict)
                    for artifact in (item.get("artifacts") or [])
                ]
                if not all_artifacts and scenario.required_artifact_extensions:
                    all_artifacts = _wait_for_conversation_media_artifact(
                        args.runtime_db,
                        conversation_id,
                        required_extensions=scenario.required_artifact_extensions,
                        timeout_seconds=20.0,
                    )
                if all_artifacts and isinstance(response, dict):
                    response = dict(response)
                    response["artifacts"] = all_artifacts
                tool_output_texts = _event_tool_output_texts_with_poll(
                    args.runtime_db, conversation_id,
                    required_output_tools=tuple(sorted(
                        set(tool_names).intersection((*scenario.required_tool_any, *scenario.required_tool_all))
                        | ({"browser_screenshot"} if scenario.require_loaded_browser_capture else set())
                    )),
                )
                structured_unavailable_tools = _structured_unavailable_required_tools(tool_output_texts)
                _assert_contract(
                    scenario,
                    response=response,
                    elapsed_ms=elapsed_total_ms,
                    tool_names=tool_names,
                    runtime_db_available=runtime_db_available,
                    tool_output_texts=tool_output_texts,
                )
                model_start_ms = _latest_latency_mark_ms_with_poll(args.runtime_db, conversation_id, "model_start")
                if model_start_ms is None and runtime_db_available:
                    model_start_ms = _latest_status_latency_mark_ms(
                        args.base_url,
                        args.timeout,
                        conversation_id,
                        "model_start",
                    )
                if scenario.max_model_start_ms is not None and runtime_db_available:
                    assert model_start_ms is not None, f"{scenario.name}: missing model_start latency mark"
                    assert model_start_ms <= scenario.max_model_start_ms, (
                        f"{scenario.name}: model_start latency {model_start_ms:.1f}ms exceeded "
                        f"{scenario.max_model_start_ms}ms"
                    )
                scenario_download_dir = (
                    args.download_dir / scenario.name / f"run-{index}"
                    if args.download_dir is not None
                    else None
                )
                downloaded = [] if args.skip_download_check else _download_and_check_artifacts(
                    base_url,
                    all_artifacts,
                    scenario=scenario,
                    timeout=args.timeout,
                    download_dir=scenario_download_dir,
                    structured_unavailable_tools=structured_unavailable_tools,
                )
                summary.append(
                    {
                        "scenario": scenario.name,
                        "conversation_id": conversation_id,
                        "elapsed_ms": round(elapsed_total_ms, 1),
                        "tools": sorted(tool_names),
                        "artifact_count": len(all_artifacts),
                        "artifact_extensions": sorted(_artifact_extensions(all_artifacts)),
                        "model_start_ms": round(model_start_ms, 1) if model_start_ms is not None else None,
                        "downloaded_artifacts": downloaded,
                    }
                )
            except Exception as exc:
                failure = f"{scenario.name}[{index}] {conversation_id}: {exc}"
                failures.append(failure)
                print(
                    json.dumps(
                        {
                            "event": "scenario_failure",
                            "scenario": scenario.name,
                            "index": index,
                            "failure": failure,
                        }
                    ),
                    flush=True,
                )
        if samples and scenario.max_average_ms is not None:
            average_ms = sum(samples) / len(samples)
            if average_ms > scenario.max_average_ms:
                failure = (
                    f"{scenario.name}: average latency {average_ms:.1f}ms exceeded {scenario.max_average_ms}ms "
                    f"samples={[round(sample, 1) for sample in samples]!r}"
                )
                failures.append(failure)
                print(
                    json.dumps(
                        {
                            "event": "scenario_failure",
                            "scenario": scenario.name,
                            "failure": failure,
                        }
                    ),
                    flush=True,
                )

    print(json.dumps({"ok": not failures, "summary": summary, "failures": failures}, indent=2, sort_keys=True))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
