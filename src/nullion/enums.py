"""Core enums for Project Nullion configuration."""

from enum import Enum


class TeamMode(str, Enum):
    LEAN = "lean"
    STANDARD = "standard"
    HIGH_ASSURANCE = "high_assurance"


class WorkerRole(str, Enum):
    MAIN_ASSISTANT = "main_assistant"
    PROJECT_MANAGER = "project_manager"
    PRIMARY_BUILDER = "primary_builder"
    SECONDARY_BUILDER = "secondary_builder"
    REVIEWER = "reviewer"
    DOCTOR = "doctor"
    NULLION = "nullion"


class VisibilityMode(str, Enum):
    OFF = "off"
    MINIMAL = "minimal"
    STANDARD = "standard"
    VERBOSE = "verbose"
