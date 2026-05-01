"""Core domain models for Project Nullion."""

from dataclasses import dataclass


@dataclass(slots=True)
class Principal:
    principal_id: str
    display_name: str
    is_admin: bool = False


@dataclass(slots=True)
class Task:
    task_id: str
    title: str
    status: str
    owner_principal_id: str


@dataclass(slots=True)
class Worker:
    worker_id: str
    role: str
    status: str
    task_id: str
