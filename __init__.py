"""
Neon Worker - A thread-safe background worker with proper synchronization.

This package provides a robust worker implementation that addresses common
synchronization issues in multi-threaded environments.
"""

from .worker import NeonWorker, Task, SynchronizedCounter
from .config import ConfigManager, WorkerConfig, get_config, update_config

__version__ = "1.0.0"
__author__ = "Neon Worker Team"

__all__ = [
    "NeonWorker",
    "Task", 
    "SynchronizedCounter",
    "ConfigManager",
    "WorkerConfig",
    "get_config",
    "update_config"
]