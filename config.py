"""
Configuration management for Neon Worker.

This module provides configuration management with proper synchronization
and thread-safe access to settings.
"""

import threading
import json
import os
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict
import logging

logger = logging.getLogger(__name__)


@dataclass
class WorkerConfig:
    """Configuration settings for NeonWorker."""
    max_workers: int = 4
    queue_size: int = 100
    shutdown_timeout: float = 30.0
    log_level: str = "INFO"
    retry_attempts: int = 3
    retry_delay: float = 1.0
    health_check_interval: float = 10.0
    enable_metrics: bool = True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'WorkerConfig':
        """Create config from dictionary."""
        return cls(**data)


class ConfigManager:
    """
    Thread-safe configuration manager with synchronization.
    
    Provides safe access to configuration settings across multiple threads
    with proper locking mechanisms.
    """
    
    def __init__(self, config_file: Optional[str] = None):
        self._config_file = config_file or "worker_config.json"
        self._config = WorkerConfig()
        self._lock = threading.RLock()
        self._load_config()
    
    def _load_config(self):
        """Load configuration from file with proper error handling."""
        with self._lock:
            if os.path.exists(self._config_file):
                try:
                    with open(self._config_file, 'r') as f:
                        data = json.load(f)
                        self._config = WorkerConfig.from_dict(data)
                        logger.info(f"Configuration loaded from {self._config_file}")
                except Exception as e:
                    logger.error(f"Error loading config: {e}, using defaults")
                    self._config = WorkerConfig()
            else:
                logger.info("Config file not found, using default configuration")
                self._save_config()
    
    def _save_config(self):
        """Save current configuration to file."""
        try:
            with open(self._config_file, 'w') as f:
                json.dump(self._config.to_dict(), f, indent=2)
                logger.debug(f"Configuration saved to {self._config_file}")
        except Exception as e:
            logger.error(f"Error saving config: {e}")
    
    def get_config(self) -> WorkerConfig:
        """Get current configuration with thread safety."""
        with self._lock:
            return WorkerConfig.from_dict(self._config.to_dict())
    
    def update_config(self, **kwargs):
        """
        Update configuration settings with thread safety.
        
        Args:
            **kwargs: Configuration parameters to update
        """
        with self._lock:
            config_dict = self._config.to_dict()
            
            # Validate and update settings
            for key, value in kwargs.items():
                if key in config_dict:
                    config_dict[key] = value
                    logger.info(f"Updated config: {key} = {value}")
                else:
                    logger.warning(f"Unknown config parameter: {key}")
            
            self._config = WorkerConfig.from_dict(config_dict)
            self._save_config()
    
    def get_setting(self, key: str, default: Any = None) -> Any:
        """
        Get a specific configuration setting with thread safety.
        
        Args:
            key: Configuration key
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        with self._lock:
            config_dict = self._config.to_dict()
            return config_dict.get(key, default)
    
    def reload_config(self):
        """Reload configuration from file with thread safety."""
        with self._lock:
            self._load_config()
            logger.info("Configuration reloaded")


# Global configuration manager instance
config_manager = ConfigManager()


def get_config() -> WorkerConfig:
    """Get current worker configuration."""
    return config_manager.get_config()


def update_config(**kwargs):
    """Update worker configuration."""
    config_manager.update_config(**kwargs)