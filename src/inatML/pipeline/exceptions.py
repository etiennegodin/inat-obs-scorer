"""
NukeKit exception hierarchy.

Provides structured error handling across the application.
"""

from typing import Any


class NukeKitError(Exception):
    """
    Base exception for all NukeKit errors.

    All custom exceptions inherit from this to allow catching
    all NukeKit-specific errors.
    """

    def __init__(self, message: str, details: dict[Any, Any] | None = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def __str__(self) -> str:
        if self.details:
            details_str = ", ".join(f"{k}={v}" for k, v in self.details.items())
            return f"{self.message} ({details_str})"
        return self.message
