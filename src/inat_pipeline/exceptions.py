"""
Pipeline exception hierarchy.

Provides structured error handling across the application.
"""

from typing import Any


class InatPipelineError(Exception):
    """
    Base exception for all Pipeline errors.

    All custom exceptions inherit from this to allow catching
    all Pipeline-specific errors.
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


# Infrastructure Layer Exceptions


class WorkflowError(InatPipelineError):
    """Errors related to workflow execution."""

    pass


class SqlError(WorkflowError):
    """Errors related to sql execution."""

    pass


__all__ = [
    "InatPipelineError",
    "WorkflowError",
    "SqlError",
]
