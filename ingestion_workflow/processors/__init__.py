"""Processing components for text, tables, and coordinate preparation."""

from .base import ProcessingContext, ProcessorError
from .default_processor import DefaultProcessor

__all__ = ["ProcessingContext", "ProcessorError", "DefaultProcessor"]
