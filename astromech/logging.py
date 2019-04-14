"""A logger for use throughout your code.

The logger level defaults to `logging.INFO`, but can be easily configured for every lambda function
using the environment variable "LOG_LEVEL" and the string representation of the level (one of
"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL").
For example,
"""
import logging
import os

logger = logging.getLogger()
"""Global logger object."""

logger.setLevel(os.environ.get('LOG_LEVEL', logging.INFO))
