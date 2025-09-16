"""
FinSight Data Fetcher Logging Module

Structured logging with correlation IDs and JSON output for production-ready
monitoring and debugging.
"""

from __future__ import annotations

import logging
import sys
import uuid
from contextvars import ContextVar
from typing import Any, Dict, Optional

import structlog
from rich.console import Console
from rich.logging import RichHandler

from .config import LogLevel, settings

# Context variable for correlation ID tracking
correlation_id: ContextVar[str] = ContextVar("correlation_id", default="")

# Rich console for beautiful output
console = Console()


def get_correlation_id() -> str:
    """Get the current correlation ID or generate a new one."""
    current_id = correlation_id.get()
    if not current_id:
        current_id = str(uuid.uuid4())[:8]
        correlation_id.set(current_id)
    return current_id


def set_correlation_id(cid: Optional[str] = None) -> str:
    """Set a new correlation ID and return it."""
    if cid is None:
        cid = str(uuid.uuid4())[:8]
    correlation_id.set(cid)
    return cid


def add_correlation_id(
    logger: structlog.BoundLogger, 
    method_name: str, 
    event_dict: Dict[str, Any]
) -> Dict[str, Any]:
    """Add correlation ID to all log entries."""
    event_dict["correlation_id"] = get_correlation_id()
    return event_dict


def add_timestamp(
    logger: structlog.BoundLogger, 
    method_name: str, 
    event_dict: Dict[str, Any]
) -> Dict[str, Any]:
    """Add timestamp to log entries."""
    event_dict["timestamp"] = structlog.stdlib.add_log_level(logger, method_name, event_dict)
    return event_dict


def add_process_info(
    logger: structlog.BoundLogger, 
    method_name: str, 
    event_dict: Dict[str, Any]
) -> Dict[str, Any]:
    """Add process information to log entries."""
    import os
    event_dict["pid"] = os.getpid()
    event_dict["component"] = "finsight-data-fetcher"
    return event_dict


def configure_logging(
    log_level: LogLevel = LogLevel.INFO,
    use_json: bool = False,
    use_rich: bool = True
) -> None:
    """
    Configure structured logging for the application.
    
    Args:
        log_level: The minimum log level to output
        use_json: Whether to use JSON formatting (for production)
        use_rich: Whether to use Rich formatting (for development)
    """
    # Configure stdlib logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stderr,
        level=getattr(logging, log_level.value),
    )
    
    # Suppress noisy yfinance logging
    logging.getLogger("yfinance").setLevel(logging.CRITICAL)
    logging.getLogger("peewee").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    
    # Configure processors
    processors = [
        structlog.contextvars.merge_contextvars,
        add_correlation_id,
        add_process_info,
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]
    
    if use_json:
        # JSON output for production
        processors.append(structlog.processors.JSONRenderer())
        formatter = structlog.stdlib.ProcessorFormatter(
            processor=structlog.processors.JSONRenderer(),
        )
    elif use_rich:
        # Rich output for development
        processors.append(structlog.dev.ConsoleRenderer(colors=True))
        formatter = structlog.stdlib.ProcessorFormatter(
            processor=structlog.dev.ConsoleRenderer(colors=True),
        )
    else:
        # Plain text output
        processors.append(structlog.processors.PlainConsoleRenderer())
        formatter = structlog.stdlib.ProcessorFormatter(
            processor=structlog.processors.PlainConsoleRenderer(),
        )
    
    # Configure structlog
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    # Update root logger handler
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    
    if use_rich and not use_json:
        handler = RichHandler(
            console=console,
            show_time=True,
            show_path=True,
            markup=True,
            rich_tracebacks=True,
        )
    else:
        handler = logging.StreamHandler(sys.stderr)
    
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)
    root_logger.setLevel(getattr(logging, log_level.value))


def get_logger(name: str) -> structlog.BoundLogger:
    """
    Get a structured logger with the given name.
    
    Args:
        name: Logger name, typically __name__
        
    Returns:
        Configured structured logger
    """
    return structlog.get_logger(name)


# Performance logging helpers
class Timer:
    """Context manager for timing operations with automatic logging."""
    
    def __init__(
        self, 
        logger: structlog.BoundLogger, 
        operation: str, 
        **context: Any
    ):
        self.logger = logger
        self.operation = operation
        self.context = context
        self.start_time: Optional[float] = None
    
    def __enter__(self) -> Timer:
        import time
        self.start_time = time.perf_counter()
        self.logger.info(
            f"Starting {self.operation}",
            operation=self.operation,
            **self.context
        )
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        import time
        if self.start_time is not None:
            duration = time.perf_counter() - self.start_time
            
            if exc_type is None:
                self.logger.info(
                    f"Completed {self.operation}",
                    operation=self.operation,
                    duration_seconds=round(duration, 3),
                    **self.context
                )
            else:
                self.logger.error(
                    f"Failed {self.operation}",
                    operation=self.operation,
                    duration_seconds=round(duration, 3),
                    error_type=exc_type.__name__ if exc_type else None,
                    error_message=str(exc_val) if exc_val else None,
                    **self.context
                )


def log_function_call(
    logger: structlog.BoundLogger,
    exclude_args: Optional[list[str]] = None
):
    """
    Decorator to automatically log function calls with parameters and results.
    
    Args:
        logger: The logger to use
        exclude_args: List of argument names to exclude from logging
    """
    def decorator(func):
        import functools
        import inspect
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            exclude_args_set = set(exclude_args or [])
            
            # Get function signature
            sig = inspect.signature(func)
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()
            
            # Filter out excluded arguments
            filtered_args = {
                k: v for k, v in bound_args.arguments.items()
                if k not in exclude_args_set
            }
            
            with Timer(logger, f"function_call:{func.__name__}", **filtered_args):
                result = func(*args, **kwargs)
                logger.debug(
                    f"Function {func.__name__} completed successfully",
                    function=func.__name__,
                    result_type=type(result).__name__
                )
                return result
        
        return wrapper
    return decorator


# Initialize logging with current settings
configure_logging(
    log_level=settings.log_level,
    use_json=False,  # Use Rich for development
    use_rich=True
)