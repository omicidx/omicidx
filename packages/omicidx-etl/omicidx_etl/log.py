"""
Centralized logging configuration for omicidx-gh-etl.

Provides structured JSON logging in CI environments and human-friendly
colorized logging for local development.

Usage:
    from omicidx_etl.log import configure_logging, get_logger, log_duration
    
    # Configure once at application startup
    configure_logging()
    
    # Get a logger with context
    log = get_logger(__name__, entity="study", date="2025-12-06")
    log.info("Processing entry", url="https://...")
    
    # Time operations
    with log_duration(log, "Downloaded XML"):
        download_file(url)
"""
import os
import sys
import time
from contextlib import contextmanager
from typing import Optional

from loguru import logger


def is_ci_environment() -> bool:
    """Detect if running in CI (GitHub Actions, GitLab CI, etc.)."""
    return any([
        os.getenv("CI") == "true",
        os.getenv("GITHUB_ACTIONS") == "true",
        os.getenv("GITLAB_CI") == "true",
        os.getenv("JENKINS_URL"),
    ])


def configure_logging(
    *,
    json_logs: Optional[bool] = None,
    level: str = "INFO",
    diagnose: bool = False,
) -> None:
    """
    Configure loguru for omicidx logging.
    
    Args:
        json_logs: If True, emit JSON. If False, emit colorized.
                   If None, auto-detect from OMICIDX_JSON_LOGS env var or CI.
        level: Minimum log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        diagnose: If True, include variables in exception traces
    """
    logger.remove()  # Remove default handler
    
    # Auto-detect JSON logging preference
    if json_logs is None:
        env_json = os.getenv("OMICIDX_JSON_LOGS", "").lower() in ("1", "true", "yes")
        # revert back to human-readable logs even in CI for now
        # Github actions logging output is hard to read with JSON
        json_logs = env_json # or is_ci_environment()
    
    if json_logs:
        # Structured JSON for CI/production
        logger.add(
            sys.stderr,
            format="{message}",
            serialize=True,
            level=level,
            diagnose=diagnose,
        )
    else:
        # Human-friendly colorized format for local development
        logger.add(
            sys.stderr,
            format=(
                "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
                "<level>{level: <8}</level> | "
                "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
                "<level>{message}</level>"
            ),
            colorize=True,
            level=level,
            diagnose=diagnose,
        )
    
    logger.info(
        "Logging configured",
        format="json" if json_logs else "human",
        level=level,
        is_ci=is_ci_environment(),
    )


def get_logger(name: str, **extra_context):
    """
    Get a logger bound with context.
    
    Args:
        name: Logger name (usually __name__)
        **extra_context: Additional context to bind (entity, date, stage, etc.)
    
    Returns:
        A loguru logger bound with the provided context
    
    Example:
        log = get_logger(__name__, entity="study", date="2025-12-06")
        log.info("Processing entry", url="https://...")
    """
    return logger.bind(name=name, **extra_context)


@contextmanager
def log_duration(log_instance, message: str, **extra_context):
    """
    Context manager to log duration of an operation.
    
    Args:
        log_instance: Logger instance (from get_logger)
        message: Message to log with duration
        **extra_context: Additional context to include
    
    Example:
        with log_duration(log, "Downloaded XML", url=url):
            download_file(url)
    """
    start = time.time()
    try:
        yield
    finally:
        duration = time.time() - start
        log_instance.info(
            message,
            duration_seconds=round(duration, 3),
            **extra_context,
        )


@contextmanager
def log_operation(
    log_instance,
    operation: str,
    success_msg: str = "Operation completed",
    error_msg: str = "Operation failed",
    **extra_context,
):
    """
    Context manager for logging start/success/failure of operations.
    
    Args:
        log_instance: Logger instance (from get_logger)
        operation: Name of the operation
        success_msg: Message to log on success
        error_msg: Message to log on error
        **extra_context: Additional context to include
    
    Example:
        with log_operation(log, "parse_xml", url=url):
            parse_xml(url)
    """
    log_instance.info(f"Starting: {operation}", **extra_context)
    start = time.time()
    try:
        yield
        duration = time.time() - start
        log_instance.info(
            success_msg,
            operation=operation,
            duration_seconds=round(duration, 3),
            **extra_context,
        )
    except Exception as e:
        duration = time.time() - start
        log_instance.error(
            error_msg,
            operation=operation,
            duration_seconds=round(duration, 3),
            error=str(e),
            error_type=type(e).__name__,
            **extra_context,
            exc_info=True,
        )
        raise


class LogProgress:
    """
    Log progress for long-running operations with periodic updates.
    
    Example:
        progress = LogProgress(log, total=1000, operation="process_records")
        for record in records:
            process(record)
            progress.update()
        progress.complete()
    """
    
    def __init__(
        self,
        log_instance,
        total: int,
        operation: str,
        log_every: int = 1000,
        **extra_context,
    ):
        """
        Initialize progress tracker.
        
        Args:
            log_instance: Logger instance (from get_logger)
            total: Total number of items to process
            operation: Name of the operation
            log_every: Log progress every N items
            **extra_context: Additional context to include
        """
        self.log = log_instance
        self.total = total
        self.operation = operation
        self.log_every = log_every
        self.extra_context = extra_context
        self.count = 0
        self.start_time = time.time()
    
    def update(self, n: int = 1):
        """
        Update progress by n items.
        
        Args:
            n: Number of items processed (default 1)
        """
        self.count += n
        if self.count % self.log_every == 0 or self.count == self.total:
            elapsed = time.time() - self.start_time
            rate = self.count / elapsed if elapsed > 0 else 0
            pct = (self.count / self.total * 100) if self.total > 0 else 0
            
            self.log.info(
                f"Progress: {self.operation}",
                processed=self.count,
                total=self.total,
                percent=round(pct, 1),
                rate_per_sec=round(rate, 1),
                elapsed_seconds=round(elapsed, 1),
                **self.extra_context,
            )
    
    def complete(self):
        """Mark operation as complete and log final stats."""
        elapsed = time.time() - self.start_time
        rate = self.count / elapsed if elapsed > 0 else 0
        
        self.log.info(
            f"Completed: {self.operation}",
            total_processed=self.count,
            duration_seconds=round(elapsed, 1),
            avg_rate_per_sec=round(rate, 1),
            **self.extra_context,
        )


# Initialize with defaults on import
# Users can call configure_logging() explicitly to override
configure_logging()
