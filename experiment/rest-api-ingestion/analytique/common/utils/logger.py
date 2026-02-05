"""Generic logging setup for data ingestion pipelines.

Provides structured logging with configurable levels and formats suitable
for both local development and GCP Dataproc execution. Can be used across
all ingestion projects.
"""

import logging
import sys
from datetime import datetime
from typing import Optional


class PipelineLogger:
    """Generic logger for data pipelines."""

    _instance: Optional["PipelineLogger"] = None
    _logger: Optional[logging.Logger] = None

    def __new__(cls) -> "PipelineLogger":
        """Singleton pattern to ensure single logger instance."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def setup(
        self,
        name: str = "data_pipeline",
        level: str = "INFO",
        log_format: Optional[str] = None,
    ) -> logging.Logger:
        """Initialize and configure the logger.

        Args:
            name: Logger name.
            level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
            log_format: Custom log format string. Uses default if not provided.

        Returns:
            Configured logger instance.
        """
        if self._logger is not None:
            return self._logger

        self._logger = logging.getLogger(name)
        self._logger.setLevel(getattr(logging, level.upper(), logging.INFO))

        if not self._logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setLevel(getattr(logging, level.upper(), logging.INFO))

            if log_format is None:
                log_format = (
                    "%(asctime)s | %(levelname)-8s | %(name)s | "
                    "%(module)s:%(funcName)s:%(lineno)d | %(message)s"
                )

            formatter = logging.Formatter(log_format, datefmt="%Y-%m-%d %H:%M:%S")
            handler.setFormatter(formatter)
            self._logger.addHandler(handler)

        return self._logger

    def get_logger(self) -> logging.Logger:
        """Get the logger instance.

        Returns:
            Logger instance. Initializes with defaults if not already setup.
        """
        if self._logger is None:
            return self.setup()
        return self._logger


def get_logger(
    name: str = "data_pipeline",
    level: str = "INFO",
) -> logging.Logger:
    """Convenience function to get a configured logger.

    Args:
        name: Logger name.
        level: Logging level.

    Returns:
        Configured logger instance.
    """
    logger_instance = PipelineLogger()
    return logger_instance.setup(name=name, level=level)


class PipelineMetrics:
    """Generic metrics tracker for data pipelines."""

    def __init__(self, job_name: str, operation: str):
        """Initialize metrics tracking.

        Args:
            job_name: Name of the job/table being processed.
            operation: Operation type (e.g., 'ingestion', 'transformation').
        """
        self.job_name = job_name
        self.operation = operation
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.records_in: int = 0
        self.records_out: int = 0
        self.batches_processed: int = 0
        self.errors: list[str] = []
        self._logger = get_logger()

    def start(self) -> None:
        """Mark the start of processing."""
        self.start_time = datetime.utcnow()
        self._logger.info(f"Starting {self.operation} for '{self.job_name}'")

    def end(self) -> None:
        """Mark the end of processing and log summary."""
        self.end_time = datetime.utcnow()
        duration = (self.end_time - self.start_time).total_seconds() if self.start_time else 0

        self._logger.info(
            f"Completed {self.operation} for '{self.job_name}' | "
            f"Duration: {duration:.2f}s | "
            f"Records in: {self.records_in} | "
            f"Records out: {self.records_out} | "
            f"Batches: {self.batches_processed} | "
            f"Errors: {len(self.errors)}"
        )

    def add_records_in(self, count: int) -> None:
        """Add to the count of input records."""
        self.records_in += count

    def add_records_out(self, count: int) -> None:
        """Add to the count of output records."""
        self.records_out += count

    def increment_batches(self) -> None:
        """Increment the batch counter."""
        self.batches_processed += 1

    def add_error(self, error_message: str) -> None:
        """Record an error."""
        self.errors.append(error_message)
        self._logger.error(f"Error in {self.job_name}: {error_message}")

    def get_summary(self) -> dict:
        """Get metrics summary as a dictionary."""
        duration = None
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()

        return {
            "job_name": self.job_name,
            "operation": self.operation,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": duration,
            "records_in": self.records_in,
            "records_out": self.records_out,
            "batches_processed": self.batches_processed,
            "error_count": len(self.errors),
            "errors": self.errors,
        }


# Alias for backward compatibility
IngestionMetrics = PipelineMetrics
