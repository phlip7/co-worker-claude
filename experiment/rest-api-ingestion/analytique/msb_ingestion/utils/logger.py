"""Logging setup for REST API ingestion pipeline.

Provides structured logging with configurable levels and formats suitable
for both local development and GCP Dataproc execution.
"""

import logging
import sys
from datetime import datetime
from typing import Optional


class IngestionLogger:
    """Custom logger for the ingestion pipeline."""

    _instance: Optional["IngestionLogger"] = None
    _logger: Optional[logging.Logger] = None

    def __new__(cls) -> "IngestionLogger":
        """Singleton pattern to ensure single logger instance."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def setup(
        self,
        name: str = "msb_ingestion",
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
    name: str = "msb_ingestion",
    level: str = "INFO",
) -> logging.Logger:
    """Convenience function to get a configured logger.

    Args:
        name: Logger name.
        level: Logging level.

    Returns:
        Configured logger instance.
    """
    logger_instance = IngestionLogger()
    return logger_instance.setup(name=name, level=level)


class IngestionMetrics:
    """Tracks and logs ingestion metrics for a single run."""

    def __init__(self, table_name: str, mode: str):
        """Initialize metrics tracking.

        Args:
            table_name: Name of the table being processed.
            mode: Ingestion mode (initial, incremental, backfill, reference).
        """
        self.table_name = table_name
        self.mode = mode
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.records_fetched: int = 0
        self.records_written: int = 0
        self.pages_processed: int = 0
        self.errors: list[str] = []
        self._logger = get_logger()

    def start(self) -> None:
        """Mark the start of ingestion."""
        self.start_time = datetime.utcnow()
        self._logger.info(
            f"Starting {self.mode} ingestion for table '{self.table_name}'"
        )

    def end(self) -> None:
        """Mark the end of ingestion and log summary."""
        self.end_time = datetime.utcnow()
        duration = (self.end_time - self.start_time).total_seconds() if self.start_time else 0

        self._logger.info(
            f"Completed {self.mode} ingestion for table '{self.table_name}' | "
            f"Duration: {duration:.2f}s | "
            f"Records fetched: {self.records_fetched} | "
            f"Records written: {self.records_written} | "
            f"Pages: {self.pages_processed} | "
            f"Errors: {len(self.errors)}"
        )

    def add_records_fetched(self, count: int) -> None:
        """Add to the count of fetched records.

        Args:
            count: Number of records fetched.
        """
        self.records_fetched += count

    def add_records_written(self, count: int) -> None:
        """Add to the count of written records.

        Args:
            count: Number of records written.
        """
        self.records_written += count

    def increment_pages(self) -> None:
        """Increment the page counter."""
        self.pages_processed += 1

    def add_error(self, error_message: str) -> None:
        """Record an error.

        Args:
            error_message: Description of the error.
        """
        self.errors.append(error_message)
        self._logger.error(f"Error in {self.table_name}: {error_message}")

    def get_summary(self) -> dict:
        """Get metrics summary as a dictionary.

        Returns:
            Dictionary containing all metrics.
        """
        duration = None
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()

        return {
            "table_name": self.table_name,
            "mode": self.mode,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": duration,
            "records_fetched": self.records_fetched,
            "records_written": self.records_written,
            "pages_processed": self.pages_processed,
            "error_count": len(self.errors),
            "errors": self.errors,
        }
