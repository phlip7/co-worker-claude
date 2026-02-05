"""MSB-specific ingestion rules for different load modes.

Provides rule-based logic for initial, incremental, backfill,
and reference table ingestion patterns for the MSB project.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession

from analytique.common.utils.logger import get_logger
from analytique.msb_ingestion.config.settings import TableConfig, GCSConfig


class IngestionMode(Enum):
    """Supported ingestion modes."""

    INITIAL = "initial"
    INCREMENTAL = "incremental"
    BACKFILL = "backfill"
    REFERENCE = "reference"


class IngestionRule(ABC):
    """Abstract base class for ingestion rules."""

    def __init__(self, spark: SparkSession, gcs_config: GCSConfig):
        """Initialize the ingestion rule.

        Args:
            spark: SparkSession instance.
            gcs_config: GCS configuration.
        """
        self.spark = spark
        self.gcs_config = gcs_config
        self.logger = get_logger()

    @abstractmethod
    def get_watermark(self, table_config: TableConfig) -> Optional[Any]:
        """Get the watermark value for incremental processing.

        Args:
            table_config: Configuration for the table.

        Returns:
            Watermark value or None for full load.
        """
        pass

    @abstractmethod
    def get_write_mode(self) -> str:
        """Get the Spark write mode for this ingestion type.

        Returns:
            Write mode string ('overwrite' or 'append').
        """
        pass

    @abstractmethod
    def should_process(
        self,
        table_config: TableConfig,
        ingestion_date: datetime,
    ) -> bool:
        """Determine if the table should be processed.

        Args:
            table_config: Configuration for the table.
            ingestion_date: Date of ingestion.

        Returns:
            True if processing should proceed.
        """
        pass

    @abstractmethod
    def update_checkpoint(
        self,
        table_config: TableConfig,
        watermark_value: Any,
        ingestion_date: datetime,
    ) -> None:
        """Update the checkpoint after successful ingestion.

        Args:
            table_config: Configuration for the table.
            watermark_value: New watermark value to store.
            ingestion_date: Date of ingestion.
        """
        pass


class InitialLoadRule(IngestionRule):
    """Rule for initial (full) data load."""

    def get_watermark(self, table_config: TableConfig) -> Optional[Any]:
        """Initial load doesn't use watermarks - loads all data.

        Args:
            table_config: Configuration for the table.

        Returns:
            None (no watermark for initial load).
        """
        return None

    def get_write_mode(self) -> str:
        """Initial load overwrites existing data.

        Returns:
            'overwrite' mode.
        """
        return "overwrite"

    def should_process(
        self,
        table_config: TableConfig,
        ingestion_date: datetime,
    ) -> bool:
        """Initial load always processes.

        Args:
            table_config: Configuration for the table.
            ingestion_date: Date of ingestion.

        Returns:
            Always True.
        """
        self.logger.info(f"Initial load for {table_config.name} - will process all data")
        return True

    def update_checkpoint(
        self,
        table_config: TableConfig,
        watermark_value: Any,
        ingestion_date: datetime,
    ) -> None:
        """Update checkpoint after initial load.

        Args:
            table_config: Configuration for the table.
            watermark_value: New watermark value.
            ingestion_date: Date of ingestion.
        """
        self.logger.info(
            f"Initial load complete for {table_config.name}. "
            f"Watermark set to: {watermark_value}"
        )


class IncrementalLoadRule(IngestionRule):
    """Rule for incremental data load based on watermarks."""

    def __init__(
        self,
        spark: SparkSession,
        gcs_config: GCSConfig,
        checkpoint_manager: "CheckpointManager",
    ):
        """Initialize incremental load rule.

        Args:
            spark: SparkSession instance.
            gcs_config: GCS configuration.
            checkpoint_manager: Manager for checkpoint operations.
        """
        super().__init__(spark, gcs_config)
        self.checkpoint_manager = checkpoint_manager

    def get_watermark(self, table_config: TableConfig) -> Optional[Any]:
        """Get last watermark for incremental processing.

        Args:
            table_config: Configuration for the table.

        Returns:
            Last watermark value or None if no checkpoint exists.
        """
        watermark = self.checkpoint_manager.get_checkpoint(table_config.name)
        self.logger.info(f"Watermark for {table_config.name}: {watermark}")
        return watermark

    def get_write_mode(self) -> str:
        """Incremental load appends to existing data.

        Returns:
            'append' mode.
        """
        return "append"

    def should_process(
        self,
        table_config: TableConfig,
        ingestion_date: datetime,
    ) -> bool:
        """Check if incremental processing should proceed.

        Args:
            table_config: Configuration for the table.
            ingestion_date: Date of ingestion.

        Returns:
            True if table supports change tracking.
        """
        if table_config.table_type != "transactional":
            self.logger.warning(
                f"Table {table_config.name} is not transactional, "
                "consider using reference mode"
            )
            return False

        if not table_config.change_tracking_column:
            self.logger.error(
                f"Table {table_config.name} has no change_tracking_column defined"
            )
            return False

        return True

    def update_checkpoint(
        self,
        table_config: TableConfig,
        watermark_value: Any,
        ingestion_date: datetime,
    ) -> None:
        """Update checkpoint with new watermark.

        Args:
            table_config: Configuration for the table.
            watermark_value: New watermark value.
            ingestion_date: Date of ingestion.
        """
        self.checkpoint_manager.save_checkpoint(
            table_name=table_config.name,
            watermark_value=watermark_value,
            ingestion_date=ingestion_date,
        )
        self.logger.info(
            f"Checkpoint updated for {table_config.name}: {watermark_value}"
        )


class BackfillRule(IngestionRule):
    """Rule for backfilling historical data."""

    def __init__(
        self,
        spark: SparkSession,
        gcs_config: GCSConfig,
        start_date: datetime,
        end_date: datetime,
    ):
        """Initialize backfill rule.

        Args:
            spark: SparkSession instance.
            gcs_config: GCS configuration.
            start_date: Start date for backfill.
            end_date: End date for backfill.
        """
        super().__init__(spark, gcs_config)
        self.start_date = start_date
        self.end_date = end_date

    def get_watermark(self, table_config: TableConfig) -> Optional[Any]:
        """Backfill uses date range instead of watermark.

        Args:
            table_config: Configuration for the table.

        Returns:
            None (backfill uses date range parameters).
        """
        return None

    def get_write_mode(self) -> str:
        """Backfill overwrites partitions for idempotency.

        Returns:
            'overwrite' mode.
        """
        return "overwrite"

    def should_process(
        self,
        table_config: TableConfig,
        ingestion_date: datetime,
    ) -> bool:
        """Check if backfill should process this date.

        Args:
            table_config: Configuration for the table.
            ingestion_date: Date of ingestion.

        Returns:
            True if date is within backfill range.
        """
        in_range = self.start_date <= ingestion_date <= self.end_date
        if in_range:
            self.logger.info(
                f"Backfill for {table_config.name} on {ingestion_date.date()}"
            )
        return in_range

    def update_checkpoint(
        self,
        table_config: TableConfig,
        watermark_value: Any,
        ingestion_date: datetime,
    ) -> None:
        """Backfill doesn't update regular checkpoints.

        Args:
            table_config: Configuration for the table.
            watermark_value: Watermark value (ignored).
            ingestion_date: Date of ingestion.
        """
        self.logger.info(
            f"Backfill complete for {table_config.name} on {ingestion_date.date()}"
        )


class ReferenceTableRule(IngestionRule):
    """Rule for reference (dimension) tables - full snapshot approach."""

    def get_watermark(self, table_config: TableConfig) -> Optional[Any]:
        """Reference tables don't use watermarks - always full snapshot.

        Args:
            table_config: Configuration for the table.

        Returns:
            None.
        """
        return None

    def get_write_mode(self) -> str:
        """Reference tables overwrite with full snapshot.

        Returns:
            'overwrite' mode.
        """
        return "overwrite"

    def should_process(
        self,
        table_config: TableConfig,
        ingestion_date: datetime,
    ) -> bool:
        """Reference tables always process full snapshot.

        Args:
            table_config: Configuration for the table.
            ingestion_date: Date of ingestion.

        Returns:
            True if table is configured as reference type.
        """
        if table_config.table_type != "reference":
            self.logger.warning(
                f"Table {table_config.name} is not a reference table"
            )
        self.logger.info(f"Reference table {table_config.name} - full snapshot")
        return True

    def update_checkpoint(
        self,
        table_config: TableConfig,
        watermark_value: Any,
        ingestion_date: datetime,
    ) -> None:
        """Reference tables log completion without checkpoint updates.

        Args:
            table_config: Configuration for the table.
            watermark_value: Watermark value (ignored).
            ingestion_date: Date of ingestion.
        """
        self.logger.info(
            f"Reference table {table_config.name} snapshot complete "
            f"for {ingestion_date.date()}"
        )


class CheckpointManager:
    """Manages ingestion checkpoints for incremental loads."""

    def __init__(self, spark: SparkSession, gcs_config: GCSConfig, checkpoint_table: str):
        """Initialize checkpoint manager.

        Args:
            spark: SparkSession instance.
            gcs_config: GCS configuration.
            checkpoint_table: Name of the checkpoint table.
        """
        self.spark = spark
        self.gcs_config = gcs_config
        self.checkpoint_table = checkpoint_table
        self.logger = get_logger()
        self._checkpoint_path = (
            f"gs://{gcs_config.bucket_name}/{gcs_config.bronze_layer_prefix}"
            f"/_checkpoints/{checkpoint_table}"
        )

    def get_checkpoint(self, table_name: str) -> Optional[Any]:
        """Get the last checkpoint value for a table.

        Args:
            table_name: Name of the table.

        Returns:
            Last watermark value or None if no checkpoint exists.
        """
        try:
            df = self.spark.read.parquet(self._checkpoint_path)
            row = df.filter(df.table_name == table_name).orderBy(
                df.ingestion_date.desc()
            ).first()

            if row:
                return row.watermark_value
            return None

        except Exception as e:
            self.logger.debug(f"No checkpoint found for {table_name}: {e}")
            return None

    def save_checkpoint(
        self,
        table_name: str,
        watermark_value: Any,
        ingestion_date: datetime,
    ) -> None:
        """Save a checkpoint for a table.

        Args:
            table_name: Name of the table.
            watermark_value: Watermark value to save.
            ingestion_date: Date of ingestion.
        """
        checkpoint_data = [(
            table_name,
            str(watermark_value),
            ingestion_date,
            datetime.utcnow(),
        )]

        df = self.spark.createDataFrame(
            checkpoint_data,
            ["table_name", "watermark_value", "ingestion_date", "created_at"]
        )

        df.write.mode("append").parquet(self._checkpoint_path)
        self.logger.info(f"Saved checkpoint for {table_name}: {watermark_value}")


class RuleFactory:
    """Factory for creating ingestion rules based on mode."""

    def __init__(self, spark: SparkSession, gcs_config: GCSConfig, checkpoint_table: str):
        """Initialize the rule factory.

        Args:
            spark: SparkSession instance.
            gcs_config: GCS configuration.
            checkpoint_table: Name of checkpoint table.
        """
        self.spark = spark
        self.gcs_config = gcs_config
        self.checkpoint_manager = CheckpointManager(spark, gcs_config, checkpoint_table)

    def create_rule(
        self,
        mode: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> IngestionRule:
        """Create an ingestion rule based on mode.

        Args:
            mode: Ingestion mode string.
            start_date: Start date for backfill mode.
            end_date: End date for backfill mode.

        Returns:
            Appropriate IngestionRule instance.

        Raises:
            ValueError: If mode is not recognized.
        """
        mode_enum = IngestionMode(mode.lower())

        if mode_enum == IngestionMode.INITIAL:
            return InitialLoadRule(self.spark, self.gcs_config)

        elif mode_enum == IngestionMode.INCREMENTAL:
            return IncrementalLoadRule(
                self.spark, self.gcs_config, self.checkpoint_manager
            )

        elif mode_enum == IngestionMode.BACKFILL:
            if not start_date or not end_date:
                raise ValueError("Backfill mode requires start_date and end_date")
            return BackfillRule(self.spark, self.gcs_config, start_date, end_date)

        elif mode_enum == IngestionMode.REFERENCE:
            return ReferenceTableRule(self.spark, self.gcs_config)

        else:
            raise ValueError(f"Unknown ingestion mode: {mode}")
