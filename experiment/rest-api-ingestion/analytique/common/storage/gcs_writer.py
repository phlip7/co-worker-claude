"""Generic GCS storage writer.

Provides reusable functionality for writing data to Google Cloud Storage
using PySpark with partitioning and idempotency support. Can be used across
all ingestion projects.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from analytique.common.utils.logger import get_logger
from analytique.common.utils.helpers import build_gcs_path, format_date


class GCSWriterError(Exception):
    """Custom exception for GCS writer errors."""

    pass


@dataclass
class GCSWriterConfig:
    """Generic GCS writer configuration."""

    bucket_name: str
    prefix: str = "bronze"
    partition_format: str = "ingestion_date={date}"
    file_format: str = "parquet"


class GCSWriter:
    """Generic writer for storing data in GCS."""

    def __init__(self, config: GCSWriterConfig, spark: SparkSession):
        """Initialize the GCS writer.

        Args:
            config: GCS writer configuration.
            spark: SparkSession instance.
        """
        self.config = config
        self.spark = spark
        self.logger = get_logger()

    def write(
        self,
        df: DataFrame,
        table_name: str,
        ingestion_date: datetime,
        mode: str = "overwrite",
    ) -> int:
        """Write DataFrame to GCS.

        Args:
            df: DataFrame to write.
            table_name: Name of the table/dataset.
            ingestion_date: Date of ingestion for partitioning.
            mode: Write mode ('overwrite' for idempotency, 'append' for accumulation).

        Returns:
            Number of records written.

        Raises:
            GCSWriterError: If writing fails.
        """
        if df.isEmpty():
            self.logger.warning(f"Empty DataFrame for {table_name}, skipping write")
            return 0

        output_path = build_gcs_path(
            bucket_name=self.config.bucket_name,
            prefix=self.config.prefix,
            table_name=table_name,
            ingestion_date=ingestion_date,
            partition_format=self.config.partition_format,
        )

        record_count = df.count()

        self.logger.info(
            f"Writing {record_count} records to {output_path} "
            f"in {self.config.file_format} format with mode={mode}"
        )

        try:
            writer = df.write.mode(mode)

            if self.config.file_format.lower() == "parquet":
                writer.parquet(output_path)
            elif self.config.file_format.lower() == "json":
                writer.json(output_path)
            elif self.config.file_format.lower() == "delta":
                writer.format("delta").save(output_path)
            else:
                raise GCSWriterError(f"Unsupported file format: {self.config.file_format}")

            self.logger.info(f"Successfully wrote {record_count} records to {output_path}")
            return record_count

        except Exception as e:
            self.logger.error(f"Failed to write data to {output_path}: {e}")
            raise GCSWriterError(f"Failed to write to GCS: {e}") from e

    def write_partitioned(
        self,
        df: DataFrame,
        table_name: str,
        partition_columns: list[str],
        mode: str = "overwrite",
    ) -> int:
        """Write DataFrame to GCS with custom partitioning.

        Args:
            df: DataFrame to write.
            table_name: Name of the table/dataset.
            partition_columns: List of columns to partition by.
            mode: Write mode.

        Returns:
            Number of records written.

        Raises:
            GCSWriterError: If writing fails.
        """
        if df.isEmpty():
            self.logger.warning(f"Empty DataFrame for {table_name}, skipping write")
            return 0

        base_path = f"gs://{self.config.bucket_name}/{self.config.prefix}/{table_name}"
        record_count = df.count()

        self.logger.info(
            f"Writing {record_count} records to {base_path} "
            f"partitioned by {partition_columns} with mode={mode}"
        )

        try:
            writer = df.write.mode(mode).partitionBy(*partition_columns)

            if self.config.file_format.lower() == "parquet":
                writer.parquet(base_path)
            elif self.config.file_format.lower() == "json":
                writer.json(base_path)
            elif self.config.file_format.lower() == "delta":
                writer.format("delta").save(base_path)
            else:
                raise GCSWriterError(f"Unsupported file format: {self.config.file_format}")

            self.logger.info(f"Successfully wrote {record_count} records to {base_path}")
            return record_count

        except Exception as e:
            self.logger.error(f"Failed to write partitioned data to {base_path}: {e}")
            raise GCSWriterError(f"Failed to write to GCS: {e}") from e

    def check_path_exists(self, path: str) -> bool:
        """Check if a path exists in GCS.

        Args:
            path: GCS path to check.

        Returns:
            True if path exists, False otherwise.
        """
        try:
            df = self.spark.read.format(self.config.file_format).load(path)
            return df.count() > 0
        except Exception:
            return False

    def read_data(self, path: str) -> Optional[DataFrame]:
        """Read data from GCS.

        Args:
            path: GCS path to read from.

        Returns:
            DataFrame with data, or None if not found.
        """
        try:
            return self.spark.read.format(self.config.file_format).load(path)
        except Exception as e:
            self.logger.debug(f"Could not read data from {path}: {e}")
            return None

    def delete_path(self, path: str) -> bool:
        """Delete a path in GCS.

        Args:
            path: GCS path to delete.

        Returns:
            True if deletion was successful, False otherwise.
        """
        self.logger.info(f"Deleting path: {path}")

        try:
            hadoop_conf = self.spark._jsc.hadoopConfiguration()
            fs_uri = self.spark._jvm.java.net.URI(path)
            fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(fs_uri, hadoop_conf)
            hadoop_path = self.spark._jvm.org.apache.hadoop.fs.Path(path)

            if fs.exists(hadoop_path):
                fs.delete(hadoop_path, True)
                self.logger.info(f"Successfully deleted: {path}")
                return True
            else:
                self.logger.info(f"Path does not exist: {path}")
                return False

        except Exception as e:
            self.logger.error(f"Failed to delete {path}: {e}")
            return False

    def get_table_path(self, table_name: str, ingestion_date: Optional[datetime] = None) -> str:
        """Build the full GCS path for a table.

        Args:
            table_name: Name of the table.
            ingestion_date: Optional ingestion date for partition path.

        Returns:
            GCS path string.
        """
        if ingestion_date:
            return build_gcs_path(
                bucket_name=self.config.bucket_name,
                prefix=self.config.prefix,
                table_name=table_name,
                ingestion_date=ingestion_date,
                partition_format=self.config.partition_format,
            )
        return f"gs://{self.config.bucket_name}/{self.config.prefix}/{table_name}"
