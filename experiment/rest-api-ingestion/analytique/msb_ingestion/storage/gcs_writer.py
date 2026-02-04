"""GCS storage writer for the bronze layer.

Provides functionality for writing data to Google Cloud Storage
using PySpark with partitioning and idempotency support.
"""

from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from analytique.msb_ingestion.config.settings import GCSConfig, TableConfig
from analytique.msb_ingestion.utils.helpers import build_gcs_path, format_date
from analytique.msb_ingestion.utils.logger import get_logger


class GCSWriterError(Exception):
    """Custom exception for GCS writer errors."""

    pass


class GCSWriter:
    """Writer for storing data in GCS bronze layer."""

    def __init__(self, config: GCSConfig, spark: SparkSession):
        """Initialize the GCS writer.

        Args:
            config: GCS configuration settings.
            spark: SparkSession instance.
        """
        self.config = config
        self.spark = spark
        self.logger = get_logger()

    def write(
        self,
        df: DataFrame,
        table_config: TableConfig,
        ingestion_date: datetime,
        mode: str = "overwrite",
    ) -> int:
        """Write DataFrame to GCS bronze layer.

        Args:
            df: DataFrame to write.
            table_config: Configuration for the table.
            ingestion_date: Date of ingestion for partitioning.
            mode: Write mode ('overwrite' for idempotency, 'append' for accumulation).

        Returns:
            Number of records written.

        Raises:
            GCSWriterError: If writing fails.
        """
        if df.isEmpty():
            self.logger.warning(f"Empty DataFrame for table {table_config.name}, skipping write")
            return 0

        output_path = build_gcs_path(
            bucket_name=self.config.bucket_name,
            prefix=self.config.bronze_layer_prefix,
            table_name=table_config.name,
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
        table_config: TableConfig,
        partition_columns: list[str],
        mode: str = "overwrite",
    ) -> int:
        """Write DataFrame to GCS with custom partitioning.

        Args:
            df: DataFrame to write.
            table_config: Configuration for the table.
            partition_columns: List of columns to partition by.
            mode: Write mode.

        Returns:
            Number of records written.

        Raises:
            GCSWriterError: If writing fails.
        """
        if df.isEmpty():
            self.logger.warning(f"Empty DataFrame for table {table_config.name}, skipping write")
            return 0

        base_path = (
            f"gs://{self.config.bucket_name}/{self.config.bronze_layer_prefix}"
            f"/{table_config.name}"
        )

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

    def check_partition_exists(
        self,
        table_config: TableConfig,
        ingestion_date: datetime,
    ) -> bool:
        """Check if a partition already exists in GCS.

        Args:
            table_config: Configuration for the table.
            ingestion_date: Date of the partition to check.

        Returns:
            True if partition exists, False otherwise.
        """
        partition_path = build_gcs_path(
            bucket_name=self.config.bucket_name,
            prefix=self.config.bronze_layer_prefix,
            table_name=table_config.name,
            ingestion_date=ingestion_date,
            partition_format=self.config.partition_format,
        )

        try:
            df = self.spark.read.format(self.config.file_format).load(partition_path)
            return df.count() > 0
        except Exception:
            return False

    def read_existing_data(
        self,
        table_config: TableConfig,
        ingestion_date: Optional[datetime] = None,
    ) -> Optional[DataFrame]:
        """Read existing data from GCS for a table.

        Args:
            table_config: Configuration for the table.
            ingestion_date: Specific partition date, or None for all data.

        Returns:
            DataFrame with existing data, or None if not found.
        """
        if ingestion_date:
            path = build_gcs_path(
                bucket_name=self.config.bucket_name,
                prefix=self.config.bronze_layer_prefix,
                table_name=table_config.name,
                ingestion_date=ingestion_date,
                partition_format=self.config.partition_format,
            )
        else:
            path = (
                f"gs://{self.config.bucket_name}/{self.config.bronze_layer_prefix}"
                f"/{table_config.name}"
            )

        try:
            return self.spark.read.format(self.config.file_format).load(path)
        except Exception as e:
            self.logger.debug(f"Could not read existing data from {path}: {e}")
            return None

    def delete_partition(
        self,
        table_config: TableConfig,
        ingestion_date: datetime,
    ) -> bool:
        """Delete an existing partition (for reruns/idempotency).

        Args:
            table_config: Configuration for the table.
            ingestion_date: Date of the partition to delete.

        Returns:
            True if deletion was successful, False otherwise.

        Note:
            This uses Spark's file system API. For production use,
            consider using the GCS Python client directly.
        """
        partition_path = build_gcs_path(
            bucket_name=self.config.bucket_name,
            prefix=self.config.bronze_layer_prefix,
            table_name=table_config.name,
            ingestion_date=ingestion_date,
            partition_format=self.config.partition_format,
        )

        self.logger.info(f"Deleting partition at {partition_path}")

        try:
            hadoop_conf = self.spark._jsc.hadoopConfiguration()
            fs_uri = self.spark._jvm.java.net.URI(partition_path)
            fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(fs_uri, hadoop_conf)
            path = self.spark._jvm.org.apache.hadoop.fs.Path(partition_path)

            if fs.exists(path):
                fs.delete(path, True)
                self.logger.info(f"Successfully deleted partition at {partition_path}")
                return True
            else:
                self.logger.info(f"Partition does not exist at {partition_path}")
                return False

        except Exception as e:
            self.logger.error(f"Failed to delete partition at {partition_path}: {e}")
            return False
