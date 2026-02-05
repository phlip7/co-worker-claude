"""MSB-specific data processor for REST API ingestion.

Handles MSB-specific data transformation, validation, and schema enforcement
for the ingestion pipeline.
"""

from datetime import datetime
from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from analytique.common.utils.helpers import (
    add_metadata_columns,
    build_spark_schema,
    sanitize_column_name,
)
from analytique.common.utils.logger import get_logger
from analytique.msb_ingestion.config.settings import TableConfig


class ProcessorError(Exception):
    """Custom exception for processor errors."""

    pass


class DataProcessor:
    """Processes and transforms data from REST API for storage."""

    def __init__(self, spark: SparkSession):
        """Initialize the data processor.

        Args:
            spark: SparkSession instance.
        """
        self.spark = spark
        self.logger = get_logger()

    def process(
        self,
        records: list[dict[str, Any]],
        table_config: TableConfig,
        ingestion_date: datetime,
        mode: str,
    ) -> DataFrame:
        """Process a batch of records into a validated DataFrame.

        Args:
            records: List of record dictionaries from API.
            table_config: Configuration for the table.
            ingestion_date: Date of ingestion.
            mode: Ingestion mode.

        Returns:
            Processed and validated DataFrame.

        Raises:
            ProcessorError: If processing fails.
        """
        if not records:
            self.logger.warning("No records to process")
            return self.spark.createDataFrame([], self._get_schema(table_config))

        self.logger.info(f"Processing {len(records)} records for {table_config.name}")

        try:
            df = self._create_dataframe(records, table_config)

            df = self._sanitize_columns(df)

            df = self._apply_schema(df, table_config)

            df = self._validate_data(df, table_config)

            df = add_metadata_columns(df, ingestion_date, {"ingestion_mode": mode})

            self.logger.info(f"Successfully processed {df.count()} records")
            return df

        except Exception as e:
            self.logger.error(f"Failed to process records: {e}")
            raise ProcessorError(f"Processing failed: {e}") from e

    def _create_dataframe(
        self,
        records: list[dict[str, Any]],
        table_config: TableConfig,
    ) -> DataFrame:
        """Create a DataFrame from records.

        Args:
            records: List of record dictionaries.
            table_config: Configuration for the table.

        Returns:
            Raw DataFrame.
        """
        return self.spark.createDataFrame(records)

    def _get_schema(self, table_config: TableConfig) -> StructType:
        """Get the schema for a table.

        Args:
            table_config: Configuration for the table.

        Returns:
            PySpark StructType schema.
        """
        if table_config.schema_fields:
            return build_spark_schema(table_config.schema_fields)
        return StructType([])

    def _sanitize_columns(self, df: DataFrame) -> DataFrame:
        """Sanitize column names for Spark/Parquet compatibility.

        Args:
            df: Input DataFrame.

        Returns:
            DataFrame with sanitized column names.
        """
        for col in df.columns:
            sanitized = sanitize_column_name(col)
            if sanitized != col:
                df = df.withColumnRenamed(col, sanitized)
        return df

    def _apply_schema(
        self,
        df: DataFrame,
        table_config: TableConfig,
    ) -> DataFrame:
        """Apply expected schema to DataFrame with type casting.

        Args:
            df: Input DataFrame.
            table_config: Configuration for the table.

        Returns:
            DataFrame with schema applied.
        """
        if not table_config.schema_fields:
            self.logger.debug("No schema defined, using inferred types")
            return df

        for field in table_config.schema_fields:
            col_name = sanitize_column_name(field["name"])
            col_type = field.get("type", "string")

            if col_name in df.columns:
                df = self._cast_column(df, col_name, col_type)
            else:
                self.logger.warning(f"Column {col_name} not found in data")

        return df

    def _cast_column(
        self,
        df: DataFrame,
        col_name: str,
        col_type: str,
    ) -> DataFrame:
        """Cast a column to the specified type.

        Args:
            df: Input DataFrame.
            col_name: Name of the column.
            col_type: Target type.

        Returns:
            DataFrame with column cast.
        """
        type_mapping = {
            "string": "string",
            "integer": "int",
            "long": "bigint",
            "number": "double",
            "double": "double",
            "boolean": "boolean",
            "timestamp": "timestamp",
            "date": "date",
        }

        spark_type = type_mapping.get(col_type.lower(), "string")

        try:
            return df.withColumn(col_name, F.col(col_name).cast(spark_type))
        except Exception as e:
            self.logger.warning(f"Failed to cast {col_name} to {spark_type}: {e}")
            return df

    def _validate_data(
        self,
        df: DataFrame,
        table_config: TableConfig,
    ) -> DataFrame:
        """Validate data against business rules.

        Args:
            df: Input DataFrame.
            table_config: Configuration for the table.

        Returns:
            Validated DataFrame.

        Raises:
            ProcessorError: If critical validation fails.
        """
        primary_key = sanitize_column_name(table_config.primary_key)
        if primary_key not in df.columns:
            raise ProcessorError(
                f"Primary key column '{primary_key}' not found in data"
            )

        null_pk_count = df.filter(F.col(primary_key).isNull()).count()
        if null_pk_count > 0:
            self.logger.warning(
                f"Found {null_pk_count} records with null primary key, filtering out"
            )
            df = df.filter(F.col(primary_key).isNotNull())

        original_count = df.count()
        df = df.dropDuplicates([primary_key])
        dedup_count = df.count()

        if dedup_count < original_count:
            self.logger.warning(
                f"Removed {original_count - dedup_count} duplicate records"
            )

        return df

    def get_max_watermark(
        self,
        df: DataFrame,
        table_config: TableConfig,
    ) -> Optional[Any]:
        """Get the maximum watermark value from processed data.

        Args:
            df: Processed DataFrame.
            table_config: Configuration for the table.

        Returns:
            Maximum watermark value or None.
        """
        if not table_config.change_tracking_column:
            return None

        col_name = sanitize_column_name(table_config.change_tracking_column)

        if col_name not in df.columns:
            self.logger.warning(f"Change tracking column {col_name} not found")
            return None

        max_row = df.agg(F.max(col_name).alias("max_watermark")).first()

        if max_row and max_row["max_watermark"]:
            return max_row["max_watermark"]

        return None

    def merge_batches(self, dataframes: list[DataFrame]) -> DataFrame:
        """Merge multiple DataFrames from batched processing.

        Args:
            dataframes: List of DataFrames to merge.

        Returns:
            Merged DataFrame.
        """
        if not dataframes:
            return self.spark.createDataFrame([], StructType([]))

        if len(dataframes) == 1:
            return dataframes[0]

        result = dataframes[0]
        for df in dataframes[1:]:
            result = result.union(df)

        return result

    def flatten_nested_data(
        self,
        df: DataFrame,
        nested_columns: Optional[list[str]] = None,
    ) -> DataFrame:
        """Flatten nested JSON structures in DataFrame.

        Args:
            df: Input DataFrame with nested columns.
            nested_columns: Specific columns to flatten, or None for auto-detect.

        Returns:
            DataFrame with flattened columns.
        """
        from pyspark.sql.types import StructType, ArrayType

        if nested_columns is None:
            nested_columns = [
                field.name
                for field in df.schema.fields
                if isinstance(field.dataType, (StructType, ArrayType))
            ]

        for col_name in nested_columns:
            if col_name not in df.columns:
                continue

            field = df.schema[col_name]

            if isinstance(field.dataType, StructType):
                for nested_field in field.dataType.fields:
                    new_col_name = f"{col_name}_{nested_field.name}"
                    df = df.withColumn(
                        new_col_name,
                        F.col(f"{col_name}.{nested_field.name}")
                    )
                df = df.drop(col_name)

            elif isinstance(field.dataType, ArrayType):
                df = df.withColumn(col_name, F.explode_outer(F.col(col_name)))

        return df
