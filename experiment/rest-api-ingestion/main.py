#!/usr/bin/env python3
"""Main orchestrator for REST API ingestion pipeline.

This script orchestrates the ingestion of data from REST APIs to GCS bronze layer.
It supports initial, incremental, backfill, and reference table loading modes.

Usage:
    python main.py --table customers --mode incremental --start-date 2024-01-01
    python main.py --table all --mode initial
    python main.py --table orders --mode backfill --start-date 2024-01-01 --end-date 2024-01-31
"""

import argparse
import sys
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession

from analytique.msb_ingestion.api.client import APIClient, APIClientError
from analytique.msb_ingestion.config.settings import Settings, TableConfig
from analytique.msb_ingestion.ingestion.processor import DataProcessor, ProcessorError
from analytique.msb_ingestion.ingestion.rules import RuleFactory, IngestionRule
from analytique.msb_ingestion.storage.gcs_writer import GCSWriter, GCSWriterError
from analytique.msb_ingestion.utils.logger import get_logger, IngestionMetrics
from analytique.msb_ingestion.utils.helpers import parse_date


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        Parsed arguments namespace.
    """
    parser = argparse.ArgumentParser(
        description="REST API to GCS Bronze Layer Ingestion Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--table",
        type=str,
        required=True,
        help="Table name to ingest, or 'all' for all tables",
    )

    parser.add_argument(
        "--mode",
        type=str,
        required=True,
        choices=["initial", "incremental", "backfill", "reference"],
        help="Ingestion mode",
    )

    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date for data extraction (YYYY-MM-DD)",
    )

    parser.add_argument(
        "--end-date",
        type=str,
        help="End date for data extraction (YYYY-MM-DD)",
    )

    parser.add_argument(
        "--config-path",
        type=str,
        default="config/ingestion_config.yaml",
        help="Path to configuration file",
    )

    return parser.parse_args()


def create_spark_session() -> SparkSession:
    """Create and configure SparkSession.

    Returns:
        Configured SparkSession instance.
    """
    return (
        SparkSession.builder
        .appName("REST-API-Ingestion")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def get_tables_to_process(
    settings: Settings,
    table_arg: str,
    mode: str,
) -> list[TableConfig]:
    """Determine which tables to process based on arguments.

    Args:
        settings: Application settings.
        table_arg: Table argument from CLI ('all' or specific table name).
        mode: Ingestion mode.

    Returns:
        List of TableConfig objects to process.
    """
    logger = get_logger()

    if table_arg.lower() == "all":
        if mode == "reference":
            tables = settings.get_tables_by_type("reference")
            logger.info(f"Processing all reference tables: {[t.name for t in tables]}")
        else:
            tables = settings.get_all_tables()
            logger.info(f"Processing all tables: {[t.name for t in tables]}")
        return tables
    else:
        table = settings.get_table(table_arg)
        logger.info(f"Processing single table: {table.name}")
        return [table]


def process_table(
    table_config: TableConfig,
    rule: IngestionRule,
    api_client: APIClient,
    processor: DataProcessor,
    gcs_writer: GCSWriter,
    ingestion_date: datetime,
    start_date: Optional[datetime],
    end_date: Optional[datetime],
    mode: str,
) -> IngestionMetrics:
    """Process a single table through the ingestion pipeline.

    Args:
        table_config: Configuration for the table.
        rule: Ingestion rule to apply.
        api_client: API client for fetching data.
        processor: Data processor.
        gcs_writer: GCS writer.
        ingestion_date: Date of ingestion.
        start_date: Start date for filtering.
        end_date: End date for filtering.
        mode: Ingestion mode.

    Returns:
        IngestionMetrics with processing statistics.
    """
    logger = get_logger()
    metrics = IngestionMetrics(table_config.name, mode)
    metrics.start()

    try:
        if not rule.should_process(table_config, ingestion_date):
            logger.info(f"Skipping {table_config.name} - rule check failed")
            metrics.end()
            return metrics

        watermark = rule.get_watermark(table_config)
        logger.info(f"Using watermark: {watermark}")

        all_dataframes = []

        for batch_records in api_client.fetch_with_pagination(
            table_config=table_config,
            start_date=start_date,
            end_date=end_date,
            watermark_value=watermark,
        ):
            metrics.increment_pages()
            metrics.add_records_fetched(len(batch_records))

            df = processor.process(
                records=batch_records,
                table_config=table_config,
                ingestion_date=ingestion_date,
                mode=mode,
            )

            all_dataframes.append(df)

        if not all_dataframes:
            logger.info(f"No data fetched for {table_config.name}")
            metrics.end()
            return metrics

        final_df = processor.merge_batches(all_dataframes)
        record_count = final_df.count()

        if record_count == 0:
            logger.info(f"No records to write for {table_config.name}")
            metrics.end()
            return metrics

        write_mode = rule.get_write_mode()
        written_count = gcs_writer.write(
            df=final_df,
            table_config=table_config,
            ingestion_date=ingestion_date,
            mode=write_mode,
        )
        metrics.add_records_written(written_count)

        new_watermark = processor.get_max_watermark(final_df, table_config)
        if new_watermark:
            rule.update_checkpoint(table_config, new_watermark, ingestion_date)

    except (APIClientError, ProcessorError, GCSWriterError) as e:
        metrics.add_error(str(e))
        logger.error(f"Failed to process {table_config.name}: {e}")

    except Exception as e:
        metrics.add_error(f"Unexpected error: {e}")
        logger.exception(f"Unexpected error processing {table_config.name}")

    finally:
        metrics.end()

    return metrics


def main() -> int:
    """Main entry point for the ingestion pipeline.

    Returns:
        Exit code (0 for success, 1 for failure).
    """
    args = parse_arguments()

    settings = Settings.from_yaml(args.config_path)

    logger = get_logger(level=settings.log_level)
    logger.info("=" * 60)
    logger.info("REST API Ingestion Pipeline Started")
    logger.info(f"Mode: {args.mode}")
    logger.info(f"Table: {args.table}")
    logger.info("=" * 60)

    start_date = parse_date(args.start_date) if args.start_date else None
    end_date = parse_date(args.end_date) if args.end_date else None
    ingestion_date = datetime.utcnow()

    if args.mode == "backfill" and (not start_date or not end_date):
        logger.error("Backfill mode requires both --start-date and --end-date")
        return 1

    spark = create_spark_session()

    try:
        rule_factory = RuleFactory(
            spark=spark,
            gcs_config=settings.gcs,
            checkpoint_table=settings.checkpoint_table,
        )
        rule = rule_factory.create_rule(
            mode=args.mode,
            start_date=start_date,
            end_date=end_date,
        )

        tables = get_tables_to_process(settings, args.table, args.mode)

        if not tables:
            logger.warning("No tables to process")
            return 0

        api_client = APIClient(settings.api)
        processor = DataProcessor(spark)
        gcs_writer = GCSWriter(settings.gcs, spark)

        all_metrics = []
        failed_tables = []

        for table_config in tables:
            logger.info("-" * 40)
            logger.info(f"Processing table: {table_config.name}")
            logger.info(f"Table type: {table_config.table_type}")
            logger.info("-" * 40)

            metrics = process_table(
                table_config=table_config,
                rule=rule,
                api_client=api_client,
                processor=processor,
                gcs_writer=gcs_writer,
                ingestion_date=ingestion_date,
                start_date=start_date,
                end_date=end_date,
                mode=args.mode,
            )

            all_metrics.append(metrics)

            if metrics.errors:
                failed_tables.append(table_config.name)

        api_client.close()

        logger.info("=" * 60)
        logger.info("Ingestion Summary")
        logger.info("=" * 60)

        total_fetched = sum(m.records_fetched for m in all_metrics)
        total_written = sum(m.records_written for m in all_metrics)
        total_errors = sum(len(m.errors) for m in all_metrics)

        logger.info(f"Tables processed: {len(tables)}")
        logger.info(f"Total records fetched: {total_fetched}")
        logger.info(f"Total records written: {total_written}")
        logger.info(f"Total errors: {total_errors}")

        if failed_tables:
            logger.error(f"Failed tables: {failed_tables}")
            return 1

        logger.info("Pipeline completed successfully")
        return 0

    except Exception as e:
        logger.exception(f"Pipeline failed with error: {e}")
        return 1

    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
