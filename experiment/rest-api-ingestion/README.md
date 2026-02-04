# REST API Ingestion Pipeline

A PySpark-based data ingestion pipeline for loading data from REST APIs into GCS bronze layer following the medallion architecture.

## Features

- **Multiple ingestion modes**: Initial, incremental, backfill, and reference table loading
- **Idempotent operations**: Safe reruns with overwrite support
- **Pagination handling**: Supports offset, cursor, and page-based pagination
- **Schema validation**: Configurable schema enforcement
- **Checkpoint management**: Watermark-based incremental loading
- **Error handling**: Comprehensive logging and error recovery

## Project Structure

```
experiment/rest-api-ingestion/
├── airflow/                    # Airflow DAGs and configs
├── analytique/
│   └── msb_ingestion/         # Core package
│       ├── api/
│       │   └── client.py      # API interaction (fetch, pagination, retry)
│       ├── storage/
│       │   └── gcs_writer.py  # Write to GCS bucket
│       ├── ingestion/
│       │   ├── processor.py   # Transformations and business logic
│       │   └── rules.py       # Ingestion rules
│       ├── config/
│       │   └── settings.py    # Configuration management
│       └── utils/
│           ├── logger.py      # Logging setup
│           └── helpers.py     # Common utilities
├── config/
│   └── ingestion_config.yaml  # Sample configuration
├── main.py                     # Orchestrator
├── tests/                      # Unit tests
├── pyproject.toml
└── README.md
```

## Installation

```bash
pip install -e ".[dev]"
```

## Usage

### Initial Load
Load all data for a table:
```bash
python main.py --table customers --mode initial --config-path config/ingestion_config.yaml
```

### Incremental Load
Load only new/changed data since last checkpoint:
```bash
python main.py --table customers --mode incremental
```

### Backfill
Reload historical data for a date range:
```bash
python main.py --table orders --mode backfill --start-date 2024-01-01 --end-date 2024-01-31
```

### Reference Tables
Full snapshot for dimension tables:
```bash
python main.py --table countries --mode reference
```

### Process All Tables
```bash
python main.py --table all --mode incremental
```

## Configuration

See `config/ingestion_config.yaml` for a complete example. Key sections:

- **api**: REST API connection settings
- **gcs**: GCS bucket and path configuration
- **tables**: Table-specific settings including schema, pagination, and change tracking

## Environment Variables

- `API_AUTH_TOKEN`: Authentication token for the REST API

## Running Tests

```bash
pytest tests/ -v
```

## Architecture

### Ingestion Modes

1. **Initial**: Full load of all data, overwrites existing
2. **Incremental**: Uses watermarks to load only changed records
3. **Backfill**: Reloads data for specific date ranges
4. **Reference**: Full snapshot for dimension tables without change tracking

### Data Flow

1. Parse CLI parameters
2. Load configuration
3. Initialize Spark session
4. For each table:
   - Apply ingestion rule
   - Fetch data from API with pagination
   - Transform and validate
   - Write to GCS bronze layer
   - Update checkpoints

## License

MIT
