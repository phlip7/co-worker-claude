# REST API Ingestion Pipeline

A PySpark-based data ingestion pipeline for loading data from REST APIs into GCS bronze layer following the medallion architecture.

## Features

- **Multiple ingestion modes**: Initial, incremental, backfill, and reference table loading
- **Idempotent operations**: Safe reruns with overwrite support
- **Pagination handling**: Supports offset, cursor, and page-based pagination
- **Schema validation**: Configurable schema enforcement
- **Checkpoint management**: Watermark-based incremental loading
- **Error handling**: Comprehensive logging and error recovery
- **Modular architecture**: Generic reusable components + project-specific modules

## Project Structure

```
experiment/rest-api-ingestion/
├── airflow/                    # Airflow DAGs and configs
├── analytique/
│   ├── common/                # SHARED: Generic reusable modules
│   │   ├── api/
│   │   │   └── client.py      # Generic API client (any REST API)
│   │   ├── storage/
│   │   │   └── gcs_writer.py  # Generic GCS writer
│   │   └── utils/
│   │       ├── logger.py      # Logging setup
│   │       └── helpers.py     # Common utilities
│   └── msb_ingestion/         # PROJECT-SPECIFIC: MSB ingestion project
│       ├── ingestion/
│       │   ├── processor.py   # MSB-specific transformations
│       │   └── rules.py       # MSB-specific ingestion rules
│       └── config/
│           └── settings.py    # MSB-specific configuration
├── config/
│   └── ingestion_config.yaml  # Sample configuration
├── main.py                     # Orchestrator
├── tests/                      # Unit tests
├── pyproject.toml
└── README.md
```

## Architecture

### Module Separation

- **`analytique/common/`**: Generic, reusable modules that can be used across all ingestion projects
  - API client with pagination support
  - GCS writer with partitioning
  - Logging and helper utilities

- **`analytique/msb_ingestion/`**: Project-specific modules for the MSB ingestion
  - Configuration settings specific to MSB
  - Data processor with MSB-specific transformations
  - Ingestion rules for different load modes

This separation allows new ingestion projects to reuse the common components while implementing their own project-specific logic.

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

## Ingestion Modes

1. **Initial**: Full load of all data, overwrites existing
2. **Incremental**: Uses watermarks to load only changed records
3. **Backfill**: Reloads data for specific date ranges
4. **Reference**: Full snapshot for dimension tables without change tracking

## Data Flow

1. Parse CLI parameters
2. Initialize logger (from common.utils)
3. Load configuration (project-specific)
4. For each table:
   - Fetch data using generic API client (common.api.client)
   - Apply ingestion rules (project-specific)
   - Transform and validate (project-specific processor)
   - Write to GCS using generic writer (common.storage.gcs_writer)
   - Update checkpoints

## License

MIT
