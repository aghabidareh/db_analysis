# Database Analysis Tool

A general-purpose database analysis tool that supports PostgreSQL, MySQL, and SQL Server.

## Features

- Database structure exploration (tables, columns, constraints, indexes)
- Relationship analysis (foreign keys and inferred relationships)
- Table statistics (row counts, sizes)
- Multi-database support (PostgreSQL, MySQL, SQL Server)
- Multiple output formats (JSON, YAML, XML)

## Installation

```bash
pip install psycopg2-binary mysql-connector-python pyodbc pyyaml dicttoxml
```

## Configuration

Edit `config.json` to set your database credentials and preferences:

```json
{
  "database": {
    "type": "postgresql",
    "host": "localhost",
    "port": 5434,
    "database": "your_database",
    "user": "your_user",
    "password": "your_password",
    "schema": "your_schema"
  },
  "output": {
    "format": "json",
    "structure_file": "db_structure",
    "relationships_file": "db_relationships",
    "stats_file": "db_stats"
  },
  "options": {
    "ignored_tables": [],
    "timeout_seconds": 60,
    "max_retries": 3
  }
}
```

### Database Types
- `postgresql` or `postgres` for PostgreSQL
- `mysql` for MySQL
- `sqlserver` or `mssql` for SQL Server

### Output Formats
- `json` (default)
- `yaml`
- `xml`

## Usage

### Explore Database Structure
```bash
python db_explorer.py
```

This generates a file with complete database structure including tables, columns, constraints, and indexes.

### Analyze Relationships
```bash
python analyze_relationships.py
```

This generates files with:
- Foreign key relationships
- Inferred relationships based on naming patterns
- Table statistics

## Output Files

- `{structure_file}.{format}` - Complete database structure
- `{relationships_file}.{format}` - Foreign key relationships
- `{relationships_file}_inferred.{format}` - Inferred relationships
- `{stats_file}.{format}` - Table statistics

## Requirements

- Python 3.7+
- Database-specific driver:
  - PostgreSQL: `psycopg2-binary`
  - MySQL: `mysql-connector-python`
  - SQL Server: `pyodbc` (requires ODBC Driver 17 for SQL Server)
- Optional: `pyyaml` for YAML output, `dicttoxml` for XML output