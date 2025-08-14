# ParqBridge

Export your Laravel database tables to real Apache Parquet files on any Storage disk (local, S3, etc.) with a simple artisan command.

ParqBridge focuses on zero PHP dependency bloat while still producing spec-compliant Parquet files by delegating the final write step to a tiny, embedded Python script using PyArrow (or any custom CLI you prefer). You keep full Laravel DX for configuration and Storage; we bridge your data to Parquet.

## Installation

- Require the package in your app (path repo or VCS):

```bash
composer require dgtlss/parqbridge
```

- Laravel will auto-discover the service provider. Alternatively, register `ParqBridge\\ParqBridgeServiceProvider` manually.

- Publish the config if you want to customize defaults:

```bash
php artisan vendor:publish --tag="parqbridge-config"
```

## Configuration

Set your export disk and options in `.env` or `config/parqbridge.php`.

- `PARQUET_DISK`: which filesystem disk to use (e.g., `s3`, `local`).
- `PARQUET_OUTPUT_DIR`: directory prefix within the disk (default `parquet-exports`).
- `PARQUET_CHUNK_SIZE`: rows per DB chunk when exporting (default 1000).
- `PARQUET_INFERENCE`: `database|sample|hybrid` (default `hybrid`).
- `PARQUET_COMPRESSION`: compression codec for Parquet (`UNCOMPRESSED`/`NONE`, `SNAPPY`, `GZIP`, `ZSTD`, `BROTLI`, `LZ4_RAW`) when using PyArrow backend.
- `PARQBRIDGE_WRITER`: `pyarrow` (default) or `custom`. If `custom`, set `PARQBRIDGE_CUSTOM_CMD`.
- `PARQBRIDGE_PYTHON`: python executable for PyArrow (default `python3`).

Example `.env`:

```ini
PARQUET_DISK=s3
PARQUET_OUTPUT_DIR=parquet-exports
PARQUET_CHUNK_SIZE=2000
```

Ensure your `filesystems` disk is configured (e.g., `s3`) in `config/filesystems.php`.

### FTP disk configuration

You can export directly to an FTP server using Laravel's `ftp` disk. Add an FTP disk to `config/filesystems.php` and reference it via `PARQUET_DISK=ftp` or `--disk=ftp`.

```php
'disks' => [
    'ftp' => [
        'driver' => 'ftp',
        'host' => env('FTP_HOST'),
        'username' => env('FTP_USERNAME'),
        'password' => env('FTP_PASSWORD'),

        // Optional FTP settings
        'port' => (int) env('FTP_PORT', 21),
        'root' => env('FTP_ROOT', ''),
        'passive' => filter_var(env('FTP_PASSIVE', true), FILTER_VALIDATE_BOOL),
        'ssl' => filter_var(env('FTP_SSL', false), FILTER_VALIDATE_BOOL),
        'timeout' => (int) env('FTP_TIMEOUT', 90),
    ],
],
```

Note: This package will coerce common FTP env values (e.g., `port`, `timeout`, `passive`, `ssl`) to the proper types before resolving the disk to avoid Flysystem type errors like "Argument #5 ($port) must be of type int, string given".

## Usage

- List tables:

```bash
php artisan parqbridge:tables
```

- Export a table to the configured disk:

```bash
php artisan parqbridge:export users --where="active = 1" --limit=1000 --output="parquet-exports" --disk=s3
```

On success, the command prints the full path written within the disk. Files are named `{table}-{YYYYMMDD_HHMMSS}.parquet`.

- Export ALL tables into one folder (timestamped subfolder inside `parqbridge.output_directory`):

```bash
php artisan parqbridge:export-all --disk=s3 --output="parquet-exports" --exclude=migrations,password_resets
```

Options:
- `--include=`: comma-separated allowlist of table names
- `--exclude=`: comma-separated denylist of table names

## Data types

The schema inferrer maps common DB types to a set of Parquet primitive types and logical annotations. With the PyArrow backend, an Arrow schema is constructed to faithfully write types:

- Primitive: `BOOLEAN`, `INT32`, `INT64`, `FLOAT`, `DOUBLE`, `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY`
- Logical: `UTF8`, `DATE`, `TIME_MILLIS`, `TIME_MICROS`, `TIMESTAMP_MILLIS`, `TIMESTAMP_MICROS`, `DECIMAL`

For decimals we write Arrow decimal types (`decimal128`/`decimal256`) with declared `precision`/`scale`.

## Testing

Run the test suite:

```bash
composer install
vendor/bin/phpunit
```

The tests bootstrap a minimal container, create a SQLite database, and verify:
- listing tables works on SQLite
- exporting a table writes a Parquet file to the configured disk (magic `PAR1`)
- schema inference on SQLite maps major families

## Backend requirements

- By default ParqBridge uses Python + PyArrow. Ensure `python3` is available and install PyArrow:

```bash
python3 -m pip install --upgrade pip
python3 -m pip install pyarrow
```

- Alternatively set a custom converter command via `PARQBRIDGE_WRITER=custom` and `PARQBRIDGE_CUSTOM_CMD` (must read `{input}` CSV and write `{output}` Parquet).

You can automate setup via the included command:

```bash
php artisan parqbridge:setup --write-env
```

Options:
- `--python=`: path/name of Python (default from config `parqbridge.pyarrow_python`)
- `--venv=`: location for virtualenv (default `./parqbridge-venv`)
- `--no-venv`: install into global Python instead of a venv
- `--write-env`: append `PARQBRIDGE_PYTHON` and `PARQBRIDGE_WRITER` to `.env`
- `--upgrade`: upgrade pip first
- `--dry-run`: print commands without executing
