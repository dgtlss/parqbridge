<?php

return [
    /*
    |--------------------------------------------------------------------------
    | Export Disk
    |--------------------------------------------------------------------------
    | The filesystem disk where Parquet files will be written. This uses
    | Laravel's Storage facade under the hood, so any disk configured in
    | config/filesystems.php is supported (e.g., "local", "s3").
    |
    | .env: PARQUET_DISK=s3
    */
    'disk' => env('PARQUET_DISK', env('FILESYSTEM_DISK', 'local')),

    /*
    |--------------------------------------------------------------------------
    | Output Directory
    |--------------------------------------------------------------------------
    | Directory path prefix inside the selected disk. The final path will be
    | {output_directory}/{table}-{timestamp}.parquet
    |
    | .env: PARQUET_OUTPUT_DIR=parquet-exports
    */
    'output_directory' => env('PARQUET_OUTPUT_DIR', 'parquet-exports'),

    /*
    |--------------------------------------------------------------------------
    | Chunk Size
    |--------------------------------------------------------------------------
    | Number of rows fetched per chunk when streaming data out of the database.
    | Larger chunks are faster but use more memory.
    |
    | .env: PARQUET_CHUNK_SIZE=1000
    */
    'chunk_size' => (int) env('PARQUET_CHUNK_SIZE', 1000),

    /*
    |--------------------------------------------------------------------------
    | Date/Time Formatting for Fallbacks
    |--------------------------------------------------------------------------
    | When a database driver returns date/time types as strings or DateTime,
    | these formats are used for the Parquet logical annotations we emit.
    | You usually don't need to change these.
    */
    'date_format' => 'Y-m-d',
    'datetime_format' => \DateTimeInterface::ATOM,
    'time_format' => 'H:i:s',

    /*
    |--------------------------------------------------------------------------
    | Schema Inference Strategy
    |--------------------------------------------------------------------------
    | "database" will use the database column types from the schema to choose
    | Parquet primitive/logical types. "sample" will inspect the first chunk
    | of data to refine types (e.g., booleans stored as tinyint(1)).
    | Options: database | sample | hybrid
    */
    'inference' => env('PARQUET_INFERENCE', 'hybrid'),

    /*
    |--------------------------------------------------------------------------
    | Compression
    |--------------------------------------------------------------------------
    | Compression codec for Parquet files. When using the PyArrow backend you
    | may choose from: NONE (alias UNCOMPRESSED), SNAPPY, GZIP, ZSTD, BROTLI,
    | LZ4_RAW. Default is UNCOMPRESSED.
    */
    'compression' => env('PARQUET_COMPRESSION', 'UNCOMPRESSED'),

    /*
    |--------------------------------------------------------------------------
    | Writer Backend
    |--------------------------------------------------------------------------
    | Controls how ParqBridge produces Apache Parquet files.
    | - pyarrow: Uses Python + PyArrow (requires `python3` with `pyarrow` installed)
    | - custom:  Uses a custom shell command template provided below
    |
    | .env: PARQBRIDGE_WRITER=pyarrow
    */
    'writer' => env('PARQBRIDGE_WRITER', 'pyarrow'),

    /*
    | Python executable name/path for the PyArrow backend. E.g., python3 or /usr/bin/python3
    | .env: PARQBRIDGE_PYTHON=python3
    */
    'pyarrow_python' => env('PARQBRIDGE_PYTHON', 'python3'),

    /*
    | Custom command template when writer=custom. Use {input} and {output} placeholders.
    | Example (DuckDB CLI): duckdb -c "COPY (SELECT * FROM read_csv_auto({input})) TO {output} (FORMAT PARQUET)"
    | .env: PARQBRIDGE_CUSTOM_CMD="duckdb -c \"COPY (SELECT * FROM read_csv_auto({input})) TO {output} (FORMAT PARQUET)\""
    */
    'custom_command' => env('PARQBRIDGE_CUSTOM_CMD', ''),

    /*
    |--------------------------------------------------------------------------
    | PyArrow CSV Read Block Size (bytes)
    |--------------------------------------------------------------------------
    | Increase this if you see errors such as: "straddling object straddles two
    | block boundaries" when reading very large TSV rows. Default: 64 MiB.
    | .env: PARQBRIDGE_PYARROW_BLOCK_SIZE=67108864
    */
    'pyarrow_block_size' => (int) env('PARQBRIDGE_PYARROW_BLOCK_SIZE', 64 * 1024 * 1024),
];
