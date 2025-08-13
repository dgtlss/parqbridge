<?php

namespace ParqBridge\Schema;

use Illuminate\Support\Facades\DB;

class SchemaInferrer
{
    /**
     * Infer a Parquet schema array for the given table.
     * Returns an array of columns: [ ['name' => 'col', 'parquet_type' => 'INT64', 'logical_type' => 'TIMESTAMP_MILLIS', 'nullable' => true, 'precision' => 10, 'scale' => 2], ... ]
     */
    public static function inferForTable(string $table): array
    {
        $driver = DB::getDriverName();
        return match ($driver) {
            'mysql' => self::inferMySql($table),
            'pgsql' => self::inferPostgres($table),
            'sqlite' => self::inferSqlite($table),
            'sqlsrv' => self::inferSqlSrv($table),
            default => throw new \RuntimeException("Unsupported driver: {$driver}"),
        };
    }

    private static function inferMySql(string $table): array
    {
        // information_schema for MySQL
        $db = DB::getDatabaseName();
        $rows = DB::select("SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION", [$db, $table]);
        return array_map(function ($r) {
            $row = (array) $r;
            $name = $row['COLUMN_NAME'];
            $dataType = strtolower($row['DATA_TYPE']);
            $columnType = strtolower($row['COLUMN_TYPE']);
            $nullable = strtoupper($row['IS_NULLABLE'] ?? 'YES') === 'YES';
            return self::mapMySqlType($name, $dataType, $columnType, $nullable);
        }, $rows);
    }

    private static function mapMySqlType(string $name, string $dataType, string $columnType, bool $nullable): array
    {
        $col = ['name' => $name, 'nullable' => $nullable];
        switch ($dataType) {
            case 'tinyint':
                if (preg_match('/tinyint\(1\)/', $columnType)) {
                    return $col + ['parquet_type' => 'BOOLEAN'];
                }
                return $col + ['parquet_type' => 'INT32'];
            case 'smallint':
            case 'year':
                return $col + ['parquet_type' => 'INT32'];
            case 'int':
            case 'integer':
            case 'mediumint':
                return $col + ['parquet_type' => 'INT32'];
            case 'bigint':
                return $col + ['parquet_type' => 'INT64'];
            case 'float':
                return $col + ['parquet_type' => 'FLOAT'];
            case 'double':
            case 'real':
                return $col + ['parquet_type' => 'DOUBLE'];
            case 'decimal':
            case 'numeric':
                if (preg_match('/decimal\((\d+),(\d+)\)/', $columnType, $m)) {
                    return $col + ['parquet_type' => 'FIXED_LEN_BYTE_ARRAY', 'logical_type' => 'DECIMAL', 'precision' => (int)$m[1], 'scale' => (int)$m[2]];
                }
                return $col + ['parquet_type' => 'FIXED_LEN_BYTE_ARRAY', 'logical_type' => 'DECIMAL', 'precision' => 18, 'scale' => 6];
            case 'bit':
                return $col + ['parquet_type' => 'INT32'];
            case 'bool':
            case 'boolean':
                return $col + ['parquet_type' => 'BOOLEAN'];
            case 'date':
                return $col + ['parquet_type' => 'INT32', 'logical_type' => 'DATE'];
            case 'datetime':
            case 'timestamp':
                return $col + ['parquet_type' => 'INT64', 'logical_type' => 'TIMESTAMP_MILLIS'];
            case 'time':
                return $col + ['parquet_type' => 'INT32', 'logical_type' => 'TIME_MILLIS'];
            case 'json':
            case 'text':
            case 'tinytext':
            case 'mediumtext':
            case 'longtext':
            case 'char':
            case 'varchar':
            case 'enum':
            case 'set':
            case 'binary':
            case 'varbinary':
            case 'blob':
            case 'tinyblob':
            case 'mediumblob':
            case 'longblob':
                return $col + ['parquet_type' => 'BYTE_ARRAY', 'logical_type' => 'UTF8'];
            default:
                return $col + ['parquet_type' => 'BYTE_ARRAY'];
        }
    }

    private static function inferPostgres(string $table): array
    {
        $rows = DB::select("SELECT column_name, data_type, is_nullable, udt_name, numeric_precision, numeric_scale FROM information_schema.columns WHERE table_schema='public' AND table_name = ? ORDER BY ordinal_position", [$table]);
        return array_map(function ($r) {
            $row = (array) $r;
            $name = $row['column_name'];
            $dataType = strtolower($row['data_type']);
            $udt = strtolower($row['udt_name'] ?? '');
            $nullable = strtolower($row['is_nullable'] ?? 'yes') === 'yes';
            $precision = isset($row['numeric_precision']) ? (int)$row['numeric_precision'] : null;
            $scale = isset($row['numeric_scale']) ? (int)$row['numeric_scale'] : null;
            return self::mapPostgresType($name, $dataType, $udt, $nullable, $precision, $scale);
        }, $rows);
    }

    private static function mapPostgresType(string $name, string $dataType, string $udt, bool $nullable, ?int $precision, ?int $scale): array
    {
        $col = ['name' => $name, 'nullable' => $nullable];
        switch ($dataType) {
            case 'smallint': return $col + ['parquet_type' => 'INT32'];
            case 'integer': return $col + ['parquet_type' => 'INT32'];
            case 'bigint': return $col + ['parquet_type' => 'INT64'];
            case 'real': return $col + ['parquet_type' => 'FLOAT'];
            case 'double precision': return $col + ['parquet_type' => 'DOUBLE'];
            case 'numeric':
                return $col + ['parquet_type' => 'FIXED_LEN_BYTE_ARRAY', 'logical_type' => 'DECIMAL', 'precision' => $precision ?? 18, 'scale' => $scale ?? 6];
            case 'boolean': return $col + ['parquet_type' => 'BOOLEAN'];
            case 'date': return $col + ['parquet_type' => 'INT32', 'logical_type' => 'DATE'];
            case 'timestamp without time zone':
            case 'timestamp with time zone':
                return $col + ['parquet_type' => 'INT64', 'logical_type' => 'TIMESTAMP_MICROS'];
            case 'time without time zone':
            case 'time with time zone':
                return $col + ['parquet_type' => 'INT64', 'logical_type' => 'TIME_MICROS'];
            case 'json':
            case 'jsonb':
            case 'text':
            case 'character varying':
            case 'character':
            case 'uuid':
            case 'bytea':
                return $col + ['parquet_type' => 'BYTE_ARRAY', 'logical_type' => 'UTF8'];
            default:
                return $col + ['parquet_type' => 'BYTE_ARRAY'];
        }
    }

    private static function inferSqlite(string $table): array
    {
        $rows = DB::select("PRAGMA table_info('".$table."')");
        return array_map(function ($r) {
            $row = (array) $r;
            $name = $row['name'];
            $type = strtolower($row['type'] ?? '');
            $nullable = (int)($row['notnull'] ?? 0) === 0;
            return self::mapSqliteType($name, $type, $nullable);
        }, $rows);
    }

    private static function mapSqliteType(string $name, string $type, bool $nullable): array
    {
        $col = ['name' => $name, 'nullable' => $nullable];
        if (str_contains($type, 'int')) return $col + ['parquet_type' => 'INT64'];
        if (str_contains($type, 'char') || str_contains($type, 'text')) return $col + ['parquet_type' => 'BYTE_ARRAY', 'logical_type' => 'UTF8'];
        if (str_contains($type, 'real') || str_contains($type, 'floa')) return $col + ['parquet_type' => 'DOUBLE'];
        if (str_contains($type, 'blob')) return $col + ['parquet_type' => 'BYTE_ARRAY'];
        return $col + ['parquet_type' => 'BYTE_ARRAY'];
    }

    private static function inferSqlSrv(string $table): array
    {
        $rows = DB::select("SELECT c.name AS column_name, t.name AS data_type, c.is_nullable, c.precision, c.scale FROM sys.columns c JOIN sys.types t ON c.user_type_id=t.user_type_id WHERE object_id = OBJECT_ID(?) ORDER BY c.column_id", [$table]);
        return array_map(function ($r) {
            $row = (array) $r;
            $name = $row['column_name'];
            $dataType = strtolower($row['data_type']);
            $nullable = (bool) ($row['is_nullable'] ?? true);
            $precision = isset($row['precision']) ? (int)$row['precision'] : null;
            $scale = isset($row['scale']) ? (int)$row['scale'] : null;
            return self::mapSqlSrvType($name, $dataType, $nullable, $precision, $scale);
        }, $rows);
    }

    private static function mapSqlSrvType(string $name, string $dataType, bool $nullable, ?int $precision, ?int $scale): array
    {
        $col = ['name' => $name, 'nullable' => $nullable];
        return match ($dataType) {
            'tinyint', 'smallint', 'int' => $col + ['parquet_type' => 'INT32'],
            'bigint' => $col + ['parquet_type' => 'INT64'],
            'real' => $col + ['parquet_type' => 'FLOAT'],
            'float' => $col + ['parquet_type' => 'DOUBLE'],
            'decimal', 'numeric' => $col + ['parquet_type' => 'FIXED_LEN_BYTE_ARRAY', 'logical_type' => 'DECIMAL', 'precision' => $precision ?? 18, 'scale' => $scale ?? 6],
            'bit' => $col + ['parquet_type' => 'BOOLEAN'],
            'date' => $col + ['parquet_type' => 'INT32', 'logical_type' => 'DATE'],
            'datetime', 'datetime2', 'smalldatetime', 'datetimeoffset' => $col + ['parquet_type' => 'INT64', 'logical_type' => 'TIMESTAMP_MILLIS'],
            'time' => $col + ['parquet_type' => 'INT64', 'logical_type' => 'TIME_MICROS'],
            'nchar', 'nvarchar', 'varchar', 'char', 'text', 'ntext', 'uniqueidentifier', 'xml', 'varbinary', 'binary', 'image' => $col + ['parquet_type' => 'BYTE_ARRAY', 'logical_type' => 'UTF8'],
            default => $col + ['parquet_type' => 'BYTE_ARRAY'],
        };
    }
}
