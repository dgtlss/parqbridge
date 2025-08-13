<?php

namespace ParqBridge\Console;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Storage;
use ParqBridge\Parquet\Writer\ExternalParquetConverter;
use ParqBridge\Schema\SchemaInferrer;

class ExportTableCommand extends Command
{
    protected $signature = 'parqbridge:export {table} {--where=} {--limit=} {--output=} {--disk=}';

    protected $description = 'Export a database table (or filtered subset) to a Parquet file on a configured Storage disk.';

    public function handle(): int
    {
        $table = (string) $this->argument('table');
        $disk = (string) ($this->option('disk') ?: config('parqbridge.disk'));
        $outputDir = (string) ($this->option('output') ?: config('parqbridge.output_directory'));
        $chunkSize = (int) config('parqbridge.chunk_size');

        try {
            // Resolve disk to ensure it's configured
            Storage::disk($disk);
        } catch (\Throwable $e) {
            $this->error("Disk '{$disk}' is not configured: ".$e->getMessage());
            return self::FAILURE;
        }

        $query = DB::table($table);
        if ($where = $this->option('where')) {
            $query->whereRaw($where);
        }
        if ($limit = $this->option('limit')) {
            $query->limit((int) $limit);
        }

        // Infer schema
        $schema = SchemaInferrer::inferForTable($table);

        $timestamp = now()->format('Ymd_His');
        $filename = $table.'-'.$timestamp.'.parquet';
        $path = trim($outputDir, '/').'/'.$filename;

        $this->info("Writing to disk '{$disk}' path '{$path}' with Apache Parquet (external backend)...");

        // Create temp CSV and stream rows
        $tmpCsv = tempnam(sys_get_temp_dir(), 'parq_csv_');
        if ($tmpCsv === false) {
            $this->error('Failed to create temporary CSV file');
            return self::FAILURE;
        }
        $fp = fopen($tmpCsv, 'w');
        if ($fp === false) {
            $this->error('Failed to open temporary CSV file');
            return self::FAILURE;
        }

        // Header
        $headers = array_map(fn($c) => $c['name'], $schema);
        fputcsv($fp, $headers);

        $rowCount = 0;
        $query->orderBy(DB::raw('1'))
            ->chunk($chunkSize, function ($rows) use (&$rowCount, $fp, $schema) {
                foreach ($rows as $row) {
                    $row = (array) $row;
                    $out = [];
                    foreach ($schema as $col) {
                        $name = $col['name'];
                        $ptype = $col['parquet_type'];
                        $logical = $col['logical_type'] ?? null;
                        $val = $row[$name] ?? null;
                        $out[] = $this->csvValue($val, $ptype, $logical);
                    }
                    fputcsv($fp, $out);
                    $rowCount++;
                }
            });

        fclose($fp);

        // Convert CSV -> Parquet
        $tmpParquet = tempnam(sys_get_temp_dir(), 'parq_out_');
        if ($tmpParquet === false) {
            @unlink($tmpCsv);
            $this->error('Failed to create temporary Parquet file');
            return self::FAILURE;
        }
        $tmpParquet .= '.parquet';

        try {
            $converter = new ExternalParquetConverter();
            $converter->convertCsvToParquet($tmpCsv, $tmpParquet, $schema, (string) config('parqbridge.compression', 'UNCOMPRESSED'));

            // Push to disk
            $bytes = @file_get_contents($tmpParquet);
            if ($bytes === false) {
                throw new \RuntimeException('Failed to read generated Parquet file');
            }
            Storage::disk($disk)->put($path, $bytes);
        } catch (\Throwable $e) {
            $this->error('Parquet conversion failed: ' . $e->getMessage());
            @unlink($tmpCsv);
            @unlink($tmpParquet);
            return self::FAILURE;
        }

        @unlink($tmpCsv);
        @unlink($tmpParquet);

        $this->info("Exported {$rowCount} rows to {$path}");
        $this->line($path);
        return self::SUCCESS;
    }

    private function csvValue($value, string $ptype, ?string $logical)
    {
        if ($value === null) {
            return '';
        }
        if ($ptype === 'BOOLEAN') {
            return $value ? 'true' : 'false';
        }
        if ($ptype === 'INT32' && $logical === 'DATE') {
            if ($value instanceof \DateTimeInterface) {
                return $value->format('Y-m-d');
            }
            return (new \DateTimeImmutable((string)$value))->format('Y-m-d');
        }
        if ($ptype === 'INT32' && $logical === 'TIME_MILLIS') {
            if ($value instanceof \DateTimeInterface) {
                return $value->format('H:i:s.v');
            }
            return (new \DateTimeImmutable((string)$value))->format('H:i:s.v');
        }
        if ($ptype === 'INT64' && in_array($logical, ['TIME_MICROS','TIMESTAMP_MICROS','TIMESTAMP_MILLIS'], true)) {
            if ($value instanceof \DateTimeInterface) {
                return $logical === 'TIMESTAMP_MILLIS' ? $value->format('Y-m-d H:i:s.v') : $value->format('Y-m-d H:i:s.u');
            }
            $dt = new \DateTimeImmutable((string)$value);
            return $logical === 'TIMESTAMP_MILLIS' ? $dt->format('Y-m-d H:i:s.v') : $dt->format('Y-m-d H:i:s.u');
        }
        if (in_array($ptype, ['BYTE_ARRAY','FIXED_LEN_BYTE_ARRAY'], true)) {
            if (($logical ?? null) === 'UTF8') {
                return (string) $value;
            }
            // Base64 encode raw binary for CSV safety, converter will decode
            if (is_resource($value)) {
                $value = stream_get_contents($value);
            }
            return base64_encode((string) $value);
        }
        return (string) $value;
    }
}
