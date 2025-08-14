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
            // Normalize common FTP config pitfalls (env strings -> proper types)
            $this->normalizeFilesystemDiskConfig($disk);
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

        $transport = (string) config('parqbridge.transport', 'jsonl');
        $isJsonl = strtolower($transport) === 'jsonl';

        // Create temp file and stream rows
        $tmpCsv = tempnam(sys_get_temp_dir(), $isJsonl ? 'parq_jsonl_' : 'parq_tsv_');
        if ($tmpCsv === false) {
            $this->error('Failed to create temporary temp file');
            return self::FAILURE;
        }
        $fp = fopen($tmpCsv, 'w');
        if ($fp === false) {
            $this->error('Failed to open temporary temp file');
            return self::FAILURE;
        }

        // Stream rows in selected transport

        $rowCount = 0;
        $query->orderBy(DB::raw('1'))
            ->chunk($chunkSize, function ($rows) use (&$rowCount, $fp, $schema, $isJsonl) {
                foreach ($rows as $row) {
                    $row = (array) $row;
                    if ($isJsonl) {
                        $obj = [];
                        foreach ($schema as $col) {
                            $name = $col['name'];
                            $ptype = $col['parquet_type'];
                            $logical = $col['logical_type'] ?? null;
                            $val = $row[$name] ?? null;
                            $obj[$name] = $this->jsonValue($val, $ptype, $logical);
                        }
                        fwrite($fp, json_encode($obj, JSON_UNESCAPED_UNICODE)."\n");
                    } else {
                        $out = [];
                        foreach ($schema as $col) {
                            $name = $col['name'];
                            $ptype = $col['parquet_type'];
                            $logical = $col['logical_type'] ?? null;
                            $val = $row[$name] ?? null;
                            $out[] = $this->csvValue($val, $ptype, $logical);
                        }
                        fputcsv($fp, $out, "\t");
                    }
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

        $converter = new ExternalParquetConverter();
        $converter->convertCsvToParquet($tmpCsv, $tmpParquet, $schema, (string) config('parqbridge.compression', 'UNCOMPRESSED'));

        // Push to disk using stream to support remote disks efficiently (e.g., FTP, S3)
        $readStream = fopen($tmpParquet, 'r');
        if ($readStream === false) {
            @unlink($tmpCsv);
            @unlink($tmpParquet);
            $this->error('Failed to open Parquet file for reading');
            return self::FAILURE;
        }
        try {
            Storage::disk($disk)->writeStream($path, $readStream);
        } finally {
            if (is_resource($readStream)) {
                fclose($readStream);
            }
        }

        @unlink($tmpCsv);
        @unlink($tmpParquet);

        $this->info("Exported {$rowCount} rows to {$path}");
        $this->line($path);
        return self::SUCCESS;
    }

    /**
     * Coerce known numeric/boolean options on the target filesystem disk to proper types.
     * This primarily helps with FTP where Flysystem uses strict types under strict_types=1.
     */
    private function normalizeFilesystemDiskConfig(string $disk): void
    {
        $key = "filesystems.disks.{$disk}";
        $cfg = config($key);
        if (!is_array($cfg)) {
            return;
        }

        // Only normalize for FTP-like configs to avoid unintended changes
        $driver = $cfg['driver'] ?? null;
        $looksLikeFtp = $driver === 'ftp' || isset($cfg['host']) && array_key_exists('port', $cfg);
        if (!$looksLikeFtp) {
            return;
        }

        $booleanKeys = [
            'ssl', 'passive', 'utf8', 'ignorePassiveAddress', 'timestampsOnUnixListingsEnabled', 'reconnectAfterTimeout',
        ];
        $intKeys = [
            'port', 'timeout', 'transferMode',
        ];

        $updated = false;

        foreach ($intKeys as $k) {
            if (isset($cfg[$k]) && !is_int($cfg[$k]) && $cfg[$k] !== null) {
                $cfg[$k] = (int) $cfg[$k];
                $updated = true;
            }
        }

        foreach ($booleanKeys as $k) {
            if (isset($cfg[$k]) && !is_bool($cfg[$k]) && $cfg[$k] !== null) {
                $v = $cfg[$k];
                if (is_string($v)) {
                    $v = strtolower($v);
                    $cfg[$k] = in_array($v, ['1','true','on','yes'], true);
                } else {
                    $cfg[$k] = (bool) $v;
                }
                $updated = true;
            }
        }

        if ($updated) {
            config([$key => $cfg]);
        }
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
                $s = (string) $value;
                // Encode UTF8 text to base64 for robust TSV transport
                return base64_encode($s);
            }
            // Base64 encode raw binary for CSV safety, converter will decode
            if (is_resource($value)) {
                $value = stream_get_contents($value);
            }
            return base64_encode((string) $value);
        }
        $s = (string) $value;
        // Numeric/boolean and other scalars are safe; ensure tabs are not present
        return str_replace("\t", ' ', $s);
    }

    private function jsonValue($value, string $ptype, ?string $logical)
    {
        if ($value === null) return null;
        // Reuse csvValue transforms for temporal formatting, then adjust string/binary handling
        if (in_array($ptype, ['INT32','INT64','FLOAT','DOUBLE','BOOLEAN'], true)) {
            return $value;
        }
        if ($ptype === 'INT32' && $logical === 'DATE') {
            return $this->csvValue($value, $ptype, $logical);
        }
        if ($ptype === 'INT32' && $logical === 'TIME_MILLIS') {
            return $this->csvValue($value, $ptype, $logical);
        }
        if ($ptype === 'INT64' && in_array($logical, ['TIME_MICROS','TIMESTAMP_MICROS','TIMESTAMP_MILLIS'], true)) {
            return $this->csvValue($value, $ptype, $logical);
        }
        if (in_array($ptype, ['BYTE_ARRAY','FIXED_LEN_BYTE_ARRAY'], true)) {
            if (($logical ?? null) === 'UTF8') {
                return (string) $value; // keep text as is; Python will keep as string
            }
            if (is_resource($value)) $value = stream_get_contents($value);
            return base64_encode((string) $value);
        }
        return (string) $value;
    }
}
