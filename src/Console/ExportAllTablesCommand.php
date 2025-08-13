<?php

namespace ParqBridge\Console;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;

class ExportAllTablesCommand extends Command
{
    protected $signature = 'parqbridge:export-all {--disk=} {--output=} {--exclude=} {--include=}';
    protected $description = 'Export all database tables to Parquet files into a single folder on the chosen disk.';

    public function handle(): int
    {
        $disk = (string) ($this->option('disk') ?: config('parqbridge.disk'));
        $rootOutput = (string) ($this->option('output') ?: config('parqbridge.output_directory'));

        $include = $this->parseCsvOption('include');
        $exclude = $this->parseCsvOption('exclude');

        $tables = $this->getTables();
        if (!empty($include)) {
            $tables = array_values(array_intersect($tables, $include));
        }
        if (!empty($exclude)) {
            $tables = array_values(array_diff($tables, $exclude));
        }

        if (empty($tables)) {
            $this->warn('No tables to export.');
            return self::SUCCESS;
        }

        $subdir = now()->format('Ymd_His');
        $finalOutput = trim($rootOutput, '/').'/'.$subdir;

        $this->info('Exporting '.count($tables).' tables to folder: '.$finalOutput.' on disk '.$disk);

        $ok = 0; $fail = 0;
        foreach ($tables as $t) {
            $exit = $this->call('parqbridge:export', [
                'table' => $t,
                '--output' => $finalOutput,
                '--disk' => $disk,
            ]);
            if ($exit === self::SUCCESS) {
                $ok++;
            } else {
                $fail++;
            }
        }

        $this->line("Completed. Success: {$ok}, Failed: {$fail}. Folder: {$finalOutput}");
        $this->line($finalOutput);
        return $fail === 0 ? self::SUCCESS : self::FAILURE;
    }

    private function parseCsvOption(string $name): array
    {
        $raw = (string) ($this->option($name) ?: '');
        if ($raw === '') return [];
        return array_values(array_filter(array_map(fn($v) => trim($v), explode(',', $raw)), fn($v) => $v !== ''));
    }

    private function getTables(): array
    {
        $driver = DB::getDriverName();
        return match ($driver) {
            'mysql', 'mariadb' => collect(DB::select('SHOW TABLES'))->map(fn($r) => array_values((array)$r)[0])->all(),
            'pgsql' => collect(DB::select("SELECT tablename FROM pg_tables WHERE schemaname = 'public'"))->pluck('tablename')->all(),
            'sqlite' => collect(DB::select("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"))->pluck('name')->all(),
            'sqlsrv' => collect(DB::select("SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'"))->pluck('table_name')->all(),
            default => throw new \RuntimeException("Unsupported driver: {$driver}"),
        };
    }
}
