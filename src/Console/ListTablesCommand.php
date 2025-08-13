<?php

namespace ParqBridge\Console;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;

class ListTablesCommand extends Command
{
    protected $signature = 'parqbridge:tables';
    protected $description = 'List database tables available for export.';

    public function handle(): int
    {
        $driver = DB::getDriverName();
        $tables = [];
        switch ($driver) {
            case 'mysql':
            case 'mariadb':
                $tables = collect(DB::select('SHOW TABLES'))->map(fn($r) => array_values((array) $r)[0])->all();
                break;
            case 'pgsql':
                $tables = collect(DB::select("SELECT tablename FROM pg_tables WHERE schemaname = 'public'"))->pluck('tablename')->all();
                break;
            case 'sqlite':
                $tables = collect(DB::select("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"))->pluck('name')->all();
                break;
            case 'sqlsrv':
                $tables = collect(DB::select("SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'"))->pluck('table_name')->all();
                break;
            default:
                $this->error("Unsupported driver: {$driver}");
                return self::FAILURE;
        }

        foreach ($tables as $t) {
            $this->line($t);
        }
        return self::SUCCESS;
    }
}
