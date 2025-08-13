<?php

namespace ParqBridge\Tests;

use PHPUnit\Framework\TestCase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Storage;
use ParqBridge\Console\ExportTableCommand;
use ParqBridge\Console\ListTablesCommand;

class ExportCommandTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        // Build a basic schema and seed data
        DB::statement('CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, active INTEGER, created_at TEXT, score REAL)');
        DB::table('users')->truncate();
        DB::table('users')->insert([
            ['name' => 'Alice', 'active' => 1, 'created_at' => '2024-01-01 12:00:00', 'score' => 12.5],
            ['name' => 'Bob', 'active' => 0, 'created_at' => '2024-01-02 13:10:11', 'score' => 7.75],
        ]);
    }

    public function test_list_tables_command_outputs_users(): void
    {
        $cmd = new ListTablesCommand();
        $tester = $this->runCommand($cmd);
        $this->assertStringContainsString('users', $tester['output']);
        $this->assertSame(0, $tester['exitCode']);
    }

    public function test_export_command_writes_file(): void
    {
        $cmd = new ExportTableCommand();
        $tester = $this->runCommand($cmd, ['table' => 'users']);
        $this->assertSame(0, $tester['exitCode']);
        $output = trim($tester['output']);
        $lines = array_filter(explode("\n", $output));
        $path = end($lines);
        $this->assertNotEmpty($path);
        $this->assertTrue(Storage::disk(config('parqbridge.disk'))->exists($path));

        $bytes = Storage::disk(config('parqbridge.disk'))->get($path);
        $this->assertStringStartsWith('PAR1', $bytes, 'Parquet magic header missing');
    }

    private function runCommand($command, array $arguments = []): array
    {
        // Minimal command tester since we avoid full Laravel testbench
        $application = app('artisan');
        $command->setLaravel(app());
        $application->add($command);

        $input = new \Symfony\Component\Console\Input\ArrayInput(array_merge(['command' => $command->getName()], $arguments));
        $output = new \Symfony\Component\Console\Output\BufferedOutput();
        $exitCode = $command->run($input, $output);
        $out = $output->fetch();
        if ($exitCode !== 0) {
            // Bubble command errors into the test output for easier debugging in CI
            fwrite(STDERR, $out);
        }
        return ['exitCode' => $exitCode, 'output' => $out];
    }
}
