<?php

namespace ParqBridge\Console;

use Illuminate\Console\Command;

class SetupCommand extends Command
{
    protected $signature = 'parqbridge:setup {--python=} {--venv=} {--no-venv} {--write-env} {--upgrade} {--dry-run}';
    protected $description = 'Set up the ParqBridge Apache Parquet backend (create venv and install PyArrow).';

    public function handle(): int
    {
        $python = (string) ($this->option('python') ?: config('parqbridge.pyarrow_python', 'python3'));
        $useVenv = !$this->option('no-venv');
        $dryRun = (bool) $this->option('dry-run');
        $writeEnv = (bool) $this->option('write-env');
        $upgrade = (bool) $this->option('upgrade');

        $basePath = function_exists('base_path') ? base_path() : getcwd();
        $venvPath = (string) ($this->option('venv') ?: ($basePath . DIRECTORY_SEPARATOR . 'parqbridge-venv'));
        $isWindows = strtoupper(substr(PHP_OS, 0, 3)) === 'WIN';

        $commands = [];
        if ($useVenv) {
            $commands[] = sprintf('%s -m venv %s', escapeshellarg($python), escapeshellarg($venvPath));
            $pip = $isWindows ? $venvPath . '\\Scripts\\pip.exe' : $venvPath . '/bin/pip';
            $py = $isWindows ? $venvPath . '\\Scripts\\python.exe' : $venvPath . '/bin/python';
        } else {
            $pip = $isWindows ? 'pip' : 'pip3';
            $py = $python;
        }

        if ($upgrade) {
            $commands[] = sprintf('%s install --upgrade pip', escapeshellarg($pip));
        }
        $commands[] = sprintf('%s install pyarrow', escapeshellarg($pip));
        $commands[] = sprintf('%s -c %s', escapeshellarg($py), escapeshellarg('import pyarrow, pyarrow.parquet as pq; print(pyarrow.__version__)'));

        $this->info('ParqBridge backend setup plan:');
        $this->line('- Python executable: ' . $python);
        if ($useVenv) {
            $this->line('- Virtualenv path: ' . $venvPath);
        } else {
            $this->line('- Using global/site Python environment');
        }

        if ($dryRun) {
            $this->line('Commands to execute:');
            foreach ($commands as $c) { $this->line($c); }
            $this->line('Dry run only. No changes made.');
            return self::SUCCESS;
        }

        try {
            foreach ($commands as $cmd) {
                $this->runShell($cmd);
            }
        } catch (\Throwable $e) {
            $this->error('Setup failed: ' . $e->getMessage());
            return self::FAILURE;
        }

        if ($writeEnv && function_exists('base_path')) {
            $envPath = base_path('.env');
            $pyPathForEnv = $useVenv ? ($isWindows ? $venvPath . '\\Scripts\\python.exe' : $venvPath . '/bin/python') : $python;
            $lines = "\nPARQBRIDGE_PYTHON=\"{$pyPathForEnv}\"\nPARQBRIDGE_WRITER=pyarrow\n";
            @file_put_contents($envPath, $lines, FILE_APPEND);
            $this->info("Appended PARQBRIDGE_PYTHON and PARQBRIDGE_WRITER to .env");
        }

        $this->info('ParqBridge backend is ready.');
        return self::SUCCESS;
    }

    private function runShell(string $cmd): void
    {
        $descriptorSpec = [1 => ['pipe', 'w'], 2 => ['pipe', 'w']];
        $proc = proc_open($cmd, $descriptorSpec, $pipes, null, null, ['bypass_shell' => false]);
        if (!is_resource($proc)) {
            throw new \RuntimeException('Failed to start process');
        }
        $stdout = stream_get_contents($pipes[1]);
        $stderr = stream_get_contents($pipes[2]);
        foreach ($pipes as $p) { if (is_resource($p)) fclose($p); }
        $status = proc_close($proc);
        if ($status !== 0) {
            throw new \RuntimeException("Command failed (exit {$status}). STDERR: {$stderr} STDOUT: {$stdout}");
        }
    }
}
