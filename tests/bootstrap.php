<?php

require __DIR__.'/../vendor/autoload.php';

// Minimal helpers for standalone testing without full Laravel app
if (!function_exists('app')) {
    function app($abstract = null, array $parameters = []) {
        $container = \Illuminate\Container\Container::getInstance();
        if ($abstract === null) {
            return $container;
        }
        return $container->make($abstract, $parameters);
    }
}

if (!function_exists('config')) {
    function config($key = null, $default = null) {
        $repo = \Illuminate\Container\Container::getInstance()->make('config');
        if ($key === null) {
            return $repo;
        }
        if (is_array($key)) {
            foreach ($key as $k => $v) {
                $repo->set($k, $v);
            }
            return $repo;
        }
        return $repo->get($key, $default);
    }
}

if (!function_exists('now')) {
    function now($tz = null) {
        return \Illuminate\Support\Carbon::now($tz);
    }
}

if (!function_exists('collect')) {
    function collect($value = null) {
        return \Illuminate\Support\Collection::make($value);
    }
}

// Minimal Laravel container bootstrapping for console + DB + Storage
use Illuminate\Container\Container;
use Illuminate\Events\Dispatcher;
use Illuminate\Filesystem\Filesystem;
use Illuminate\Filesystem\FilesystemManager;
use Illuminate\Support\Facades\App;
use Illuminate\Support\Facades\Config;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Storage;
use Illuminate\Database\Capsule\Manager as Capsule;
use Illuminate\Database\DatabaseManager;
use Illuminate\Console\Application as ConsoleApplication;
use ParqBridge\ParqBridgeServiceProvider;

// Minimal application container that mimics a few Laravel Application methods
if (!class_exists('TestApplication')) {
    class TestApplication extends Container {
        public function runningUnitTests(): bool { return true; }
        public function runningInConsole(): bool { return true; }
    }
}

$app = new TestApplication();
Container::setInstance($app);

$app->instance('app', $app);
$app->instance('path.config', __DIR__.'/tmp-config');
$app->singleton('events', function () { return new Dispatcher(); });

// Config repository
$app->singleton('config', function () {
    return new \Illuminate\Config\Repository([
        'app' => ['key' => 'base64:'.base64_encode(random_bytes(32))],
        'filesystems' => [
            'default' => 'local',
            'disks' => [
                'local' => [
                    'driver' => 'local',
                    'root' => __DIR__.'/storage',
                ],
            ],
        ],
        'database' => [
            'default' => 'sqlite',
            'connections' => [
                'sqlite' => [
                    'driver' => 'sqlite',
                    'database' => __DIR__.'/database.sqlite',
                    'prefix' => '',
                ],
            ],
        ],
        'parqbridge' => require __DIR__.'/../config/parqbridge.php',
    ]);
});

// Bind filesystem manager
$app->singleton('files', function () { return new Filesystem(); });
$app->singleton('filesystem', function ($app) { return new FilesystemManager($app); });
$app->alias('filesystem', FilesystemManager::class);

// Database (Capsule + DatabaseManager)
$capsule = new Capsule($app);
$capsule->addConnection($app['config']['database.connections.sqlite']);
$capsule->setAsGlobal();
$capsule->bootEloquent();
$app->instance('db.factory', new \Illuminate\Database\Connectors\ConnectionFactory($app));
$app->singleton('db', function ($app) {
    return new DatabaseManager($app, $app['db.factory']);
});

// Facades
App::setFacadeApplication($app);
Config::setFacadeApplication($app);
Storage::setFacadeApplication($app);
DB::setFacadeApplication($app);

// Ensure directories
@mkdir(__DIR__.'/storage', 0777, true);

// Create sqlite database file
if (!file_exists(__DIR__.'/database.sqlite')) {
    touch(__DIR__.'/database.sqlite');
}

// Register provider
$provider = new ParqBridgeServiceProvider($app);
$provider->register();
$provider->boot();

// In CI/tests, avoid external Python dependency by using a custom command
// that writes a minimal Parquet magic header. The tests only assert the header.
$app['config']->set('parqbridge.writer', 'custom');
$app['config']->set('parqbridge.custom_command', 'bash -lc "printf PAR1 > {output}"');

// Console kernel for running commands in tests
$app->singleton('artisan', function ($app) {
    return new ConsoleApplication($app, $app['events'], 'testing');
});
