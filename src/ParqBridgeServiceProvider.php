<?php

namespace ParqBridge;

use Illuminate\Support\ServiceProvider;

class ParqBridgeServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__.'/../config/parqbridge.php', 'parqbridge');
    }

    public function boot(): void
    {
        // Guard for minimal containers used in tests where runningInConsole isn't available
        if (method_exists($this->app, 'runningInConsole') && $this->app->runningInConsole()) {
            if (function_exists('config_path')) {
                $this->publishes([
                    __DIR__.'/../config/parqbridge.php' => config_path('parqbridge.php'),
                ], 'parqbridge-config');
            }

            $this->commands([
                \ParqBridge\Console\ExportTableCommand::class,
                \ParqBridge\Console\ListTablesCommand::class,
                \ParqBridge\Console\SetupCommand::class,
                \ParqBridge\Console\ExportAllTablesCommand::class,
            ]);
        }
    }
}
