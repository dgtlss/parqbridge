<?php

namespace ParqBridge\Tests;

use PHPUnit\Framework\TestCase;
use Illuminate\Support\Facades\DB;
use ParqBridge\Schema\SchemaInferrer;

class SchemaInferenceTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        DB::statement('DROP TABLE IF EXISTS data_types');
        DB::statement('CREATE TABLE data_types (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            i64 INTEGER,
            txt TEXT,
            real_num REAL,
            blob_col BLOB
        )');
    }

    public function test_sqlite_inference_covers_types(): void
    {
        $schema = SchemaInferrer::inferForTable('data_types');
        $by = [];
        foreach ($schema as $c) { $by[$c['name']] = $c; }

        $this->assertSame('INT64', $by['i64']['parquet_type']);
        $this->assertSame('BYTE_ARRAY', $by['txt']['parquet_type']);
        $this->assertSame('DOUBLE', $by['real_num']['parquet_type']);
        $this->assertSame('BYTE_ARRAY', $by['blob_col']['parquet_type']);
    }
}
