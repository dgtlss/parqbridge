<?php

namespace ParqBridge\Parquet\Writer;

class ExternalParquetConverter
{
    /**
     * Convert a delimited/JSONL file to a true Apache Parquet file using an external backend (default: PyArrow via python3).
     *
     * @param string $csvPath Local filesystem path to input CSV with header.
     * @param string $parquetPath Local filesystem path to output Parquet file.
     * @param array $schema ParqBridge schema inference array.
     * @param string $compression Compression codec (UNCOMPRESSED|SNAPPY|GZIP|ZSTD|BROTLI|LZ4_RAW).
     * @throws \RuntimeException on failure.
     */
    public function convertCsvToParquet(string $csvPath, string $parquetPath, array $schema, string $compression = 'UNCOMPRESSED'): void
    {
        $backend = config('parqbridge.writer', 'pyarrow');
        if ($backend === 'pyarrow') {
            $this->convertWithPyArrow($csvPath, $parquetPath, $schema, $compression);
            return;
        }
        if ($backend === 'custom') {
            $template = (string) config('parqbridge.custom_command');
            if ($template === '') {
                throw new \RuntimeException('parqbridge.custom_command must be configured when writer=custom');
            }
            $cmd = str_replace(['{input}', '{output}'], [escapeshellarg($csvPath), escapeshellarg($parquetPath)], $template);
            $this->runShell($cmd);
            return;
        }
        throw new \RuntimeException("Unsupported writer backend: {$backend}");
    }

    private function convertWithPyArrow(string $csvPath, string $parquetPath, array $schema, string $compression): void
    {
        $python = (string) config('parqbridge.pyarrow_python', 'python3');
        $block = (int) config('parqbridge.pyarrow_block_size', 64 * 1024 * 1024);
        $scriptPath = tempnam(sys_get_temp_dir(), 'parq_py_');
        $schemaPath = tempnam(sys_get_temp_dir(), 'parq_schema_');
        if ($scriptPath === false || $schemaPath === false) {
            throw new \RuntimeException('Failed to allocate temporary files.');
        }
        $scriptPath .= '.py';
        $schemaJson = json_encode($this->buildArrowSchemaSpec($schema), JSON_UNESCAPED_SLASHES);
        if ($schemaJson === false) {
            throw new \RuntimeException('Failed to encode schema JSON');
        }
        file_put_contents($schemaPath, $schemaJson);

        $py = $this->buildPyArrowScript();
        file_put_contents($scriptPath, $py);

        $codec = strtoupper($compression);
        if ($codec === 'UNCOMPRESSED') {
            $codec = 'NONE';
        }

        $cmd = escapeshellarg($python) . ' ' . escapeshellarg($scriptPath) . ' ' . escapeshellarg($csvPath) . ' ' . escapeshellarg($parquetPath) . ' ' . escapeshellarg($schemaPath) . ' ' . escapeshellarg($codec) . ' ' . escapeshellarg((string)$block);
        $this->runShell($cmd);

        @unlink($scriptPath);
        @unlink($schemaPath);
    }

    private function runShell(string $cmd): void
    {
        $descriptorSpec = [1 => ['pipe', 'w'], 2 => ['pipe', 'w']];
        $proc = proc_open($cmd, $descriptorSpec, $pipes, null, null, ['bypass_shell' => false]);
        if (!\is_resource($proc)) {
            throw new \RuntimeException('Failed to start external process');
        }
        $stdout = stream_get_contents($pipes[1]);
        $stderr = stream_get_contents($pipes[2]);
        foreach ($pipes as $p) { if (is_resource($p)) fclose($p); }
        $status = proc_close($proc);
        if ($status !== 0) {
            throw new \RuntimeException("External converter failed (exit {$status}). STDERR: {$stderr} STDOUT: {$stdout}");
        }
    }

    private function buildArrowSchemaSpec(array $schema): array
    {
        $cols = [];
        foreach ($schema as $col) {
            $cols[] = [
                'name' => (string) $col['name'],
                'ptype' => (string) $col['parquet_type'],
                'logical' => isset($col['logical_type']) ? (string) $col['logical_type'] : null,
                'nullable' => (bool) ($col['nullable'] ?? true),
                'precision' => isset($col['precision']) ? (int) $col['precision'] : null,
                'scale' => isset($col['scale']) ? (int) $col['scale'] : null,
                // We base64-encode all byte arrays in TSV transport
                'binary_base64' => in_array($col['parquet_type'], ['BYTE_ARRAY','FIXED_LEN_BYTE_ARRAY'], true),
                'is_utf8' => (($col['parquet_type'] === 'BYTE_ARRAY') && (($col['logical_type'] ?? null) === 'UTF8')),
            ];
        }
        return ['columns' => $cols];
    }

    private function buildPyArrowScript(): string
    {
        return <<<'PY'
import sys, json, base64
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow.compute as pc
from datetime import datetime

def arrow_type(ptype, logical, precision, scale):
    if ptype == 'BOOLEAN':
        return pa.bool_()
    if ptype == 'INT32':
        if logical == 'DATE':
            return pa.date32()
        if logical == 'TIME_MILLIS':
            return pa.time32('ms')
        return pa.int32()
    if ptype == 'INT64':
        if logical == 'TIME_MICROS':
            return pa.time64('us')
        if logical == 'TIMESTAMP_MICROS':
            return pa.timestamp('us')
        if logical == 'TIMESTAMP_MILLIS':
            return pa.timestamp('ms')
        return pa.int64()
    if ptype == 'FLOAT':
        return pa.float32()
    if ptype == 'DOUBLE':
        return pa.float64()
    if ptype == 'FIXED_LEN_BYTE_ARRAY' and logical == 'DECIMAL':
        prec = precision or 18
        scl = scale or 0
        if prec <= 38:
            return pa.decimal128(prec, scl)
        return pa.decimal256(prec, scl)
    # BYTE_ARRAY
    if logical == 'UTF8':
        return pa.string()
    return pa.binary()

def main():
    csv_path, out_path, schema_json_path, compression, block_size = sys.argv[1:6]
    with open(schema_json_path, 'r') as f:
        schema_spec = json.load(f)
    column_types = {}
    binary_b64_cols = set()
    utf8_cols = set()
    ts_micros_cols = set()
    ts_millis_cols = set()
    for c in schema_spec['columns']:
        name = c['name']
        # Only treat TIMESTAMP_* as strings for post-parse; let DATE/TIME be typed directly
        logical = c['logical']
        if logical in ('TIMESTAMP_MICROS', 'TIMESTAMP_MILLIS'):
            column_types[name] = pa.string()
            if logical == 'TIMESTAMP_MICROS':
                ts_micros_cols.add(name)
            elif logical == 'TIMESTAMP_MILLIS':
                ts_millis_cols.add(name)
        else:
            t = arrow_type(c['ptype'], logical, c.get('precision'), c.get('scale'))
            column_types[name] = t
        if c.get('binary_base64'):
            binary_b64_cols.add(name)
        if c.get('is_utf8'):
            utf8_cols.add(name)
    convert_opts = pv.ConvertOptions(column_types=column_types, null_values=['', 'NULL', 'NaN'])
    # Detect JSONL vs TSV by peeking first non-empty char
    # Determine transport from schema hint or file content
    transport = 'jsonl'
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            ch = f.read(1)
            if ch and ch not in '{[':
                transport = 'tsv'
    except Exception:
        transport = 'tsv'

    if transport == 'jsonl':
        # JSON Lines ingestion
        rows = []
        with open(csv_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rows.append(json.loads(line))
        # Build columns in schema order for stable typing
        arrays = []
        names = []
        for c in schema_spec['columns']:
            name = c['name']
            logical = c['logical']
            is_utf8 = (c.get('is_utf8') is True)
            if logical in ('TIMESTAMP_MICROS','TIMESTAMP_MILLIS'):
                # parse after creating table
                col = pa.array([r.get(name) for r in rows], type=pa.string())
            elif is_utf8:
                col = pa.array([r.get(name) for r in rows], type=pa.string())
            elif c.get('binary_base64'):
                col = pa.array([r.get(name) for r in rows], type=pa.string())
            else:
                # Let Arrow infer via convert later if needed
                col = pa.array([r.get(name) for r in rows])
            arrays.append(col)
            names.append(name)
        table = pa.table(arrays, names=names)
    else:
        # Read as TSV to avoid conflicts with commas/quotes inside JSON/text fields
        table = pv.read_csv(
            csv_path,
            read_options=pv.ReadOptions(autogenerate_column_names=False, block_size=int(block_size)),
            # Keep standard quoting to remain compatible across PyArrow versions
            parse_options=pv.ParseOptions(delimiter='\t', newlines_in_values=True, quote_char='"', double_quote=True, escape_char='"'),
            convert_options=convert_opts
        )
    # Decode base64 binary columns
    cols = []
    names = []
    for field in table.schema:
        name = field.name
        col = table[name]
        if name in binary_b64_cols:
            pylist = col.to_pylist()
            if name in utf8_cols:
                decoded_text = []
                for x in pylist:
                    if x is None or x == '':
                        decoded_text.append(None)
                        continue
                    try:
                        decoded_text.append(base64.b64decode(x).decode('utf-8'))
                    except Exception:
                        decoded_text.append(x)
                col = pa.array(decoded_text, type=pa.string())
            else:
                decoded_bytes = []
                for x in pylist:
                    if x is None or x == '':
                        decoded_bytes.append(None)
                        continue
                    try:
                        decoded_bytes.append(base64.b64decode(x))
                    except Exception:
                        decoded_bytes.append(x.encode('utf-8'))
                col = pa.array(decoded_bytes, type=pa.binary())
        if name in ts_micros_cols:
            # Parse with Python for robustness (handles variable fraction length)
            pylist = col.to_pylist()
            out = []
            for s in pylist:
                if s is None or s == '':
                    out.append(None)
                    continue
                try:
                    dt = datetime.strptime(s, '%Y-%m-%d %H:%M:%S.%f')
                except Exception:
                    try:
                        dt = datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
                    except Exception:
                        raise
                out.append(dt)
            col = pa.array(out, type=pa.timestamp('us'))
        if name in ts_millis_cols:
            pylist = col.to_pylist()
            out = []
            for s in pylist:
                if s is None or s == '':
                    out.append(None)
                    continue
                try:
                    dt = datetime.strptime(s, '%Y-%m-%d %H:%M:%S.%f')
                except Exception:
                    try:
                        dt = datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
                    except Exception:
                        raise
                out.append(dt)
            col = pa.array(out, type=pa.timestamp('ms'))
        cols.append(col)
        names.append(name)
    table = pa.table(cols, names=names)
    if compression == 'NONE':
        compression = None
    pq.write_table(table, out_path, compression=compression)

if __name__ == '__main__':
    main()
PY;
    }
}
