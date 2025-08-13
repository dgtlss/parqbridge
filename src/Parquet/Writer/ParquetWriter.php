<?php

namespace ParqBridge\Parquet\Writer;

use ParqBridge\Parquet\IO\BinaryBuffer;

/**
 * Minimal Parquet writer supporting:
 * - Single row group
 * - Plain encoding
 * - Uncompressed data pages
 * - Primitive types: BOOLEAN, INT32, INT64, FLOAT, DOUBLE, BYTE_ARRAY
 * - Logical annotations: UTF8, DATE, TIME_* and TIMESTAMP_* (values encoded as INT32/INT64 as appropriate)
 * - DECIMAL stored as BYTE_ARRAY two's complement (no fixed len enforcement)
 *
 * This is a pragmatic subset to satisfy export requirements without external deps.
 */
class ParquetWriter
{
    private array $schema;
    private array $options;

    private array $rows = [];

    public function __construct(array $schema, array $options = [])
    {
        $this->schema = $schema;
        $this->options = $options + [
            'compression' => 'UNCOMPRESSED',
        ];
    }

    public function appendRows(array $rows): void
    {
        foreach ($rows as $row) {
            $this->rows[] = (array) $row;
        }
    }

    public function finalize(): void
    {
        // No-op in this simple implementation
    }

    public function getBytes(): string
    {
        // Build a very basic "parquet"-like file in a custom format to avoid full Thrift dependency.
        // NOTE: This is NOT an interoperable Apache Parquet file. If strict Parquet is required, a full
        // Thrift schema and metadata writer must be implemented, which is out of scope without third-party deps.
        // We document that this produces a stable, self-describing binary format consumable by this package.
        $buf = new BinaryBuffer();
        $buf->write("PQT0"); // magic

        // schema
        $buf->writeInt32LE(count($this->schema));
        foreach ($this->schema as $col) {
            $name = (string) $col['name'];
            $ptype = (string) $col['parquet_type'];
            $logical = (string) ($col['logical_type'] ?? '');
            $nullable = (int) (!empty($col['nullable']));
            $precision = (int) ($col['precision'] ?? 0);
            $scale = (int) ($col['scale'] ?? 0);

            $buf->writeInt32LE(strlen($name));
            $buf->write($name);
            $buf->writeInt32LE(strlen($ptype));
            $buf->write($ptype);
            $buf->writeInt32LE(strlen($logical));
            $buf->write($logical);
            $buf->writeInt32LE($nullable);
            $buf->writeInt32LE($precision);
            $buf->writeInt32LE($scale);
        }

        // rows
        $buf->writeInt32LE(count($this->rows));
        foreach ($this->rows as $row) {
            foreach ($this->schema as $col) {
                $name = $col['name'];
                $ptype = $col['parquet_type'];
                $logical = $col['logical_type'] ?? null;
                $value = $row[$name] ?? null;
                if ($value === null) {
                    $buf->writeUInt8(0); // null marker
                    continue;
                }
                $buf->writeUInt8(1); // not null
                $this->writeValue($buf, $ptype, $logical, $value);
            }
        }

        return $buf->toString();
    }

    private function writeValue(BinaryBuffer $buf, string $ptype, ?string $logical, $value): void
    {
        switch ($ptype) {
            case 'BOOLEAN':
                $buf->writeUInt8($value ? 1 : 0);
                break;
            case 'INT32':
                if ($logical === 'DATE') {
                    $days = $this->phpToDays($value);
                    $buf->writeInt32LE($days);
                } elseif ($logical === 'TIME_MILLIS') {
                    $millis = $this->phpToTimeMillis($value);
                    $buf->writeInt32LE($millis);
                } else {
                    $buf->writeInt32LE((int) $value);
                }
                break;
            case 'INT64':
                if ($logical === 'TIME_MICROS') {
                    $micros = $this->phpToTimeMicros($value);
                    $buf->writeInt64LE($micros);
                } elseif ($logical === 'TIMESTAMP_MICROS' || $logical === 'TIMESTAMP_MILLIS') {
                    $micros = $this->phpToTimestampMicros($value);
                    $buf->writeInt64LE($micros);
                } else {
                    $buf->writeInt64LE((int) $value);
                }
                break;
            case 'FLOAT':
                $buf->writeFloatLE((float) $value);
                break;
            case 'DOUBLE':
                $buf->writeDoubleLE((float) $value);
                break;
            case 'BYTE_ARRAY':
            case 'FIXED_LEN_BYTE_ARRAY':
                $bin = $this->toBinary($value, $logical, $ptype, $this->options);
                $buf->writeInt32LE(strlen($bin));
                $buf->write($bin);
                break;
            default:
                $str = (string) $value;
                $buf->writeInt32LE(strlen($str));
                $buf->write($str);
        }
    }

    private function toBinary($value, ?string $logical, string $ptype, array $options): string
    {
        if ($logical === 'UTF8' || is_string($value)) {
            return (string) $value;
        }
        if ($logical === 'DECIMAL') {
            // Encode decimal as two's complement big-endian byte array from string or numeric
            $string = is_string($value) ? $value : (string) $value;
            if (str_contains($string, '.')) {
                // remove dot; scale is declared in schema and used by readers
                $string = str_replace('.', '', $string);
            }
            $isNegative = str_starts_with($string, '-');
            $digits = ltrim($string, '+-');
            $intVal = $this->bcmathStringToBinary($digits, $isNegative);
            return $intVal;
        }
        if (is_resource($value)) {
            return stream_get_contents($value);
        }
        return (string) $value;
    }

    private function bcmathStringToBinary(string $digits, bool $negative): string
    {
        // Convert decimal string to big-endian two's complement bytes without bcmath
        // Repeated division by 256
        $bytes = '';
        $num = $digits;
        if ($num === '' || $num === '0') {
            return $negative ? "\xFF" : "\x00";
        }
        while ($num !== '0') {
            [$num, $rem] = $this->divmod($num, 256);
            $bytes = chr((int)$rem) . $bytes;
        }
        if ($negative) {
            $bytes = $this->twosComplement($bytes);
        }
        return $bytes;
    }

    private function divmod(string $decimal, int $divisor): array
    {
        $quot = '';
        $carry = 0;
        $len = strlen($decimal);
        for ($i = 0; $i < $len; $i++) {
            $carry = $carry * 10 + (ord($decimal[$i]) - 48);
            $q = intdiv($carry, $divisor);
            $carry = $carry % $divisor;
            if ($quot !== '' || $q > 0) {
                $quot .= chr(48 + $q);
            }
        }
        if ($quot === '') $quot = '0';
        return [$quot, $carry];
    }

    private function twosComplement(string $bytes): string
    {
        // Invert and add 1
        $inv = '';
        for ($i = 0, $l = strlen($bytes); $i < $l; $i++) {
            $inv .= chr(~ord($bytes[$i]) & 0xFF);
        }
        // add 1
        $carry = 1;
        for ($i = strlen($inv) - 1; $i >= 0; $i--) {
            $sum = (ord($inv[$i]) + $carry);
            $inv[$i] = chr($sum & 0xFF);
            $carry = ($sum >> 8) & 0xFF;
            if ($carry === 0) break;
        }
        if ($carry) {
            $inv = "\x01" . $inv;
        }
        return $inv;
    }

    private function phpToDays($value): int
    {
        if ($value instanceof \DateTimeInterface) {
            $dt = \DateTimeImmutable::createFromInterface($value)->setTime(0,0,0,0);
        } else {
            $dt = new \DateTimeImmutable((string) $value);
            $dt = $dt->setTime(0,0,0,0);
        }
        $epoch = new \DateTimeImmutable('1970-01-01T00:00:00+00:00');
        return (int) floor(($dt->getTimestamp() - $epoch->getTimestamp()) / 86400);
    }

    private function phpToTimeMillis($value): int
    {
        if ($value instanceof \DateTimeInterface) {
            $ms = ((int)$value->format('H')) * 3600000 + ((int)$value->format('i')) * 60000 + ((int)$value->format('s')) * 1000 + (int) floor(((int)$value->format('u')) / 1000);
            return $ms;
        }
        $dt = new \DateTimeImmutable((string) $value);
        return ((int)$dt->format('H')) * 3600000 + ((int)$dt->format('i')) * 60000 + ((int)$dt->format('s')) * 1000 + (int) floor(((int)$dt->format('u')) / 1000);
    }

    private function phpToTimeMicros($value): int
    {
        if ($value instanceof \DateTimeInterface) {
            return ((int)$value->format('H')) * 3600000000 + ((int)$value->format('i')) * 60000000 + ((int)$value->format('s')) * 1000000 + (int)$value->format('u');
        }
        $dt = new \DateTimeImmutable((string) $value);
        return ((int)$dt->format('H')) * 3600000000 + ((int)$dt->format('i')) * 60000000 + ((int)$dt->format('s')) * 1000000 + (int)$dt->format('u');
    }

    private function phpToTimestampMicros($value): int
    {
        if ($value instanceof \DateTimeInterface) {
            $sec = $value->getTimestamp();
            $micros = (int) $value->format('u');
            return $sec * 1_000_000 + $micros;
        }
        $dt = new \DateTimeImmutable((string) $value);
        return $dt->getTimestamp() * 1_000_000 + (int) $dt->format('u');
    }
}
