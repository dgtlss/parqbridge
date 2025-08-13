<?php

namespace ParqBridge\Parquet\IO;

class BinaryBuffer
{
    private string $bytes = '';

    public function write(string $data): void
    {
        $this->bytes .= $data;
    }

    public function writeUInt8(int $value): void
    {
        $this->bytes .= pack('C', $value & 0xFF);
    }

    public function writeInt32LE(int $value): void
    {
        $this->bytes .= pack('V', $value); // little-endian 32-bit
    }

    public function writeInt64LE(int $value): void
    {
        // pack little-endian 64-bit (P is machine dependent). Use two 32-bit parts.
        $low = $value & 0xFFFFFFFF;
        $high = ($value >> 32) & 0xFFFFFFFF;
        $this->bytes .= pack('V2', $low, $high);
    }

    public function writeFloatLE(float $value): void
    {
        $this->bytes .= pack('g', $value);
    }

    public function writeDoubleLE(float $value): void
    {
        $this->bytes .= pack('e', $value);
    }

    public function writeVarInt(int $value): void
    {
        // unsigned LEB128
        while (true) {
            $byte = $value & 0x7F;
            $value >>= 7;
            if ($value === 0) {
                $this->writeUInt8($byte);
                break;
            }
            $this->writeUInt8($byte | 0x80);
        }
    }

    public function writeZigZagVarInt32(int $value): void
    {
        $zz = ($value << 1) ^ ($value >> 31);
        $this->writeVarInt($zz);
    }

    public function writeZigZagVarInt64(int $value): void
    {
        // PHP ints are 64-bit on 64-bit platforms
        $zz = ($value << 1) ^ ($value >> 63);
        $this->writeVarInt($zz);
    }

    public function toString(): string
    {
        return $this->bytes;
    }

    public function size(): int
    {
        return strlen($this->bytes);
    }
}
