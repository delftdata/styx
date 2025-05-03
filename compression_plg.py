import time
import zstandard as zstd
import lz4.frame
import gzip
from msgspec.msgpack import encode
import os

def generate_compressible_data(size_bytes: int) -> bytes:
    # Create repeating text pattern
    chunk = os.urandom(size_bytes)
    return chunk

data = encode(generate_compressible_data(1024 * 1024 * 1024)) # 1 GB of random compressible bytes

def benchmark(name, compress_func, decompress_func):
    print(f"\nBenchmarking {name}...")

    # Compression
    start = time.time()
    compressed = compress_func(data)
    compress_time = time.time() - start

    # Decompression
    start = time.time()
    decompressed = decompress_func(compressed)
    decompress_time = time.time() - start

    # Verify correctness
    assert decompressed == data, f"{name} decompressed data does not match original!"

    print(f"Compression time:   {compress_time:.4f} s")
    print(f"Decompression time: {decompress_time:.4f} s")
    print(f"Compressed size:    {len(compressed)} bytes")
    print(f"Compression ratio:  {len(compressed)/len(data):.2%}")

# Zstandard
zstd_cctx = zstd.ZstdCompressor(level=3)
zstd_dctx = zstd.ZstdDecompressor()
benchmark("Zstandard",
          compress_func=lambda d: zstd_cctx.compress(d),
          decompress_func=lambda d: zstd_dctx.decompress(d))

# LZ4
benchmark("LZ4",
          compress_func=lambda d: lz4.frame.compress(d),
          decompress_func=lambda d: lz4.frame.decompress(d))

# GZIP
benchmark("GZIP",
          compress_func=lambda d: gzip.compress(d),
          decompress_func=lambda d: gzip.decompress(d))
