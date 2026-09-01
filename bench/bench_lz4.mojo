"""Block and frame compress/decompress throughput over 64 MiB, through the
shared harness (magmalake/bench.mojo).

    pixi run -e bench bench
    pixi run -e bench bench -- --json
    pixi run -e bench bench -- --only bench_decompress_block

Each body rebuilds its input and, for the decompress benchmarks, recompresses
it. The harness re-enters a body once per phase and times only what is inside
`b.iter`, so that is wall-clock cost and never enters the numbers.
"""

from bench import Benchmark, BenchSuite, Metric, keep

from lz4 import (
    compress_block,
    compress_frame,
    decompress_block,
    decompress_frame,
)

comptime SIZE = 64 * 1024 * 1024
comptime PHRASE: StaticString = (
    "lz4.mojo — Parquet LZ4_RAW and Iceberg Puffin LZ4F. "
)


def _pattern(n: Int) -> List[UInt8]:
    var phrase = String(PHRASE).as_bytes()
    var out = List[UInt8](capacity=n)
    var i = 0
    while len(out) < n:
        out.append(phrase[i % len(phrase)])
        i += 1
    return out^


def bench_compress_block(mut b: Benchmark) raises:
    var src = _pattern(SIZE)
    b.throughput(Metric.bytes(), SIZE)

    @parameter
    def call() raises:
        var block = compress_block(src)
        keep(block)

    b.iter[call]()
    keep(src)


def bench_decompress_block(mut b: Benchmark) raises:
    var src = _pattern(SIZE)
    var block = compress_block(src)
    # Rate is against the uncompressed size: payload bytes recovered/second.
    b.throughput(Metric.bytes(), SIZE)

    @parameter
    def call() raises:
        var back = decompress_block(block, SIZE)
        keep(back)

    b.iter[call]()
    keep(src)
    keep(block)


def bench_compress_frame(mut b: Benchmark) raises:
    var src = _pattern(SIZE)
    b.throughput(Metric.bytes(), SIZE)

    @parameter
    def call() raises:
        var frame = compress_frame(src)
        keep(frame)

    b.iter[call]()
    keep(src)


def bench_decompress_frame(mut b: Benchmark) raises:
    var src = _pattern(SIZE)
    var frame = compress_frame(src)
    b.throughput(Metric.bytes(), SIZE)

    @parameter
    def call() raises:
        var back = decompress_frame(frame)
        keep(back)

    b.iter[call]()
    keep(src)
    keep(frame)


def _print_shape() raises:
    """Compressed sizes and the round-trip checks, once. The old bench
    asserted round-trip length inside its timing region; that does not
    belong there."""
    var src = _pattern(SIZE)
    var block = compress_block(src)
    var frame = compress_frame(src)
    if len(decompress_block(block, SIZE)) != SIZE:
        raise Error("block round trip size mismatch")
    if len(decompress_frame(frame)) != SIZE:
        raise Error("frame round trip size mismatch")
    print(
        "input", SIZE // (1024 * 1024), "MiB compressible |",
        "block ->", len(block) // 1024, "KiB",
        "(", Float64(SIZE) / Float64(len(block)), "x ) |",
        "frame ->", len(frame) // 1024, "KiB",
        "(", Float64(SIZE) / Float64(len(frame)), "x )",
    )


def main() raises:
    _print_shape()
    BenchSuite.run[__functions_in_module()]()
