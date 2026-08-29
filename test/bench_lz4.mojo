"""Throughput bench: block and frame compress/decompress on 64 MiB of
compressible data. `pixi run bench`."""

from std.time import perf_counter_ns

from lz4 import compress_block, decompress_block, compress_frame, decompress_frame


def _pattern(n: Int) -> List[UInt8]:
    comptime PHRASE: StaticString = "lz4.mojo — Parquet LZ4_RAW and Iceberg Puffin LZ4F. "
    var phrase_bytes = List[UInt8]()
    var p = String(PHRASE).unsafe_ptr()
    for i in range(String(PHRASE).byte_length()):
        phrase_bytes.append(p[unsafe_offset=i])
    var out = List[UInt8](capacity=n)
    var i = 0
    while len(out) < n:
        out.append(phrase_bytes[i % len(phrase_bytes)])
        i += 1
    return out^


def _mb_per_s(bytes_len: Int, nanos: Int) -> Float64:
    var seconds = Float64(nanos) / 1_000_000_000.0
    var mb = Float64(bytes_len) / (1024.0 * 1024.0)
    return mb / seconds


def main() raises:
    comptime SIZE = 64 * 1024 * 1024
    var src = _pattern(SIZE)
    print("lz4.mojo bench: ", SIZE // (1024 * 1024), " MiB compressible input", sep="")

    var t0 = perf_counter_ns()
    var block = compress_block(src)
    var t1 = perf_counter_ns()
    print(
        "compress_block:   ", _mb_per_s(SIZE, t1 - t0), " MB/s (-> ",
        len(block), " bytes)", sep="",
    )

    t0 = perf_counter_ns()
    var back = decompress_block(block, SIZE)
    t1 = perf_counter_ns()
    print("decompress_block: ", _mb_per_s(SIZE, t1 - t0), " MB/s", sep="")
    if len(back) != SIZE:
        raise Error("bench: block round trip size mismatch")

    t0 = perf_counter_ns()
    var frame = compress_frame(src)
    t1 = perf_counter_ns()
    print(
        "compress_frame:   ", _mb_per_s(SIZE, t1 - t0), " MB/s (-> ",
        len(frame), " bytes)", sep="",
    )

    t0 = perf_counter_ns()
    var back_frame = decompress_frame(frame)
    t1 = perf_counter_ns()
    print("decompress_frame: ", _mb_per_s(SIZE, t1 - t0), " MB/s", sep="")
    if len(back_frame) != SIZE:
        raise Error("bench: frame round trip size mismatch")
