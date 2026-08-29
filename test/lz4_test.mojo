"""Unit tests for lz4.mojo: block and frame round trips at several sizes,
known-vector cross-checks against CPython's `lz4` package (`lz4.block` /
`lz4.frame`, baked in as constants), corrupt-input error paths, and a
self-built Hadoop-framed legacy `LZ4` chunk stream."""

from std.testing import TestSuite, assert_equal, assert_true, assert_raises

from lz4 import (
    compress_block,
    compress_block_hc,
    decompress_block,
    decompress_block_into,
    compress_frame,
    decompress_frame,
    frame_content_size,
    is_lz4_frame,
    decompress_hadoop,
)


def _bytes_of(s: String) -> List[UInt8]:
    var out = List[UInt8]()
    var p = s.unsafe_ptr()
    for i in range(s.byte_length()):
        out.append(p[unsafe_offset=i])
    return out^


def _pattern(n: Int) -> List[UInt8]:
    """Highly compressible: a short phrase repeated to length `n`."""
    comptime PHRASE: StaticString = "lz4.mojo — Parquet LZ4_RAW and Iceberg Puffin LZ4F. "
    var phrase_bytes = _bytes_of(String(PHRASE))
    var out = List[UInt8](capacity=n)
    var i = 0
    while len(out) < n:
        out.append(phrase_bytes[i % len(phrase_bytes)])
        i += 1
    return out^


def _random_bytes(n: Int) -> List[UInt8]:
    """Deterministic xorshift32 fill — incompressible, unlike `_pattern`."""
    var state: UInt32 = 0x2545F491
    var out = List[UInt8](capacity=n)
    for _ in range(n):
        state ^= state << 13
        state ^= state >> 17
        state ^= state << 5
        out.append(UInt8(state & 0xFF))
    return out^


def _assert_bytes_equal(got: List[UInt8], expected: List[UInt8]) raises:
    assert_equal(len(got), len(expected))
    for i in range(len(expected)):
        if got[i] != expected[i]:
            raise Error(
                "byte mismatch at index "
                + String(i)
                + ": got "
                + String(got[i])
                + ", expected "
                + String(expected[i])
            )


# ===----------------------------------------------------------------------===#
# Round trips: 0 B, 1 B, 1 KiB, 1 MiB compressible, 1 MiB random.
# ===----------------------------------------------------------------------===#


def _roundtrip_sizes() -> List[Int]:
    return [0, 1, 1024, 1024 * 1024]


def test_block_roundtrip() raises:
    for n in _roundtrip_sizes():
        var src = _pattern(n)
        var comp = compress_block(src)
        var back = decompress_block(comp, n)
        _assert_bytes_equal(back, src)

    var random_src = _random_bytes(1024 * 1024)
    var comp = compress_block(random_src)
    var back = decompress_block(comp, len(random_src))
    _assert_bytes_equal(back, random_src)


def test_block_hc_roundtrip() raises:
    for n in _roundtrip_sizes():
        var src = _pattern(n)
        var comp = compress_block_hc(src)
        var back = decompress_block(comp, n)
        _assert_bytes_equal(back, src)


def test_block_decompress_into() raises:
    var src = _pattern(4096)
    var comp = compress_block(src)
    var dst = List[UInt8](capacity=len(src))
    dst.resize(len(src), 0)
    var written = decompress_block_into(comp, dst)
    assert_equal(written, len(src))
    _assert_bytes_equal(dst, src)


def test_frame_roundtrip() raises:
    for n in _roundtrip_sizes():
        var src = _pattern(n)
        var comp = compress_frame(src)
        assert_true(is_lz4_frame(comp))
        var back = decompress_frame(comp)
        _assert_bytes_equal(back, src)

    var random_src = _random_bytes(1024 * 1024)
    var comp = compress_frame(random_src)
    var back = decompress_frame(comp)
    _assert_bytes_equal(back, random_src)


def test_frame_roundtrip_no_content_size() raises:
    # Without a recorded content size, decompress_frame must fall back to
    # streaming growth instead of trusting frame_content_size.
    var src = _pattern(1024 * 1024)
    var comp = compress_frame(src, content_size=False)
    assert_true(frame_content_size(comp) is None)
    var back = decompress_frame(comp)
    _assert_bytes_equal(back, src)


def test_frame_content_size() raises:
    var src = _pattern(12345)
    var comp = compress_frame(src, content_size=True)
    var size = frame_content_size(comp)
    assert_true(size is not None)
    assert_equal(size.value(), 12345)


def test_is_lz4_frame() raises:
    var src = _pattern(64)
    var block = compress_block(src)
    var frame = compress_frame(src)
    assert_true(not is_lz4_frame(block))
    assert_true(is_lz4_frame(frame))
    assert_true(not is_lz4_frame(List[UInt8]()))


# ===----------------------------------------------------------------------===#
# Known vectors, produced by CPython `lz4` 4.4.5:
#   lz4.block.compress(msg, store_size=False)
#   lz4.frame.compress(msg, content_checksum=True, store_size=True)
# for msg = b"The quick brown fox jumps over the lazy dog. LZ4 is fast!"
# (57 bytes). Baked in so the test has no Python dependency at run time.
# ===----------------------------------------------------------------------===#


def _known_message() -> String:
    return "The quick brown fox jumps over the lazy dog. LZ4 is fast!"


def _known_block_bytes() -> List[UInt8]:
    return [
        240, 42, 84, 104, 101, 32, 113, 117, 105, 99, 107, 32, 98, 114, 111,
        119, 110, 32, 102, 111, 120, 32, 106, 117, 109, 112, 115, 32, 111,
        118, 101, 114, 32, 116, 104, 101, 32, 108, 97, 122, 121, 32, 100,
        111, 103, 46, 32, 76, 90, 52, 32, 105, 115, 32, 102, 97, 115, 116, 33,
    ]


def _known_frame_bytes() -> List[UInt8]:
    return [
        4, 34, 77, 24, 108, 64, 57, 0, 0, 0, 0, 0, 0, 0, 103, 57, 0, 0, 128,
        84, 104, 101, 32, 113, 117, 105, 99, 107, 32, 98, 114, 111, 119, 110,
        32, 102, 111, 120, 32, 106, 117, 109, 112, 115, 32, 111, 118, 101,
        114, 32, 116, 104, 101, 32, 108, 97, 122, 121, 32, 100, 111, 103, 46,
        32, 76, 90, 52, 32, 105, 115, 32, 102, 97, 115, 116, 33, 0, 0, 0, 0,
        162, 202, 207, 200,
    ]


def test_decompress_known_block() raises:
    var expected = _bytes_of(_known_message())
    var back = decompress_block(_known_block_bytes(), len(expected))
    _assert_bytes_equal(back, expected)


def test_decompress_known_frame() raises:
    var expected = _bytes_of(_known_message())
    var frame = _known_frame_bytes()
    assert_true(is_lz4_frame(frame))
    var size = frame_content_size(frame)
    assert_true(size is not None)
    assert_equal(size.value(), len(expected))
    var back = decompress_frame(frame)
    _assert_bytes_equal(back, expected)


# ===----------------------------------------------------------------------===#
# Corrupt input.
# ===----------------------------------------------------------------------===#


def test_corrupt_block_raises() raises:
    var garbage: List[UInt8] = [0xFF, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05]
    with assert_raises():
        _ = decompress_block(garbage, 4096)


def test_corrupt_frame_raises() raises:
    var garbage: List[UInt8] = [0x04, 0x22, 0x4D, 0x18, 0xFF, 0xFF, 0xFF, 0xFF]
    with assert_raises():
        _ = decompress_frame(garbage)


def test_frame_content_size_rejects_garbage() raises:
    var garbage: List[UInt8] = [1, 2, 3, 4, 5, 6, 7, 8]
    with assert_raises():
        _ = frame_content_size(garbage)


# ===----------------------------------------------------------------------===#
# Legacy Hadoop-framed Parquet `LZ4`: self-built, since real historical
# fixtures aren't in-repo. Confirms decompress_hadoop's chunk-header parsing
# against ordinary LZ4_RAW blocks produced by compress_block.
# ===----------------------------------------------------------------------===#


def _build_hadoop_stream(chunks: List[List[UInt8]]) raises -> List[UInt8]:
    var out = List[UInt8]()
    for ref chunk in chunks:
        var comp = compress_block(chunk)
        var uncompressed_len = UInt32(len(chunk))
        var compressed_len = UInt32(len(comp))
        for shift in [24, 16, 8, 0]:
            out.append(UInt8((uncompressed_len >> UInt32(shift)) & 0xFF))
        for shift in [24, 16, 8, 0]:
            out.append(UInt8((compressed_len >> UInt32(shift)) & 0xFF))
        out.extend(comp^)
    return out^


def test_decompress_hadoop() raises:
    var chunk_a = _pattern(300)
    var chunk_b = _pattern(700)
    var stream = _build_hadoop_stream([chunk_a.copy(), chunk_b.copy()])

    var expected = List[UInt8]()
    expected.extend(chunk_a^)
    expected.extend(chunk_b^)

    var back = decompress_hadoop(stream, len(expected))
    _assert_bytes_equal(back, expected)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
