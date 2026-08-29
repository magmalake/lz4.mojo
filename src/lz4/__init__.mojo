"""`lz4` — Mojo bindings to liblz4 (block format + frame format) via a thin C
shim (liblz4mojo.{dylib,so}), loaded through an `OwnedDLHandle`.

Two independent codecs live in one liblz4:

- **Block format** (`LZ4_RAW` in Parquet): a bare compressed blob with no
  size prefix — the uncompressed length must be supplied out of band (that's
  exactly what a Parquet page header carries). Legacy Parquet's `LZ4`
  compression carries the same blocks but wrapped in a Hadoop framing of
  `[BE u32 uncompressed_len][BE u32 compressed_len]` pairs.
- **Frame format** (`LZ4F`): a self-describing container with a magic
  number, optional content-size field, and (optionally) checksums. This is
  the *only* codec Apache Iceberg allows for Puffin file footers, which is
  why `compress_frame` always sets the content-size flag by default.

Mirrors zlib.mojo's FFI pattern: a single-call C wrapper
(shim/lz4_wrapper.c, built to $CONDA_PREFIX/lib/liblz4mojo.{dylib,so} by the
lz4-shim pixi package) so Mojo never reads back internal library state after
a foreign call. The `OwnedDLHandle` is passed as a borrowed (default-
convention) parameter to each worker function, so Mojo's ASAP destruction
can't `dlclose` the library before the C call inside that worker runs.
"""

from std.os import getenv
from std.ffi import OwnedDLHandle, c_int, c_long_long

comptime _MAGIC_LE: SIMD[DType.uint8, 4] = [0x04, 0x22, 0x4D, 0x18]


def _find_lib() raises -> OwnedDLHandle:
    """Open liblz4mojo from `$CONDA_PREFIX/lib` (installed by the lz4-shim
    pixi package), else `build/` for a bare checkout. CMake names the shim
    with the platform's natural shared-library extension, so this tries
    `.dylib` then `.so`."""
    var prefix = getenv("CONDA_PREFIX", "")
    var base = String("")
    if prefix == "":
        base += "build/liblz4mojo"
    else:
        base += prefix
        base += "/lib/liblz4mojo"

    try:
        return OwnedDLHandle(base + ".dylib")
    except:
        pass
    try:
        return OwnedDLHandle(base + ".so")
    except:
        pass
    raise Error(
        "lz4.mojo: could not load liblz4mojo (.dylib/.so) from " + base
    )


def _ptr_of(data: Span[UInt8, _]) -> Int:
    """Address of `data`'s backing storage, or 0 for an empty span (the C
    side never dereferences the pointer when the length is 0)."""
    if len(data) == 0:
        return 0
    return Int(data.unsafe_ptr())


# ===----------------------------------------------------------------------===#
# Block format (LZ4_RAW) — no size carried in the stream.
# ===----------------------------------------------------------------------===#


def _do_compress_block(
    lib: OwnedDLHandle, data: Span[UInt8, _], acceleration: Int
) raises -> List[UInt8]:
    var bound_fn = lib.get_function[c_int]("lz4m_compress_bound")
    var compress_fn = lib.get_function[c_int]("lz4m_compress_block")

    var in_len = len(data)
    var cap = Int(bound_fn(c_int(in_len)))
    if cap <= 0:
        cap = 16

    var out = List[UInt8](capacity=cap)
    out.resize(cap, 0)
    var written = Int(
        compress_fn(
            _ptr_of(data),
            c_int(in_len),
            Int(out.unsafe_ptr()),
            c_int(cap),
            c_int(acceleration),
        )
    )
    if written < 0:
        raise Error(
            "lz4.compress_block failed (rc=" + String(written) + ")"
        )
    out.resize(written, 0)
    return out^


def compress_block(
    data: Span[UInt8, _], acceleration: Int = 1
) raises -> List[UInt8]:
    """Compress to a raw LZ4 block (`LZ4_RAW`) — no size prefix. The caller
    (e.g. a Parquet page writer) must record the uncompressed length out of
    band; `decompress_block` needs it back to decode.

    Args:
        data: Bytes to compress.
        acceleration: >=1; higher trades compression ratio for speed
            (1 matches `LZ4_compress_default`).
    """
    var lib = _find_lib()
    return _do_compress_block(lib, data, acceleration)


def _do_compress_block_hc(
    lib: OwnedDLHandle, data: Span[UInt8, _], level: Int
) raises -> List[UInt8]:
    var bound_fn = lib.get_function[c_int]("lz4m_compress_bound")
    var compress_fn = lib.get_function[c_int]("lz4m_compress_block_hc")

    var in_len = len(data)
    var cap = Int(bound_fn(c_int(in_len)))
    if cap <= 0:
        cap = 16

    var out = List[UInt8](capacity=cap)
    out.resize(cap, 0)
    var written = Int(
        compress_fn(
            _ptr_of(data), c_int(in_len), Int(out.unsafe_ptr()), c_int(cap),
            c_int(level),
        )
    )
    if written < 0:
        raise Error(
            "lz4.compress_block_hc failed (rc=" + String(written) + ")"
        )
    out.resize(written, 0)
    return out^


def compress_block_hc(
    data: Span[UInt8, _], level: Int = 9
) raises -> List[UInt8]:
    """Compress to a raw LZ4 block using the high-compression (HC) encoder.
    Same wire format as `compress_block` — slower, smaller output.

    Args:
        data: Bytes to compress.
        level: 1..12 (LZ4HC_CLEVEL_MIN..MAX); higher is slower and smaller.
    """
    var lib = _find_lib()
    return _do_compress_block_hc(lib, data, level)


def _do_decompress_block_into(
    lib: OwnedDLHandle, data: Span[UInt8, _], dst: Span[mut=True, UInt8, _]
) raises -> Int:
    var decompress_fn = lib.get_function[c_int]("lz4m_decompress_block")

    var out_ptr = Int(0)
    if len(dst) > 0:
        out_ptr = Int(dst.unsafe_ptr())
    var written = Int(
        decompress_fn(
            _ptr_of(data), c_int(len(data)), out_ptr, c_int(len(dst))
        )
    )
    if written < 0:
        raise Error("lz4.decompress_block failed: corrupt input or " \
            "destination too small (rc=" + String(written) + ")")
    return written


def decompress_block_into(
    data: Span[UInt8, _], dst: Span[mut=True, UInt8, _]
) raises -> Int:
    """Decompress a raw LZ4 block into a caller-supplied buffer. `dst` must
    be at least as large as the true uncompressed size — LZ4 blocks carry no
    size of their own, so this can't grow the buffer for you.

    Returns the number of bytes written.
    """
    var lib = _find_lib()
    return _do_decompress_block_into(lib, data, dst)


def decompress_block(
    data: Span[UInt8, _], uncompressed_size: Int
) raises -> List[UInt8]:
    """Decompress a raw LZ4 block. `uncompressed_size` must be exact —
    Parquet page headers carry it, since LZ4 blocks carry no size prefix.
    """
    var out = List[UInt8](capacity=uncompressed_size)
    out.resize(uncompressed_size, 0)
    var written = decompress_block_into(data, Span(out))
    if written != uncompressed_size:
        out.resize(written, 0)
    return out^


# ===----------------------------------------------------------------------===#
# Frame format (LZ4F) — the only codec Apache Iceberg allows in Puffin
# footers. Self-describing: magic number, optional content size, optional
# checksums.
# ===----------------------------------------------------------------------===#


def _do_compress_frame(
    lib: OwnedDLHandle, data: Span[UInt8, _], level: Int, content_size: Bool
) raises -> List[UInt8]:
    var bound_fn = lib.get_function[c_int]("lz4m_compress_frame_bound")
    var compress_fn = lib.get_function[c_int]("lz4m_compress_frame")

    var in_len = len(data)
    var cap = Int(bound_fn(c_int(in_len)))
    if cap <= 0:
        cap = 64

    var out = List[UInt8](capacity=cap)
    out.resize(cap, 0)
    var written = Int(
        compress_fn(
            _ptr_of(data),
            c_int(in_len),
            Int(out.unsafe_ptr()),
            c_int(cap),
            c_int(level),
            c_int(1) if content_size else c_int(0),
        )
    )
    if written < 0:
        raise Error(
            "lz4.compress_frame failed (rc=" + String(written) + ")"
        )
    out.resize(written, 0)
    return out^


def compress_frame(
    data: Span[UInt8, _], level: Int = 0, content_size: Bool = True
) raises -> List[UInt8]:
    """Compress to a single LZ4 frame (`LZ4F`) — the format Apache Iceberg
    requires for Puffin file footers. `content_size=True` (the default,
    and the Puffin requirement) writes the uncompressed length into the
    frame header so a reader can size its output buffer up front.

    Args:
        data: Bytes to compress.
        level: 0 = fast mode (LZ4 default); >=1 selects the HC encoder
            (1..12, higher is slower and smaller).
        content_size: Whether to record the uncompressed length in the
            frame header.
    """
    var lib = _find_lib()
    return _do_compress_frame(lib, data, level, content_size)


def _do_frame_content_size(
    lib: OwnedDLHandle, data: Span[UInt8, _]
) raises -> Int:
    var size_fn = lib.get_function[c_long_long]("lz4m_frame_content_size")
    return Int(size_fn(_ptr_of(data), c_int(len(data))))


def frame_content_size(data: Span[UInt8, _]) raises -> Optional[Int]:
    """Read the uncompressed content size from an LZ4 frame header without
    decompressing, if the frame carries one.

    Returns `None` if the frame doesn't carry a content size (LZ4F itself
    can't distinguish "unspecified" from a genuine zero-length payload —
    both are stored as 0), or raises if `data` isn't a valid frame header.
    """
    var lib = _find_lib()
    var size = _do_frame_content_size(lib, data)
    if size < 0:
        raise Error("lz4.frame_content_size: not a valid LZ4 frame header")
    if size == 0:
        return None
    return size


def is_lz4_frame(data: Span[UInt8, _]) -> Bool:
    """Whether `data` starts with the LZ4 frame magic number (`0x184D2204`,
    little-endian) — the standard way to tell an LZ4F frame apart from a
    bare LZ4 block, which has no magic at all."""
    if len(data) < 4:
        return False
    for i in range(4):
        if data[i] != _MAGIC_LE[i]:
            return False
    return True


def _do_decompress_frame(
    lib: OwnedDLHandle, data: Span[UInt8, _], cap: Int
) raises -> Optional[List[UInt8]]:
    """Attempt one decompression pass at capacity `cap`. Returns `None` if
    `cap` was too small (caller should grow and retry), the decoded bytes
    otherwise, or raises on genuinely corrupt/truncated input."""
    var decompress_fn = lib.get_function[c_int]("lz4m_decompress_frame")

    var out = List[UInt8](capacity=cap)
    out.resize(cap, 0)
    var out_ptr = Int(0)
    if cap > 0:
        out_ptr = Int(out.unsafe_ptr())
    var written = Int(
        decompress_fn(_ptr_of(data), c_int(len(data)), out_ptr, c_int(cap))
    )
    if written == -2:
        return None
    if written < 0:
        raise Error("lz4.decompress_frame failed: corrupt or truncated input")
    out.resize(written, 0)
    return out^


def decompress_frame(data: Span[UInt8, _]) raises -> List[UInt8]:
    """Decompress a single LZ4 frame. Uses the frame header's content-size
    field to size the output buffer exactly when present (the case for
    every frame `compress_frame` produces, and the Puffin convention);
    otherwise grows the buffer and retries until the whole frame fits."""
    var lib = _find_lib()

    var known_size = _do_frame_content_size(lib, data)
    var cap: Int
    if known_size > 0:
        cap = known_size
    else:
        cap = len(data) * 4
        if cap < 4096:
            cap = 4096

    while True:
        var result = _do_decompress_frame(lib, data, cap)
        if result:
            return result.take()
        cap *= 2


# ===----------------------------------------------------------------------===#
# Legacy Hadoop-framed Parquet `LZ4` (not `LZ4_RAW`): a sequence of
# [BE u32 uncompressed_len][BE u32 compressed_len][block…] chunks, each
# block itself an ordinary LZ4_RAW block. Superseded by `LZ4_RAW`; kept
# here only to read old files.
# ===----------------------------------------------------------------------===#


def _read_be_u32(data: Span[UInt8, _], offset: Int) raises -> Int:
    if offset + 4 > len(data):
        raise Error("lz4.decompress_hadoop: truncated chunk header")
    return (
        (Int(data[offset]) << 24)
        | (Int(data[offset + 1]) << 16)
        | (Int(data[offset + 2]) << 8)
        | Int(data[offset + 3])
    )


def decompress_hadoop(
    data: Span[UInt8, _], uncompressed_size: Int
) raises -> List[UInt8]:
    """Decompress legacy Parquet `LZ4` (Hadoop-framed): a sequence of
    `[BE u32 uncompressed_len][BE u32 compressed_len][block…]` chunks, each
    block an ordinary `LZ4_RAW` block. This is the format `parquet-mr`
    historically wrote for codec `LZ4`; new writers should use `LZ4_RAW`
    (`compress_block`/`decompress_block`) instead — see PARQUET-1974.
    """
    var lib = _find_lib()
    var out = List[UInt8](capacity=uncompressed_size)
    var offset = 0

    while len(out) < uncompressed_size:
        var chunk_uncompressed_len = _read_be_u32(data, offset)
        var chunk_compressed_len = _read_be_u32(data, offset + 4)
        offset += 8
        if offset + chunk_compressed_len > len(data):
            raise Error("lz4.decompress_hadoop: truncated block")

        var chunk = List[UInt8](capacity=chunk_uncompressed_len)
        chunk.resize(chunk_uncompressed_len, 0)
        var block_span = data[offset : offset + chunk_compressed_len]
        var written = _do_decompress_block_into(lib, block_span, Span(chunk))
        if written != chunk_uncompressed_len:
            raise Error(
                "lz4.decompress_hadoop: block size mismatch (got "
                + String(written)
                + ", expected "
                + String(chunk_uncompressed_len)
                + ")"
            )
        out.extend(chunk^)
        offset += chunk_compressed_len

    if len(out) != uncompressed_size:
        raise Error(
            "lz4.decompress_hadoop: total size mismatch (got "
            + String(len(out))
            + ", expected "
            + String(uncompressed_size)
            + ")"
        )
    return out^
