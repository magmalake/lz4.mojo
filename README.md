# lz4.mojo

[![mojoshelf](https://mojoshelf.org/badge/lz4-mojo.svg)](https://mojoshelf.org/tins/lz4-mojo) [![mojo nightly](https://mojoshelf.org/badge/lz4-mojo/nightly.svg)](https://mojoshelf.org/tins/lz4-mojo)

A Mojo binding to **liblz4** — block format and frame format, compress and
decompress. A small C shim (`shim/lz4_wrapper.c`) is compiled to
**`liblz4mojo.{dylib,so}`** and loaded through an `OwnedDLHandle`. No link
flags for consumers; the shim is `dlopen`ed at runtime.

Part of [**magmalake**](https://magmalake.org) — data lake building blocks in Mojo.

## Why lz4.mojo

LZ4 shows up twice in the data lake formats magmalake targets, as two
*different* wire formats from the same C library:

- **Parquet page compression.** `LZ4_RAW` is a bare compressed block with no
  size prefix — the page header carries the uncompressed length instead.
  Legacy Parquet's `LZ4` compression (superseded by `LZ4_RAW`, see
  [PARQUET-1974](https://issues.apache.org/jira/browse/PARQUET-1974)) wraps
  the same blocks in Hadoop framing:
  `[BE u32 uncompressed_len][BE u32 compressed_len][block…]` repeated.
- **Apache Iceberg Puffin footers.** The
  [Puffin spec](https://iceberg.apache.org/puffin-spec/) allows exactly one
  compression codec for a blob: `lz4`, meaning the self-describing **LZ4
  frame format** (`LZ4F`) — magic number, and, per this binding's default, a
  content-size field so a reader can size its output buffer before
  decompressing.

One library, three consumers: `compress_block`/`decompress_block` for
Parquet pages, `compress_frame`/`decompress_frame` for Puffin footers, and
`decompress_hadoop` to read old Parquet files still using the legacy framing.

## Prerequisites

- [pixi](https://pixi.sh) — manages the Mojo toolchain and the conda-forge
  `lz4-c` dependency.
- macOS (`osx-arm64`) or Linux (`linux-64`, `linux-aarch64`).

Everything else — the Mojo compiler, liblz4, and the CMake shim build — is
resolved and built by `pixi install`.

## Use

```mojo
from lz4 import compress_block, decompress_block, compress_frame, decompress_frame

# Parquet LZ4_RAW page: no size prefix, caller tracks the uncompressed length.
var block = compress_block(page_bytes)
var page = decompress_block(block, uncompressed_size)

# Iceberg Puffin blob: self-describing frame, content size on by default.
var frame = compress_frame(blob_bytes)
var blob = decompress_frame(frame)
```

Build the shim once, then build a consumer with this package on the import
path:

```sh
pixi install                                            # builds liblz4mojo.{dylib,so} -> $CONDA_PREFIX/lib
mojo build your.mojo -I ../lz4.mojo/src -o your-bin      # no link flags needed
```

`_find_lib()` resolves the shim at `$CONDA_PREFIX/lib/liblz4mojo.dylib` (or
`.so`; CMake picks the platform's natural extension), falling back to
`build/liblz4mojo.{dylib,so}` for a bare checkout outside pixi.

## API

Block format (`LZ4_RAW` — no size carried in the stream):

```mojo
def compress_block(data: Span[UInt8], acceleration: Int = 1) raises -> List[UInt8]
def compress_block_hc(data: Span[UInt8], level: Int = 9) raises -> List[UInt8]
def decompress_block(data: Span[UInt8], uncompressed_size: Int) raises -> List[UInt8]
def decompress_block_into(data: Span[UInt8], dst: Span[UInt8]) raises -> Int
```

Frame format (`LZ4F` — self-describing, the Puffin codec):

```mojo
def compress_frame(data: Span[UInt8], level: Int = 0, content_size: Bool = True) raises -> List[UInt8]
def decompress_frame(data: Span[UInt8]) raises -> List[UInt8]
def frame_content_size(data: Span[UInt8]) raises -> Optional[Int]
def is_lz4_frame(data: Span[UInt8]) -> Bool
```

Legacy Hadoop-framed Parquet `LZ4` (read-only; new writers use `LZ4_RAW`):

```mojo
def decompress_hadoop(data: Span[UInt8], uncompressed_size: Int) raises -> List[UInt8]
```

All functions raise with a descriptive message on corrupt or truncated
input.

## Test

```sh
pixi run test     # nightly (default env)
pixi run -e stable test
```

Covers block and frame round trips at 0 B, 1 B, 1 KiB, 1 MiB (compressible)
and 1 MiB (random); known-vector cross-checks against bytes produced by
CPython's `lz4` package (`lz4.block.compress(..., store_size=False)` and
`lz4.frame.compress(..., content_checksum=True, store_size=True)`, baked in
as constants — no Python dependency at test run time); corrupt-input error
paths; and a self-built Hadoop-framed chunk stream. Frames produced by
`compress_frame` were independently confirmed to round-trip through
CPython's `lz4.frame.decompress`.

## Perf

```sh
pixi run bench    # MB/s on 64 MiB
```

Measured on an Apple Silicon (osx-arm64) M-series core, 64 MiB of highly
compressible synthetic input (a repeating phrase, ~250x ratio) — LZ4's
match-heavy fast path, so treat these as an upper bound rather than a
prediction for real, less-repetitive column data:

| Operation           | Throughput   |
|----------------------|-------------:|
| `compress_block`      | ~9.9 GB/s   |
| `decompress_block`    | ~8.0 GB/s   |
| `compress_frame`      | ~18.9 GB/s  |
| `decompress_frame`    | ~18.6 GB/s  |

## Shim build

`shim/` is a [pixi-build-cmake](https://pixi.sh) package: `CMakeLists.txt`
links `shim/lz4_wrapper.c` against conda-forge's `lz4-c`, producing
`liblz4mojo.{dylib,so}` (natural extension per platform), installed to
`$CONDA_PREFIX/lib` by `pixi install`. The wrapper is a single-call C API
per operation — Mojo never reads back internal library state after a
foreign call (`LZ4F_decompress`'s streaming loop runs entirely inside C).

**Why a C shim, not calling liblz4 directly?** Same reasoning as zlib.mojo:
a single-call API means Mojo never reads back state after a foreign call,
and `dlopen`ing the shim at runtime means consumers never need `-l` link
flags. The handle is opened once per process and never closed, and is passed
as a borrowed parameter to each worker function so Mojo's ASAP destruction
can't `dlclose` the library before the C call inside that worker runs.

Caching it matters more than it looks: on macOS a `dlopen`/`dlclose` cycle of
an already-resident library costs around 450 µs, so opening one per call put
a fixed ~450 µs floor under every `compress_block`/`decompress_block`. Against
a Parquet page that is three orders of magnitude more than the compression
itself — a 300-page file spent well over 100 ms in `dlopen` alone. The
throughput table above barely moves (it compresses 64 MiB in one call), but
page-sized calls got ~300x cheaper.

## Status / scope

- Sizes are capped at `int` (~2 GiB) on both the Mojo and C sides — this
  targets Parquet pages and Puffin footers, never multi-gigabyte buffers.
- `frame_content_size` returns `None` both when a frame genuinely omits the
  content-size field and when it's recorded as exactly 0 — LZ4F's own wire
  format can't tell those apart.
- Frames this binding decompresses may carry LZ4F content or block
  checksums; they're validated transparently by the underlying decoder.

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
