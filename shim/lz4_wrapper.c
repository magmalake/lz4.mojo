/*
 * lz4.mojo — minimal liblz4 wrapper for Mojo FFI.
 *
 * Mirrors zlib.mojo's shim: a single-call API per operation so Mojo never has
 * to read back internal state after a foreign call. Pointer args are void*
 * (passed from Mojo as an address-sized Int); integers are C int. The caller
 * (Mojo side) pre-allocates the output buffer; the return is bytes-written
 * (>=0) or a negative sentinel on error. All sizes are plain `int` — this
 * binding targets Parquet pages and Puffin footers, never multi-GiB buffers,
 * so the 2 GiB ceiling of LZ4's own `int`-sized API is not a practical limit.
 *
 * Build: shim/CMakeLists.txt -> $CONDA_PREFIX/lib/liblz4mojo.{dylib,so}
 */

#include <lz4.h>
#include <lz4hc.h>
#include <lz4frame.h>
#include <string.h>

#define LZ4M_ERR_GENERIC   (-1)
#define LZ4M_ERR_TOO_SMALL (-2)

/* ------------------------------------------------------------------ */
/* Block format (LZ4_RAW): no size is carried in the stream.           */
/* ------------------------------------------------------------------ */

int lz4m_compress_bound(int in_len) {
    return LZ4_compressBound(in_len);
}

/* LZ4_compress_fast: acceleration >= 1 (1 == default LZ4_compress_default
 * behavior; higher trades ratio for speed). Returns bytes written, or 0 on
 * failure (destination too small) which we normalize to LZ4M_ERR_TOO_SMALL. */
int lz4m_compress_block(const void *in_buf, int in_len,
                        void *out_buf, int out_cap, int acceleration) {
    if (in_len == 0) return 0;
    int written = LZ4_compress_fast(
        (const char *)in_buf, (char *)out_buf, in_len, out_cap, acceleration
    );
    if (written <= 0) return LZ4M_ERR_TOO_SMALL;
    return written;
}

int lz4m_compress_block_hc(const void *in_buf, int in_len,
                           void *out_buf, int out_cap, int level) {
    if (in_len == 0) return 0;
    int written = LZ4_compress_HC(
        (const char *)in_buf, (char *)out_buf, in_len, out_cap, level
    );
    if (written <= 0) return LZ4M_ERR_TOO_SMALL;
    return written;
}

/* LZ4_decompress_safe: `out_cap` must be the true uncompressed size (Parquet
 * always supplies it out of band; LZ4 blocks carry no size of their own).
 * Returns bytes written (>=0), or a negative LZ4 error we normalize to
 * LZ4M_ERR_GENERIC (corrupt / truncated input, or the real size was larger
 * than out_cap). */
int lz4m_decompress_block(const void *in_buf, int in_len,
                          void *out_buf, int out_cap) {
    if (in_len == 0) return 0;
    int written = LZ4_decompress_safe(
        (const char *)in_buf, (char *)out_buf, in_len, out_cap
    );
    if (written < 0) return LZ4M_ERR_GENERIC;
    return written;
}

/* ------------------------------------------------------------------ */
/* Frame format (LZ4F): the only codec Iceberg Puffin allows.          */
/* ------------------------------------------------------------------ */

int lz4m_compress_frame_bound(int in_len) {
    size_t rc = LZ4F_compressFrameBound((size_t)in_len, NULL);
    if (LZ4F_isError(rc)) return LZ4M_ERR_GENERIC;
    return (int)rc;
}

/* Single frame, one shot. `content_size` != 0 sets frameInfo.contentSize so
 * the frame header carries the uncompressed length up front — the property
 * Puffin footers rely on. */
int lz4m_compress_frame(const void *in_buf, int in_len,
                        void *out_buf, int out_cap,
                        int level, int content_size) {
    LZ4F_preferences_t prefs;
    memset(&prefs, 0, sizeof(prefs));
    prefs.compressionLevel = level;
    if (content_size) {
        prefs.frameInfo.contentSize = (unsigned long long)in_len;
    }
    size_t rc = LZ4F_compressFrame(
        out_buf, (size_t)out_cap, in_buf, (size_t)in_len, &prefs
    );
    if (LZ4F_isError(rc)) return LZ4M_ERR_TOO_SMALL;
    return (int)rc;
}

/* Reads the frame header's content-size field without decompressing.
 * Returns the content size (>0), 0 if the frame doesn't carry one (LZ4F
 * treats 0 itself as "unspecified" so the two cases are indistinguishable
 * at the protocol level), or LZ4M_ERR_GENERIC if `in_buf` isn't a valid
 * frame header. */
long long lz4m_frame_content_size(const void *in_buf, int in_len) {
    LZ4F_decompressionContext_t ctx;
    size_t rc = LZ4F_createDecompressionContext(&ctx, LZ4F_VERSION);
    if (LZ4F_isError(rc)) return LZ4M_ERR_GENERIC;

    LZ4F_frameInfo_t info;
    memset(&info, 0, sizeof(info));
    size_t consumed = (size_t)in_len;
    rc = LZ4F_getFrameInfo(ctx, &info, in_buf, &consumed);
    LZ4F_freeDecompressionContext(ctx);
    if (LZ4F_isError(rc)) return LZ4M_ERR_GENERIC;
    return (long long)info.contentSize;
}

/* Decompresses one frame in a bounded internal loop (the streaming LZ4F API
 * driven entirely inside C, so Mojo only ever makes one call). Returns bytes
 * written (>=0) once the frame is fully decoded, LZ4M_ERR_TOO_SMALL if
 * `out_cap` ran out before the frame finished (caller should grow the buffer
 * and retry from scratch), or LZ4M_ERR_GENERIC on corrupt/truncated input. */
int lz4m_decompress_frame(const void *in_buf, int in_len,
                          void *out_buf, int out_cap) {
    LZ4F_decompressionContext_t ctx;
    size_t rc = LZ4F_createDecompressionContext(&ctx, LZ4F_VERSION);
    if (LZ4F_isError(rc)) return LZ4M_ERR_GENERIC;

    const char *src = (const char *)in_buf;
    char *dst = (char *)out_buf;
    size_t src_remaining = (size_t)in_len;
    size_t dst_remaining = (size_t)out_cap;
    size_t total_written = 0;
    size_t hint = 1;

    while (hint != 0) {
        size_t src_size = src_remaining;
        size_t dst_size = dst_remaining;
        hint = LZ4F_decompress(ctx, dst, &dst_size, src, &src_size, NULL);
        if (LZ4F_isError(hint)) {
            LZ4F_freeDecompressionContext(ctx);
            return LZ4M_ERR_GENERIC;
        }
        dst += dst_size;
        dst_remaining -= dst_size;
        total_written += dst_size;
        src += src_size;
        src_remaining -= src_size;

        if (hint == 0) break; /* frame fully decoded */
        if (src_size == 0 && dst_size == 0) {
            /* No progress this round but the frame isn't finished: either the
             * input was truncated (nothing left to feed) or out_cap is 0. */
            LZ4F_freeDecompressionContext(ctx);
            return src_remaining == 0 ? LZ4M_ERR_GENERIC : LZ4M_ERR_TOO_SMALL;
        }
        if (dst_remaining == 0) {
            LZ4F_freeDecompressionContext(ctx);
            return LZ4M_ERR_TOO_SMALL;
        }
        if (src_remaining == 0) {
            /* Ran out of input before the frame reported completion. */
            LZ4F_freeDecompressionContext(ctx);
            return LZ4M_ERR_GENERIC;
        }
    }
    LZ4F_freeDecompressionContext(ctx);
    return (int)total_written;
}
