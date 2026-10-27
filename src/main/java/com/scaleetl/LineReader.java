package com.scaleetl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

/**
 * Zero-copy-ish line iterator over a byte range of a file.
 *
 * <p>Ownership rule: the reader returns every line whose first byte lies in [start, end). A line that
 * starts before {@code end} is read to completion even if it runs past {@code end}; a line that
 * started before {@code start} is skipped. Adjacent ranges therefore partition a file exactly.
 *
 * <p>After {@link #next()} returns true the line is {@code buf[ls..le)} (without '\n' / '\r') and
 * {@link #lineOffset} is its absolute file offset. These are valid until the next call.
 */
final class LineReader {
    private final FileChannel ch;
    private final long end;
    byte[] buf;
    private int pos, lim, scan;
    private long bufFileStart;
    private boolean eof;

    int ls, le;
    long lineOffset;

    LineReader(FileChannel ch, long start, long end, int bufSize) throws IOException {
        this.ch = ch;
        this.end = end;
        this.buf = new byte[bufSize];
        if (start > 0) {
            // Begin one byte early so a line starting exactly at `start` is kept, and discard the partial line.
            bufFileStart = start - 1;
            next();
        }
    }

    /** Absolute file offset of the first byte not yet consumed. */
    long position() { return bufFileStart + pos; }

    boolean next() throws IOException {
        for (;;) {
            if (bufFileStart + pos >= end) return false;
            if (scan < pos) scan = pos;
            final byte[] b = buf;
            final int l = lim;
            int i = scan;
            while (i < l && b[i] != '\n') i++;
            if (i < l) {
                set(pos, i);
                pos = i + 1;
                scan = pos;
                return true;
            }
            scan = l;
            if (eof) {
                if (pos < lim) {
                    set(pos, lim);
                    pos = lim;
                    scan = pos;
                    return true;
                }
                return false;
            }
            fill();
        }
    }

    private void set(int s, int e) {
        ls = s;
        le = (e > s && buf[e - 1] == '\r') ? e - 1 : e;
        lineOffset = bufFileStart + s;
    }

    private void fill() throws IOException {
        if (pos > 0) {
            int rem = lim - pos;
            System.arraycopy(buf, pos, buf, 0, rem);
            bufFileStart += pos;
            scan -= pos;
            lim = rem;
            pos = 0;
        }
        if (lim == buf.length) buf = Arrays.copyOf(buf, buf.length * 2);
        int n = ch.read(ByteBuffer.wrap(buf, lim, buf.length - lim), bufFileStart + lim);
        if (n < 0) eof = true;
        else lim += n;
    }
}
