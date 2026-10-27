package com.scaleetl;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Byte-level CSV helpers. Nothing here allocates per row.
 * Quoted fields are supported (delimiters inside quotes, "" escapes) but records must not
 * contain embedded newlines: the parallel splitter cuts files on '\n'.
 */
final class Csv {
    private Csv() {}

    /** 64-bit FNV-1a followed by a murmur3 finalizer; used for partitioning and the index. */
    static long hash(byte[] b, int s, int e) {
        long h = 0xcbf29ce484222325L;
        for (int i = s; i < e; i++) {
            h ^= (b[i] & 0xff);
            h *= 0x100000001b3L;
        }
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= h >>> 33;
        return h;
    }

    static int partitionOf(long h, int n) { return (int) ((h >>> 32) % n); }

    /** Locates field {@code col}; bounds exclude surrounding quotes. */
    static boolean field(byte[] b, int s, int e, byte delim, int col, int[] out) {
        int idx = 0, fs = s;
        boolean q = false;
        for (int i = s; i <= e; i++) {
            if (i == e || (b[i] == delim && !q)) {
                if (idx == col) {
                    int a = fs, z = i;
                    if (z - a >= 2 && b[a] == '"' && b[z - 1] == '"') { a++; z--; }
                    out[0] = a;
                    out[1] = z;
                    return true;
                }
                idx++;
                fs = i + 1;
            } else if (b[i] == '"') {
                q = !q;
            }
        }
        return false;
    }

    /** Splits a line into raw field bounds (quotes included). Returns the field count (capped at starts.length). */
    static int split(byte[] b, int s, int e, byte delim, int[] starts, int[] ends) {
        int n = 0, fs = s;
        boolean q = false;
        for (int i = s; i <= e; i++) {
            if (i == e || (b[i] == delim && !q)) {
                if (n >= starts.length) return n;
                starts[n] = fs;
                ends[n] = i;
                n++;
                fs = i + 1;
            } else if (b[i] == '"') {
                q = !q;
            }
        }
        return n;
    }

    static boolean equals(byte[] b, int s, int e, byte[] v) {
        if (e - s != v.length) return false;
        for (int i = 0; i < v.length; i++) if (b[s + i] != v[i]) return false;
        return true;
    }

    static int indexOf(byte[] b, int s, int e, byte[] pat) {
        int m = pat.length;
        if (m == 0) return s;
        byte f = pat[0];
        for (int i = s, last = e - m; i <= last; i++) {
            if (b[i] != f) continue;
            int j = 1;
            while (j < m && b[i + j] == pat[j]) j++;
            if (j == m) return i;
        }
        return -1;
    }

    /** Parses a plain decimal number; returns NaN if the bytes are not a number. */
    static double number(byte[] b, int a, int z) {
        while (a < z && (b[a] == ' ' || b[a] == '\t')) a++;
        while (z > a && (b[z - 1] == ' ' || b[z - 1] == '\t')) z--;
        if (a >= z) return Double.NaN;
        if (z - a <= 18) {
            int i = a;
            boolean neg = false;
            if (b[i] == '-' || b[i] == '+') { neg = b[i] == '-'; i++; }
            long mant = 0;
            int scale = 0, digits = 0;
            boolean dot = false, ok = i < z;
            for (; i < z; i++) {
                byte c = b[i];
                if (c >= '0' && c <= '9') { mant = mant * 10 + (c - '0'); digits++; if (dot) scale++; }
                else if (c == '.' && !dot) dot = true;
                else { ok = false; break; }
            }
            if (ok && digits > 0) {
                double v = mant;
                while (scale-- > 0) v /= 10.0;
                return neg ? -v : v;
            }
        }
        try {
            return Double.parseDouble(new String(b, a, z - a, StandardCharsets.ISO_8859_1));
        } catch (NumberFormatException ex) {
            return Double.NaN;
        }
    }

    record Header(byte[] raw, List<String> names, long dataStart) {}

    static Header readHeader(FileChannel ch, byte delim) throws IOException {
        LineReader r = new LineReader(ch, 0, Long.MAX_VALUE, 1 << 16);
        if (!r.next()) throw new IOException("empty file");
        byte[] raw = Arrays.copyOfRange(r.buf, r.ls, r.le);
        return new Header(raw, names(raw, delim), r.position());
    }

    static List<String> names(byte[] line, byte delim) {
        int[] st = new int[4096], en = new int[4096];
        int n = split(line, 0, line.length, delim, st, en);
        List<String> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            int a = st[i], z = en[i];
            if (i == 0 && z - a >= 3 && (line[a] & 0xff) == 0xEF && (line[a + 1] & 0xff) == 0xBB && (line[a + 2] & 0xff) == 0xBF) a += 3;
            while (a < z && line[a] == ' ') a++;
            while (z > a && line[z - 1] == ' ') z--;
            if (z - a >= 2 && line[a] == '"' && line[z - 1] == '"') { a++; z--; }
            out.add(new String(line, a, z - a, StandardCharsets.UTF_8));
        }
        return out;
    }

    static int columnIndex(List<String> names, String col) {
        int i = names.indexOf(col);
        if (i >= 0) return i;
        try {
            int k = Integer.parseInt(col);
            if (k >= 0 && k < names.size()) return k;
        } catch (NumberFormatException ignored) { }
        throw new IllegalArgumentException("column '" + col + "' not found; available: " + names);
    }
}
