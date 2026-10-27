package com.scaleetl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

/**
 * "Load" stage. Two targets:
 * <ul>
 *   <li>default: build an on-disk hash index per partition (key hash -> byte offset) for O(1) point lookups;</li>
 *   <li>{@code --postgres}: stream each partition into PostgreSQL through {@code psql \copy} in parallel.</li>
 * </ul>
 *
 * Index file layout (big-endian): magic 'SEIX', version, capacity, reserved, rows(long), then
 * {@code capacity} slots of (hash long, offset+1 long); offset+1 == 0 marks an empty slot.
 */
final class Loader {
    private Loader() {}

    static final int MAGIC = 0x53454958;
    static final int HEADER_BYTES = 24;

    static void run(Args a) throws Exception {
        Path dir = Path.of(a.req("dir"));
        Manifest m = Manifest.load(dir);
        int threads = a.intOr("threads", Util.cpus());
        if (a.has("postgres")) postgres(a, dir, m, threads);
        else index(dir, m, threads);
    }

    // ---------------------------------------------------------------- index

    private static void index(Path dir, Manifest m, int threads) throws Exception {
        long t0 = System.nanoTime();
        AtomicLong rows = new AtomicLong();
        long total = 0;
        for (int p = 0; p < m.partitions; p++) total += Files.size(Manifest.partFile(dir, p));
        System.err.printf("indexing %d partitions (%s) on key '%s' with %d threads%n", m.partitions, Util.bytes(total), m.keyColumn, threads);
        try (Pipeline.Progress prog = new Pipeline.Progress("load", total)) {
            try {
                Pipeline.each(m.partitions, threads, p -> rows.addAndGet(buildIndex(dir, m, p, prog)));
            } catch (OutOfMemoryError oom) {
                throw new IOException("out of memory while indexing; use more --partitions or set SCALEETL_OPTS=-Xmx<larger>", oom);
            }
        }
        double sec = (System.nanoTime() - t0) / 1e9;
        System.err.printf("done: indexed %,d rows in %s (%s/s)%n", rows.get(), Util.dur(sec), Util.bytes((long) (total / sec)));
    }

    private static long buildIndex(Path dir, Manifest m, int p, Pipeline.Progress prog) throws IOException {
        Path data = Manifest.partFile(dir, p);
        long[] hs = new long[1 << 16];
        long[] offs = new long[1 << 16];
        int rows = 0;
        int[] fo = new int[2];
        try (FileChannel ch = FileChannel.open(data, StandardOpenOption.READ)) {
            LineReader lr = new LineReader(ch, 0, Long.MAX_VALUE, 4 << 20);
            if (lr.next()) { /* header */ }
            long local = 0;
            while (lr.next()) {
                if (lr.le == lr.ls) continue;
                long h = Csv.field(lr.buf, lr.ls, lr.le, m.delim, m.keyIndex, fo) ? Csv.hash(lr.buf, fo[0], fo[1]) : Csv.hash(lr.buf, 0, 0);
                if (rows == hs.length) { hs = Arrays.copyOf(hs, rows * 2); offs = Arrays.copyOf(offs, rows * 2); }
                hs[rows] = h;
                offs[rows] = lr.lineOffset;
                rows++;
                local += lr.le - lr.ls + 1;
                if (local >= (4 << 20)) { prog.add(local); local = 0; }
            }
            prog.add(local);
        }

        long want = Math.max(16, rows * 2L);
        if (want > (1L << 30)) throw new IOException("partition " + p + " has too many rows to index; use more --partitions");
        int cap = 16;
        while (cap < want) cap <<= 1;
        long[] tk = new long[cap];
        long[] tv = new long[cap];
        int mask = cap - 1;
        for (int i = 0; i < rows; i++) {
            int slot = (int) (hs[i] & mask);
            while (tv[slot] != 0) slot = (slot + 1) & mask;
            tk[slot] = hs[i];
            tv[slot] = offs[i] + 1;
        }
        hs = null;
        offs = null;

        Path idx = Manifest.idxFile(dir, p);
        Path tmp = idx.resolveSibling(idx.getFileName() + ".tmp");
        try (FileChannel oc = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            ByteBuffer hb = ByteBuffer.allocate(HEADER_BYTES);
            hb.putInt(MAGIC).putInt(1).putInt(cap).putInt(0).putLong(rows).flip();
            while (hb.hasRemaining()) oc.write(hb);
            ByteBuffer bb = ByteBuffer.allocate(1 << 20);
            for (int i = 0; i < cap; i++) {
                if (bb.remaining() < 16) { bb.flip(); while (bb.hasRemaining()) oc.write(bb); bb.clear(); }
                bb.putLong(tk[i]).putLong(tv[i]);
            }
            bb.flip();
            while (bb.hasRemaining()) oc.write(bb);
        }
        Files.move(tmp, idx, StandardCopyOption.REPLACE_EXISTING);
        return rows;
    }

    // ------------------------------------------------------------- postgres

    private static void postgres(Args a, Path dir, Manifest m, int threads) throws Exception {
        String conn = a.req("postgres");
        String table = a.req("table");
        if (!table.matches("[A-Za-z_][A-Za-z0-9_.]*")) throw new IllegalArgumentException("--table must be a plain (optionally schema-qualified) identifier");
        String d = m.delim == '\t' ? "E'\\t'" : "'" + (char) m.delim + "'";
        String copy = "\\copy " + table + " FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER " + d + ")";
        long t0 = System.nanoTime();
        System.err.printf("loading %d partitions into %s via psql with %d parallel sessions%n", m.partitions, table, threads);
        AtomicLong done = new AtomicLong();
        Pipeline.each(m.partitions, threads, p -> {
            Path f = Manifest.partFile(dir, p);
            Process pr = new ProcessBuilder("psql", conn, "-v", "ON_ERROR_STOP=1", "-q", "-c", copy)
                    .redirectInput(f.toFile())
                    .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                    .redirectError(ProcessBuilder.Redirect.INHERIT)
                    .start();
            int rc = pr.waitFor();
            if (rc != 0) throw new IOException("psql failed for " + f.getFileName() + " (exit " + rc + ")");
            System.err.printf("  loaded %s (%d/%d)%n", f.getFileName(), done.incrementAndGet(), m.partitions);
        });
        System.err.printf("done in %s%n", Util.dur((System.nanoTime() - t0) / 1e9));
    }
}
