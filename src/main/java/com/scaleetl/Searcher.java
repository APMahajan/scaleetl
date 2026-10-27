package com.scaleetl;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * Two search modes:
 * <ul>
 *   <li><b>indexed</b> ({@code --dir D --key K}): hash K, pick the partition, probe the on-disk index, read the row. O(1).</li>
 *   <li><b>scan</b> ({@code --in PATH --col C --value V} and/or {@code --contains TEXT}): parallel scan of a file or
 *       partition directory. If C is the partition key and indexes exist, the index is used automatically.</li>
 * </ul>
 */
final class Searcher {
    private Searcher() {}

    static void run(Args a) throws Exception {
        long t0 = System.nanoTime();
        Path in = Path.of(a.has("dir") ? a.req("dir") : a.req("in"));
        long limit = a.longOr("limit", Long.MAX_VALUE);
        boolean countOnly = a.bool("count");
        int threads = a.intOr("threads", Util.cpus());
        PrintStream so = new PrintStream(new BufferedOutputStream(System.out, 1 << 20), false);
        long found;

        boolean haveIndex = Files.isDirectory(in) && Manifest.exists(in) && Files.exists(Manifest.idxFile(in, 0));
        List<String> keys = new ArrayList<>(a.list("key"));
        if (keys.isEmpty() && haveIndex && a.has("col") && a.has("value")) {
            Manifest m = Manifest.load(in);
            if (a.str("col", "").equals(m.keyColumn) && !a.has("contains")) keys.addAll(a.list("value"));
        }

        if (!keys.isEmpty()) {
            if (!haveIndex) throw new IOException("no index found in " + in + " - run `scaleetl load --dir " + in + "` first");
            found = indexed(a, in, keys, limit, countOnly, so);
            if (countOnly) so.println(found);
            so.flush();
            System.err.printf("%,d match(es) in %.1f ms (indexed)%n", found, (System.nanoTime() - t0) / 1e6);
        } else {
            found = scan(a, in, limit, countOnly, threads, so);
            if (countOnly) so.println(found);
            so.flush();
            System.err.printf("%,d match(es) in %s (scan)%n", found, Util.dur((System.nanoTime() - t0) / 1e9));
        }
    }

    // ------------------------------------------------------------- indexed

    private static final class Part implements AutoCloseable {
        final FileChannel idx, data;
        final int cap;

        Part(Path dir, int p) throws IOException {
            idx = FileChannel.open(Manifest.idxFile(dir, p), StandardOpenOption.READ);
            data = FileChannel.open(Manifest.partFile(dir, p), StandardOpenOption.READ);
            ByteBuffer h = ByteBuffer.allocate(Loader.HEADER_BYTES);
            readFully(idx, h, 0);
            h.flip();
            if (h.getInt() != Loader.MAGIC) throw new IOException("bad index file for partition " + p);
            h.getInt();
            cap = h.getInt();
        }

        @Override public void close() throws IOException { idx.close(); data.close(); }
    }

    private static long indexed(Args a, Path dir, List<String> keys, long limit, boolean countOnly, PrintStream out) throws IOException {
        Manifest m = Manifest.load(dir);
        Map<Integer, Part> open = new HashMap<>();
        long found = 0;
        try {
            if (a.bool("header") && !countOnly) {
                try (FileChannel c = FileChannel.open(Manifest.partFile(dir, 0), StandardOpenOption.READ)) {
                    byte[] l = readLine(c, 0);
                    out.write(l, 0, l.length);
                    out.write('\n');
                }
            }
            ByteBuffer slot = ByteBuffer.allocate(16);
            int[] fo = new int[2];
            for (String k : keys) {
                byte[] kb = k.getBytes(StandardCharsets.UTF_8);
                long h = Csv.hash(kb, 0, kb.length);
                int p = Csv.partitionOf(h, m.partitions);
                Part part = open.get(p);
                if (part == null) { part = new Part(dir, p); open.put(p, part); }
                int mask = part.cap - 1;
                int s = (int) (h & mask);
                for (int probes = 0; probes < part.cap; probes++) {
                    slot.clear();
                    readFully(part.idx, slot, Loader.HEADER_BYTES + (long) s * 16);
                    long hk = slot.getLong(0), v = slot.getLong(8);
                    if (v == 0) break;
                    if (hk == h) {
                        byte[] line = readLine(part.data, v - 1);
                        if (Csv.field(line, 0, line.length, m.delim, m.keyIndex, fo) && Csv.equals(line, fo[0], fo[1], kb)) {
                            if (found >= limit) return found;
                            found++;
                            if (!countOnly) { out.write(line, 0, line.length); out.write('\n'); }
                        }
                    }
                    s = (s + 1) & mask;
                }
            }
        } finally {
            for (Part p : open.values()) p.close();
        }
        return found;
    }

    private static void readFully(FileChannel ch, ByteBuffer bb, long pos) throws IOException {
        while (bb.hasRemaining()) {
            int n = ch.read(bb, pos);
            if (n < 0) throw new IOException("unexpected end of file");
            pos += n;
        }
    }

    private static byte[] readLine(FileChannel ch, long off) throws IOException {
        byte[] buf = new byte[4096];
        int len = 0;
        for (;;) {
            int n = ch.read(ByteBuffer.wrap(buf, len, buf.length - len), off + len);
            if (n < 0) return Arrays.copyOf(buf, len);
            for (int i = len; i < len + n; i++) if (buf[i] == '\n') return Arrays.copyOf(buf, i);
            len += n;
            if (len == buf.length) buf = Arrays.copyOf(buf, buf.length * 2);
        }
    }

    // ---------------------------------------------------------------- scan

    private record Task(Path file, long start, long end, int col, byte delim) {}

    private static long scan(Args a, Path in, long limit, boolean countOnly, int threads, PrintStream out) throws Exception {
        String col = a.str("col", null);
        byte[] value = a.has("value") ? a.str("value", "").getBytes(StandardCharsets.UTF_8) : null;
        byte[] contains = a.has("contains") ? a.str("contains", "").getBytes(StandardCharsets.UTF_8) : null;
        if ((col == null) != (value == null)) throw new IllegalArgumentException("--col and --value go together");
        if (col == null && contains == null) throw new IllegalArgumentException("search needs --key, or --col/--value, or --contains");

        byte delim = Manifest.exists(in) ? Manifest.load(in).delim : Util.delim(a);
        List<Path> files;
        if (Files.isDirectory(in)) {
            try (Stream<Path> s = Files.list(in)) {
                files = s.filter(p -> p.getFileName().toString().endsWith(".csv") && !p.getFileName().toString().startsWith("_")).sorted().toList();
            }
        } else {
            files = List.of(in);
        }

        List<Task> tasks = new ArrayList<>();
        long total = 0;
        long rangeSize = 64L << 20;
        for (Path f : files) {
            long size = Files.size(f);
            total += size;
            try (FileChannel ch = FileChannel.open(f, StandardOpenOption.READ)) {
                Csv.Header h = Csv.readHeader(ch, delim);
                int ci = col == null ? -1 : Csv.columnIndex(h.names(), col);
                for (long s = h.dataStart(); s < size; s += rangeSize) tasks.add(new Task(f, s, Math.min(size, s + rangeSize), ci, delim));
            }
        }

        if (a.bool("header") && !countOnly) {
            try (FileChannel ch = FileChannel.open(files.get(0), StandardOpenOption.READ)) {
                byte[] raw = Csv.readHeader(ch, delim).raw();
                out.write(raw, 0, raw.length);
                out.write('\n');
            }
        }

        AtomicLong found = new AtomicLong();
        Object outLock = new Object();
        try (Pipeline.Progress prog = new Pipeline.Progress("search", total)) {
            Pipeline.each(tasks.size(), threads, i -> {
                Task t = tasks.get(i);
                try (FileChannel ch = FileChannel.open(t.file(), StandardOpenOption.READ)) {
                    LineReader lr = new LineReader(ch, t.start(), t.end(), 4 << 20);
                    Util.ByteSink o = new Util.ByteSink(1 << 16);
                    int[] fo = new int[2];
                    while (found.get() < limit && lr.next()) {
                        if (lr.le == lr.ls) continue;
                        byte[] b = lr.buf;
                        boolean hit = true;
                        if (t.col() >= 0) hit = Csv.field(b, lr.ls, lr.le, t.delim(), t.col(), fo) && Csv.equals(b, fo[0], fo[1], value);
                        if (hit && contains != null) hit = Csv.indexOf(b, lr.ls, lr.le, contains) >= 0;
                        if (!hit) continue;
                        if (found.incrementAndGet() > limit) break;
                        if (!countOnly) {
                            o.put(b, lr.ls, lr.le);
                            o.put('\n');
                            if (o.n > (1 << 20)) { flush(o, out, outLock); }
                        }
                    }
                    flush(o, out, outLock);
                    prog.add(t.end() - t.start());
                }
            });
        }
        return Math.min(found.get(), limit);
    }

    private static void flush(Util.ByteSink o, OutputStream out, Object lock) throws IOException {
        if (o.n == 0) return;
        synchronized (lock) { out.write(o.a, 0, o.n); }
        o.n = 0;
    }
}
