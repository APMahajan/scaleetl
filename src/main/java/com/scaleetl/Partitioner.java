package com.scaleetl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Hash-partitions a CSV by a key column into N files. The input is cut into byte ranges that
 * worker threads pull from a shared counter; each worker batches rows per partition in memory
 * and appends full buffers to the partition files under a per-partition lock.
 */
final class Partitioner {
    private Partitioner() {}

    static void run(Args a) throws Exception {
        Path in = Path.of(a.req("in"));
        Path out = Path.of(a.req("out"));
        String key = a.req("key");
        byte delim = Util.delim(a);
        int threads = a.intOr("threads", Util.cpus());
        long size = Files.size(in);
        long t0 = System.nanoTime();
        Files.createDirectories(out);

        try (FileChannel ch = FileChannel.open(in, StandardOpenOption.READ)) {
            Csv.Header hdr = Csv.readHeader(ch, delim);
            int keyIdx = Csv.columnIndex(hdr.names(), key);
            String keyName = hdr.names().get(keyIdx);

            int n;
            String pa = a.str("partitions", "auto");
            if (pa.equals("auto")) {
                long target = 256L << 20; // ~256 MB per partition
                n = (int) Math.max(8, Math.min(1024, (size + target - 1) / target));
            } else {
                n = Integer.parseInt(pa);
            }
            if (n < 1 || n > 4096) throw new IllegalArgumentException("--partitions must be 1..4096 (mind `ulimit -n`)");

            // Open partition files and write the header into each.
            FileChannel[] chans = new FileChannel[n];
            long[] filePos = new long[n];
            byte[] headerLine = Arrays.copyOf(hdr.raw(), hdr.raw().length + 1);
            headerLine[headerLine.length - 1] = '\n';
            try {
                for (int p = 0; p < n; p++) {
                    chans[p] = FileChannel.open(Manifest.partFile(out, p), StandardOpenOption.CREATE,
                            StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
                    chans[p].write(ByteBuffer.wrap(headerLine), 0);
                    filePos[p] = headerLine.length;
                }
            } catch (IOException e) {
                throw new IOException("could not open " + n + " partition files (" + e.getMessage()
                        + "). Try fewer --partitions or raise `ulimit -n`.", e);
            }

            long dataStart = hdr.dataStart();
            long dataLen = size - dataStart;
            long rangeSize = Math.max(8L << 20, Math.min(128L << 20, dataLen / (threads * 8L) + 1));
            int numRanges = (int) ((dataLen + rangeSize - 1) / rangeSize);
            int bufCap = (int) Math.max(16 << 10, Math.min(1 << 20, (64L << 20) / n));

            AtomicInteger nextRange = new AtomicInteger();
            AtomicLongArray totals = new AtomicLongArray(n);
            System.err.printf("partitioning %s (%s) by '%s' into %d partitions with %d threads%n",
                    in, Util.bytes(size), keyName, n, threads);

            try (Pipeline.Progress prog = new Pipeline.Progress("partition", dataLen)) {
                final int N = n;
                final int kIdx = keyIdx;
                Pipeline.workers(threads, w -> {
                    byte[][] bufs = new byte[N][];
                    int[] cnt = new int[N];
                    long[] rows = new long[N];
                    int[] fo = new int[2];
                    int r;
                    while ((r = nextRange.getAndIncrement()) < numRanges) {
                        long s = dataStart + r * rangeSize;
                        long e = Math.min(size, s + rangeSize);
                        LineReader lr = new LineReader(ch, s, e, 4 << 20);
                        long local = 0;
                        while (lr.next()) {
                            if (lr.le == lr.ls) continue;
                            byte[] b = lr.buf;
                            long h = Csv.field(b, lr.ls, lr.le, delim, kIdx, fo) ? Csv.hash(b, fo[0], fo[1]) : Csv.hash(b, 0, 0);
                            int p = Csv.partitionOf(h, N);
                            int len = lr.le - lr.ls + 1;
                            if (len > bufCap) {
                                flush(chans, filePos, bufs, cnt, p);
                                byte[] tmp = Arrays.copyOfRange(b, lr.ls, lr.le + 1);
                                tmp[len - 1] = '\n';
                                write(chans, filePos, p, tmp, 0, len);
                            } else {
                                if (cnt[p] + len > bufCap) flush(chans, filePos, bufs, cnt, p);
                                byte[] d = bufs[p];
                                if (d == null) d = bufs[p] = new byte[bufCap];
                                System.arraycopy(b, lr.ls, d, cnt[p], len - 1);
                                d[cnt[p] + len - 1] = '\n';
                                cnt[p] += len;
                            }
                            rows[p]++;
                            local += len;
                            if (local >= (4 << 20)) { prog.add(local); local = 0; }
                        }
                        prog.add(local);
                    }
                    for (int p = 0; p < N; p++) flush(chans, filePos, bufs, cnt, p);
                    for (int p = 0; p < N; p++) totals.addAndGet(p, rows[p]);
                });
            } finally {
                for (FileChannel c : chans) if (c != null) c.close();
            }

            Manifest m = new Manifest();
            m.partitions = n;
            m.keyColumn = keyName;
            m.keyIndex = keyIdx;
            m.delim = delim;
            m.partRows = new long[n];
            long total = 0, max = 0, min = Long.MAX_VALUE;
            for (int p = 0; p < n; p++) {
                m.partRows[p] = totals.get(p);
                total += m.partRows[p];
                max = Math.max(max, m.partRows[p]);
                min = Math.min(min, m.partRows[p]);
            }
            m.rows = total;
            m.save(out);

            double sec = (System.nanoTime() - t0) / 1e9;
            System.err.printf("done: %,d rows -> %d partitions in %s (%s/s, %,.0f rows/s)%n", total, n, Util.dur(sec),
                    Util.bytes((long) (size / sec)), total / sec);
            System.err.printf("partition rows: min %,d  avg %,d  max %,d%n", min, total / n, max);
            if (max > 2 * Math.max(1, total / n)) System.err.println("warning: skewed key distribution (max partition > 2x average)");
            if (total / n > 20_000_000L) System.err.println("note: >20M rows/partition; `load` indexing needs a large heap - use more --partitions");
        }
    }

    private static void flush(FileChannel[] chans, long[] filePos, byte[][] bufs, int[] cnt, int p) throws IOException {
        if (cnt[p] == 0) return;
        write(chans, filePos, p, bufs[p], 0, cnt[p]);
        cnt[p] = 0;
    }

    private static void write(FileChannel[] chans, long[] filePos, int p, byte[] data, int off, int len) throws IOException {
        FileChannel c = chans[p];
        synchronized (c) {
            ByteBuffer bb = ByteBuffer.wrap(data, off, len);
            long pos = filePos[p];
            while (bb.hasRemaining()) pos += c.write(bb, pos);
            filePos[p] = pos;
        }
    }
}
