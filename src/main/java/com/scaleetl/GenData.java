package com.scaleetl;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Locale;
import java.util.SplittableRandom;

/** Synthetic CSV generator for testing and benchmarking (about 90 bytes per row). */
final class GenData {
    private GenData() {}

    private static final String[] FIRST = {"Aarav", "Vihaan", "Ananya", "Diya", "Rohan", "Priya", "Arjun", "Sneha", "Kabir", "Isha", "Liam", "Emma", "Noah", "Olivia", "Mateo", "Sofia"};
    private static final String[] LAST = {"Sharma", "Patil", "Mehta", "Iyer", "Singh", "Khan", "Smith", "Garcia", "Chen", "Muller"};
    private static final String[] COUNTRY = {"IN", "US", "GB", "DE", "FR", "JP", "BR", "CA", "AU", "SG"};
    private static final String[] STATUS = {"active", "pending", "closed", "failed"};

    private static byte[][] bytes(String[] s) {
        byte[][] r = new byte[s.length][];
        for (int i = 0; i < s.length; i++) r[i] = s[i].getBytes(StandardCharsets.UTF_8);
        return r;
    }

    static void run(Args a) throws Exception {
        Path out = Path.of(a.req("out"));
        long rows = a.longOr("rows", 1_000_000);
        long seed = a.longOr("seed", 42);
        int threads = a.intOr("threads", Util.cpus());
        long users = Math.max(1, rows / 10);
        final int chunkRows = 250_000;
        int chunks = (int) ((rows + chunkRows - 1) / chunkRows);
        byte[][] first = bytes(FIRST), last = bytes(LAST), country = bytes(COUNTRY), status = bytes(STATUS);
        byte[][] firstLower = bytes(java.util.Arrays.stream(FIRST).map(s -> s.toLowerCase(Locale.ROOT)).toArray(String[]::new));
        byte[][] lastLower = bytes(java.util.Arrays.stream(LAST).map(s -> s.toLowerCase(Locale.ROOT)).toArray(String[]::new));
        byte[] suffix = "@example.com".getBytes(StandardCharsets.UTF_8);
        long t0 = System.nanoTime();
        if (out.getParent() != null) Files.createDirectories(out.getParent());

        try (FileChannel oc = FileChannel.open(out, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            long[] pos = {0};
            byte[] hdr = "id,user_id,country,status,amount,created_at,email,name\n".getBytes(StandardCharsets.UTF_8);
            oc.write(ByteBuffer.wrap(hdr), 0);
            pos[0] = hdr.length;
            Pipeline.ordered(chunks, threads, c -> {
                SplittableRandom rng = new SplittableRandom(seed * 1_000_003L + c);
                long from = (long) c * chunkRows + 1;
                long to = Math.min(rows, (long) (c + 1) * chunkRows);
                Util.ByteSink o = new Util.ByteSink((int) ((to - from + 1) * 100));
                for (long id = from; id <= to; id++) {
                    int f = rng.nextInt(first.length), l = rng.nextInt(last.length);
                    long cents = rng.nextInt(100_000);
                    o.putLong(id); o.put(',');
                    o.putLong(1 + rng.nextLong(users)); o.put(',');
                    o.put(country[rng.nextInt(country.length)]); o.put(',');
                    o.put(status[rng.nextInt(status.length)]); o.put(',');
                    o.putLong(cents / 100); o.put('.'); o.pad2((int) (cents % 100)); o.put(',');
                    o.put('2'); o.put('0'); o.put('2'); o.put('5'); o.put('-');
                    o.pad2(1 + rng.nextInt(12)); o.put('-'); o.pad2(1 + rng.nextInt(28)); o.put('T');
                    o.pad2(rng.nextInt(24)); o.put(':'); o.pad2(rng.nextInt(60)); o.put(':'); o.pad2(rng.nextInt(60)); o.put(',');
                    o.put(firstLower[f]); o.put('.'); o.put(lastLower[l]); o.putLong(id); o.put(suffix); o.put(',');
                    if (rng.nextInt(100) == 0) { // ~1% quoted names containing the delimiter
                        o.put('"'); o.put(last[l]); o.put(','); o.put(' '); o.put(first[f]); o.put('"');
                    } else {
                        o.put(first[f]); o.put(' '); o.put(last[l]);
                    }
                    o.put('\n');
                }
                return o;
            }, (i, o) -> {
                ByteBuffer bb = ByteBuffer.wrap(o.a, 0, o.n);
                long p = pos[0];
                while (bb.hasRemaining()) p += oc.write(bb, p);
                pos[0] = p;
            });
            double sec = (System.nanoTime() - t0) / 1e9;
            System.err.printf("generated %,d rows (%s) in %s%n", rows, Util.bytes(pos[0]), Util.dur(sec));
        }
    }
}
