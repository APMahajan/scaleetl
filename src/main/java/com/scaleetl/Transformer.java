package com.scaleetl;

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
 * Streaming transform: filter rows, select/reorder columns, rename, trim, upper/lower.
 * Input is cut into byte ranges processed in parallel; output keeps input order.
 */
final class Transformer {
    private Transformer() {}

    static final int EQ = 0, NE = 1, GT = 2, GE = 3, LT = 4, LE = 5, CT = 6;

    record Filter(int col, int op, boolean numeric, double num, byte[] lit) {
        boolean test(byte[] b, int a, int z) {
            if (op == CT) return Csv.indexOf(b, a, z, lit) >= 0;
            if (numeric) {
                double v = Csv.number(b, a, z);
                if (Double.isNaN(v)) return false;
                return switch (op) {
                    case EQ -> v == num;
                    case NE -> v != num;
                    case GT -> v > num;
                    case GE -> v >= num;
                    case LT -> v < num;
                    default -> v <= num;
                };
            }
            int c = cmp(b, a, z, lit);
            return switch (op) {
                case EQ -> c == 0;
                case NE -> c != 0;
                case GT -> c > 0;
                case GE -> c >= 0;
                case LT -> c < 0;
                default -> c <= 0;
            };
        }

        private static int cmp(byte[] b, int a, int z, byte[] l) {
            int n = Math.min(z - a, l.length);
            for (int i = 0; i < n; i++) {
                int d = (b[a + i] & 0xff) - (l[i] & 0xff);
                if (d != 0) return d;
            }
            return (z - a) - l.length;
        }
    }

    /** Per-file execution plan resolved against that file's header. */
    private static final class Plan {
        byte delim;
        int[] select;          // null = all columns
        byte[] caseMode;       // per source column: 0 none, 1 upper, 2 lower
        boolean trim, passthrough, anyCase;
        Filter[] filters;
        byte[] header;         // output header line (no newline)

        boolean apply(byte[] b, int s, int e, int[] st, int[] en, Util.ByteSink o) {
            int n = 0;
            if (!passthrough || filters.length > 0) n = Csv.split(b, s, e, delim, st, en);
            for (Filter f : filters) {
                if (f.col() >= n) return false;
                int a = st[f.col()], z = en[f.col()];
                if (z - a >= 2 && b[a] == '"' && b[z - 1] == '"') { a++; z--; }
                if (!f.test(b, a, z)) return false;
            }
            if (passthrough) {
                o.put(b, s, e);
                o.put('\n');
                return true;
            }
            int cnt = select == null ? n : select.length;
            for (int k = 0; k < cnt; k++) {
                int c = select == null ? k : select[k];
                if (k > 0) o.put(delim);
                if (c >= n) continue;
                int a = st[c], z = en[c];
                if (trim) {
                    while (a < z && (b[a] == ' ' || b[a] == '\t')) a++;
                    while (z > a && (b[z - 1] == ' ' || b[z - 1] == '\t')) z--;
                }
                byte mode = anyCase && c < caseMode.length ? caseMode[c] : 0;
                if (mode == 0) {
                    o.put(b, a, z);
                } else {
                    o.ensure(z - a);
                    for (int i = a; i < z; i++) {
                        byte x = b[i];
                        if (mode == 1 && x >= 'a' && x <= 'z') x -= 32;
                        else if (mode == 2 && x >= 'A' && x <= 'Z') x += 32;
                        o.a[o.n++] = x;
                    }
                }
            }
            o.put('\n');
            return true;
        }
    }

    static void run(Args a) throws Exception {
        Path in = Path.of(a.req("in"));
        Path out = Path.of(a.req("out"));
        if (Files.isDirectory(in)) {
            Files.createDirectories(out);
            List<Path> files;
            try (Stream<Path> s = Files.list(in)) {
                files = s.filter(p -> p.getFileName().toString().endsWith(".csv") && !p.getFileName().toString().startsWith("_"))
                        .sorted().toList();
            }
            for (Path f : files) one(a, f, out.resolve(f.getFileName()));
        } else {
            if (out.getParent() != null) Files.createDirectories(out.getParent());
            one(a, in, out);
        }
    }

    private static void one(Args a, Path in, Path out) throws Exception {
        byte delim = Util.delim(a);
        int threads = a.intOr("threads", Util.cpus());
        long size = Files.size(in);
        long t0 = System.nanoTime();

        try (FileChannel ch = FileChannel.open(in, StandardOpenOption.READ);
             FileChannel oc = FileChannel.open(out, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            Csv.Header hdr = Csv.readHeader(ch, delim);
            Plan plan = buildPlan(a, hdr, delim);

            byte[] hl = Arrays.copyOf(plan.header, plan.header.length + 1);
            hl[hl.length - 1] = '\n';
            long[] outPos = {0};
            oc.write(ByteBuffer.wrap(hl), 0);
            outPos[0] = hl.length;

            long dataStart = hdr.dataStart();
            long dataLen = size - dataStart;
            long rangeSize = Math.max(4L << 20, Math.min(32L << 20, dataLen / (threads * 4L) + 1));
            int numRanges = (int) Math.max(0, (dataLen + rangeSize - 1) / rangeSize);
            AtomicLong rowsIn = new AtomicLong(), rowsOut = new AtomicLong();

            System.err.printf("transform %s -> %s (%s, %d threads)%n", in, out, Util.bytes(size), threads);
            try (Pipeline.Progress prog = new Pipeline.Progress("transform", dataLen)) {
                Pipeline.ordered(numRanges, threads, r -> {
                    long s = dataStart + r * rangeSize;
                    long e = Math.min(size, s + rangeSize);
                    LineReader lr = new LineReader(ch, s, e, 4 << 20);
                    Util.ByteSink o = new Util.ByteSink((int) Math.min(rangeSize, 1 << 24));
                    int[] st = new int[4096], en = new int[4096];
                    long rin = 0, rout = 0;
                    while (lr.next()) {
                        if (lr.le == lr.ls) continue;
                        rin++;
                        if (plan.apply(lr.buf, lr.ls, lr.le, st, en, o)) rout++;
                    }
                    rowsIn.addAndGet(rin);
                    rowsOut.addAndGet(rout);
                    prog.add(e - s);
                    return o;
                }, (i, o) -> {
                    ByteBuffer bb = ByteBuffer.wrap(o.a, 0, o.n);
                    long pos = outPos[0];
                    while (bb.hasRemaining()) pos += oc.write(bb, pos);
                    outPos[0] = pos;
                });
            }
            double sec = (System.nanoTime() - t0) / 1e9;
            System.err.printf("done: %,d rows in, %,d rows out in %s (%s/s)%n", rowsIn.get(), rowsOut.get(), Util.dur(sec),
                    Util.bytes((long) (size / sec)));
        }
    }

    private static Plan buildPlan(Args a, Csv.Header hdr, byte delim) {
        Plan p = new Plan();
        p.delim = delim;
        List<String> names = hdr.names();
        int ncols = names.size();

        // raw header fields (to preserve original quoting when not renamed)
        int[] hs = new int[4096], he = new int[4096];
        int hn = Csv.split(hdr.raw(), 0, hdr.raw().length, delim, hs, he);

        Map<String, String> rename = new HashMap<>();
        if (a.has("rename")) {
            for (String pair : a.str("rename", "").split(",")) {
                if (pair.isBlank()) continue;
                int c = pair.indexOf(':');
                if (c <= 0) throw new IllegalArgumentException("--rename expects old:new[,old:new...]");
                rename.put(pair.substring(0, c).trim(), pair.substring(c + 1).trim());
            }
        }

        if (a.has("select")) {
            String[] sel = a.str("select", "").split(",");
            p.select = new int[sel.length];
            for (int i = 0; i < sel.length; i++) p.select[i] = Csv.columnIndex(names, sel[i].trim());
        }

        p.caseMode = new byte[ncols];
        for (String c : a.str("upper", "").split(",")) if (!c.isBlank()) { p.caseMode[Csv.columnIndex(names, c.trim())] = 1; p.anyCase = true; }
        for (String c : a.str("lower", "").split(",")) if (!c.isBlank()) { p.caseMode[Csv.columnIndex(names, c.trim())] = 2; p.anyCase = true; }
        p.trim = a.bool("trim");
        p.passthrough = p.select == null && !p.trim && !p.anyCase;

        List<Filter> fl = new ArrayList<>();
        for (String f : a.list("filter")) fl.add(parseFilter(f, names));
        p.filters = fl.toArray(new Filter[0]);

        // output header
        Util.ByteSink hb = new Util.ByteSink(256);
        int cnt = p.select == null ? ncols : p.select.length;
        for (int k = 0; k < cnt; k++) {
            int c = p.select == null ? k : p.select[k];
            if (k > 0) hb.put(delim);
            String nm = names.get(c);
            if (rename.containsKey(nm)) hb.put(rename.get(nm).getBytes(StandardCharsets.UTF_8));
            else if (c < hn) hb.put(hdr.raw(), hs[c], he[c]);
        }
        p.header = Arrays.copyOf(hb.a, hb.n);
        return p;
    }

    private static Filter parseFilter(String f, List<String> names) {
        int i = 0;
        for (; i < f.length(); i++) {
            char c = f.charAt(i);
            if (c == '>' || c == '<' || c == '=' || c == '!' || c == '~') break;
        }
        if (i == 0 || i >= f.length()) throw new IllegalArgumentException("bad --filter '" + f + "' (use col>=10, col=US, col!=x, col~text)");
        String col = f.substring(0, i).trim();
        int op;
        int len;
        String two = f.substring(i, Math.min(f.length(), i + 2));
        switch (two) {
            case ">=" -> { op = GE; len = 2; }
            case "<=" -> { op = LE; len = 2; }
            case "!=" -> { op = NE; len = 2; }
            case "==" -> { op = EQ; len = 2; }
            default -> {
                len = 1;
                switch (f.charAt(i)) {
                    case '>' -> op = GT;
                    case '<' -> op = LT;
                    case '=' -> op = EQ;
                    case '~' -> op = CT;
                    default -> throw new IllegalArgumentException("bad operator in --filter '" + f + "'");
                }
            }
        }
        String lit = f.substring(i + len).trim();
        boolean forceString = lit.length() >= 2 && lit.startsWith("\"") && lit.endsWith("\"");
        if (forceString) lit = lit.substring(1, lit.length() - 1);
        byte[] lb = lit.getBytes(StandardCharsets.UTF_8);
        double num = forceString || op == CT ? Double.NaN : Csv.number(lb, 0, lb.length);
        return new Filter(Csv.columnIndex(names, col), op, !Double.isNaN(num), num, lb);
    }
}
