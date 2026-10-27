package com.scaleetl;

import java.util.Arrays;
import java.util.Locale;

final class Util {
    private Util() {}

    static int cpus() { return Runtime.getRuntime().availableProcessors(); }

    static String bytes(long b) {
        if (b < 1024) return b + " B";
        String[] u = {"KB", "MB", "GB", "TB"};
        double v = b;
        int i = -1;
        while (v >= 1024 && i < 3) { v /= 1024; i++; }
        return String.format(Locale.ROOT, "%.1f %s", v, u[i]);
    }

    static String dur(double sec) {
        if (sec < 90) return String.format(Locale.ROOT, "%.1fs", sec);
        long s = (long) sec;
        return String.format(Locale.ROOT, "%dm%02ds", s / 60, s % 60);
    }

    static long parseSize(String s) {
        s = s.trim().toUpperCase(Locale.ROOT);
        long mul = 1;
        if (s.endsWith("KB")) { mul = 1L << 10; s = s.substring(0, s.length() - 2); }
        else if (s.endsWith("MB")) { mul = 1L << 20; s = s.substring(0, s.length() - 2); }
        else if (s.endsWith("GB")) { mul = 1L << 30; s = s.substring(0, s.length() - 2); }
        else if (s.endsWith("B")) { s = s.substring(0, s.length() - 1); }
        return (long) (Double.parseDouble(s.trim()) * mul);
    }

    static byte delim(Args a) {
        String d = a.str("delim", ",");
        if (d.equalsIgnoreCase("tab") || d.equals("\\t")) return '\t';
        if (d.length() != 1) throw new IllegalArgumentException("--delim must be a single character or 'tab'");
        return (byte) d.charAt(0);
    }

    /** Growable byte buffer used to assemble output without per-row allocation. */
    static final class ByteSink {
        byte[] a;
        int n;

        ByteSink(int cap) { a = new byte[Math.max(16, cap)]; }

        void ensure(int extra) {
            if (n + extra > a.length) a = Arrays.copyOf(a, (int) Math.min(Integer.MAX_VALUE - 16, Math.max(a.length * 2L, (long) n + extra)));
        }

        void put(int b) { ensure(1); a[n++] = (byte) b; }

        void put(byte[] s, int from, int to) {
            int l = to - from;
            ensure(l);
            System.arraycopy(s, from, a, n, l);
            n += l;
        }

        void put(byte[] s) { put(s, 0, s.length); }

        void putLong(long v) {
            ensure(21);
            if (v == 0) { a[n++] = '0'; return; }
            if (v < 0) { a[n++] = '-'; v = -v; }
            int st = n;
            while (v > 0) { a[n++] = (byte) ('0' + v % 10); v /= 10; }
            for (int i = st, j = n - 1; i < j; i++, j--) { byte t = a[i]; a[i] = a[j]; a[j] = t; }
        }

        void pad2(int v) { ensure(2); a[n++] = (byte) ('0' + v / 10); a[n++] = (byte) ('0' + v % 10); }
    }
}
