package com.scaleetl;

import java.util.ArrayDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/** Small concurrency helpers: unordered fan-out, long-lived workers, and ordered map-then-sink. */
final class Pipeline {
    private Pipeline() {}

    interface IntTask { void run(int i) throws Exception; }
    interface WorkerTask { void run(int worker) throws Exception; }
    interface Work<R> { R run(int i) throws Exception; }
    interface Sink<R> { void accept(int i, R r) throws Exception; }

    /** Runs tasks 0..n-1 on up to {@code threads} threads, in no particular order. */
    static void each(int n, int threads, IntTask t) throws Exception {
        AtomicInteger next = new AtomicInteger();
        workers(Math.max(1, Math.min(threads, n)), w -> {
            int i;
            while ((i = next.getAndIncrement()) < n) t.run(i);
        });
    }

    /** Runs {@code threads} workers; the first failure stops the rest (cooperatively) and is rethrown. */
    static void workers(int threads, WorkerTask t) throws Exception {
        AtomicReference<Throwable> err = new AtomicReference<>();
        Thread[] ts = new Thread[threads];
        for (int k = 0; k < threads; k++) {
            final int id = k;
            ts[k] = new Thread(() -> {
                try { t.run(id); } catch (Throwable e) { err.compareAndSet(null, e); }
            }, "scaleetl-" + k);
            ts[k].start();
        }
        for (Thread th : ts) th.join();
        Throwable e = err.get();
        if (e instanceof Exception ex) throw ex;
        if (e instanceof Error er) throw er;
    }

    /** Computes tasks in parallel but hands results to {@code sink} strictly in index order, with bounded memory. */
    static <R> void ordered(int n, int threads, Work<R> work, Sink<R> sink) throws Exception {
        ExecutorService ex = Executors.newFixedThreadPool(threads, r -> {
            Thread t = new Thread(r, "scaleetl-ordered");
            t.setDaemon(true);
            return t;
        });
        try {
            int window = threads * 2;
            ArrayDeque<Future<R>> q = new ArrayDeque<>();
            int submitted = 0;
            while (submitted < n && q.size() < window) { final int k = submitted++; q.add(ex.submit(() -> work.run(k))); }
            for (int i = 0; i < n; i++) {
                R r = get(q.poll());
                sink.accept(i, r);
                if (submitted < n) { final int k = submitted++; q.add(ex.submit(() -> work.run(k))); }
            }
        } finally {
            ex.shutdownNow();
        }
    }

    private static <R> R get(Future<R> f) throws Exception {
        try {
            return f.get();
        } catch (ExecutionException e) {
            Throwable c = e.getCause();
            if (c instanceof Exception ex) throw ex;
            if (c instanceof Error er) throw er;
            throw e;
        }
    }

    /** Periodic progress line on stderr. */
    static final class Progress implements AutoCloseable {
        private final LongAdder done = new LongAdder();
        private final long total;
        private final String label;
        private final long t0 = System.nanoTime();
        private final Thread th;

        Progress(String label, long total) {
            this.label = label;
            this.total = total;
            th = new Thread(() -> {
                try {
                    for (;;) { Thread.sleep(3000); print(); }
                } catch (InterruptedException ignored) { }
            }, "scaleetl-progress");
            th.setDaemon(true);
            th.start();
        }

        void add(long b) { done.add(b); }

        private void print() {
            long d = done.sum();
            double s = Math.max(1e-3, (System.nanoTime() - t0) / 1e9);
            double rate = d / s;
            String eta = (total > 0 && rate > 0) ? Util.dur(Math.max(0, (total - d) / rate)) : "?";
            System.err.printf("[%s] %s / %s (%d%%)  %s/s  eta %s%n", label, Util.bytes(d), Util.bytes(total),
                    total > 0 ? Math.min(100, d * 100 / total) : 0, Util.bytes((long) rate), eta);
        }

        @Override public void close() { th.interrupt(); }
    }
}
