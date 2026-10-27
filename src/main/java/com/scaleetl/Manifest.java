package com.scaleetl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/** Describes a partitioned dataset directory (written by {@code partition}, read by {@code load} / {@code search}). */
final class Manifest {
    static final String FILE = "_manifest.properties";

    int partitions;
    String keyColumn;
    int keyIndex;
    byte delim;
    long rows;
    long[] partRows;

    static Path partFile(Path dir, int p) { return dir.resolve(String.format("part-%05d.csv", p)); }
    static Path idxFile(Path dir, int p) { return dir.resolve(String.format("part-%05d.idx", p)); }

    static boolean exists(Path dir) { return Files.isRegularFile(dir.resolve(FILE)); }

    void save(Path dir) throws IOException {
        Properties p = new Properties();
        p.setProperty("format", "scaleetl-1");
        p.setProperty("partitions", Integer.toString(partitions));
        p.setProperty("keyColumn", keyColumn);
        p.setProperty("keyIndex", Integer.toString(keyIndex));
        p.setProperty("delimiter", Integer.toString(delim & 0xff));
        p.setProperty("rows", Long.toString(rows));
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < partRows.length; i++) sb.append(i == 0 ? "" : ",").append(partRows[i]);
        p.setProperty("partitionRows", sb.toString());
        try (OutputStream os = Files.newOutputStream(dir.resolve(FILE))) {
            p.store(os, "ScaleETL partition manifest");
        }
    }

    static Manifest load(Path dir) throws IOException {
        if (!exists(dir)) throw new IOException("no " + FILE + " in " + dir + " (run `scaleetl partition` first)");
        Properties p = new Properties();
        try (InputStream is = Files.newInputStream(dir.resolve(FILE))) { p.load(is); }
        Manifest m = new Manifest();
        m.partitions = Integer.parseInt(p.getProperty("partitions"));
        m.keyColumn = p.getProperty("keyColumn");
        m.keyIndex = Integer.parseInt(p.getProperty("keyIndex"));
        m.delim = (byte) Integer.parseInt(p.getProperty("delimiter"));
        m.rows = Long.parseLong(p.getProperty("rows"));
        String[] pr = p.getProperty("partitionRows", "").split(",");
        m.partRows = new long[m.partitions];
        for (int i = 0; i < pr.length && i < m.partitions; i++) if (!pr[i].isEmpty()) m.partRows[i] = Long.parseLong(pr[i]);
        return m;
    }
}
