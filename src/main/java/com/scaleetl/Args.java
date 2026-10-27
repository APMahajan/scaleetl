package com.scaleetl;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Minimal dependency-free CLI parser: {@code command --flag value --flag=value --bool}. */
final class Args {
    final String command;
    private final Map<String, List<String>> m = new LinkedHashMap<>();

    Args(String[] argv) {
        command = argv.length > 0 ? argv[0] : "help";
        for (int i = 1; i < argv.length; i++) {
            String t = argv[i];
            if (!t.startsWith("--")) throw new IllegalArgumentException("unexpected argument: " + t);
            String k = t.substring(2);
            String v;
            int eq = k.indexOf('=');
            if (eq >= 0) {
                v = k.substring(eq + 1);
                k = k.substring(0, eq);
            } else if (i + 1 < argv.length && !argv[i + 1].startsWith("--")) {
                v = argv[++i];
            } else {
                v = "true";
            }
            m.computeIfAbsent(k, x -> new ArrayList<>()).add(v);
        }
    }

    boolean has(String k) { return m.containsKey(k); }

    String str(String k, String def) {
        List<String> l = m.get(k);
        return l == null ? def : l.get(l.size() - 1);
    }

    String req(String k) {
        String v = str(k, null);
        if (v == null) throw new IllegalArgumentException("missing required option --" + k);
        return v;
    }

    List<String> list(String k) {
        List<String> l = m.get(k);
        return l == null ? List.of() : l;
    }

    int intOr(String k, int def) {
        String v = str(k, null);
        return v == null ? def : Integer.parseInt(v.replace("_", "").replace(",", ""));
    }

    long longOr(String k, long def) {
        String v = str(k, null);
        return v == null ? def : Long.parseLong(v.replace("_", "").replace(",", ""));
    }

    boolean bool(String k) {
        String v = str(k, null);
        return v != null && !v.equalsIgnoreCase("false");
    }
}
