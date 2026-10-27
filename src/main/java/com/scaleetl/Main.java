package com.scaleetl;

public final class Main {
    private Main() {}

    static final String USAGE = """
            ScaleETL - partition, transform, load and search very large CSV files

            USAGE
              scaleetl <command> [options]

            COMMANDS
              partition  Hash-partition a CSV by a key column into N files
                --in FILE --out DIR --key COL [--partitions N|auto] [--delim ,] [--threads T]

              transform  Filter rows / select, rename, trim, upper/lower columns (file or partition dir)
                --in FILE|DIR --out FILE|DIR
                [--select a,b,c] [--rename old:new,...] [--filter 'amount>=100']... [--trim]
                [--upper col,...] [--lower col,...] [--delim ,] [--threads T]
                filter ops: = != > >= < <= ~ (contains); "quoted" literals force string comparison

              load       Build per-partition indexes (default) or bulk-load into PostgreSQL
                --dir DIR [--threads T]
                --dir DIR --postgres CONNINFO --table TABLE      (needs psql on PATH)

              search     Indexed point lookup, or parallel scan
                --dir DIR --key VALUE [--key VALUE...]           indexed lookup on the partition key
                --in FILE|DIR --col COL --value V                scan (uses the index if COL is the partition key)
                --in FILE|DIR --contains TEXT                    substring scan over whole rows
                [--limit N] [--count] [--header] [--threads T]

              gen        Generate synthetic test data
                --out FILE [--rows N] [--seed S] [--threads T]

            NOTES
              * Records must not contain embedded newlines (quoted delimiters are fine).
              * Env: SCALEETL_OPTS overrides JVM flags (e.g. "-Xmx8g -XX:+UseParallelGC").
            """;

    public static void main(String[] argv) {
        try {
            Args a = new Args(argv);
            switch (a.command) {
                case "partition" -> Partitioner.run(a);
                case "transform" -> Transformer.run(a);
                case "load" -> Loader.run(a);
                case "search" -> Searcher.run(a);
                case "gen" -> GenData.run(a);
                case "help", "--help", "-h" -> System.out.print(USAGE);
                default -> {
                    System.err.println("unknown command: " + a.command + "\n");
                    System.err.print(USAGE);
                    System.exit(2);
                }
            }
        } catch (IllegalArgumentException e) {
            System.err.println("error: " + e.getMessage());
            System.exit(2);
        } catch (Throwable e) {
            System.err.println("error: " + e);
            if (System.getenv("SCALEETL_DEBUG") != null) e.printStackTrace();
            System.exit(1);
        }
    }
}
