# ScaleETL

![CI](https://github.com/APMahajan/scaleetl/actions/workflows/ci.yml/badge.svg)

High-performance, dependency-free Java 21 CLI to **partition, transform, load and search** very large CSV files.

```
./build.sh                      # javac + jar, no Maven needed
./bin/scaleetl help
./test.sh 2000000               # end-to-end correctness test against sort/awk
```

## Pipeline

```bash
# 1. hash-partition by key (N files, rows with the same key land in the same file)
scaleetl partition --in big.csv --out parts/ --key user_id            # --partitions auto (~256 MB each)

# 2. transform (works on a file or a whole partition directory)
scaleetl transform --in parts/ --out clean/ \
    --filter 'amount>=100' --filter 'status=active' \
    --select id,user_id,amount --rename amount:amount_inr --trim

# 3. load: build per-partition hash indexes ...
scaleetl load --dir parts/
#    ... or stream into PostgreSQL (needs psql on PATH; table must exist)
scaleetl load --dir parts/ --postgres "postgresql://user:pw@host/db" --table events

# 4. search
scaleetl search --dir parts/ --key 123456 --header                    # indexed, ~30 ms
scaleetl search --in big.csv --col country --value JP --count         # parallel scan
scaleetl search --in parts/ --contains "@example.com" --limit 10
```

## How it is fast

- **No `String`s on the hot path.** Rows stay as `byte[]` ranges; fields are located and hashed in place.
- **Parallel byte-range splitting.** The file is cut into ranges; each thread owns the lines that *start* in its range (`LineReader`), so there is no coordination and no pre-scan.
- **Batched partition writes.** Each worker buffers rows per partition and appends full buffers under a per-partition lock.
- **Ordered parallel transform.** Ranges are processed in parallel, results are written back in input order with bounded memory.
- **O(1) search.** `load` writes an open-addressing hash table (key hash -> byte offset) per partition; a lookup is hash -> partition -> ~1-2 `pread`s -> one row read.

## Measured (2 vCPU / 7 GB sandbox, 20M rows, 1.8 GB)

| step | time | throughput |
|---|---|---|
| partition (64 parts) | 4.9 s | ~377 MB/s, 4.1M rows/s |
| load (index) | 4.1 s | ~448 MB/s |
| transform (filter+select) | 3.4 s | ~535 MB/s |
| scan search | 1.7 s | ~1 GB/s |
| indexed search | ~30 ms | - |

Linear extrapolation to 1 billion rows (~90 GB) on this class of machine is roughly 4 min partition,
3-4 min index. Real numbers depend on disk speed (the page cache helped here); measure on your hardware.

## Limits / conventions

- Records must not contain **embedded newlines** (quoted delimiters and `""` escapes are fine).
- CRLF input is normalised to LF on output.
- Keys containing `"` are unsupported for hash/lookup (quotes are stripped from the key before hashing).
- Hash partitioning only (no range partitioning yet); keep rows/partition under ~20M for indexing, which holds a partition's index in memory (`--partitions` larger, or `SCALEETL_OPTS=-Xmx...`).
- Many partitions = many open files; raise `ulimit -n` for > ~900 partitions.
- `load --postgres` shells out to `psql \copy`; it is **not tested** in this repo's test suite.

## Layout

```
src/main/java/com/scaleetl/
  Main, Args, Util           CLI + helpers
  Csv, LineReader            byte-level parsing, range-aligned line iteration
  Partitioner, Transformer   partition / transform commands
  Loader, Searcher           index build + PostgreSQL load / indexed + scan search
  Manifest, Pipeline         dataset metadata, concurrency helpers
  GenData                    synthetic data generator
```
