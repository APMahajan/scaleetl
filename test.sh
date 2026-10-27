#!/usr/bin/env bash
# End-to-end correctness test. Usage: ./test.sh [rows]   (default 2,000,000)
set -euo pipefail
cd "$(dirname "$0")"
ROWS="${1:-2000000}"
WORK="${SCALEETL_TEST_DIR:-$(mktemp -d)}"
S=./bin/scaleetl
fail() { echo "FAIL: $*"; exit 1; }
ok()   { echo "ok:   $*"; }
q()    { "$@" 2> >(grep -v 'Picked up' >&2); }

echo "== work dir: $WORK  rows: $ROWS"
q $S gen --out "$WORK/src.csv" --rows "$ROWS"
[ "$(wc -l < "$WORK/src.csv")" -eq $((ROWS + 1)) ] || fail "gen row count"
ok "gen"

q $S partition --in "$WORK/src.csv" --out "$WORK/parts" --key user_id --partitions 16
N=$(ls "$WORK"/parts/part-*.csv | wc -l)
[ "$N" -eq 16 ] || fail "expected 16 partitions, got $N"
TOTAL=$(cat "$WORK"/parts/part-*.csv | wc -l)
[ "$TOTAL" -eq $((ROWS + 16)) ] || fail "partition row count $TOTAL"
A=$(tail -n +2 "$WORK/src.csv" | LC_ALL=C sort | md5sum)
B=$(for f in "$WORK"/parts/part-*.csv; do tail -n +2 "$f"; done | LC_ALL=C sort | md5sum)
[ "$A" = "$B" ] || fail "partition content differs from source"
ok "partition (no rows lost/duplicated, content identical)"

# every row of a partition must hash to that partition: a key never appears in two partitions
DUP=$(for f in "$WORK"/parts/part-*.csv; do tail -n +2 "$f" | cut -d, -f2 | LC_ALL=C sort -u; done | LC_ALL=C sort | uniq -d | wc -l)
[ "$DUP" -eq 0 ] || fail "a key appears in more than one partition"
ok "partition locality (each key lives in exactly one partition)"

q $S load --dir "$WORK/parts"
ls "$WORK"/parts/part-*.idx > /dev/null || fail "no index files"
ok "load (index built)"

for UID_ in 1 777 "$((ROWS / 10))" 99999999; do
  EXP=$(tail -n +2 "$WORK/src.csv" | awk -F, -v u="$UID_" '$2==u' | LC_ALL=C sort)
  GOT=$(q $S search --dir "$WORK/parts" --key "$UID_" | LC_ALL=C sort)
  [ "$EXP" = "$GOT" ] || fail "indexed search for user_id=$UID_"
done
ok "indexed search matches awk for 4 keys (incl. a missing key)"

EXP=$(tail -n +2 "$WORK/src.csv" | awk -F, '$3=="JP" && $4=="failed"' | wc -l)
GOT=$(q $S search --in "$WORK/src.csv" --col country --value JP --contains ,failed, --count)
[ "$EXP" -eq "$GOT" ] || fail "scan count: expected $EXP got $GOT"
ok "scan search (--col + --contains) count = $GOT"

EXP=$(tail -n +2 "$WORK/src.csv" | awk -F, '$5>=500 && $4=="active"' | wc -l)
q $S transform --in "$WORK/src.csv" --out "$WORK/t.csv" --filter 'amount>=500' --filter 'status=active' \
  --select id,user_id,amount --rename amount:amount_inr
GOT=$(($(wc -l < "$WORK/t.csv") - 1))
[ "$EXP" -eq "$GOT" ] || fail "transform rows: expected $EXP got $GOT"
[ "$(head -1 "$WORK/t.csv")" = "id,user_id,amount_inr" ] || fail "transform header: $(head -1 "$WORK/t.csv")"
EXP2=$(tail -n +2 "$WORK/src.csv" | awk -F, '$5>=500 && $4=="active"{print $1","$2","$5}' | LC_ALL=C sort | md5sum)
GOT2=$(tail -n +2 "$WORK/t.csv" | LC_ALL=C sort | md5sum)
[ "$EXP2" = "$GOT2" ] || fail "transform content"
ok "transform (filter+select+rename) rows=$GOT, content identical to awk"

# order preservation + quoted-field handling (names like "Smith, John" must stay intact)
q $S transform --in "$WORK/src.csv" --out "$WORK/t2.csv" --upper country --trim
[ "$(wc -l < "$WORK/t2.csv")" -eq $((ROWS + 1)) ] || fail "passthrough transform row count"
diff <(cut -d, -f1 "$WORK/src.csv") <(cut -d, -f1 "$WORK/t2.csv") > /dev/null || fail "order not preserved"
QSRC=$(grep -c '"' "$WORK/src.csv" || true); QOUT=$(grep -c '"' "$WORK/t2.csv" || true)
[ "$QSRC" -eq "$QOUT" ] || fail "quoted rows changed"
ok "transform preserves order and quoted fields ($QSRC quoted rows)"

echo "ALL TESTS PASSED"
[ -n "${SCALEETL_TEST_DIR:-}" ] || rm -rf "$WORK"
