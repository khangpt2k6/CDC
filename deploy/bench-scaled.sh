#!/usr/bin/env bash
# Aggregate throughput benchmark for the scaled (3-worker) CDC pipeline.
# Loads N changes into Postgres in committed chunks and samples the cluster-wide
# sum(cdc_rows_written_total) from Prometheus every second.
#
# Usage:
#   bash deploy/bench-scaled.sh
#   TOTAL=500000 CHUNK=25000 bash deploy/bench-scaled.sh
#
# Prints a per-second table, then a SCALED SUMMARY with avg sustained + peak
# rows/s and end-to-end drain time. Compare to the ~19,000-20,000 rows/s
# single-worker baseline. Requires the 3-worker stack up (docker compose up -d)
# and the connector registered.
set -uo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

TOTAL=${TOTAL:-2000000}
CHUNK=${CHUNK:-50000}
PROM=http://localhost:9090

pg() { docker compose exec -T postgres psql -U cdc -d cdc -v ON_ERROR_STOP=1 -tA -c "$1" 2>&1; }
psum() { curl -s "$PROM/api/v1/query?query=sum(cdc_rows_written_total)" \
           | grep -o '"value":\[[0-9.]*,"[0-9.e+]*"\]' | grep -o '"[0-9.e+]*"\]$' | tr -d '"]'; }

echo "== prep procedure + clean prior bench rows =="
pg "DROP PROCEDURE IF EXISTS bench_load(int,int);" >/dev/null
pg "CREATE PROCEDURE bench_load(total int, chunk int) LANGUAGE plpgsql AS \$\$
DECLARE i int := 0;
BEGIN
  WHILE i < total LOOP
    INSERT INTO public.customers (email, full_name, country)
    SELECT 'sbench-'||g||'@example.com','Bench '||g,'US'
    FROM generate_series(i+1, i+chunk) g
    ON CONFLICT (email) DO NOTHING;
    COMMIT; i := i + chunk;
  END LOOP;
END \$\$;" >/dev/null
pg "DELETE FROM public.customers WHERE email LIKE 'sbench-%';" >/dev/null
sleep 4

base=$(psum); base=${base:-0}
echo "baseline sum(rows_written)=$base"
t0=$(date +%s.%N)
( docker compose exec -T postgres psql -U cdc -d cdc -c "CALL bench_load($TOTAL,$CHUNK);" >/tmp/sb.out 2>&1 ) &

prev=$base; peak=0
printf "%6s %12s %10s\n" elapsed rows_delta per_sec
while true; do
  now=$(date +%s.%N); el=$(awk -v a="$t0" -v b="$now" 'BEGIN{printf "%.1f", b-a}')
  r=$(psum); r=${r:-$prev}
  dr=$(awk -v a="$r" -v b="$base" 'BEGIN{printf "%.0f", a-b}')
  ps=$(awk -v a="$r" -v p="$prev" 'BEGIN{printf "%.0f", a-p}')
  awk -v p="$peak" -v c="$ps" 'BEGIN{exit !(c>p)}' && peak=$ps
  printf "%6s %12s %10s\n" "$el" "$dr" "$ps"
  prev=$r
  awk -v d="$dr" -v t="$TOTAL" 'BEGIN{exit !(d>=t)}' && { echo ">> drained $TOTAL"; break; }
  awk -v e="$el" 'BEGIN{exit !(e>300)}' && { echo ">> timeout"; break; }
  sleep 1
done
t_end=$(date +%s.%N)
echo "================ SCALED SUMMARY ================"
echo "end-to-end ($TOTAL -> ClickHouse): $(awk -v a="$t0" -v b="$t_end" 'BEGIN{printf "%.1fs", b-a}')"
echo "avg sustained: $(awk -v d="$prev" -v b="$base" -v a="$t0" -v e="$t_end" 'BEGIN{printf "%.0f rows/s", (d-b)/(e-a)}')"
echo "peak 1s sample: $peak rows/s"
docker compose exec -T postgres psql -U cdc -d cdc -tAc "DELETE FROM public.customers WHERE email LIKE 'sbench-%';" >/dev/null 2>&1
