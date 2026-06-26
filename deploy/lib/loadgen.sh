#!/usr/bin/env bash
# Reusable Postgres load generator for the CDC pipeline.
#
# Source this file (it depends on pg_exec from lib/verify.sh) and call
# generate_load in a background subshell; trap its PID for cleanup:
#
#   source "$SCRIPT_DIR/lib/verify.sh"
#   source "$SCRIPT_DIR/lib/loadgen.sh"
#   generate_load "demo-load" 0.5 & GEN_PID=$!
#   ...
#   kill "$GEN_PID"; cleanup_load "demo-load"
#
# Functions only; no side effects on load.

# generate_load <email-prefix> [pace-seconds] runs a steady stream of
# inserts/updates/deletes against the demo schema until the process is killed.
# Each round inserts a sentinel customer + order, updates the order's
# status/amount, and every 5th round deletes an older sentinel order -- so c/u/d
# all flow continuously. Every row is tagged with <email-prefix> so cleanup_load
# can remove exactly what this generator created, leaving the seed data intact.
#
# Errors are swallowed (set +e) so a transient failure -- e.g. while a worker is
# being killed in a resilience test -- never aborts the generator.
generate_load() {
  local prefix=${1:?generate_load: email prefix required}
  local pace=${2:-0.2}
  (
    set +e
    local i=0 email old
    while true; do
      i=$((i + 1))
      email="${prefix}-${i}@example.com"
      pg_exec "INSERT INTO public.customers (email, full_name, country)
               VALUES ('$email', 'Load $i', 'US')
               ON CONFLICT (email) DO NOTHING;" >/dev/null 2>&1
      pg_exec "INSERT INTO public.orders (customer_id, status, total_amount, currency)
               SELECT id, 'pending', 10.00, 'USD' FROM public.customers
               WHERE email = '$email';" >/dev/null 2>&1
      pg_exec "UPDATE public.orders SET status = 'paid', total_amount = 20.00 + $i, updated_at = now()
               WHERE customer_id = (SELECT id FROM public.customers WHERE email = '$email');" >/dev/null 2>&1
      if [ $((i % 5)) -eq 0 ] && [ "$i" -gt 5 ]; then
        old="${prefix}-$((i - 5))@example.com"
        pg_exec "DELETE FROM public.orders
                 WHERE customer_id = (SELECT id FROM public.customers WHERE email = '$old');" >/dev/null 2>&1
      fi
      sleep "$pace"
    done
  )
}

# cleanup_load <email-prefix> removes every row generate_load created under that
# prefix, so a rerun against the same volume starts from the seed story.
cleanup_load() {
  local prefix=${1:?cleanup_load: email prefix required}
  pg_exec "DELETE FROM public.orders
           WHERE customer_id IN (SELECT id FROM public.customers
                                 WHERE email LIKE '${prefix}-%');" >/dev/null 2>&1
  pg_exec "DELETE FROM public.customers
           WHERE email LIKE '${prefix}-%';" >/dev/null 2>&1
}
