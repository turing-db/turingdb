#!/usr/bin/env bash
#
# Benchmark quantified-path edge type filtering on reactome.
#
# Compares typed versus untyped variable-length path queries and
# checks that untyped queries stay flat after the feature lands.
#
# Prerequisites:
#   - turingdb built and on PATH (source setup.sh)
#   - reactome graph loaded (LOAD JSONL 'reactome.jsonl')
#
# Examples:
#   ./bench_qpp.sh

set -e -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PORT=6668
DATABASE="reactome"
WARMUP_ROUNDS=1
BENCH_ROUNDS=5
CURL_TIMEOUT=180

QUERY_NAMES=(
    "untyped_forward_2"
    "typed_hasevent_forward_2"
    "typed_hasevent_forward_3"
    "typed_hasevent_forward_4"
    "typed_species_forward_2"
    "typed_hascomponent_forward_2"
    "untyped_undirected_2"
    "typed_hasevent_undirected_2"
)

QUERIES=(
    "MATCH (a:Pathway)-[e]->{1,2}(b) RETURN count(*) AS c"
    "MATCH (a:Pathway)-[e:hasEvent]->{1,2}(b) RETURN count(*) AS c"
    "MATCH (a:Pathway)-[e:hasEvent]->{1,3}(b) RETURN count(*) AS c"
    "MATCH (a:Pathway)-[e:hasEvent]->{1,4}(b) RETURN count(*) AS c"
    "MATCH (a:Pathway)-[e:species]->{1,2}(b) RETURN count(*) AS c"
    "MATCH (a:Complex)-[e:hasComponent]->{1,2}(b) RETURN count(*) AS c"
    "MATCH (a:Pathway)-[e]-{1,2}(b) RETURN count(*) AS c"
    "MATCH (a:Pathway)-[e:hasEvent]-{1,2}(b) RETURN count(*) AS c"
)

stop_turingdb() {
    pkill -9 turingdb 2>/dev/null || true
    for _ in $(seq 1 50); do
        nc -z 127.0.0.1 $PORT 2>/dev/null || break
        sleep 0.1
    done
    sleep 0.3
}

start_turingdb() {
    turingdb start -demon -p $PORT -load "$DATABASE" -start-timeout 30000
    if ! nc -z 127.0.0.1 $PORT 2>/dev/null; then
        echo "ERROR: TuringDB failed to start on port $PORT" >&2
        exit 1
    fi
    sleep 1
}

run_query() {
    local query="$1"
    local resp

    resp=$(curl -s --max-time $CURL_TIMEOUT \
        -X POST "http://127.0.0.1:${PORT}/query?graph=${DATABASE}" \
        -d "$query" 2>&1) || { echo "TIMEOUT"; return 0; }

    if echo "$resp" | grep -q '"error"'; then
        echo "ERROR: $(echo "$resp" | grep -o '"error_details":"[^"]*"')" >&2
        echo "ERROR"
        return 0
    fi

    echo "$resp" | grep -o '"time":[0-9.]*' | head -1 | cut -d: -f2
}

bench_query() {
    local name="$1"
    local query="$2"
    local times=()
    local t

    for _ in $(seq 1 $WARMUP_ROUNDS); do
        t=$(run_query "$query") || true
    done

    for _ in $(seq 1 $BENCH_ROUNDS); do
        t=$(run_query "$query")
        if [ "$t" = "TIMEOUT" ] || [ "$t" = "ERROR" ]; then
            _LAST_MEDIAN="FAILED"
            printf "  %-35s  FAILED (%s)\n" "$name" "$t"
            return
        fi
        times+=("$t")
    done

    IFS=$'\n' sorted=($(sort -g <<<"${times[*]}")); unset IFS

    local min=${sorted[0]}
    local max=${sorted[$((${#sorted[@]} - 1))]}
    local mid_idx=$(( ${#sorted[@]} / 2 ))
    local median=${sorted[$mid_idx]}
    _LAST_MEDIAN="$median"

    printf "  %-35s  min=%10s ms  median=%10s ms  max=%10s ms\n" \
        "$name" "$min" "$median" "$max"
}

REPORT_DIR="$SCRIPT_DIR/reports"
mkdir -p "$REPORT_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
REPORT_FILE="$REPORT_DIR/qpp_queries_${TIMESTAMP}.txt"

echo ""
echo "=== Quantified-Path Edge Type Filter Benchmark ==="
echo "Database:      $DATABASE"
echo "Warmup:        $WARMUP_ROUNDS round(s)"
echo "Bench rounds:  $BENCH_ROUNDS"
echo "Timeout:       ${CURL_TIMEOUT}s per query"
echo "Timestamp:     $TIMESTAMP"
echo ""
echo "Comparisons:"
echo "  - untyped vs typed at equal hops (expect typed faster)"
echo "  - typed at {1,2}/{1,3}/{1,4} (expect sub-superlinear growth)"
echo "  - high-selectivity (species) vs low-selectivity (hasEvent)"
echo "  - undirected typed vs untyped"
echo ""

{
echo "--------------------------------------------------------------"
echo "  Quantified-path edge type filter queries"
echo "--------------------------------------------------------------"
stop_turingdb
start_turingdb

MEDIANS=()
num_queries=${#QUERIES[@]}
for i in $(seq 0 $((num_queries - 1))); do
    bench_query "${QUERY_NAMES[$i]}" "${QUERIES[$i]}"
    MEDIANS+=("$_LAST_MEDIAN")
done

stop_turingdb

TABLE_LINE="+-----------------------------------+----------------------+"
echo ""
echo "$TABLE_LINE"
printf "| %-33s | %20s |\n" "Query" "Median (ms)"
echo "$TABLE_LINE"
for i in $(seq 0 $((num_queries - 1))); do
    printf "| %-33s | %20s |\n" "${QUERY_NAMES[$i]}" "${MEDIANS[$i]}"
done
echo "$TABLE_LINE"

echo ""
echo "Interpretation checklist:"
echo "  1. typed_hasevent_forward_2 << untyped_forward_2  (frontier prune)"
echo "  2. typed_species_forward_2   << typed_hasevent_forward_2  (selectivity)"
echo "  3. forward_2 < forward_3 < forward_4 without cliff growth"
echo "  4. Re-run untyped_forward_2 against a pre-change binary; must be flat"
echo ""
echo "--------------------------------------------------------------"
echo "  Done. Report: $REPORT_FILE"
echo "--------------------------------------------------------------"

} 2>&1 | tee "$REPORT_FILE"
