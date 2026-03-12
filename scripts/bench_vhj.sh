#!/usr/bin/env bash
#
# Benchmark value hash join queries on reactome.
#
# Sends each query to TuringDB and reports server-side query time,
# with and without TURING_VALUE_HASH_JOIN.
#
# Prerequisites:
#   - turingdb built and on PATH (source setup.sh)
#   - reactome graph loaded (LOAD JSONL 'reactome.jsonl')
#
# Examples:
#   ./bench_vhj.sh

set -e -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PORT=6667
DATABASE="reactome"
WARMUP_ROUNDS=1
BENCH_ROUNDS=5
CURL_TIMEOUT=120

# ---------------------------------------------------------------------------
# Queries designed to exercise value hash join
# ---------------------------------------------------------------------------
QUERY_NAMES=(
    "pathways_sharing_reaction"
    "shared_complex_components"
    "same_species_pathway_join"
    "cross_species_gene_products"
    "same_compartment_reactions"
    "edge_id_join_hasEvent"
)

QUERIES=(
    # Q1: Different pathways containing the same reaction (stId join)
    "MATCH (a:Pathway)-[:hasEvent]->(r1:ReactionLikeEvent), (b:Pathway)-[:hasEvent]->(r2:ReactionLikeEvent) WHERE r1.stId = r2.stId AND a.dbId <> b.dbId RETURN a.displayName, b.displayName, r1.stId LIMIT 1000"

    # Q2: Complexes sharing a component with the same displayName
    "MATCH (c1:Complex)-[:hasComponent]->(e1:PhysicalEntity), (c2:Complex)-[:hasComponent]->(e2:PhysicalEntity) WHERE e1.displayName = e2.displayName AND c1.dbId <> c2.dbId RETURN c1.displayName, c2.displayName, e1.displayName LIMIT 1000"

    # Q3: Pathways joined by same species (integer hash join on dbId)
    "MATCH (p1:Pathway)-[:species]->(s1:Species), (p2:Pathway)-[:species]->(s2:Species) WHERE s1.dbId = s2.dbId RETURN p1.displayName, p2.displayName, s1.displayName LIMIT 1000"

    # Q4: Gene products with same geneName across different species
    "MATCH (g1:ReferenceGeneProduct)-[:species]->(s1:Species), (g2:ReferenceGeneProduct)-[:species]->(s2:Species) WHERE g1.geneName = g2.geneName AND s1.displayName <> s2.displayName RETURN g1.identifier, g2.identifier, g1.geneName, s1.displayName, s2.displayName LIMIT 1000"

    # Q5: Reactions whose inputs/outputs share a compartment
    "MATCH (r:ReactionLikeEvent)-[:input]->(p:PhysicalEntity)-[:compartment]->(c1:Compartment), (r2:ReactionLikeEvent)-[:output]->(p2:PhysicalEntity)-[:compartment]->(c2:Compartment) WHERE c1.displayName = c2.displayName RETURN r.displayName, r2.displayName, c1.displayName LIMIT 1000"

    # Q6: Join on edge ID – same edge variable across two patterns
    "MATCH (a:Pathway)-[e:hasEvent]->(b), (c)-[e]->(d) RETURN a.displayName, b.displayName LIMIT 1000"
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
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

# Run a single query. Prints the server-reported time in ms.
# Returns 1 if the query errors or times out.
run_query() {
    local query="$1"
    local resp

    resp=$(curl -s --max-time $CURL_TIMEOUT \
        -X POST "http://127.0.0.1:${PORT}/query?graph=${DATABASE}" \
        -d "$query" 2>&1) || { echo "TIMEOUT"; return 0; }

    # Check for error
    if echo "$resp" | grep -q '"error"'; then
        echo "ERROR: $(echo "$resp" | grep -o '"error_details":"[^"]*"')" >&2
        echo "ERROR"
        return 0
    fi

    # Extract server time (in ms)
    echo "$resp" | grep -o '"time":[0-9.]*' | head -1 | cut -d: -f2
}

# Run a query multiple times and report min/median/max.
bench_query() {
    local name="$1"
    local query="$2"
    local times=()
    local t

    # Warmup
    for _ in $(seq 1 $WARMUP_ROUNDS); do
        t=$(run_query "$query") || true
    done

    # Timed runs
    for _ in $(seq 1 $BENCH_ROUNDS); do
        t=$(run_query "$query")
        if [ "$t" = "TIMEOUT" ] || [ "$t" = "ERROR" ]; then
            _LAST_MEDIAN="FAILED"
            printf "  %-35s  FAILED (%s)\n" "$name" "$t"
            return
        fi
        times+=("$t")
    done

    # Sort
    IFS=$'\n' sorted=($(sort -g <<<"${times[*]}")); unset IFS

    local min=${sorted[0]}
    local max=${sorted[$((${#sorted[@]} - 1))]}
    local mid_idx=$(( ${#sorted[@]} / 2 ))
    local median=${sorted[$mid_idx]}
    _LAST_MEDIAN="$median"

    printf "  %-35s  min=%10s ms  median=%10s ms  max=%10s ms\n" \
        "$name" "$min" "$median" "$max"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
REPORT_DIR="$SCRIPT_DIR/reports"
mkdir -p "$REPORT_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
REPORT_FILE="$REPORT_DIR/vhj_queries_${TIMESTAMP}.txt"

echo ""
echo "=== Value Hash Join Query Benchmark ==="
echo "Database:      $DATABASE"
echo "Warmup:        $WARMUP_ROUNDS round(s)"
echo "Bench rounds:  $BENCH_ROUNDS"
echo "Timeout:       ${CURL_TIMEOUT}s per query"
echo "Timestamp:     $TIMESTAMP"
echo ""

{

num_queries=${#QUERIES[@]}

# --- WITHOUT value hash join ---
echo "--------------------------------------------------------------"
echo "  WITHOUT value hash join (CartesianProduct + Filter)"
echo "--------------------------------------------------------------"
stop_turingdb
export TURING_VALUE_HASH_JOIN=0
start_turingdb

MEDIANS_WITHOUT=()
for i in $(seq 0 $((num_queries - 1))); do
    bench_query "${QUERY_NAMES[$i]}" "${QUERIES[$i]}"
    MEDIANS_WITHOUT+=("$_LAST_MEDIAN")
done

stop_turingdb

# --- WITH value hash join ---
echo ""
echo "--------------------------------------------------------------"
echo "  WITH value hash join"
echo "--------------------------------------------------------------"
export TURING_VALUE_HASH_JOIN=1
start_turingdb

MEDIANS_WITH=()
for i in $(seq 0 $((num_queries - 1))); do
    bench_query "${QUERY_NAMES[$i]}" "${QUERIES[$i]}"
    MEDIANS_WITH+=("$_LAST_MEDIAN")
done

stop_turingdb
unset TURING_VALUE_HASH_JOIN

# --- Comparison table ---
TABLE_LINE="+-----------------------------------+----------------------+----------------------+----------+"
echo ""
echo "$TABLE_LINE"
printf "| %-33s | %20s | %20s | %8s |\n" \
    "Query" "Without VHJ (median)" "With VHJ (median)" "Speedup"
echo "$TABLE_LINE"
for i in $(seq 0 $((num_queries - 1))); do
    wo="${MEDIANS_WITHOUT[$i]}"
    wi="${MEDIANS_WITH[$i]}"
    if [ "$wo" = "FAILED" ] || [ "$wi" = "FAILED" ]; then
        printf "| %-33s | %20s | %20s | %8s |\n" \
            "${QUERY_NAMES[$i]}" "$wo" "$wi" "N/A"
    else
        awk -v n="${QUERY_NAMES[$i]}" -v wo="$wo" -v wi="$wi" \
            'BEGIN { printf "| %-33s | %17.3f ms | %17.3f ms | %7.1fx |\n", n, wo, wi, wo/wi }'
    fi
done
echo "$TABLE_LINE"

echo ""
echo "--------------------------------------------------------------"
echo "  Done. Report: $REPORT_FILE"
echo "--------------------------------------------------------------"

} 2>&1 | tee "$REPORT_FILE"
