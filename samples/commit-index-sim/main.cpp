#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <exception>
#include <string>

#include <argparse.hpp>

#include "Simulator.h"

using namespace db;

namespace {

// Formats a latency in ns, switching to us / ms so columns stay readable.
void formatLatency(double ns, char* out, size_t size) {
    if (ns < 1000.0) {
        snprintf(out, size, "%.1f ns", ns);
    } else if (ns < 1000000.0) {
        snprintf(out, size, "%.2f us", ns / 1000.0);
    } else {
        snprintf(out, size, "%.2f ms", ns / 1000000.0);
    }
}

void formatBytes(double bytes, char* out, size_t size) {
    if (bytes < 1024.0) {
        snprintf(out, size, "%.0f B", bytes);
    } else if (bytes < 1024.0 * 1024.0) {
        snprintf(out, size, "%.1f KB", bytes / 1024.0);
    } else if (bytes < 1024.0 * 1024.0 * 1024.0) {
        snprintf(out, size, "%.1f MB", bytes / (1024.0 * 1024.0));
    } else {
        snprintf(out, size, "%.2f GB", bytes / (1024.0 * 1024.0 * 1024.0));
    }
}

// Renders an integer-valued double with thousands separators.
void formatCount(double value, char* out, size_t size) {
    char raw[64];
    snprintf(raw, sizeof(raw), "%.0f", value);

    const size_t length = strlen(raw);
    size_t written = 0;
    for (size_t i = 0; i < length && written + 1 < size; i++) {
        const size_t remaining = length - i;
        if (i != 0 && remaining % 3 == 0) {
            out[written++] = ',';
            if (written + 1 >= size) {
                break;
            }
        }
        out[written++] = raw[i];
    }
    out[written] = '\0';
}

void printReadRow(const char* mix, const char* design, const ReadStats& stats) {
    char meanBuf[32];
    char medianBuf[32];
    char p99Buf[32];

    formatLatency(stats._meanNs, meanBuf, sizeof(meanBuf));
    formatLatency(stats._medianNs, medianBuf, sizeof(medianBuf));
    formatLatency(stats._p99Ns, p99Buf, sizeof(p99Buf));

    printf("  %-8s %-27s %12s %12s %12s\n", mix, design, meanBuf, medianBuf, p99Buf);
}

void printWriteRow(const char* design, const WriteStats& stats, bool isPageTable) {
    char meanBuf[32];
    char maxBuf[32];
    char pagesBuf[32];
    char bytesBuf[32];

    formatLatency(stats._meanNsPerCommit, meanBuf, sizeof(meanBuf));
    formatLatency(stats._maxNsPerCommit, maxBuf, sizeof(maxBuf));

    if (isPageTable) {
        formatCount(stats._meanRewrittenPages, pagesBuf, sizeof(pagesBuf));
        formatBytes(stats._meanBytesPerCommit, bytesBuf, sizeof(bytesBuf));
    } else {
        snprintf(pagesBuf, sizeof(pagesBuf), "-");
        snprintf(bytesBuf, sizeof(bytesBuf), "-");
    }

    printf("  %-27s %12s %12s %12s %14s\n", design, meanBuf, maxBuf, pagesBuf, bytesBuf);
}

}

int main(int argc, char** argv) {
    argparse::ArgumentParser parser("commit-index-sim", "1.0", argparse::default_arguments::help);
    parser.add_description(
        "Estimates per-operation cost of a per-commit page-table neighborhood directory\n"
        "versus TuringDB's current design of probing every reachable datapart.");

    Workload workload;
    PageTableConfig pageTable;
    LatencyModel model;

    parser.add_argument("--nodes")
        .store_into(workload._numNodes)
        .help("Number of nodes created by the bulk load (default: 1000000)");
    parser.add_argument("--avg-degree")
        .store_into(workload._avgInitialDegree)
        .help("Average out-degree from the bulk load (default: 8)");
    parser.add_argument("--load-dataparts")
        .store_into(workload._loadDataparts)
        .help("Dataparts produced by the bulk load (default: 1)");
    parser.add_argument("--commits")
        .store_into(workload._mutationCommits)
        .help("Number of small mutation commits appended after the load (default: 2000)");
    parser.add_argument("--edges-per-commit")
        .store_into(workload._avgEdgesPerCommit)
        .help("Edges added by each mutation commit (default: 500)");
    parser.add_argument("--patch-fraction")
        .store_into(workload._patchFraction)
        .help("Fraction of commits that patch existing nodes; the rest are append-only "
              "with empty patch maps (default: 1.0; realistic append-heavy: ~0.05-0.2)");
    parser.add_argument("--skew")
        .store_into(workload._skew)
        .help("Edge endpoint skew; >1 concentrates on hub nodes / categories (default: 1.6)");
    parser.add_argument("--read-samples")
        .store_into(workload._readSamples)
        .help("Neighborhood reads sampled per mix (default: 100000)");
    parser.add_argument("--seed")
        .store_into(workload._seed)
        .help("RNG seed for reproducibility (default: 42)");

    parser.add_argument("--level1-bits")
        .store_into(pageTable._level1Bits)
        .help("High NodeID bits indexing the root page directory (default: 16)");
    parser.add_argument("--page-bits")
        .store_into(pageTable._pageBits)
        .help("NodeID bits consumed by each deeper page-table level (default: 8)");

    parser.add_argument("--dram-ns")
        .store_into(model._dramNs)
        .help("Latency of a cache miss to DRAM, ns (default: 90)");
    parser.add_argument("--hash-hit-ns")
        .store_into(model._hashProbeHitNs)
        .help("Latency of a populated hash-map probe, ns (default: 160)");
    parser.add_argument("--hash-miss-ns")
        .store_into(model._hashProbeMissNs)
        .help("Latency of a missing hash-map probe, ns (default: 95)");
    parser.add_argument("--bandwidth")
        .store_into(model._bandwidthBytesPerNs)
        .help("Sequential read bandwidth, bytes/ns (default: 12)");

    try {
        parser.parse_args(argc, argv);
    } catch (const std::exception& error) {
        fprintf(stderr, "%s\n", error.what());
        return EXIT_FAILURE;
    }

    if (workload._numNodes < 2) {
        fprintf(stderr, "--nodes must be at least 2\n");
        return EXIT_FAILURE;
    }
    if (pageTable._pageBits < 1) {
        fprintf(stderr, "--page-bits must be at least 1\n");
        return EXIT_FAILURE;
    }
    if (workload._patchFraction < 0.0 || workload._patchFraction > 1.0) {
        fprintf(stderr, "--patch-fraction must be between 0 and 1\n");
        return EXIT_FAILURE;
    }

    pageTable.compute(workload._numNodes, model);

    CommitIndexSimulator simulator(model, workload, pageTable);
    simulator.run();
    const SimResults& results = simulator.getResults();

    char buf[64];
    char buf2[64];

    printf("=== TuringDB commit-index simulator ===\n");
    printf("per-commit page-table neighborhood directory  vs  current probe-every-datapart design\n\n");

    printf("Workload\n");
    formatCount((double)workload._numNodes, buf, sizeof(buf));
    printf("  nodes ................. %s\n", buf);
    printf("  avg initial degree .... %zu  (load dataparts: %zu)\n",
           workload._avgInitialDegree,
           workload._loadDataparts);
    formatCount((double)workload._mutationCommits, buf, sizeof(buf));
    formatCount((double)workload._avgEdgesPerCommit, buf2, sizeof(buf2));
    printf("  mutation commits ...... %s  x  %s edges/commit\n", buf, buf2);
    printf("  patch fraction ........ %.2f  (rest are append-only, empty patch maps)\n",
           workload._patchFraction);
    printf("  edge skew ............. %.2f\n", workload._skew);
    formatCount((double)workload._readSamples, buf, sizeof(buf));
    printf("  read samples .......... %s per mix\n", buf);
    printf("  seed .................. %llu\n\n", (unsigned long long)workload._seed);

    printf("Page-table geometry\n");
    printf("  effective id bits ..... %zu\n", pageTable._effectiveBits);
    formatBytes((double)pageTable._rootBytes, buf, sizeof(buf));
    printf("  level-1 bits .......... %zu  (root directory: %s)\n", pageTable._level1Bits, buf);
    formatBytes((double)pageTable._innerPageBytes, buf, sizeof(buf));
    printf("  page bits ............. %zu  (inner page: %s)\n", pageTable._pageBits, buf);
    printf("  levels (walk depth) ... %zu\n\n", pageTable._levels);

    printf("Latency assumptions\n");
    printf("  DRAM miss ............. %.0f ns      hash probe hit ... %.0f ns\n",
           model._dramNs,
           model._hashProbeHitNs);
    printf("  L1 / L2 / L3 .......... %.0f / %.0f / %.0f ns   hash probe miss .. %.0f ns\n",
           model._l1Ns,
           model._l2Ns,
           model._l3Ns,
           model._hashProbeMissNs);
    printf("  seq bandwidth ......... %.0f B/ns      page alloc ....... %.0f ns\n\n",
           model._bandwidthBytesPerNs,
           model._allocNs);

    printf("Simulated graph\n");
    formatCount((double)results._patchingCommits, buf, sizeof(buf));
    formatCount((double)workload._mutationCommits, buf2, sizeof(buf2));
    printf("  patching commits ...... %s of %s  (rest append-only)\n", buf, buf2);
    formatCount((double)results._totalEdges, buf, sizeof(buf));
    printf("  total edges ........... %s\n", buf);
    formatCount((double)results._maxDegree, buf, sizeof(buf));
    printf("  avg / max degree ...... %.1f / %s\n", results._avgDegree, buf);
    formatCount((double)results._maxTouchCount, buf, sizeof(buf));
    printf("  avg / max touches ..... %.2f / %s  (mutation commits that patched a node)\n\n",
           results._avgTouchCount,
           buf);

    printf("--- READ: neighborhood lookup (per operation) ---\n");
    printf("  %-8s %-27s %12s %12s %12s\n", "mix", "design", "mean", "median", "p99");
    printReadRow("hot", "current", results._currentRead[0]);
    printReadRow("hot", "page-table (delta)", results._pageTableDeltaRead[0]);
    printReadRow("hot", "page-table (consolidated)", results._pageTableConsolidatedRead[0]);
    printReadRow("uniform", "current", results._currentRead[1]);
    printReadRow("uniform", "page-table (delta)", results._pageTableDeltaRead[1]);
    printReadRow("uniform", "page-table (consolidated)", results._pageTableConsolidatedRead[1]);

    const double hotDeltaSpeedup = results._currentRead[0]._meanNs / results._pageTableDeltaRead[0]._meanNs;
    const double hotConsSpeedup = results._currentRead[0]._meanNs / results._pageTableConsolidatedRead[0]._meanNs;
    const double uniformDeltaSpeedup = results._currentRead[1]._meanNs / results._pageTableDeltaRead[1]._meanNs;
    const double uniformConsSpeedup = results._currentRead[1]._meanNs / results._pageTableConsolidatedRead[1]._meanNs;

    printf("\n  mean read speedup vs current:\n");
    printf("    page-table (delta) ........ hot %.1fx   uniform %.1fx\n",
           hotDeltaSpeedup,
           uniformDeltaSpeedup);
    printf("    page-table (consolidated) . hot %.1fx   uniform %.1fx\n\n",
           hotConsSpeedup,
           uniformConsSpeedup);

    printf("--- WRITE: cost of committing one patching commit ---\n");
    printf("  %-27s %12s %12s %12s %14s\n", "design", "mean", "max", "pages", "index bytes");
    printWriteRow("current", results._currentWrite, false);
    printWriteRow("page-table (delta)", results._pageTableDeltaWrite, true);
    printWriteRow("page-table (consolidated)", results._pageTableConsolidatedWrite, true);

    const double deltaWriteOverhead =
        results._pageTableDeltaWrite._meanNsPerCommit / results._currentWrite._meanNsPerCommit;
    const double consWriteOverhead =
        results._pageTableConsolidatedWrite._meanNsPerCommit / results._currentWrite._meanNsPerCommit;
    printf("\n  write cost vs current:  delta %.2fx   consolidated %.2fx\n\n",
           deltaWriteOverhead,
           consWriteOverhead);

    printf("--- MEMORY: index structure (excludes raw edges, identical for all) ---\n");
    printf("  %-27s %14s %16s\n", "design", "per commit", "total (retained)");
    formatBytes(results._currentWrite._meanBytesPerCommit, buf, sizeof(buf));
    formatBytes(results._currentIndexBytes, buf2, sizeof(buf2));
    printf("  %-27s %14s %16s\n", "current", buf, buf2);
    formatBytes(results._pageTableDeltaWrite._meanBytesPerCommit, buf, sizeof(buf));
    formatBytes(results._pageTableDeltaIndexBytes, buf2, sizeof(buf2));
    printf("  %-27s %14s %16s\n", "page-table (delta)", buf, buf2);
    formatBytes(results._pageTableConsolidatedWrite._meanBytesPerCommit, buf, sizeof(buf));
    formatBytes(results._pageTableConsolidatedIndexBytes, buf2, sizeof(buf2));
    printf("  %-27s %14s %16s\n", "page-table (consolidated)", buf, buf2);
    formatBytes(results._initialIndexBytes, buf, sizeof(buf));
    printf("  (page-table also shares a %s root/inner tree once, retained across commits)\n\n",
           buf);

    // Dynamic, parameter-derived summary so the report stands on its own.
    const double readFloorNs = (double)workload._mutationCommits * model._hashProbeMissNs;
    const double walkNs = model.cacheLatencyForBytes(pageTable._rootBytes)
                        + (double)(pageTable._levels - 1) * model._dramNs;
    const double rootCopyNs = (double)pageTable._rootBytes / model._bandwidthBytesPerNs;
    const double rootCopyShare = 100.0 * rootCopyNs / results._pageTableDeltaWrite._meanNsPerCommit;

    char floorBuf[32];
    char walkBuf[32];
    formatLatency(readFloorNs, floorBuf, sizeof(floorBuf));
    formatLatency(walkNs, walkBuf, sizeof(walkBuf));

    printf("Takeaways\n");
    printf("  - Reads today scan every reachable datapart: a floor of ~%s per read\n",
           floorBuf);
    printf("    (%zu probes x %.0f ns miss), independent of node degree or which part holds the edges.\n",
           workload._mutationCommits,
           model._hashProbeMissNs);
    printf("  - The page-table replaces that with a fixed %zu-level walk (~%s), so the read\n",
           pageTable._levels,
           walkBuf);
    printf("    win grows linearly with commit-history depth (merge/compaction shrinks it).\n");
    printf("  - Page-table writes path-copy ~%.0f pages/commit; rewriting the root alone is\n",
           results._pageTableDeltaWrite._meanRewrittenPages);
    printf("    %.0f%% of the delta-write cost -> keep --level1-bits small for write-heavy loads.\n",
           rootCopyShare);
    printf("  - Consolidated leaves give the best reads but re-copy hub adjacency on every\n");
    printf("    touch (%.2fx write cost); the delta variant avoids that (%.2fx) at the price of\n",
           consWriteOverhead,
           deltaWriteOverhead);
    printf("    chaining per-commit spans on read.\n");

    return EXIT_SUCCESS;
}
