#include <stdint.h>
#include <stdlib.h>
#include <algorithm>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <memory>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <argparse.hpp>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringConfig.h"
#include "TuringDB.h"

#include "Graph.h"
#include "JobSystem.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "iterators/ChunkConfig.h"
#include "iterators/PartDirectory.h"
#include "iterators/PathDistanceIndex.h"
#include "iterators/PathExplorationDir.h"
#include "iterators/PathExplorator.h"
#include "iterators/PathTargetIndex.h"
#include "iterators/ScanNodesByLabelIterator.h"
#include "list/PathTrie.h"
#include "metadata/LabelSet.h"
#include "metadata/LabelSetHandle.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/ChangeAccessor.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitBuilder.h"
#include "versioning/CommitHash.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"

#include "LocalMemory.h"
#include "NLOutputSink.h"
#include "Path.h"
#include "TuringTime.h"

using namespace db;

namespace {

// Counts result rows without materializing them.
class CountingSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        _rowCount += rowCount;
    }

    size_t getRowCount() const { return _rowCount; }

private:
    size_t _rowCount {0};
};

// Median of a vector of millisecond samples, sorting it in place.
double median(std::vector<double>& samples) {
    std::sort(samples.begin(), samples.end());
    const size_t count = samples.size();

    if (count == 0) {
        return 0.0;
    } else if (count % 2 == 1) {
        return samples[count / 2];
    } else {
        return 0.5 * (samples[count / 2 - 1] + samples[count / 2]);
    }
}

// Writes a double formatted to a fixed number of decimals.
void formatFixed(std::string& text, double value, int decimals) {
    std::ostringstream stream;
    stream << std::fixed << std::setprecision(decimals) << value;
    text = stream.str();
}

// Appends a table cell holding a fixed-decimal value and an optional unit suffix.
void appendFixed(std::vector<std::string>& row, double value, int decimals, std::string_view suffix = {}) {
    std::string& cell = row.emplace_back();

    formatFixed(cell, value, decimals);
    cell.append(suffix);
}

// Prints a bordered ASCII table with auto-sized, left-aligned columns.
void printAsciiTable(const std::vector<std::string>& headers,
                     const std::vector<std::vector<std::string>>& rows) {
    const size_t columnCount = headers.size();

    std::vector<size_t> widths(columnCount, 0);
    for (size_t column = 0; column < columnCount; column++) {
        widths[column] = headers[column].size();
    }
    for (const std::vector<std::string>& row : rows) {
        for (size_t column = 0; column < columnCount; column++) {
            widths[column] = std::max(widths[column], row[column].size());
        }
    }

    const auto printSeparator = [&]() {
        std::cout << "+";
        for (size_t column = 0; column < columnCount; column++) {
            std::cout << std::string(widths[column] + 2, '-') << "+";
        }
        std::cout << "\n";
    };

    const auto printRow = [&](const std::vector<std::string>& cells) {
        std::cout << "|";
        for (size_t column = 0; column < columnCount; column++) {
            std::cout << " " << cells[column] << std::string(widths[column] - cells[column].size() + 1, ' ') << "|";
        }
        std::cout << "\n";
    };

    printSeparator();
    printRow(headers);
    printSeparator();
    for (const std::vector<std::string>& row : rows) {
        printRow(row);
    }
    printSeparator();
}

// The shape of the generated graph: every node fans out to `_degree` pseudo-random nodes,
// one node in `_seedStride` carries the seed label S, one in `_endStride` the end label T,
// and one edge in four is of type B rather than A.
struct GraphShape {
    size_t _nodeCount {0};
    size_t _degree {0};
    size_t _seedStride {0};
    size_t _endStride {0};
    uint64_t _randomSeed {0};
};

// What the build leaves behind for the runs: the resolved labels and types and the
// generator's own name for the graph.
struct BenchGraph {
    LabelSet _seedLabels;
    LabelSet _endLabels;
    EdgeTypeID _typeA;
    EdgeTypeID _typeB;
};

// One step of the 64-bit linear congruential generator every pseudo-random choice uses.
uint64_t nextRandom(uint64_t& state) {
    state = state * 6364136223846793005ull + 1442695040888963407ull;
    return state >> 33;
}

// Builds the graph in one commit: nodes first, labels by stride, then the random edges.
void buildGraph(Graph& graph, JobSystem& jobSystem, const GraphShape& shape, BenchGraph& bench) {
    std::unique_ptr<Change> change = graph.newChange();
    CommitBuilder* commit = change->access().getTip();
    DataPartBuilder& builder = commit->newBuilder();
    MetadataBuilder& metadata = builder.getMetadata();

    const LabelID plain = metadata.getOrCreateLabel("N");
    const LabelID seed = metadata.getOrCreateLabel("S");
    const LabelID end = metadata.getOrCreateLabel("T");
    bench._typeA = metadata.getOrCreateEdgeType("A");
    bench._typeB = metadata.getOrCreateEdgeType("B");
    bench._seedLabels = LabelSet::fromList({seed});
    bench._endLabels = LabelSet::fromList({end});

    std::vector<NodeID> nodes;
    nodes.reserve(shape._nodeCount);
    for (size_t node = 0; node < shape._nodeCount; node++) {
        LabelSet labels = LabelSet::fromList({plain});
        if (node % shape._seedStride == 0) {
            labels.set(seed);
        }
        if (node % shape._endStride == 0) {
            labels.set(end);
        }

        nodes.push_back(builder.addNode(labels));
    }

    uint64_t state = shape._randomSeed;
    for (const NodeID source : nodes) {
        for (size_t edge = 0; edge < shape._degree; edge++) {
            const NodeID target = nodes[nextRandom(state) % shape._nodeCount];
            const EdgeTypeID type = nextRandom(state) % 4 == 0 ? bench._typeB : bench._typeA;
            builder.addEdge(type, source, target);
        }
    }

    if (!change->access().submit(jobSystem)) {
        throw std::runtime_error("failed to submit the generated graph");
    }
}

// The nodes carrying the label, as the by-label scan yields them.
void collectNodes(const GraphView& view, const LabelSet& labels, ColumnNodeIDs& nodes) {
    const LabelSetHandle handle(labels);
    ScanNodesByLabelChunkWriter writer(view, handle);

    ColumnNodeIDs chunk;
    writer.setNodeIDs(&chunk);

    nodes.clear();
    while (writer.isValid()) {
        writer.fill(ChunkConfig::CHUNK_SIZE);
        for (const NodeID node : chunk) {
            nodes.push_back(node);
        }
    }
}

// Everything one storage-level run sets on the explorator beyond the seeds and bounds.
struct ExplorationSettings {
    size_t _lookahead {1};
    const LabelSet* _endLabels {nullptr};
    const PathDistanceIndex* _distances {nullptr};
    const ColumnNodeIDs* _endNodes {nullptr};
    const PathTargetIndex* _targets {nullptr};
    bool _distinct {false};
    bool _materializePaths {true};
};

// What one storage-level run produced and cost.
struct ExplorationRun {
    size_t _rows {0};
    size_t _candidateChecks {0};
    double _milliseconds {0.0};
};

// Drives the explorator to exhaustion over the seeds with the given settings.
void timeExploration(const GraphView& view,
                     const ColumnNodeIDs& seeds,
                     uint64_t minHops,
                     uint64_t maxHops,
                     const ExplorationSettings& settings,
                     ExplorationRun& run) {
    ColumnVector<size_t> indices;
    ColumnNodeIDs targets;
    ColumnVector<PathRef> paths;
    PathTrie trie;

    PathExplorator explorator(view, &seeds, PathExplorationDir::FORWARD, minHops, maxHops);
    explorator.setIndices(&indices);
    explorator.setTargets(&targets);
    if (settings._materializePaths && !settings._distinct) {
        explorator.setPaths(&paths, &trie);
    }
    explorator.setEndLabels(settings._endLabels);
    explorator.setDistanceIndex(settings._distances);
    explorator.setEndNodes(settings._endNodes);
    explorator.setTargetIndex(settings._targets);
    explorator.setDistinctEnds(settings._distinct);
    explorator.setCandidateLookahead(settings._lookahead);

    run = ExplorationRun {};

    const TimePoint start = Clock::now();
    while (explorator.isValid()) {
        explorator.fill(ChunkConfig::CHUNK_SIZE);
        run._rows += indices.size();
    }
    const TimePoint end = Clock::now();

    run._milliseconds = duration<Milliseconds>(start, end);
    run._candidateChecks = explorator.getCandidateCheckCount();
}

// Repeats a storage-level run after one warm-up, keeping the median time and the last counts.
void timeExplorationRepeatedly(const GraphView& view,
                               const ColumnNodeIDs& seeds,
                               uint64_t minHops,
                               uint64_t maxHops,
                               const ExplorationSettings& settings,
                               int iterations,
                               ExplorationRun& run) {
    timeExploration(view, seeds, minHops, maxHops, settings, run);

    std::vector<double> samples;
    for (int iteration = 0; iteration < iterations; iteration++) {
        timeExploration(view, seeds, minHops, maxHops, settings, run);
        samples.push_back(run._milliseconds);
    }

    run._milliseconds = median(samples);
}

// Appends the row of one storage-level run: rows, candidate checks and throughput.
void appendRunCells(std::vector<std::string>& row, const ExplorationRun& run) {
    row.push_back(std::to_string(run._rows));
    row.push_back(std::to_string(run._candidateChecks));
    appendFixed(row, run._milliseconds, 2);

    const double seconds = run._milliseconds / 1000.0;
    const double checksPerSecond = seconds > 0.0 ? static_cast<double>(run._candidateChecks) / seconds / 1.0e6 : 0.0;
    appendFixed(row, checksPerSecond, 2);
}

// Runs one Cypher query through the MLIR engine, returning its wall time and row count.
double timeQuery(QueryInterpreterV3& interpreter,
                 const std::string& graphName,
                 LocalMemory& memory,
                 const std::string& query,
                 size_t& rowCountOut) {
    CountingSink sink;
    QueryStatus status;

    const TimePoint start = Clock::now();
    interpreter.execute(status, query, graphName, CommitHash::head(), ChangeID::head(), &memory, &sink);
    const TimePoint end = Clock::now();

    if (!status.isOk()) {
        throw std::runtime_error("query failed: " + query + "\n" + std::string(status.getError()));
    }

    rowCountOut = sink.getRowCount();
    return duration<Milliseconds>(start, end);
}

// Whether the selected section, or all of them, includes the named one.
bool runsSection(const std::string& selected, std::string_view section) {
    return selected == "all" || selected == section;
}

// A graph name no earlier run of the same turing directory can have used.
std::string uniqueGraphName() {
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();

    return "path_bench_" + std::to_string(now);
}

}

int main(int argc, char** argv) {
    argparse::ArgumentParser parser("path_bench");
    parser.add_description("Benchmark variable-length paths on a generated out-of-cache graph: "
                           "the explorator's candidate lookahead, the end-label "
                           "and bound-end pruning indexes against their cost gate, the distinct "
                           "mode, and the same shapes as Cypher through the MLIR engine");

    GraphShape shape;
    size_t maxHops = 0;
    int iterations = 0;
    std::string turingDir;
    std::string section;

    parser.add_argument("-nodes")
        .default_value(size_t {2000000})
        .scan<'u', size_t>()
        .store_into(shape._nodeCount);
    parser.add_argument("-degree")
        .default_value(size_t {8})
        .scan<'u', size_t>()
        .store_into(shape._degree);
    parser.add_argument("-seed-stride")
        .default_value(size_t {2000})
        .scan<'u', size_t>()
        .store_into(shape._seedStride);
    parser.add_argument("-end-stride")
        .default_value(size_t {64})
        .scan<'u', size_t>()
        .store_into(shape._endStride);
    parser.add_argument("-random-seed")
        .default_value(uint64_t {12345})
        .scan<'u', uint64_t>()
        .store_into(shape._randomSeed);
    parser.add_argument("-max-hops")
        .default_value(size_t {3})
        .scan<'u', size_t>()
        .store_into(maxHops);
    parser.add_argument("-iters")
        .default_value(5)
        .scan<'i', int>()
        .store_into(iterations);
    parser.add_argument("-turing-dir")
        .default_value(std::string("path_bench.out"))
        .store_into(turingDir);
    parser.add_argument("-section")
        .default_value(std::string("all"))
        .choices("all", "lookahead", "labels", "bound", "distinct", "cypher")
        .store_into(section);

    try {
        parser.parse_args(argc, argv);
    } catch (const std::exception& error) {
        std::cerr << error.what() << "\n" << parser;
        return EXIT_FAILURE;
    }

    if (shape._nodeCount == 0 || shape._degree == 0 || shape._seedStride == 0 || shape._endStride == 0 || maxHops == 0) {
        std::cerr << "error: nodes, degree, strides and max-hops must be positive\n";
        return EXIT_FAILURE;
    }

    try {
        TuringConfig config;
        config.setTuringDirectory(fs::Path(turingDir));
        config.setSyncedOnDisk(false);

        TuringDB db(&config);
        db.init();

        JobSystem jobSystem;
        jobSystem.init();

        const std::string graphName = uniqueGraphName();
        Graph* graph = nullptr;
        {
            SystemAccessor system = db.getSystemManager().accessUnique();
            graph = system.createGraph(graphName);
        }
        if (!graph) {
            throw std::runtime_error("failed to create the graph");
        }

        std::cout << "Building " << shape._nodeCount << " nodes with " << shape._degree
                  << " out-edges each, a seed every " << shape._seedStride
                  << " nodes and an end every " << shape._endStride << "...\n";

        BenchGraph bench;
        const TimePoint buildStart = Clock::now();
        buildGraph(*graph, jobSystem, shape, bench);
        std::string buildSeconds;
        formatFixed(buildSeconds, duration<Milliseconds>(buildStart, Clock::now()) / 1000.0, 2);
        std::cout << "Built in " << buildSeconds << " s\n\n";

        const FrozenCommitTx transaction = graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView& view = reader.getView();

        ColumnNodeIDs seeds;
        collectNodes(view, bench._seedLabels, seeds);

        ColumnNodeIDs ends;
        collectNodes(view, bench._endLabels, ends);

        std::cout << seeds.size() << " seeds, " << ends.size() << " labelled ends, hops 1 to " << maxHops
                  << ", one warm-up then " << iterations << " iterations per cell (median reported)\n\n";

        const std::vector<std::string> runHeaders {"rows", "candidate checks", "median ms", "M checks/s"};

        // --- Candidate lookahead: how far ahead of the walk a node's data is fetched ---
        if (runsSection(section, "lookahead")) {
            std::vector<std::string> headers {"lookahead"};
            headers.insert(headers.end(), runHeaders.begin(), runHeaders.end());

            std::vector<std::vector<std::string>> rows;
            for (const size_t lookahead : {size_t {0}, size_t {1}, size_t {2}, size_t {4}}) {
                ExplorationSettings settings;
                settings._lookahead = lookahead;

                ExplorationRun run;
                timeExplorationRepeatedly(view, seeds, 1, maxHops, settings, iterations, run);

                std::vector<std::string>& row = rows.emplace_back();
                row.push_back(std::to_string(lookahead));
                appendRunCells(row, run);
            }

            std::cout << "==== Unconstrained walk: candidate lookahead ====\n";
            printAsciiTable(headers, rows);
            std::cout << "\n";
        }

        // --- End labels: the label filter alone against the reverse-distance index ---
        if (runsSection(section, "labels")) {
            std::vector<std::string> headers {"end labels", "index build ms"};
            headers.insert(headers.end(), runHeaders.begin(), runHeaders.end());

            std::vector<std::vector<std::string>> rows;

            ExplorationSettings filtered;
            filtered._endLabels = &bench._endLabels;

            ExplorationRun filteredRun;
            timeExplorationRepeatedly(view, seeds, 1, maxHops, filtered, iterations, filteredRun);

            std::vector<std::string>& filteredRow = rows.emplace_back();
            filteredRow.push_back("filter at emission");
            filteredRow.push_back("-");
            appendRunCells(filteredRow, filteredRun);

            PathDistanceIndex distances;
            const TimePoint indexStart = Clock::now();
            distances.build(view, bench._endLabels, PathExplorationDir::FORWARD, {}, maxHops);
            const double indexMilliseconds = duration<Milliseconds>(indexStart, Clock::now());

            ExplorationSettings pruned = filtered;
            pruned._distances = &distances;

            ExplorationRun prunedRun;
            timeExplorationRepeatedly(view, seeds, 1, maxHops, pruned, iterations, prunedRun);

            std::vector<std::string>& prunedRow = rows.emplace_back();
            prunedRow.push_back("distance index");
            appendFixed(prunedRow, indexMilliseconds, 2);
            appendRunCells(prunedRow, prunedRun);

            const PartDirectory parts(view);
            PathDistanceIndex::SeedExpansion expansion;
            PathDistanceIndex::sampleSeedExpansion(parts, PathExplorationDir::FORWARD, {}, seeds.getRaw(), expansion);
            const double walkChecks = PathDistanceIndex::estimatedEnumerationChecks(parts, expansion, seeds.size(), maxHops);
            PathDistanceIndex gated;
            const bool worthBuilding = gated.buildWithin(view, bench._endLabels, PathExplorationDir::FORWARD, {}, maxHops, walkChecks);

            std::cout << "==== End labels: filter against the reverse-distance index ====\n";
            printAsciiTable(headers, rows);
            std::cout << "Gate verdict for " << seeds.size() << " seeds: " << (worthBuilding ? "build" : "skip")
                      << " (reached " << distances.getReachedCount() << " nodes)\n\n";
        }

        // --- Bound ends: every seed aimed at a pseudo-random node, with and without the target index ---
        if (runsSection(section, "bound")) {
            std::vector<std::string> headers {"bound ends", "index build ms"};
            headers.insert(headers.end(), runHeaders.begin(), runHeaders.end());

            ColumnNodeIDs targets;
            uint64_t state = shape._randomSeed + 1;
            for (size_t row = 0; row < seeds.size(); row++) {
                targets.push_back(NodeID(nextRandom(state) % shape._nodeCount));
            }

            std::vector<NodeID> distinctTargets(targets.begin(), targets.end());
            std::sort(distinctTargets.begin(), distinctTargets.end());
            distinctTargets.erase(std::unique(distinctTargets.begin(), distinctTargets.end()), distinctTargets.end());

            std::vector<std::vector<std::string>> rows;

            ExplorationSettings bound;
            bound._endNodes = &targets;

            ExplorationRun boundRun;
            timeExplorationRepeatedly(view, seeds, 1, maxHops, bound, iterations, boundRun);

            std::vector<std::string>& boundRow = rows.emplace_back();
            boundRow.push_back("filter at emission");
            boundRow.push_back("-");
            appendRunCells(boundRow, boundRun);

            PathTargetIndex targetIndex;
            const TimePoint indexStart = Clock::now();
            targetIndex.build(view, distinctTargets, PathExplorationDir::FORWARD, {}, maxHops);
            const double indexMilliseconds = duration<Milliseconds>(indexStart, Clock::now());

            ExplorationSettings pruned = bound;
            pruned._targets = &targetIndex;

            ExplorationRun prunedRun;
            timeExplorationRepeatedly(view, seeds, 1, maxHops, pruned, iterations, prunedRun);

            std::vector<std::string>& prunedRow = rows.emplace_back();
            prunedRow.push_back("target index");
            appendFixed(prunedRow, indexMilliseconds, 2);
            appendRunCells(prunedRow, prunedRun);

            PathDistanceIndex::SeedExpansion expansion;
            PathDistanceIndex::sampleSeedExpansion(PartDirectory(view), PathExplorationDir::FORWARD, {}, seeds.getRaw(), expansion);
            const bool worthBuilding = PathTargetIndex::isWorthBuilding(view, PathExplorationDir::FORWARD, {}, expansion, seeds.size(), distinctTargets.size(), maxHops);

            std::cout << "==== Bound ends: filter against the target index ====\n";
            printAsciiTable(headers, rows);
            std::cout << "Gate verdict for " << seeds.size() << " seeds and " << distinctTargets.size()
                      << " targets in " << targetIndex.getBatchCount() << " batch(es): "
                      << (worthBuilding ? "build" : "skip") << " (" << (targetIndex.isDense() ? "dense" : "sparse")
                      << " batches reaching " << targetIndex.getReachedCount() << " nodes in all)\n\n";
        }

        // --- Distinct ends: the enumeration against the multi-source search ---
        if (runsSection(section, "distinct")) {
            std::vector<std::string> headers {"mode"};
            headers.insert(headers.end(), runHeaders.begin(), runHeaders.end());

            std::vector<std::vector<std::string>> rows;

            ExplorationSettings enumerating;
            enumerating._materializePaths = false;

            ExplorationRun enumeratedRun;
            timeExplorationRepeatedly(view, seeds, 1, maxHops, enumerating, iterations, enumeratedRun);

            std::vector<std::string>& enumeratedRow = rows.emplace_back();
            enumeratedRow.push_back("enumeration, no path");
            appendRunCells(enumeratedRow, enumeratedRun);

            ExplorationSettings distinct;
            distinct._distinct = true;

            ExplorationRun distinctRun;
            timeExplorationRepeatedly(view, seeds, 1, maxHops, distinct, iterations, distinctRun);

            std::vector<std::string>& distinctRow = rows.emplace_back();
            distinctRow.push_back("distinct (seed, end) pairs");
            appendRunCells(distinctRow, distinctRun);

            std::cout << "==== Distinct ends: enumeration against the multi-source search ====\n";
            printAsciiTable(headers, rows);
            std::cout << "\n";
        }

        // --- The same shapes as Cypher, through the passes and the interpreter ---
        if (runsSection(section, "cypher")) {
            LocalMemory memory;
            QueryInterpreterV3 interpreter(&db.getSystemManager());

            const std::string hops = "{1," + std::to_string(maxHops) + "}";
            const std::vector<std::string> queries {
                "MATCH (n:S)-[e]->" + hops + "(m) RETURN count(*)",
                "MATCH (n:S)-[e:A]->" + hops + "(m) RETURN count(*)",
                "MATCH (n:S)-[e]->" + hops + "(m) RETURN n, e, m",
                "MATCH (n:S)-[e]->" + hops + "(m:T) RETURN count(*)",
                "MATCH (n:S)-[e]->" + hops + "(m) RETURN DISTINCT n, m",
                "MATCH (n:S)-->(b), (n)-[e]->" + hops + "(b) RETURN count(*)",
            };

            std::vector<std::vector<std::string>> rows;
            for (const std::string& query : queries) {
                size_t rowCount = 0;
                timeQuery(interpreter, graphName, memory, query, rowCount);

                std::vector<double> samples;
                for (int iteration = 0; iteration < iterations; iteration++) {
                    samples.push_back(timeQuery(interpreter, graphName, memory, query, rowCount));
                }

                std::vector<std::string>& row = rows.emplace_back();
                row.push_back(query);
                row.push_back(std::to_string(rowCount));
                appendFixed(row, median(samples), 2);
            }

            std::cout << "==== Cypher through the MLIR engine ====\n";
            printAsciiTable({"query", "rows", "median ms"}, rows);
        }

        jobSystem.terminate();
    } catch (const std::exception& error) {
        std::cerr << "error: " << error.what() << "\n";
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
