#include <stdlib.h>
#include <sys/resource.h>

#include <algorithm>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <optional>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <argparse.hpp>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"

#include "QueryCallbacks.h"
#include "QueryConfig.h"
#include "QueryState.h"
#include "QueryStatus.h"
#include "SystemManager.h"
#include "TuringConfig.h"
#include "TuringDB.h"
#include "ID.h"
#include "dataframe/Dataframe.h"
#include "iterators/ChunkConfig.h"

#include "LocalMemory.h"
#include "Path.h"
#include "TuringTime.h"

using namespace db;

namespace {

// What the RETURN projects over the product, which sets how wide one product row is.
enum class Projection {
    Ids,
    Properties,
};

// Which engine runs a product.
enum class Engine {
    V2,
    V3,
};

// One product to benchmark: the labels whose nodes are crossed and what the RETURN
// projects. Sizing goes through the two label counts, so how many rows the product
// makes - and how many of them an engine has to hold at once - is known before it runs.
struct ProductCase {
    std::string_view _name;
    std::string_view _leftLabel;
    std::string_view _rightLabel;
    Projection _projection {Projection::Ids};
    size_t _limit {0};
};

// The products, ordered by the rows they make on reactome. Each crosses two label
// scans with no predicate relating them, so neither engine can turn one into a join:
// v2 runs CartesianProductProcessor and v3 runs nl.cross_product.
constexpr ProductCase productCases[] = {
    {"species_x_species", "Species", "Species", Projection::Ids},
    {"compartment_x_compartment", "Compartment", "Compartment", Projection::Ids},
    {"toplevel_x_toplevel", "TopLevelPathway", "TopLevelPathway", Projection::Ids},
    {"pathway_x_species", "Pathway", "Species", Projection::Ids},
    {"pathway_x_compartment", "Pathway", "Compartment", Projection::Ids},
    {"pathway_x_toplevel", "Pathway", "TopLevelPathway", Projection::Ids},
    {"reaction_x_compartment", "Reaction", "Compartment", Projection::Ids},
    {"physical_entity_x_compartment", "PhysicalEntity", "Compartment", Projection::Ids},
    {"pathway_x_pathway", "Pathway", "Pathway", Projection::Ids},
    {"pathway_x_species_props", "Pathway", "Species", Projection::Properties},
    {"pathway_x_toplevel_props", "Pathway", "TopLevelPathway", Projection::Properties},
    {"reaction_x_reaction", "Reaction", "Reaction", Projection::Ids},
    {"pathway_x_pathway_limited", "Pathway", "Pathway", Projection::Ids, 1000000},
    {"reaction_x_reaction_limited", "Reaction", "Reaction", Projection::Ids, 1000},
};

// How big a case's product is, and how much of it an engine must hold at once. The
// runner reads this before letting the case run.
struct ProductSize {
    size_t _leftRowCount {0};
    size_t _rightRowCount {0};
    size_t _productRowCount {0};
    size_t _peakRowCount {0};
    size_t _peakBytes {0};
};

// What one engine produced for one case over every timed round.
struct BenchResult {
    std::vector<double> _milliseconds;
    size_t _rowCount {0};
    size_t _baseResidentBytes {0};
    size_t _peakResidentBytes {0};
    bool _ok {false};
    bool _skipped {false};
    std::string _error;

    // How far the run pushed the resident size past where it started. The graph itself
    // stays resident throughout and dwarfs every product, so the growth is the only
    // part of the peak that the product accounts for.
    size_t getResidentGrowthBytes() const {
        return _peakResidentBytes > _baseResidentBytes ? _peakResidentBytes - _baseResidentBytes : 0;
    }
};

// Counts result rows without materializing them, so the v3 path pays no output cost
// the v2 path does not.
class CountingSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        _rowCount += rowCount;
    }

    size_t getRowCount() const { return _rowCount; }

private:
    size_t _rowCount {0};
};

// The bytes one product row costs once materialized: the two crossed node ids, plus -
// when the projection reads a property off each side - the nullable string columns
// the projection lays out over the very same rows.
size_t productRowBytes(Projection projection) {
    const size_t idBytes = 2 * sizeof(NodeID);

    if (projection == Projection::Properties) {
        return idBytes + 2 * sizeof(std::optional<std::string_view>);
    }

    return idBytes;
}

// Fills how big a case's product is from the two label counts.
//
// Both engines stream the product a chunk of output at a time - v2 through
// CartesianProductProcessor's cursor, v3 through the loop nl.cross_product drives -
// so whatever the two sides measure, a step holds one chunk.
void computeProductSize(const ProductCase& productCase,
                        size_t leftRowCount,
                        size_t rightRowCount,
                        ProductSize& size) {
    const size_t chunkSize = ChunkConfig::CHUNK_SIZE;
    const size_t rowBytes = productRowBytes(productCase._projection);

    // A LIMIT caps both the rows the query emits and the rows either engine lays out
    // for it: each stops once the budget is spent, and a step lays out only the prefix
    // the budget can still take.
    const size_t limit = productCase._limit > 0 ? productCase._limit : std::numeric_limits<size_t>::max();

    size._leftRowCount = leftRowCount;
    size._rightRowCount = rightRowCount;
    size._productRowCount = std::min(leftRowCount * rightRowCount, limit);

    size._peakRowCount = std::min(size._productRowCount, chunkSize);
    size._peakBytes = size._peakRowCount * rowBytes;
}

// The Cypher for one case: two label scans with nothing relating them, so the whole
// product reaches the projection.
void buildProductQuery(const ProductCase& productCase, std::string& query) {
    query = "MATCH (a:";
    query += productCase._leftLabel;
    query += "), (b:";
    query += productCase._rightLabel;
    query += ") RETURN ";

    if (productCase._projection == Projection::Properties) {
        query += "a.displayName, b.displayName";
    } else {
        query += "a, b";
    }

    if (productCase._limit > 0) {
        query += " LIMIT ";
        query += std::to_string(productCase._limit);
    }
}

// The Cypher that streams one label's nodes; its row count is how many nodes carry it.
void buildCountQuery(std::string_view label, std::string& query) {
    query = "MATCH (n:";
    query += label;
    query += ") RETURN n";
}

// Resets this process's peak-resident-size watermark, so the next reading measures
// what the run following it touched rather than the whole session.
void resetPeakResidentBytes() {
    std::ofstream clearRefs("/proc/self/clear_refs");

    if (clearRefs) {
        clearRefs << "5\n";
    }
}

// The bytes a /proc/self/status field reports, or zero when the kernel does not
// report the field.
size_t statusFieldBytes(std::string_view field) {
    std::ifstream status("/proc/self/status");
    std::string line;

    while (std::getline(status, line)) {
        if (line.rfind(field.data(), 0, field.size()) != 0) {
            continue;
        }

        std::istringstream fields(line);
        std::string name;
        size_t kilobytes = 0;
        fields >> name >> kilobytes;

        return kilobytes * 1024;
    }

    return 0;
}

// This process's peak resident size in bytes since the last reset.
size_t peakResidentBytes() {
    return statusFieldBytes("VmHWM:");
}

// This process's resident size in bytes right now.
size_t residentBytes() {
    return statusFieldBytes("VmRSS:");
}

// Caps this process's address space, so a product bigger than the machine can hold
// fails with std::bad_alloc - which the runner catches and reports - instead of the
// kernel's OOM killer taking the whole benchmark down with it.
void capAddressSpace(size_t bytes) {
    const rlimit limit {bytes, bytes};

    if (setrlimit(RLIMIT_AS, &limit) != 0) {
        throw std::runtime_error("could not cap the address space");
    }
}

// median of a copy-sorted vector of millisecond samples
double median(std::vector<double> samples) {
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

// A double formatted to a fixed number of decimals, as a string.
std::string formatFixed(double value, int decimals) {
    std::ostringstream stream;
    stream << std::fixed << std::setprecision(decimals) << value;
    return stream.str();
}

// A byte count as mebibytes, to one decimal.
std::string formatMebibytes(size_t bytes) {
    return formatFixed(static_cast<double>(bytes) / (1024.0 * 1024.0), 1);
}

// The status name and message of a failed query, as one line.
std::string statusText(const QueryStatus& status) {
    std::string text {QueryStatusDescription::value(status.getStatus())};
    if (status.hasErrorMessage()) {
        text += ": ";
        text += status.getError();
    }

    return text;
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
            const size_t padding = widths[column] - cells[column].size();
            std::cout << " " << cells[column] << std::string(padding + 1, ' ') << "|";
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

// Runs one query through the v2 pipeline once, on a memory of its own so the peak the
// run reaches is its own. Returns its wall time in ms and fills the number of rows
// the callbacks received.
double runV2Once(TuringDB& database,
                 const std::string& graphName,
                 const QueryConfig& queryConfig,
                 const std::string& query,
                 size_t& rowCountOut) {
    LocalMemory memory;

    size_t rowCount = 0;
    QueryCallbacks callbacks;
    callbacks.setOnOutputData([&](const Dataframe* dataframe) {
        rowCount += dataframe->getLogicalRowCount();
    });

    const QueryState state(graphName, &memory, &queryConfig, &callbacks, CommitHash::head(), ChangeID::head());

    const TimePoint start = Clock::now();
    const QueryStatus status = database.query(query, state);
    const TimePoint end = Clock::now();

    if (!status.isOk()) {
        throw std::runtime_error(statusText(status));
    }

    rowCountOut = rowCount;
    return duration<Milliseconds>(start, end);
}

// Runs one query through the v3 MLIR engine once, on a memory of its own. Returns its
// wall time in ms and fills the number of rows the sink received.
double runV3Once(QueryInterpreterV3& interpreter,
                 const std::string& graphName,
                 const std::string& query,
                 size_t& rowCountOut) {
    LocalMemory memory;
    CountingSink sink;

    QueryStatus status;
    const TimePoint start = Clock::now();
    interpreter.execute(status, query, graphName, CommitHash::head(), ChangeID::head(), &memory, &sink);
    const TimePoint end = Clock::now();

    if (!status.isOk()) {
        throw std::runtime_error(statusText(status));
    }

    rowCountOut = sink.getRowCount();
    return duration<Milliseconds>(start, end);
}

// The whole set of runs one engine makes for one case: the warmup rounds thrown away,
// then every timed round, with the peak resident size reached across all of them.
class BenchRunner {
public:
    BenchRunner(TuringDB& database, const std::string& graphName)
        : _database(database),
        _graphName(graphName),
        _v2Config(),
        _v3Interpreter(&database.getSystemManager())
    {
        _v2Config.getPlanGenConfig().setUseValueHashJoin(false);
        _v3Interpreter.setUseValueHashJoin(false);
    }

    const QueryConfig& getV2Config() const { return _v2Config; }

    void run(Engine engine, const std::string& query, size_t warmupRounds, size_t rounds, BenchResult& result) {
        resetPeakResidentBytes();
        result._baseResidentBytes = residentBytes();

        try {
            size_t rowCount = 0;

            for (size_t round = 0; round < warmupRounds; round++) {
                runOnce(engine, query, rowCount);
            }

            for (size_t round = 0; round < rounds; round++) {
                result._milliseconds.push_back(runOnce(engine, query, rowCount));
            }

            result._rowCount = rowCount;
            result._ok = true;
        } catch (const std::exception& e) {
            result._ok = false;
            result._error = e.what();
        }

        result._peakResidentBytes = peakResidentBytes();
    }

private:
    TuringDB& _database;
    const std::string& _graphName;

    QueryConfig _v2Config;
    QueryInterpreterV3 _v3Interpreter;

    double runOnce(Engine engine, const std::string& query, size_t& rowCountOut) {
        if (engine == Engine::V2) {
            return runV2Once(_database, _graphName, _v2Config, query, rowCountOut);
        }

        return runV3Once(_v3Interpreter, _graphName, query, rowCountOut);
    }
};

// The number of nodes carrying a label, read by streaming them and counting the rows.
size_t countLabel(TuringDB& database,
                  const std::string& graphName,
                  const QueryConfig& queryConfig,
                  std::string_view label) {
    std::string query;
    buildCountQuery(label, query);

    size_t rowCount = 0;
    runV2Once(database, graphName, queryConfig, query, rowCount);

    return rowCount;
}

}

int main(int argc, char** argv) {
    argparse::ArgumentParser parser("cartesian_bench");
    parser.add_description("Benchmark the v3 nl.cross_product against the v2 CartesianProductProcessor on the same products");

    std::string turingDirectory;
    std::string graphName;
    std::string caseFilter;
    int iterations = 0;
    int warmupRounds = 0;
    double budgetGigabytes = 0.0;
    double addressSpaceGigabytes = 0.0;
    double maxProductGigarows = 0.0;

    parser.add_argument("-turing-dir")
        .default_value(std::string(""))
        .store_into(turingDirectory);
    parser.add_argument("-graph")
        .default_value(std::string("reactome"))
        .store_into(graphName);
    parser.add_argument("-case")
        .default_value(std::string(""))
        .store_into(caseFilter)
        .help("Benchmark only the cases whose name contains this text");
    parser.add_argument("-iters")
        .default_value(5)
        .scan<'i', int>()
        .store_into(iterations);
    parser.add_argument("-warmup")
        .default_value(1)
        .scan<'i', int>()
        .store_into(warmupRounds);
    parser.add_argument("-budget-gb")
        .default_value(4.0)
        .scan<'g', double>()
        .store_into(budgetGigabytes)
        .help("Skip an engine on a case whose product would make it hold more than this at once");
    parser.add_argument("-address-space-gb")
        .default_value(24.0)
        .scan<'g', double>()
        .store_into(addressSpaceGigabytes)
        .help("Hard address-space cap, so an over-budget product throws instead of being OOM killed");
    parser.add_argument("-max-rows-g")
        .default_value(1.0)
        .scan<'g', double>()
        .store_into(maxProductGigarows)
        .help("Skip a case whose product exceeds this many billion rows, however it is streamed");

    try {
        parser.parse_args(argc, argv);
    } catch (const std::exception& e) {
        std::cerr << e.what() << "\n" << parser;
        return EXIT_FAILURE;
    }

    const double bytesPerGigabyte = 1024.0 * 1024.0 * 1024.0;
    const size_t budgetBytes = static_cast<size_t>(budgetGigabytes * bytesPerGigabyte);
    const size_t maxProductRows = static_cast<size_t>(maxProductGigarows * 1000000000.0);

    try {
        capAddressSpace(static_cast<size_t>(addressSpaceGigabytes * bytesPerGigabyte));

        TuringConfig config;
        if (!turingDirectory.empty()) {
            config.setTuringDirectory(fs::Path(turingDirectory));
        }
        config.setSyncedOnDisk(false);

        TuringDB database(&config);
        database.init();

        {
            LocalMemory memory;
            const QueryConfig& queryConfig = database.getDefaultQueryConfig();
            const QueryCallbacks callbacks;
            const QueryState state("", &memory, &queryConfig, &callbacks);
            const QueryStatus status = database.query("load graph " + graphName, state);
            if (!status.isOk()) {
                throw std::runtime_error("failed to load graph '" + graphName + "': " + statusText(status));
            }
        }

        BenchRunner runner(database, graphName);

        std::vector<ProductCase> cases;
        for (const ProductCase& productCase : productCases) {
            if (caseFilter.empty() || productCase._name.find(caseFilter) != std::string_view::npos) {
                cases.push_back(productCase);
            }
        }

        if (cases.empty()) {
            throw std::runtime_error("no case matches '" + caseFilter + "'");
        }

        std::vector<ProductSize> sizes(cases.size());
        for (size_t caseIndex = 0; caseIndex < cases.size(); caseIndex++) {
            const ProductCase& productCase = cases[caseIndex];

            const size_t leftRowCount = countLabel(database, graphName, runner.getV2Config(), productCase._leftLabel);
            const size_t rightRowCount = productCase._rightLabel == productCase._leftLabel
                ? leftRowCount
                : countLabel(database, graphName, runner.getV2Config(), productCase._rightLabel);

            computeProductSize(productCase, leftRowCount, rightRowCount, sizes[caseIndex]);
        }

        std::cout << "Graph:         " << graphName << "\n";
        std::cout << "Chunk size:    " << ChunkConfig::CHUNK_SIZE << " rows\n";
        std::cout << "Rounds:        " << iterations << " timed, after " << warmupRounds << " warmup\n";
        std::cout << "Budget:        " << formatFixed(budgetGigabytes, 1) << " GiB held at once\n";
        std::cout << "Address space: " << formatFixed(addressSpaceGigabytes, 1) << " GiB capped\n";
        std::cout << "Row cap:       " << maxProductRows << " product rows\n\n";

        std::vector<std::string> sizeHeaders = {"case",
                                                "left rows",
                                                "right rows",
                                                "product rows",
                                                "row bytes",
                                                "holds (MiB)",
                                                "verdict"};
        std::vector<std::vector<std::string>> sizeRows;

        std::vector<bool> runsV2(cases.size(), false);
        std::vector<bool> runsV3(cases.size(), false);

        for (size_t caseIndex = 0; caseIndex < cases.size(); caseIndex++) {
            const ProductCase& productCase = cases[caseIndex];
            const ProductSize& size = sizes[caseIndex];

            const bool overRowCap = size._productRowCount > maxProductRows;
            const bool overBudget = size._peakBytes > budgetBytes;

            runsV2[caseIndex] = !overRowCap && !overBudget;
            runsV3[caseIndex] = !overRowCap && !overBudget;

            std::string verdict = "both run";
            if (overRowCap) {
                verdict = "skipped: over the row cap";
            } else if (overBudget) {
                verdict = "skipped: over budget";
            }

            sizeRows.push_back({std::string(productCase._name),
                                std::to_string(size._leftRowCount),
                                std::to_string(size._rightRowCount),
                                std::to_string(size._productRowCount),
                                std::to_string(productRowBytes(productCase._projection)),
                                formatMebibytes(size._peakBytes),
                                verdict});
        }

        std::cout << "==== What each engine has to hold at once ====\n";
        printAsciiTable(sizeHeaders, sizeRows);
        std::cout << "\n";

        std::vector<BenchResult> v2Results(cases.size());
        std::vector<BenchResult> v3Results(cases.size());

        for (size_t caseIndex = 0; caseIndex < cases.size(); caseIndex++) {
            const ProductCase& productCase = cases[caseIndex];

            std::string query;
            buildProductQuery(productCase, query);

            std::cout << "-- " << productCase._name << "\n";
            std::cout << "   " << query << "\n";

            if (runsV2[caseIndex]) {
                runner.run(Engine::V2,
                           query,
                           static_cast<size_t>(warmupRounds),
                           static_cast<size_t>(iterations),
                           v2Results[caseIndex]);
            } else {
                v2Results[caseIndex]._skipped = true;
            }

            if (runsV3[caseIndex]) {
                runner.run(Engine::V3,
                           query,
                           static_cast<size_t>(warmupRounds),
                           static_cast<size_t>(iterations),
                           v3Results[caseIndex]);
            } else {
                v3Results[caseIndex]._skipped = true;
            }

            const auto report = [](std::string_view label, const BenchResult& result) {
                if (result._skipped) {
                    std::cout << "   " << label << ": skipped\n";
                } else if (result._ok) {
                    std::cout << "   " << label << ": median " << formatFixed(median(result._milliseconds), 2)
                              << " ms, " << result._rowCount << " rows, held "
                              << formatMebibytes(result.getResidentGrowthBytes()) << " MiB\n";
                } else {
                    std::cout << "   " << label << ": FAILED " << result._error << "\n";
                }
            };

            report("v2 product", v2Results[caseIndex]);
            report("v3 product", v3Results[caseIndex]);

            std::cout << "\n";
        }

        std::vector<std::string> timeHeaders = {"case",
                                                "product rows",
                                                "v2 (ms)",
                                                "v3 (ms)",
                                                "v3 speedup",
                                                "v2 rows/ms",
                                                "v3 rows/ms",
                                                "v2 held (MiB)",
                                                "v3 held (MiB)"};
        std::vector<std::vector<std::string>> timeRows;

        for (size_t caseIndex = 0; caseIndex < cases.size(); caseIndex++) {
            const BenchResult& v2Result = v2Results[caseIndex];
            const BenchResult& v3Result = v3Results[caseIndex];

            const double v2Median = v2Result._ok ? median(v2Result._milliseconds) : 0.0;
            const double v3Median = v3Result._ok ? median(v3Result._milliseconds) : 0.0;

            const std::string speedup = v2Result._ok && v3Result._ok && v3Median > 0.0
                ? formatFixed(v2Median / v3Median, 2) + "x"
                : "-";

            const double productRows = static_cast<double>(sizes[caseIndex]._productRowCount);

            timeRows.push_back({std::string(cases[caseIndex]._name),
                                std::to_string(sizes[caseIndex]._productRowCount),
                                v2Result._ok ? formatFixed(v2Median, 2) : "-",
                                v3Result._ok ? formatFixed(v3Median, 2) : "-",
                                speedup,
                                v2Result._ok ? formatFixed(productRows / v2Median, 0) : "-",
                                v3Result._ok ? formatFixed(productRows / v3Median, 0) : "-",
                                v2Result._ok ? formatMebibytes(v2Result.getResidentGrowthBytes()) : "-",
                                v3Result._ok ? formatMebibytes(v3Result.getResidentGrowthBytes()) : "-"});
        }

        std::cout << "==== Median product time, v2 pipeline vs v3 MLIR ====\n";
        printAsciiTable(timeHeaders, timeRows);

    } catch (const std::exception& e) {
        std::cerr << "error: " << e.what() << "\n";
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
