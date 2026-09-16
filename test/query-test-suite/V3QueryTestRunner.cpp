#include "V3QueryTestRunner.h"

#include <fstream>
#include <span>
#include <string>
#include <vector>

#include <spdlog/fmt/bundled/format.h>

#include "NLOutputSink.h"

#include "BioAssert.h"
#include "TuringException.h"

#include "Graph.h"
#include "ID.h"
#include "QueryConfig.h"
#include "QueryInterpreterV3.h"
#include "QueryResultFormatter.h"
#include "QueryState.h"
#include "QueryStatus.h"
#include "QueryTestRunner.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringConfig.h"
#include "TuringDB.h"
#include "TuringTestEnv.h"
#include "TuringTime.h"
#include "columns/ColumnVector.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

using namespace db;

namespace turing::test {

namespace {

// A LOAD CSV resolves its path inside the data directory of the running instance, which is
// created empty per test: the files a query reads are copied in from the suite's own data
// directory, so the query reads the same records wherever the test runs.
void stageDataFiles(const fs::Path& dataDir) {
    const fs::Path suiteDataDir {QUERY_TEST_SUITE_DATA_DIR};
    if (!suiteDataDir.exists()) {
        return;
    }

    const fs::Result<std::vector<fs::Path>> files = suiteDataDir.listDir();
    if (!files.has_value()) {
        throw TuringException(fmt::format("Cannot list the suite data directory {}", suiteDataDir.get()));
    }

    for (const fs::Path& file : files.value()) {
        std::ifstream source(file.c_str(), std::ios::binary);
        if (!source) {
            throw TuringException(fmt::format("Cannot read the suite data file {}", file.get()));
        }

        const fs::Path target = dataDir / file.filename();
        std::ofstream destination(target.c_str(), std::ios::binary);
        destination << source.rdbuf();
    }
}

class ChangeIDNLSink : public NLOutputSink {
public:
    explicit ChangeIDNLSink(ChangeID& changeID)
        : _changeID(changeID)
    {
    }

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        bioassert(rowCount == 1, "Expected 1 change");

        _changeID = (*static_cast<const ColumnVector<ChangeID>*>(chunks[0]))[offset];
    }

private:
    ChangeID& _changeID;
};

class CollectingNLSink : public NLOutputSink {
public:
    CollectingNLSink(std::vector<std::string>& columnNames,
                     std::vector<std::vector<std::string>>& rows)
        : _columnNames(columnNames),
        _rows(rows)
    {
    }

    void declareOutput(std::span<const std::string_view> names,
                       std::span<const Column* const> chunks) override {
        _columnNames.assign(names.begin(), names.end());
    }

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        QueryResultFormatter::appendChunkRows(_rows, _values, chunks, offset, rowCount);
    }

private:
    std::vector<std::string>& _columnNames;
    std::vector<std::vector<std::string>>& _rows;
    std::vector<std::string> _values;
};

void explainDBProgram(std::string& mlirOutput,
                      QueryInterpreterV3& interpreter,
                      const QueryTestSpec& spec,
                      LocalMemory* mem) {
    const std::string explainQuery = "EXPLAIN(db) " + spec._query;

    std::vector<std::string> columnNames;
    std::vector<std::vector<std::string>> rows;
    CollectingNLSink sink(columnNames, rows);

    QueryStatus status;
    interpreter.execute(status,
                        explainQuery,
                        spec._graphName,
                        CommitHash::head(),
                        ChangeID::head(),
                        mem,
                        &sink);

    if (!status.isOk()) {
        mlirOutput = QueryResultFormatter::formatResultOutput(status, columnNames, rows);
        return;
    }

    mlirOutput.clear();
    for (const std::vector<std::string>& row : rows) {
        mlirOutput += row.back();
    }
}

}

V3QueryTestResult V3QueryTestRunner::runTest(const QueryTestSpec& spec, const fs::Path& outDir) {
    V3QueryTestResult result;
    result._name = spec._name;

    auto env = turing::test::TuringTestEnv::create(outDir);
    stageDataFiles(env->getConfig().getDataDir());

    Graph* graph = nullptr;
    {
        SystemAccessor system = env->getSystemManager().accessUnique();
        graph = system.createGraph(spec._graphName);
    }
    SimpleGraph::createSimpleGraph(graph);
    TuringDB* db = &env->getDB();

    QueryInterpreterV3 interpreter(&env->getSystemManager());

    std::string mlirOutput;
    explainDBProgram(mlirOutput, interpreter, spec, &env->getMem());

    QueryConfig queryConfig;

    ChangeID changeID = ChangeID::head();
    if (spec._writeRequired) {
        ChangeIDNLSink changeNewSink(changeID);
        const QueryState changeNewState(spec._graphName, &env->getMem(), &queryConfig, &changeNewSink);
        db->query("CHANGE NEW", changeNewState);
    }

    std::vector<std::string> columnNames;
    std::vector<std::vector<std::string>> rows;
    CollectingNLSink sink(columnNames, rows);

    QueryStatus status;

    const auto queryStart = Clock::now();
    interpreter.execute(status, spec._query, spec._graphName, CommitHash::head(), changeID, &env->getMem(), &sink);
    const auto queryEnd = Clock::now();

    result._timeUs = static_cast<uint64_t>(duration<Microseconds>(queryStart, queryEnd));

    if (spec._writeRequired) {
        const QueryState submitState(spec._graphName, &env->getMem(), &queryConfig, nullptr,
                                     CommitHash::head(), changeID);
        db->query("CHANGE SUBMIT", submitState);
    }

    QueryTestRunner::normalizeOutput(
        result._resultOutput,
        QueryResultFormatter::formatResultOutput(status, columnNames, rows));
    QueryTestRunner::normalizeOutput(result._mlirOutput, mlirOutput);

    std::string expected;

    QueryTestRunner::normalizeOutput(expected, spec._expectResult);
    result._resultMatched = expected == result._resultOutput;

    QueryTestRunner::normalizeOutput(expected, spec._expectMlir);
    result._mlirMatched = expected == result._mlirOutput;

    return result;
}

}
