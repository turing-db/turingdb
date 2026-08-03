#include <gtest/gtest.h>

#include <algorithm>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <range/v3/view/zip.hpp>

#include "columns/ColumnConst.h"
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"
#include "dataframe/Dataframe.h"
#include "dataframe/NamedColumn.h"
#include "metadata/PropertyNull.h"

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"

#include "Graph.h"
#include "QueryConfig.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "versioning/Change.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace rg = ranges;
namespace rv = rg::views;

namespace {

using Row = std::vector<std::string>;
using Rows = std::vector<Row>;

template <typename T>
bool renderValueCell(const Column* column, size_t row, std::string& out) {
    if (const auto* constCol = dynamic_cast<const ColumnConst<T>*>(column)) {
        const T value = constCol->at(0);
        if constexpr (std::is_same_v<T, std::string_view>) {
            out = std::string(value);
        } else {
            out = std::to_string(value);
        }
        return true;
    }

    const auto* values = dynamic_cast<const ColumnOptVector<T>*>(column);
    if (!values) {
        return false;
    }

    const std::optional<T> value = (*values)[row];
    if (!value) {
        out = "null";
        return true;
    }

    if constexpr (std::is_same_v<T, std::string_view>) {
        out = std::string(*value);
    } else {
        out = std::to_string(*value);
    }

    return true;
}

void renderCell(const Column* column, size_t row, std::string& out) {
    if (const auto* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(column)) {
        out = std::to_string((*nodeIDs)[row].getValue());
    } else if (const auto* edgeIDs = dynamic_cast<const ColumnEdgeIDs*>(column)) {
        out = std::to_string((*edgeIDs)[row].getValue());
    } else if (const auto* edgeTypes = dynamic_cast<const ColumnEdgeTypes*>(column)) {
        out = std::to_string((*edgeTypes)[row].getValue());
    } else if (dynamic_cast<const ColumnConst<PropertyNull>*>(column)) {
        out = "null";
    } else if (renderValueCell<int64_t>(column, row, out)
               || renderValueCell<uint64_t>(column, row, out)
               || renderValueCell<double>(column, row, out)
               || renderValueCell<std::string_view>(column, row, out)) {
        // Rendered by the helper for whichever value type matched
    } else {
        throw std::runtime_error("SetEquivalenceTest: unsupported output column type");
    }
}

void collectPipelineRows(const Dataframe* dataframe, Rows& rows) {
    const Dataframe::NamedColumns& columns = dataframe->cols();
    const size_t rowCount = dataframe->getLogicalRowCount();

    for (size_t row = 0; row < rowCount; row++) {
        Row& cells = rows.emplace_back();
        cells.resize(columns.size());

        for (size_t column = 0; column < columns.size(); column++) {
            renderCell(columns[column]->getColumn(), row, cells[column]);
        }
    }
}

// Discards output — the write queries under test produce no rows worth inspecting.
class NullSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const>, size_t, size_t) override {}
};

}

class SetEquivalenceTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        system.createGraph(_v2GraphName);
        system.createGraph(_v3GraphName);

        _db = &_env->getDB();
        _interp3 = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    // Apply a write query to a graph via the v2 pipeline and submit the change.
    void applyV2(std::string_view graphName, std::string_view writeQuery) {
        ChangeID changeID;
        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            auto res = system.newChange(graphName);
            ASSERT_TRUE(res);
            changeID = res.value()->id();
        }

        QueryCallbacks callbacks;
        callbacks.setOnOutputData([](const Dataframe*) {});

        const QueryState writeState(graphName, &_env->getMem(), &_queryConfig, &callbacks, CommitHash::head(), changeID);
        const QueryStatus writeStatus = _db->query(writeQuery, writeState);
        ASSERT_TRUE(writeStatus.isOk()) << "V2 write failed: " << writeQuery;

        const QueryState submitState(graphName, &_env->getMem(), &_queryConfig, &callbacks, CommitHash::head(), changeID);
        const QueryStatus submitStatus = _db->query("CHANGE SUBMIT", submitState);
        ASSERT_TRUE(submitStatus.isOk()) << "V2 CHANGE SUBMIT failed";
    }

    // Apply a write query to a graph via the v3 MLIR executor and submit the change.
    void applyV3(std::string_view graphName, std::string_view writeQuery) {
        ChangeID changeID;
        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            auto res = system.newChange(graphName);
            ASSERT_TRUE(res);
            changeID = res.value()->id();
        }

        NullSink sink;
        QueryStatus writeStatus;
        _interp3->execute(writeStatus, writeQuery, graphName, CommitHash::head(), changeID, &_env->getMem(), &sink);
        ASSERT_TRUE(writeStatus.isOk()) << "V3 write failed: " << writeQuery << " — " << writeStatus.getError();

        QueryCallbacks callbacks;
        callbacks.setOnOutputData([](const Dataframe*) {});

        const QueryState submitState(graphName, &_env->getMem(), &_queryConfig, &callbacks, CommitHash::head(), changeID);
        const QueryStatus submitStatus = _db->query("CHANGE SUBMIT", submitState);
        ASSERT_TRUE(submitStatus.isOk()) << "V3 CHANGE SUBMIT failed";
    }

    // Run a MATCH query via the v2 pipeline and collect results as strings.
    void matchV2(std::string_view graphName, std::string_view matchQuery, Rows& rows) {
        QueryCallbacks callbacks;
        callbacks.setOnOutputData([&rows](const Dataframe* dataframe) {
            ASSERT_TRUE(dataframe != nullptr);
            collectPipelineRows(dataframe, rows);
        });

        const QueryState state(graphName, &_env->getMem(), &_queryConfig, &callbacks);
        const QueryStatus status = _db->query(matchQuery, state);
        ASSERT_TRUE(status.isOk()) << "MATCH query failed: " << matchQuery;
    }

    // Seed both graphs identically (via v2), then apply setQuery via v2 on one graph
    // and via v3 on the other, and finally compare the MATCH read-back. Seeding both
    // sides the same way isolates the SET behaviour as the only difference.
    void expectSetEquivalent(std::string_view seedQuery,
                             std::string_view setQuery,
                             std::string_view matchQuery) {
        applyV2(_v2GraphName, seedQuery);
        applyV2(_v3GraphName, seedQuery);

        applyV2(_v2GraphName, setQuery);
        applyV3(_v3GraphName, setQuery);

        Rows v2Rows;
        matchV2(_v2GraphName, matchQuery, v2Rows);
        Rows v3Rows;
        matchV2(_v3GraphName, matchQuery, v3Rows);

        std::ranges::sort(v2Rows);
        std::ranges::sort(v3Rows);

        ASSERT_EQ(v2Rows.size(), v3Rows.size())
            << "Row count mismatch for SET: " << setQuery;

        for (auto [v2Row, v3Row] : rv::zip(v2Rows, v3Rows)) {
            EXPECT_EQ(v2Row, v3Row) << "Row mismatch for SET: " << setQuery;
        }
    }

    const std::string _v2GraphName = "v2Graph";
    const std::string _v3GraphName = "v3Graph";
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
    std::unique_ptr<QueryInterpreterV3> _interp3;
    QueryConfig _queryConfig;
};

TEST_F(SetEquivalenceTest, setNewNodeProperty) {
    expectSetEquivalent(
        R"(CREATE (n:Person {name: "Alice"}))",
        R"(MATCH (n:Person) SET n.age = 32)",
        "MATCH (n:Person) RETURN n.name, n.age");
}

TEST_F(SetEquivalenceTest, overwriteExistingNodeProperty) {
    expectSetEquivalent(
        R"(CREATE (n:Person {name: "Alice", age: 10}))",
        R"(MATCH (n:Person) SET n.age = 32)",
        "MATCH (n:Person) RETURN n.age");
}

TEST_F(SetEquivalenceTest, setStringProperty) {
    expectSetEquivalent(
        R"(CREATE (n:Person {name: "Alice"}))",
        R"(MATCH (n:Person) SET n.city = "Paris")",
        "MATCH (n:Person) RETURN n.city");
}

TEST_F(SetEquivalenceTest, multipleItemsInOneSet) {
    expectSetEquivalent(
        R"(CREATE (n:Person {name: "Alice"}))",
        R"(MATCH (n:Person) SET n.age = 32, n.city = "Paris")",
        "MATCH (n:Person) RETURN n.age, n.city");
}

TEST_F(SetEquivalenceTest, chainedSetStatements) {
    expectSetEquivalent(
        R"(CREATE (n:Person {name: "Alice"}))",
        R"(MATCH (n:Person) SET n.age = 1 SET n.age = 2)",
        "MATCH (n:Person) RETURN n.age");
}

TEST_F(SetEquivalenceTest, setAcrossMultipleMatchedNodes) {
    expectSetEquivalent(
        R"(CREATE (a:Person {name: "Alice"}), (b:Person {name: "Bob"}))",
        R"(MATCH (n:Person) SET n.age = 40)",
        "MATCH (n:Person) RETURN n.name, n.age");
}

TEST_F(SetEquivalenceTest, setFromAnotherProperty) {
    expectSetEquivalent(
        R"(CREATE (a:Person {name: "Alice", age: 10}), (b:Person {name: "Bob", age: 20}))",
        R"(MATCH (n:Person) SET n.years = n.age)",
        "MATCH (n:Person) RETURN n.name, n.years");
}

TEST_F(SetEquivalenceTest, setOnlyMatchedLabel) {
    expectSetEquivalent(
        R"(CREATE (a:Person {name: "Alice", rank: 1}), (b:Robot {name: "R2", rank: 99}))",
        R"(MATCH (n:Person) SET n.rank = 5)",
        "MATCH (n) RETURN n.name, n.rank");
}

TEST_F(SetEquivalenceTest, setEdgeProperty) {
    expectSetEquivalent(
        R"(CREATE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"}))",
        R"(MATCH (a:Person)-[e:KNOWS]->(b:Person) SET e.since = 1999)",
        "MATCH (a:Person)-[e:KNOWS]->(b:Person) RETURN e.since");
}
