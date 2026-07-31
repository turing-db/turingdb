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
        throw std::runtime_error("CreateEquivalenceTest: unsupported output column type");
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

// Discards output — CREATE-only programs produce no rows worth inspecting.
class NullSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const>, size_t, size_t) override {}
};

}

class CreateEquivalenceTest : public TuringTest {
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
    // Apply a CREATE query to a graph via the v2 pipeline and submit the change.
    void applyV2(std::string_view graphName, std::string_view createQuery) {
        ChangeID changeID;
        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            auto res = system.newChange(graphName);
            ASSERT_TRUE(res);
            changeID = res.value()->id();
        }

        QueryCallbacks callbacks;
        callbacks.setOnOutputData([](const Dataframe*) {});

        const QueryState createState(graphName, &_env->getMem(), &_queryConfig, &callbacks, CommitHash::head(), changeID);
        const QueryStatus createStatus = _db->query(createQuery, createState);
        ASSERT_TRUE(createStatus.isOk()) << "V2 CREATE failed: " << createQuery;

        const QueryState submitState(graphName, &_env->getMem(), &_queryConfig, &callbacks, CommitHash::head(), changeID);
        const QueryStatus submitStatus = _db->query("CHANGE SUBMIT", submitState);
        ASSERT_TRUE(submitStatus.isOk()) << "V2 CHANGE SUBMIT failed";
    }

    // Apply a CREATE query to a graph via the v3 MLIR executor and submit the change.
    void applyV3(std::string_view graphName, std::string_view createQuery) {
        ChangeID changeID;
        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            auto res = system.newChange(graphName);
            ASSERT_TRUE(res);
            changeID = res.value()->id();
        }

        NullSink sink;
        QueryStatus createStatus;
        _interp3->execute(createStatus, createQuery, graphName, CommitHash::head(), changeID, &_env->getMem(), &sink);
        ASSERT_TRUE(createStatus.isOk()) << "V3 CREATE failed: " << createQuery << " — " << createStatus.getError();

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

    // Apply createQuery via both v2 and v3, then compare MATCH results.
    void expectCreateEquivalent(std::string_view createQuery, std::string_view matchQuery) {
        applyV2(_v2GraphName, createQuery);
        applyV3(_v3GraphName, createQuery);

        Rows v2Rows;
        matchV2(_v2GraphName, matchQuery, v2Rows);
        Rows v3Rows;
        matchV2(_v3GraphName, matchQuery, v3Rows);

        std::ranges::sort(v2Rows);
        std::ranges::sort(v3Rows);

        ASSERT_EQ(v2Rows.size(), v3Rows.size())
            << "Row count mismatch for CREATE: " << createQuery;

        for (auto [v2Row, v3Row] : rv::zip(v2Rows, v3Rows)) {
            EXPECT_EQ(v2Row, v3Row) << "Row mismatch for CREATE: " << createQuery;
        }
    }

    const std::string _v2GraphName = "v2Graph";
    const std::string _v3GraphName = "v3Graph";
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
    std::unique_ptr<QueryInterpreterV3> _interp3;
    QueryConfig _queryConfig;
};

TEST_F(CreateEquivalenceTest, singleNode) {
    expectCreateEquivalent(
        "CREATE (n:Person)",
        "MATCH (n:Person) RETURN count(n)");
}

TEST_F(CreateEquivalenceTest, nodeWithStringProperty) {
    expectCreateEquivalent(
        R"(CREATE (n:Person {name: "Alice"}))",
        "MATCH (n:Person) RETURN n.name");
}

TEST_F(CreateEquivalenceTest, nodeWithMultipleProperties) {
    expectCreateEquivalent(
        R"(CREATE (n:Book {title: "Dune", year: 1965}))",
        "MATCH (n:Book) RETURN n.title, n.year");
}

TEST_F(CreateEquivalenceTest, edgeCreate) {
    expectCreateEquivalent(
        R"(CREATE (a:City {name: "Paris"})-[:LOCATED_IN]->(b:Country {name: "France"}))",
        "MATCH (a:City)-[:LOCATED_IN]->(b:Country) RETURN a.name, b.name");
}

TEST_F(CreateEquivalenceTest, multiplePatternsInOneClause) {
    expectCreateEquivalent(
        R"(CREATE (a:Person {name: "Alice"}), (b:Person {name: "Bob"}))",
        "MATCH (n:Person) RETURN n.name");
}

TEST_F(CreateEquivalenceTest, multiplePatternsWithEdgeInOneClause) {
    expectCreateEquivalent(
        R"(CREATE (a:City {name: "Paris"}), (b:Country {name: "France"})-[:HAS_CAPITAL]->(c:City {name: "Lyon"}))",
        "MATCH (a:Country)-[:HAS_CAPITAL]->(b:City) RETURN a.name, b.name");
}

TEST_F(CreateEquivalenceTest, nodeWithMultipleLabels) {
    expectCreateEquivalent(
        R"(CREATE (n:Person:Employee {name: "Alice"}))",
        "MATCH (n:Employee) RETURN n.name");
}

TEST_F(CreateEquivalenceTest, multipleSequentialCreates) {
    applyV2(_v2GraphName, R"(CREATE (n:Person {name: "Alice"}))");
    applyV2(_v2GraphName, R"(CREATE (n:Person {name: "Bob"}))");
    applyV3(_v3GraphName, R"(CREATE (n:Person {name: "Alice"}))");
    applyV3(_v3GraphName, R"(CREATE (n:Person {name: "Bob"}))");

    Rows v2Rows;
    matchV2(_v2GraphName, "MATCH (n:Person) RETURN n.name", v2Rows);
    Rows v3Rows;
    matchV2(_v3GraphName, "MATCH (n:Person) RETURN n.name", v3Rows);

    std::ranges::sort(v2Rows);
    std::ranges::sort(v3Rows);

    ASSERT_EQ(v2Rows.size(), v3Rows.size());

    for (auto [v2Row, v3Row] : rv::zip(v2Rows, v3Rows)) {
        EXPECT_EQ(v2Row, v3Row);
    }
}
