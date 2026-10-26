#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "QueryConfig.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"

#include "columns/ColumnConst.h"
#include "columns/ColumnVector.h"
#include "CellTag.h"
#include "map/MapEntryView.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// The cell a dynamic map key read hands back, inspected as the CellView it is.
// The values themselves are pinned by the dynamic-map-key-* query suite tests.
class DynamicMapKeyTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void openChange(ChangeID& changeID) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const auto res = system.newChange(_graphName);
        ASSERT_TRUE(res);

        changeID = res.value()->id();
    }

    void submit(const ChangeID& changeID) {
        const QueryState submitState(_graphName,
                                     &_env->getMem(),
                                     &_env->getCompilerContext(),
                                     &_queryConfig,
                                     nullptr,
                                     CommitHash::head(),
                                     changeID);
        const QueryStatus status = _env->getDB().query("CHANGE SUBMIT", submitState);
        ASSERT_TRUE(status.isOk()) << "CHANGE SUBMIT failed";
    }

    void write(std::string_view query) {
        ChangeID changeID;
        openChange(changeID);

        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              changeID,
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        submit(changeID);
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    void expectError(std::string_view query, std::string_view expectedError) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);

        ASSERT_FALSE(status.isOk()) << "query: " << query << "\nexpected it to fail";
        EXPECT_NE(status.getError().find(expectedError), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

namespace {

// Collects the tag of every cell a map read emits
class CellTagSink : public db::NLOutputSink {
public:
    void appendChunks(std::span<const db::Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_FALSE(chunks.empty());

        const db::Column* const column = chunks.back();
        const bool holdsOneCellForEveryRow =
            column->getKind() == db::ColumnConst<db::CellView>::staticKind();

        ASSERT_TRUE(holdsOneCellForEveryRow
                    || column->getKind() == db::ColumnVector<db::CellView>::staticKind());

        for (size_t row = offset; row < offset + rowCount; row++) {
            const db::CellView cell =
                holdsOneCellForEveryRow
                    ? (*static_cast<const db::ColumnConst<db::CellView>*>(column))[row]
                    : (*static_cast<const db::ColumnVector<db::CellView>*>(column))[row];

            _tags.push_back(cell.getTag());
        }
    }

    const std::vector<db::CellTag>& tags() const { return _tags; }

private:
    std::vector<db::CellTag> _tags;
};

}

TEST_F(DynamicMapKeyTest, readsAMissingKeyAsANullCell) {
    CellTagSink sink;
    QueryStatus status;
    _interpreter->execute(status,
                          "UNWIND ['a', 'nope', 'gone'] AS k WITH {a: 1} AS m, k RETURN m[k]",
                          _graphName,
                          CommitHash::head(),
                          ChangeID::head(),
                          &sink);
    ASSERT_TRUE(status.isOk()) << status.getError();

    const std::vector<CellTag> expected {CellTag::Int, CellTag::Null, CellTag::Null};
    EXPECT_EQ(sink.tags(), expected);
}

TEST_F(DynamicMapKeyTest, readsAStoredMissingKeyAsANullCell) {
    write("CREATE (n:Tagged {name: 'a', attrs: {x: 1}})");

    CellTagSink sink;
    QueryStatus status;
    _interpreter->execute(status,
                          "MATCH (t:Tagged) UNWIND ['x', 'absent'] AS k RETURN t.attrs[k]",
                          _graphName,
                          CommitHash::head(),
                          ChangeID::head(),
                          &sink);
    ASSERT_TRUE(status.isOk()) << status.getError();

    const std::vector<CellTag> expected {CellTag::Int, CellTag::Null};
    EXPECT_EQ(sink.tags(), expected);
}

TEST_F(DynamicMapKeyTest, readsANullKeyAsANullCell) {
    CellTagSink sink;
    QueryStatus status;
    _interpreter->execute(status,
                          "UNWIND ['a', null] AS k WITH {a: 1} AS m, k RETURN m[k]",
                          _graphName,
                          CommitHash::head(),
                          ChangeID::head(),
                          &sink);
    ASSERT_TRUE(status.isOk()) << status.getError();

    const std::vector<CellTag> expected {CellTag::Int, CellTag::Null};
    EXPECT_EQ(sink.tags(), expected);
}

// The suite cannot express this one: it runs a single query, and a property that query
// creates itself is invisible to analyzePropertyLookupExpr, which reads only
// _graphMetadata.propTypes(). Seeding attrs in its own committed write is the whole point.
TEST_F(DynamicMapKeyTest, readsAKeyOfAMapPropertyOfACallResult) {
    write("MATCH (p:Person {name: 'Remy'}) SET p.attrs = {w: 7}");
    write("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) CREATE (a)-[e:TAGGED]->(b)");

    expectRows("MATCH ()-[r:TAGGED]->() WITH r, 'w' AS k RETURN startNode(r).attrs[k]", {{"7"}});
}
