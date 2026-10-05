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
#include "map/MapBufferTypeTag.h"
#include "map/MapEntryView.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// The entry a dynamic map key read hands back, inspected as the MapEntryView it is.
// A rendered result shows the value alone, so only a sink reading the cell can tell
// which key a null entry is named by - and the binary protocol writes that key.
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

// Collects the key and the tag of every entry a map read emits, which no rendered row shows
class MapEntrySink : public db::NLOutputSink {
public:
    struct Entry {
        std::string _key;
        db::MapBufferTypeTag _tag {db::MapBufferTypeTag::Null};
    };

    void appendChunks(std::span<const db::Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_FALSE(chunks.empty());

        const db::Column* const column = chunks.back();
        const bool holdsOneEntryForEveryRow =
            column->getKind() == db::ColumnConst<db::MapEntryView>::staticKind();

        ASSERT_TRUE(holdsOneEntryForEveryRow
                    || column->getKind() == db::ColumnVector<db::MapEntryView>::staticKind());

        for (size_t row = offset; row < offset + rowCount; row++) {
            const db::MapEntryView entry =
                holdsOneEntryForEveryRow
                    ? (*static_cast<const db::ColumnConst<db::MapEntryView>*>(column))[row]
                    : (*static_cast<const db::ColumnVector<db::MapEntryView>*>(column))[row];

            _entries.push_back(Entry {std::string {entry.getKey()}, entry.getValueTag()});
        }
    }

    const std::vector<Entry>& entries() const { return _entries; }

private:
    std::vector<Entry> _entries;
};

}

TEST_F(DynamicMapKeyTest, namesAMissingKeyByTheKeyTheRowAskedFor) {
    MapEntrySink sink;
    QueryStatus status;
    _interpreter->execute(status,
                          "UNWIND ['a', 'nope', 'gone'] AS k WITH {a: 1} AS m, k RETURN m[k]",
                          _graphName,
                          CommitHash::head(),
                          ChangeID::head(),
                          &sink);
    ASSERT_TRUE(status.isOk()) << status.getError();

    const std::vector<MapEntrySink::Entry>& entries = sink.entries();
    ASSERT_EQ(entries.size(), 3u);

    EXPECT_EQ(entries[0]._key, "a");
    EXPECT_NE(entries[0]._tag, MapBufferTypeTag::Null);

    EXPECT_EQ(entries[1]._key, "nope");
    EXPECT_EQ(entries[1]._tag, MapBufferTypeTag::Null);

    EXPECT_EQ(entries[2]._key, "gone");
    EXPECT_EQ(entries[2]._tag, MapBufferTypeTag::Null);
}

TEST_F(DynamicMapKeyTest, namesAStoredMissingKeyByTheKeyTheRowAskedFor) {
    write("CREATE (n:Tagged {name: 'a', attrs: {x: 1}})");

    MapEntrySink sink;
    QueryStatus status;
    _interpreter->execute(status,
                          "MATCH (t:Tagged) UNWIND ['x', 'absent'] AS k RETURN t.attrs[k]",
                          _graphName,
                          CommitHash::head(),
                          ChangeID::head(),
                          &sink);
    ASSERT_TRUE(status.isOk()) << status.getError();

    const std::vector<MapEntrySink::Entry>& entries = sink.entries();
    ASSERT_EQ(entries.size(), 2u);

    EXPECT_EQ(entries[0]._key, "x");
    EXPECT_NE(entries[0]._tag, MapBufferTypeTag::Null);

    EXPECT_EQ(entries[1]._key, "absent");
    EXPECT_EQ(entries[1]._tag, MapBufferTypeTag::Null);
}

TEST_F(DynamicMapKeyTest, leavesANullKeyEntryUnnamed) {
    MapEntrySink sink;
    QueryStatus status;
    _interpreter->execute(status,
                          "UNWIND ['a', null] AS k WITH {a: 1} AS m, k RETURN m[k]",
                          _graphName,
                          CommitHash::head(),
                          ChangeID::head(),
                          &sink);
    ASSERT_TRUE(status.isOk()) << status.getError();

    const std::vector<MapEntrySink::Entry>& entries = sink.entries();
    ASSERT_EQ(entries.size(), 2u);

    EXPECT_EQ(entries[0]._key, "a");
    EXPECT_EQ(entries[0]._tag, MapBufferTypeTag::Int);

    EXPECT_TRUE(entries[1]._key.empty());
    EXPECT_EQ(entries[1]._tag, MapBufferTypeTag::Null);
}

// The suite cannot express this one: it runs a single query, and a property that query
// creates itself is invisible to analyzePropertyLookupExpr, which reads only
// _graphMetadata.propTypes(). Seeding attrs in its own committed write is the whole point.
TEST_F(DynamicMapKeyTest, readsAKeyOfAMapPropertyOfACallResult) {
    write("MATCH (p:Person {name: 'Remy'}) SET p.attrs = {w: 7}");
    write("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) CREATE (a)-[e:TAGGED]->(b)");

    expectRows("MATCH ()-[r:TAGGED]->() WITH r, 'w' AS k RETURN startNode(r).attrs[k]", {{"7"}});
}
