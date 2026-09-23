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
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

// 300 entries of 25 bytes each (a 16-byte key view, a tag, an integer) outgrow one 4096-byte
// MapBuffer chunk. Zero-padded keys keep the sorted order the same as the written one.
void longMapEntries(std::string& literal, std::string& rendered) {
    constexpr size_t entryCount = 300;

    for (size_t entry = 1; entry <= entryCount; entry++) {
        if (entry > 1) {
            literal += ", ";
            rendered += ", ";
        }

        std::string key = std::to_string(entry);
        key.insert(0, 3 - key.size(), '0');

        literal += "k" + key + ": " + std::to_string(entry);
        rendered += "k" + key + ": " + std::to_string(entry);
    }
}

}

class MapPropertyTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

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
                              &_env->getMem(),
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        submit(changeID);
    }

    void expectRows(std::string_view query, const Rows& expected) {
        expectRowsIn(ChangeID::head(), query, expected);
    }

    void expectWrittenRows(std::string_view query, const Rows& expected) {
        ChangeID changeID;
        openChange(changeID);

        expectRowsIn(changeID, query, expected);
    }

    void expectRowsIn(const ChangeID& changeID, std::string_view query, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              changeID,
                              &_env->getMem(),
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

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

TEST_F(MapPropertyTest, storesAScalarMap) {
    write("CREATE (n:Tagged {name: 'a', attrs: {a: 1, b: 'x', c: true, d: 2.5}})");
    expectRows("MATCH (n:Tagged) RETURN n.attrs", {{"{a: 1, b: x, c: true, d: 2.500000}"}});
}

TEST_F(MapPropertyTest, returnsTheKeysSorted) {
    write("CREATE (n:Tagged {name: 'a', attrs: {b: 1, c: 2, a: 3}})");
    expectRows("MATCH (n:Tagged) RETURN n.attrs", {{"{a: 3, b: 1, c: 2}"}});
}

TEST_F(MapPropertyTest, storesAMapHoldingNull) {
    write("CREATE (n:Tagged {name: 'a', attrs: {a: null, b: 1}})");
    expectRows("MATCH (n:Tagged) RETURN n.attrs", {{"{a: null, b: 1}"}});
}

TEST_F(MapPropertyTest, storesANestedMap) {
    write("CREATE (n:Tagged {name: 'a', attrs: {outer: {inner: 'x', deeper: {n: 1}}}})");
    expectRows("MATCH (n:Tagged) RETURN n.attrs", {{"{outer: {deeper: {n: 1}, inner: x}}"}});
}

TEST_F(MapPropertyTest, storesAMapHoldingLists) {
    write("CREATE (n:Tagged {name: 'a', attrs: {tags: ['x', 'y'], nested: [[1], [2, 3]]}})");
    expectRows("MATCH (n:Tagged) RETURN n.attrs", {{"{nested: [[1], [2, 3]], tags: [x, y]}"}});
}

TEST_F(MapPropertyTest, storesAnEmptyMap) {
    write("CREATE (n:Tagged {name: 'a', attrs: {}})");
    expectRows("MATCH (n:Tagged) RETURN n.attrs", {{"{}"}});
}

TEST_F(MapPropertyTest, storesAMapLargerThanOneBufferChunk) {
    std::string literal;
    std::string rendered;
    longMapEntries(literal, rendered);

    write("CREATE (n:Tagged {name: 'a', attrs: {" + literal + "}})");
    expectRows("MATCH (n:Tagged) RETURN n.attrs", {{"{" + rendered + "}"}});
}

TEST_F(MapPropertyTest, storesAMapReadingARow) {
    write("MATCH (n:Person {name: 'Remy'}) CREATE (t:Tagged {name: 'a', attrs: {who: n.name, age: 1}})");
    expectRows("MATCH (n:Tagged) RETURN n.attrs", {{"{age: 1, who: Remy}"}});
}

TEST_F(MapPropertyTest, storesOneMapPerNode) {
    write("CREATE (a:Tagged {name: 'a', attrs: {x: 1}})");
    write("CREATE (b:Tagged {name: 'b', attrs: {y: 'z'}})");

    expectRows("MATCH (n:Tagged) RETURN n.name, n.attrs",
               {{"a", "{x: 1}"}, {"b", "{y: z}"}});
}

TEST_F(MapPropertyTest, readsNullWhereTheNodeHasNoMap) {
    write("CREATE (a:Tagged {name: 'a', attrs: {x: 1}})");
    write("CREATE (b:Tagged {name: 'b'})");

    expectRows("MATCH (n:Tagged) RETURN n.name, n.attrs",
               {{"a", "{x: 1}"}, {"b", "null"}});
}

TEST_F(MapPropertyTest, setsAMapOnAMatchedNode) {
    write("MATCH (n:Person {name: 'Remy'}) SET n.attrs = {x: 7}");
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.attrs", {{"{x: 7}"}});
}

TEST_F(MapPropertyTest, replacesAStoredMap) {
    write("CREATE (n:Tagged {name: 'a', attrs: {x: 1, y: 2}})");
    write("MATCH (n:Tagged) SET n.attrs = {z: 3}");

    expectRows("MATCH (n:Tagged) RETURN n.attrs", {{"{z: 3}"}});
}

TEST_F(MapPropertyTest, storesAMapOnAnEdge) {
    write("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
          "CREATE (a)-[e:TAGGED {attrs: {w: 1.5}}]->(b)");

    expectRows("MATCH (:Person)-[e:TAGGED]->(:Person) RETURN e.attrs", {{"{w: 1.500000}"}});
}

TEST_F(MapPropertyTest, copiesAStoredMapOntoAnotherNode) {
    write("CREATE (a:Tagged {name: 'a', attrs: {x: 'y', l: [1]}})");
    write("MATCH (a:Tagged {name: 'a'}) CREATE (b:Copied {name: 'b', attrs: a.attrs})");

    expectRows("MATCH (n:Copied) RETURN n.attrs", {{"{l: [1], x: y}"}});
}

TEST_F(MapPropertyTest, setsAMapPropertyToAnAbsentMap) {
    write("CREATE (a:Tagged {name: 'a', attrs: {x: 1}})");
    write("CREATE (b:Tagged {name: 'b'})");
    write("MATCH (a:Tagged {name: 'a'}), (b:Tagged {name: 'b'}) SET a.attrs = b.attrs");

    expectRows("MATCH (n:Tagged) RETURN n.name, n.attrs", {{"a", "null"}, {"b", "null"}});
}

TEST_F(MapPropertyTest, createsANodeFromAnAbsentMap) {
    write("CREATE (a:Tagged {name: 'a', attrs: {x: 1}})");
    write("CREATE (b:Tagged {name: 'b'})");
    write("MATCH (b:Tagged {name: 'b'}) CREATE (c:Copied {name: 'c', attrs: b.attrs})");

    expectRows("MATCH (n:Copied) RETURN n.attrs", {{"null"}});
}

TEST_F(MapPropertyTest, returnsAStoredMapInsideAMapLiteral) {
    write("CREATE (n:Tagged {name: 'a', attrs: {x: 1}})");
    expectRows("MATCH (n:Tagged) RETURN {inner: n.attrs}", {{"{inner: {x: 1}}"}});
}

TEST_F(MapPropertyTest, readsAMapTheSameQueryCreated) {
    expectWrittenRows("CREATE (n:Tagged {name: 'a', attrs: {x: 1, s: 'y'}}) RETURN n.attrs",
                      {{"{s: y, x: 1}"}});
}

TEST_F(MapPropertyTest, readsAMapTheSameQuerySet) {
    expectWrittenRows("MATCH (n:Person {name: 'Remy'}) SET n.attrs = {x: 1} RETURN n.attrs",
                      {{"{x: 1}"}});
}

TEST_F(MapPropertyTest, storesAMapHoldingAListOfMaps) {
    write("CREATE (n:Tagged {name: 'a', attrs: {items: [{a: 1}, {b: [{c: 2}]}]}})");
    expectRows("MATCH (n:Tagged) RETURN n.attrs", {{"{items: [{a: 1}, {b: [{c: 2}]}]}"}});
}
