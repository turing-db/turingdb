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

// A map value is read by a key spelled in the query: m.key off a variable bound to a map,
// n.attrs.key off a stored map property, and startNode(r).attrs.key off a call's result.
class MapKeyReadTest : public TuringTest {
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
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
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

    void expectError(std::string_view query, std::string_view expectedError) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
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

TEST_F(MapKeyReadTest, readsAKeyOfAMapLiteral) {
    expectRows("WITH {a: 1, b: 'x'} AS m RETURN m.a, m.b", {{"1", "x"}});
}

TEST_F(MapKeyReadTest, readsEveryScalarTagOfAMapLiteral) {
    expectRows("WITH {i: 1, d: 2.5, b: true, s: 'x', n: null} AS m "
               "RETURN m.i, m.d, m.b, m.s, m.n",
               {{"1", "2.500000", "true", "x", "null"}});
}

TEST_F(MapKeyReadTest, readsAListAndAMapOutOfAMapLiteral) {
    expectRows("WITH {l: [1, 2], m: {k: 3}} AS outer RETURN outer.l, outer.m",
               {{"[1, 2]", "{k: 3}"}});
}

TEST_F(MapKeyReadTest, readsNullForAKeyTheMapLiteralHasNot) {
    expectRows("WITH {a: 1} AS m RETURN m.nosuch", {{"null"}});
}

TEST_F(MapKeyReadTest, readsAKeyOfAStoredMap) {
    write("CREATE (n:Tagged {name: 'a', attrs: {x: 1, s: 'y'}})");
    expectRows("MATCH (n:Tagged) RETURN n.attrs.x, n.attrs.s", {{"1", "y"}});
}

TEST_F(MapKeyReadTest, readsNullForAKeyTheStoredMapHasNot) {
    write("CREATE (n:Tagged {name: 'a', attrs: {x: 1}})");
    expectRows("MATCH (n:Tagged) RETURN n.attrs.nosuch", {{"null"}});
}

TEST_F(MapKeyReadTest, readsNullWhereTheNodeCarriesNoMap) {
    write("CREATE (a:Tagged {name: 'a', attrs: {x: 1}})");
    write("CREATE (b:Tagged {name: 'b'})");

    expectRows("MATCH (n:Tagged) RETURN n.name, n.attrs.x", {{"a", "1"}, {"b", "null"}});
}

TEST_F(MapKeyReadTest, readsOneKeyPerRowOverMapsOfDifferentShapes) {
    write("CREATE (a:Tagged {name: 'a', attrs: {x: 1}})");
    write("CREATE (b:Tagged {name: 'b', attrs: {y: 'z'}})");

    expectRows("MATCH (n:Tagged) RETURN n.name, n.attrs.x, n.attrs.y",
               {{"a", "1", "null"}, {"b", "null", "z"}});
}

// The map a variable binds can come from a stored property as well as a literal, which is
// the ColumnOptVector<MapView> shape rather than the constant one.
TEST_F(MapKeyReadTest, readsAKeyOfAMapBoundFromAStoredProperty) {
    write("CREATE (n:Tagged {name: 'a', attrs: {x: 1, s: 'y'}})");
    write("CREATE (n:Tagged {name: 'b'})");

    expectRows("MATCH (n:Tagged) WITH n.attrs AS m RETURN m.x, m.s, m.nosuch",
               {{"1", "y", "null"}, {"null", "null", "null"}});
}

// An edge carries a map property as a node does, and its key is read through the edge
// fetch rather than the node one.
TEST_F(MapKeyReadTest, readsAKeyOfAMapOnAnEdge) {
    write("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
          "CREATE (a)-[e:TAGGED {attrs: {w: 7, s: 'z'}}]->(b)");

    expectRows("MATCH ()-[r:TAGGED]->() RETURN r.attrs.w, r.attrs.s, r.attrs.nosuch",
               {{"7", "z", "null"}});
}

TEST_F(MapKeyReadTest, readsAKeyOfAMapPropertyOfACallResult) {
    write("MATCH (p:Person {name: 'Remy'}) SET p.attrs = {w: 7}");
    write("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) "
          "CREATE (a)-[e:TAGGED]->(b)");

    expectRows("MATCH ()-[r:TAGGED]->() RETURN startNode(r).attrs.w", {{"7"}});
}

TEST_F(MapKeyReadTest, readsAKeyOfAMapTheSameQueryCreated) {
    write("CREATE (n:Tagged {name: 'a', attrs: {x: 1}})");
    expectRows("MATCH (n:Tagged) RETURN n.attrs.x", {{"1"}});
}

TEST_F(MapKeyReadTest, limitsRowsCarryingAMapValue) {
    write("CREATE (a:Tagged {name: 'a', attrs: {x: 1}})");
    write("CREATE (b:Tagged {name: 'b', attrs: {x: 2}})");

    RowSink sink;
    QueryStatus status;
    _interpreter->execute(status,
                          "MATCH (n:Tagged) RETURN n.attrs.x LIMIT 1",
                          _graphName,
                          CommitHash::head(),
                          ChangeID::head(),
                          &_env->getMem(),
                          &sink);

    ASSERT_TRUE(status.isOk()) << status.getError();

    Rows actual;
    sink.sortedRows(actual);
    EXPECT_EQ(actual.size(), 1u);
}

TEST_F(MapKeyReadTest, readsAMapValueBesideACrossProduct) {
    write("CREATE (a:Tagged {name: 'a', attrs: {x: 1}})");

    expectRows("MATCH (n:Tagged), (p:Person {name: 'Remy'}) RETURN n.attrs.x, p.name",
               {{"1", "Remy"}});
}

// The key of a constant map is read once, above the loop the UNWIND opens, so the read has
// to be placed at its operand rather than wherever the lowering last left its builder.
TEST_F(MapKeyReadTest, readsAConstantMapKeyBesideAnUnwind) {
    expectRows("WITH {a: 1} AS m UNWIND [1, 2, 3] AS x RETURN x, m.a",
               {{"1", "1"}, {"2", "1"}, {"3", "1"}});
}

TEST_F(MapKeyReadTest, rejectsAKeyOfANestedMap) {
    expectError("WITH {a: {b: 1}} AS m RETURN m.a.b", "map held by another map");
}

// A key is read off a property rather than being one, so it can name no write target. The
// message has to say key rather than datetime component, which is the other dotted name
// the same guard turns away.
TEST_F(MapKeyReadTest, rejectsAWriteToAKey) {
    write("CREATE (n:Tagged {name: 'a', attrs: {x: 1}})");
    expectError("MATCH (n:Tagged) SET n.attrs.x = 2", "A map key is a value inside a property, not a property of its own. Only a property can be set, removed or indexed.");
}

TEST_F(MapKeyReadTest, rejectsARemoveOfAKey) {
    write("CREATE (n:Tagged {name: 'a', attrs: {x: 1}})");
    expectError("MATCH (n:Tagged) REMOVE n.attrs.x", "A map key is a value inside a property, not a property of its own. Only a property can be set, removed or indexed.");
}

TEST_F(MapKeyReadTest, rejectsAWriteToAKeyOfAMapVariable) {
    expectError("WITH {a: 1} AS m SET m.a = 2", "A map key is a value inside a property, not a property of its own. Only a property can be set, removed or indexed.");
}

// The three-name spelling off a map variable names a key twice over, so the rejection must
// still say key rather than reaching for the graph's property types.
TEST_F(MapKeyReadTest, rejectsAWriteToANestedKeyOfAMapVariable) {
    expectError("WITH {a: {b: 1}} AS m SET m.a.b = 2", "A map key is a value inside a property, not a property of its own. Only a property can be set, removed or indexed.");
}

TEST_F(MapKeyReadTest, rejectsARemoveOfAKeyOfAMapVariable) {
    write("CREATE (n:Tagged {name: 'a', attrs: {x: 1}})");
    expectError("MATCH (n:Tagged) WITH n.attrs AS m REMOVE m.x", "A map key is a value inside a property, not a property of its own. Only a property can be set, removed or indexed.");
}

TEST_F(MapKeyReadTest, rejectsAKeyOfANonMapProperty) {
    expectError("MATCH (n:Person) RETURN n.name.year",
                "Property 'name' is 'String', only a datetime or a map has components");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
