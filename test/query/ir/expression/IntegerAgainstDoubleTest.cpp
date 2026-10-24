#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "JobSystem.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "list/ListBuffer.h"
#include "list/ListContainer.h"
#include "map/MapBuffer.h"
#include "map/MapContainer.h"
#include "metadata/PropertyType.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"
#include "writers/GraphWriter.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using MapEntry = MapBuffer<>::MapKeyValuePair;
using ListItem = ListBuffer<>::ListItemVariant;

}

// An integer and a float compare by their exact values. 9007199254740993 is 2^53 + 1, which
// no double holds, so it is greater than 9007199254740992.0 and not equal to it. An
// unsigned integer has no literal, so the graph carries 10^19 and 10^19 + 1 as unsigned
// properties next to the double 1e19, which holds 10^19 exactly.
class IntegerAgainstDoubleTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        buildGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());
    }

protected:
    template <typename Type>
    void addNumber(GraphWriter& writer, std::string_view name, std::string_view propertyName, typename Type::Primitive number) {
        const NodeID node = writer.addNode({"Item"});
        writer.addNodeProperty<types::String>(node, "name", std::string_view {name});
        writer.addNodeProperty<Type>(node, propertyName, typename Type::Primitive {number});

        const std::vector<ListItem> elements = {number};
        writer.addNodeProperty<types::List>(node, "tags", _lists.insert(elements));

        const std::vector<MapEntry> entries = {{"a", number}};
        writer.addNodeProperty<types::Map>(node, "attrs", _maps.insert(entries));
    }

    void buildGraph(Graph* graph) {
        JobSystem jobSystem;
        jobSystem.init();

        GraphWriter writer(graph, &jobSystem);
        addNumber<types::UInt64>(writer, "u0", "unsigned", 10000000000000000000ULL);
        addNumber<types::UInt64>(writer, "u1", "unsigned", 10000000000000000001ULL);
        addNumber<types::Double>(writer, "d", "double", 1e19);
        writer.submit();

        jobSystem.terminate();
    }

    void runQuery(std::string_view query, RowSink& sink) {
        QueryStatus status;

        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();
    }

    void expectRows(std::string_view query, Rows expected) {
        RowSink sink;
        runQuery(query, sink);

        Rows rows;
        sink.sortedRows(rows);

        std::sort(expected.begin(), expected.end());
        EXPECT_EQ(rows, expected) << "query: " << query;
    }

    void expectOrderedRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query;
    }

    void expectCount(std::string_view query, size_t expected) {
        RowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.rows().size(), expected) << "query: " << query;
    }

    const std::string _graphName = "numbers";
    ListContainer _lists;
    MapContainer _maps;
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(IntegerAgainstDoubleTest, scalarsOrderExactly) {
    expectRows("RETURN 9007199254740993 > 9007199254740992.0", {{"true"}});
    expectRows("RETURN 9007199254740992.0 < 9007199254740993", {{"true"}});
    expectRows("RETURN 9007199254740993 <= 9007199254740992.0", {{"false"}});
    expectRows("RETURN -9007199254740993 < -9007199254740992.0", {{"true"}});
    expectRows("RETURN 9223372036854775807 < 9223372036854775808.0", {{"true"}});
    expectRows("RETURN 9007199254740992 >= 9007199254740992.0", {{"true"}});
    expectRows("RETURN 2 > 1.5", {{"true"}});
}

TEST_F(IntegerAgainstDoubleTest, unsignedScalarsOrderExactly) {
    expectRows("MATCH (a:Item), (b:Item) WHERE a.unsigned > b.double RETURN a.name", {{"u1"}});
    expectRows("MATCH (a:Item), (b:Item) WHERE a.unsigned >= b.double RETURN a.name", {{"u0"}, {"u1"}});
    expectRows("MATCH (a:Item), (b:Item) WHERE b.double < a.unsigned RETURN a.name", {{"u1"}});
}

TEST_F(IntegerAgainstDoubleTest, unsignedScalarsEqualExactly) {
    expectRows("MATCH (a:Item), (b:Item) WHERE a.unsigned = b.double RETURN a.name", {{"u0"}});
    expectRows("MATCH (a:Item), (b:Item) WHERE a.unsigned <> b.double RETURN a.name", {{"u1"}});
    expectRows("MATCH (a:Item) WHERE a.unsigned = 1e19 RETURN a.name", {{"u0"}});
    expectRows("MATCH (a:Item {unsigned: 1e19}) RETURN a.name", {{"u0"}});
}

TEST_F(IntegerAgainstDoubleTest, listsCompareExactly) {
    expectRows("RETURN [9007199254740993] = [9007199254740992.0]", {{"false"}});
    expectRows("RETURN [9007199254740992] = [9007199254740992.0]", {{"true"}});
    expectRows("RETURN [9007199254740993] > [9007199254740992.0]", {{"true"}});
    expectRows("RETURN [1] = [1.0]", {{"true"}});
}

TEST_F(IntegerAgainstDoubleTest, mapsCompareExactly) {
    expectRows("RETURN [{a: 9007199254740993}] = [{a: 9007199254740992.0}]", {{"false"}});
    expectRows("RETURN [{a: 9007199254740992}] = [{a: 9007199254740992.0}]", {{"true"}});
}

TEST_F(IntegerAgainstDoubleTest, inComparesExactly) {
    expectRows("RETURN 9007199254740993 IN [9007199254740992.0]", {{"false"}});
    expectRows("RETURN 9007199254740992.0 IN [9007199254740993]", {{"false"}});
    expectRows("RETURN 9007199254740992 IN [9007199254740992.0]", {{"true"}});
}

TEST_F(IntegerAgainstDoubleTest, taggedCellsOrderExactly) {
    expectRows("UNWIND [9007199254740993, 9007199254740992.0, 'a'] AS x WITH x WHERE x > 9007199254740992.0 RETURN x",
               {{"9007199254740993"}});
    expectRows("UNWIND [9007199254740993, 'a'] AS x UNWIND [9007199254740992.0, 'b'] AS y WITH x, y WHERE x > y RETURN x",
               {{"9007199254740993"}});
}

TEST_F(IntegerAgainstDoubleTest, orderByIsExact) {
    expectOrderedRows("UNWIND [[9007199254740993, 'integer'], [9007199254740992.0, 'double']] AS x RETURN x[1] ORDER BY x[0]",
                      {{"double"}, {"integer"}});
    expectOrderedRows("UNWIND [[[9007199254740993], 'integer'], [[9007199254740992.0], 'double']] AS x RETURN x[1] ORDER BY x[0]",
                      {{"double"}, {"integer"}});
}

TEST_F(IntegerAgainstDoubleTest, distinctAgreesWithEquality) {
    expectCount("UNWIND [[9007199254740993], [9007199254740992.0]] AS x RETURN DISTINCT x", 2);
    expectCount("UNWIND [[9007199254740992], [9007199254740992.0]] AS x RETURN DISTINCT x", 1);
    expectCount("UNWIND [{a: 9007199254740993}, {a: 9007199254740992.0}] AS x RETURN DISTINCT x", 2);
    expectCount("UNWIND [{a: 9007199254740992}, {a: 9007199254740992.0}] AS x RETURN DISTINCT x", 1);
    expectCount("UNWIND [[1], [1.0]] AS x RETURN DISTINCT x", 1);
    expectCount("UNWIND [[-9223372036854775807 - 1], [-9223372036854775808.0]] AS x RETURN DISTINCT x", 1);
}

TEST_F(IntegerAgainstDoubleTest, unsignedAgreesWithEquality) {
    expectRows("MATCH (a:Item), (b:Item) WHERE a.tags = b.tags AND a.name < b.name RETURN a.name, b.name", {{"d", "u0"}});
    expectRows("MATCH (a:Item), (b:Item) WHERE a.attrs = b.attrs AND a.name < b.name RETURN a.name, b.name", {{"d", "u0"}});
    expectCount("MATCH (n:Item) RETURN DISTINCT n.tags", 2);
    expectCount("MATCH (n:Item) RETURN DISTINCT n.attrs", 2);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
