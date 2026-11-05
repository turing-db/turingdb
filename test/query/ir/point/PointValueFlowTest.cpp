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

class PointValueFlowTest : public TuringTest {
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

    void run(std::string_view query, RowSink& sink, QueryStatus& status) {
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        run(query, sink, status);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    void expectOrderedRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        run(query, sink, status);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::string actualText;
        describeRows(sink.rows(), actualText);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query << "\ngot:\n" << actualText;
    }

    void expectError(std::string_view query, std::string_view expectedError) {
        RowSink sink;
        QueryStatus status;
        run(query, sink, status);

        ASSERT_FALSE(status.isOk()) << "query: " << query << "\nexpected it to fail";
        EXPECT_NE(status.getError().find(expectedError), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }

    void createFourCities() {
        write("CREATE (n:City {name: 'Paris', at: point({longitude: 2.3522, latitude: 48.8566})})");
        write("CREATE (n:City {name: 'London', at: point({longitude: -0.1276, latitude: 51.5072})})");
        write("CREATE (n:City {name: 'Grid', at: point({x: 3, y: 4})})");
        write("CREATE (n:City {name: 'Tower', at: point({x: 3, y: 4, z: 5})})");
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

TEST_F(PointValueFlowTest, coalescesAPoint) {
    expectRows("RETURN coalesce(null, point({x: 1, y: 2}))", {{"point({x: 1.0, y: 2.0, crs: 'cartesian'})"}});
}

TEST_F(PointValueFlowTest, selectsAPointInACase) {
    expectRows("UNWIND [true, false] AS b RETURN CASE WHEN b THEN point({x: 1, y: 2}) ELSE null END",
               {{"point({x: 1.0, y: 2.0, crs: 'cartesian'})"}, {"null"}});
    expectRows("UNWIND [true, false] AS b RETURN CASE WHEN b THEN point({x: 1, y: 2}) ELSE 1 END",
               {{"point({x: 1.0, y: 2.0, crs: 'cartesian'})"}, {"1"}});
}

TEST_F(PointValueFlowTest, unitesPoints) {
    expectRows("RETURN point({x: 1, y: 2}) AS p UNION RETURN point({x: 1, y: 2}) AS p",
               {{"point({x: 1.0, y: 2.0, crs: 'cartesian'})"}});
    expectRows("RETURN point({x: 1, y: 2}) AS p UNION ALL RETURN point({x: 1, y: 2, crs: 'wgs-84'}) AS p",
               {{"point({x: 1.0, y: 2.0, crs: 'cartesian'})"}, {"point({x: 1.0, y: 2.0, crs: 'wgs-84'})"}});
}

TEST_F(PointValueFlowTest, comparesListsAndMapsHoldingPoints) {
    expectRows("RETURN [point({x: 1, y: 2})] = [point({x: 1, y: 2})]", {{"true"}});
    expectRows("RETURN [point({x: 1, y: 2})] = [point({x: 1, y: 3})]", {{"false"}});
    expectRows("RETURN {a: point({x: 1, y: 2})} = {a: point({x: 1, y: 2})}", {{"true"}});
}

TEST_F(PointValueFlowTest, comparesAPointCarriedByAVariable) {
    expectRows("WITH point({x: 1, y: 2}) AS p RETURN p = p, p <> p", {{"true", "false"}});
}

TEST_F(PointValueFlowTest, findsAPointAmongValuesOfOtherTypes) {
    expectRows("RETURN point({x: 1, y: 2}) IN [1, 'a', point({x: 1, y: 2})]", {{"true"}});
    expectRows("RETURN coll.indexOf([1, point({x: 1, y: 2})], point({x: 1, y: 2}))", {{"1"}});
}

TEST_F(PointValueFlowTest, ordersPointsAmongValuesOfOtherTypes) {
    expectOrderedRows("UNWIND [1, 'a', point({x: 1, y: 2}), true] AS v RETURN v ORDER BY v",
                      {{"point({x: 1.0, y: 2.0, crs: 'cartesian'})"}, {"a"}, {"true"}, {"1"}});
}

TEST_F(PointValueFlowTest, countsDistinctPointsOfAList) {
    expectRows("UNWIND [point({x: 1, y: 2}), point({x: 1, y: 2}), point({x: 1, y: 2, crs: 'wgs-84'})] AS p "
               "RETURN count(DISTINCT p)",
               {{"2"}});
}

TEST_F(PointValueFlowTest, collectsDistinctPointProperties) {
    createFourCities();
    write("CREATE (n:City {name: 'Twin', at: point({x: 3, y: 4})})");

    expectRows("MATCH (n:City) WHERE n.name IN ['Grid', 'Twin'] RETURN collect(DISTINCT n.at)",
               {{"[point({x: 3.0, y: 4.0, crs: 'cartesian'})]"}});
}

TEST_F(PointValueFlowTest, filtersOnAListOfPoints) {
    createFourCities();
    expectRows("MATCH (n:City) WHERE n.at IN [point({x: 3, y: 4}), point({x: 3, y: 4, z: 5})] RETURN n.name",
               {{"Grid"}, {"Tower"}});
}

TEST_F(PointValueFlowTest, joinsOnAPoint) {
    createFourCities();
    write("CREATE (n:City {name: 'Twin', at: point({x: 3, y: 4})})");

    expectRows("MATCH (a:City), (b:City) WHERE a.at = b.at AND a.name < b.name RETURN a.name, b.name",
               {{"Grid", "Twin"}});
}

TEST_F(PointValueFlowTest, mergesOnAPoint) {
    write("MERGE (n:City {at: point({x: 1, y: 2})})");
    write("MERGE (n:City {at: point({x: 1, y: 2})})");
    write("MERGE (n:City {at: point({x: 1, y: 2, crs: 'wgs-84'})})");

    expectRows("MATCH (n:City) RETURN n.at",
               {{"point({x: 1.0, y: 2.0, crs: 'cartesian'})"}, {"point({x: 1.0, y: 2.0, crs: 'wgs-84'})"}});
}

TEST_F(PointValueFlowTest, matchesAPointGivenInline) {
    createFourCities();
    expectRows("MATCH (n:City {at: point({x: 3, y: 4})}) RETURN n.name", {{"Grid"}});
}

TEST_F(PointValueFlowTest, projectsAPointPropertyIntoAMap) {
    write("CREATE (n:City {name: 'Grid', at: point({x: 3, y: 4})})");

    expectRows("MATCH (n:City) RETURN n {.at}", {{"{at: point({x: 3.0, y: 4.0, crs: 'cartesian'})}"}});
    expectRows("MATCH (n:City) RETURN properties(n)", {{"{at: point({x: 3.0, y: 4.0, crs: 'cartesian'}), name: Grid}"}});
}

TEST_F(PointValueFlowTest, buildsAPointFromAMapParameterOfARow) {
    expectRows("WITH {longitude: 2.3522, latitude: 48.8566} AS m RETURN point(m)",
               {{"point({x: 2.3522, y: 48.8566, crs: 'wgs-84'})"}});
}

TEST_F(PointValueFlowTest, writesAPointReadOffAListElement) {
    write("UNWIND [point({x: 1, y: 2})] AS p CREATE (n:City {at: p})");
    expectRows("MATCH (n:City) RETURN n.at", {{"point({x: 1.0, y: 2.0, crs: 'cartesian'})"}});
}

TEST_F(PointValueFlowTest, convertsAPointToNothingButAString) {
    expectError("RETURN toInteger(point({x: 1, y: 2}))", "toInteger");
    expectRows("RETURN toIntegerOrNull(point({x: 1, y: 2}))", {{"null"}});
    expectRows("RETURN toStringOrNull(point({x: 1, y: 2}))", {{"point({x: 1.0, y: 2.0, crs: 'cartesian'})"}});
}
