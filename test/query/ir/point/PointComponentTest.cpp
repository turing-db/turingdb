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

class PointComponentTest : public TuringTest {
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

TEST_F(PointComponentTest, readsTheCoordinatesOfAPoint) {
    expectRows("RETURN point({x: 1, y: 2}).x, point({x: 1, y: 2}).y", {{"1.000000", "2.000000"}});
    expectRows("WITH point({x: 1, y: 2, z: 3}) AS p RETURN p.x, p.y, p.z", {{"1.000000", "2.000000", "3.000000"}});
}

TEST_F(PointComponentTest, readsTheGeographicNamesOfTheCoordinates) {
    expectRows("WITH point({longitude: 2.5, latitude: 48.5, height: 35}) AS p "
               "RETURN p.longitude, p.latitude, p.height, p.x, p.y, p.z",
               {{"2.500000", "48.500000", "35.000000", "2.500000", "48.500000", "35.000000"}});
}

TEST_F(PointComponentTest, readsTheCRSAndTheSRID) {
    expectRows("UNWIND [{x: 1, y: 2}, {x: 1, y: 2, z: 3}, {longitude: 1, latitude: 2}, "
               "{longitude: 1, latitude: 2, height: 3}] AS m "
               "WITH point(m) AS p RETURN p.crs, p.srid",
               {{"cartesian", "7203"}, {"cartesian-3d", "9157"}, {"wgs-84", "4326"}, {"wgs-84-3d", "4979"}});
}

TEST_F(PointComponentTest, readsTheComponentsOfAStoredPoint) {
    write("CREATE (n:City {name: 'Paris', at: point({longitude: 2.5, latitude: 48.5})})");
    expectRows("MATCH (n:City) RETURN n.at.longitude, n.at.latitude, n.at.crs",
               {{"2.500000", "48.500000", "wgs-84"}});
}

TEST_F(PointComponentTest, computesWithAComponent) {
    expectRows("WITH point({x: 3, y: 4}) AS p RETURN sqrt(p.x * p.x + p.y * p.y)", {{"5.000000"}});
}

TEST_F(PointComponentTest, filtersAndOrdersOnAComponent) {
    write("CREATE (n:City {name: 'a', at: point({x: 3, y: 1})})");
    write("CREATE (n:City {name: 'b', at: point({x: 1, y: 2})})");
    write("CREATE (n:City {name: 'c', at: point({x: 2, y: 3})})");

    expectRows("MATCH (n:City) WHERE n.at.x >= 2 RETURN n.name", {{"a"}, {"c"}});
    expectOrderedRows("MATCH (n:City) RETURN n.name ORDER BY n.at.y DESC", {{"c"}, {"b"}, {"a"}});
}

TEST_F(PointComponentTest, readsNullOffANullPoint) {
    write("CREATE (n:City {name: 'a'})");
    expectRows("MATCH (n:City) RETURN n.at.x, n.at.crs, n.at.srid", {{"null", "null", "null"}});
}

TEST_F(PointComponentTest, rejectsAThirdCoordinateOfA2DPoint) {
    expectError("WITH point({x: 1, y: 2}) AS p RETURN p.z",
                "The point point({x: 1.0, y: 2.0, crs: 'cartesian'}) has no z");
    expectError("WITH point({longitude: 1, latitude: 2}) AS p RETURN p.height",
                "The point point({x: 1.0, y: 2.0, crs: 'wgs-84'}) has no height");
}

TEST_F(PointComponentTest, rejectsAGeographicNameOfACartesianPoint) {
    expectError("WITH point({x: 1, y: 2}) AS p RETURN p.latitude", "has no latitude");
    expectError("WITH point({x: 1, y: 2, z: 3}) AS p RETURN p.height", "has no height");
}

TEST_F(PointComponentTest, rejectsANameThatIsNoComponent) {
    expectError("WITH point({x: 1, y: 2}) AS p RETURN p.w", "'w' is not a component of a point");
    expectError("RETURN point({x: 1, y: 2}).w", "'w' is not a component of a point");
}

TEST_F(PointComponentTest, readsTheComponentOfAPointAListHolds) {
    expectRows("UNWIND [point({x: 1, y: 2}), point({longitude: 3, latitude: 4})] AS p RETURN p.x, p.crs, p.srid",
               {{"1.000000", "cartesian", "7203"}, {"3.000000", "wgs-84", "4326"}});
}

TEST_F(PointComponentTest, rejectsANameThatIsNoComponentOfAPointAListHolds) {
    expectError("UNWIND [point({x: 1, y: 2})] AS p RETURN p.w", "'w' is not a component of a point");
    expectError("UNWIND [point({x: 1, y: 2})] AS p RETURN p.z", "has no z");
}

TEST_F(PointComponentTest, projectsTheComponentsOfAPoint) {
    expectRows("WITH point({longitude: 1, latitude: 2}) AS p RETURN p {.longitude, .crs, n: 1}",
               {{"{crs: wgs-84, longitude: 1.000000, n: 1}"}});
}

TEST_F(PointComponentTest, rejectsEveryPropertyOfAPoint) {
    expectError("WITH point({x: 1, y: 2}) AS p RETURN p {.*}", "A map projection reads .* off a node, a relationship or a map");
}

TEST_F(PointComponentTest, rejectsAComponentAsTheTargetOfAWrite) {
    write("CREATE (n:City {name: 'a', at: point({x: 1, y: 2})})");
    expectError("MATCH (n:City) SET n.at.x = 3", "component");
}
