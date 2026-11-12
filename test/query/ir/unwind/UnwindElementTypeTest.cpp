#include <gtest/gtest.h>

#include <algorithm>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

#include "ParameterMap.h"
#include "ParameterValue.h"
#include "TuringProtoEncoder.h"
#include "TuringProtoHeaders.h"
#include "columns/AllowedKinds.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "metadata/DateTime.h"
#include "metadata/Duration.h"
#include "metadata/Point.h"

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace db;
using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;
using WireType = net::proto::ColumnInternalKind;

// Reads each output column's type as the wire header names it to a client, beside the rows
class WireTypeRowSink : public StringRowSink {
public:
    void declareOutput(std::span<const std::string_view> names,
                       std::span<const Column* const> chunks) override {
        StringRowSink::declareOutput(names, chunks);

        using HeaderDispatcher = ColumnSingleDispatcher<OutputtedTypes::Allowed,
                                                        net::proto::ColumnHeaderWriter,
                                                        OutputtedTypes::Excluded>;

        for (const Column* chunk : chunks) {
            net::proto::ColumnWireHeader header;
            net::proto::ColumnHeaderWriter writer(header);
            HeaderDispatcher::dispatch(chunk, writer);

            _wireTypes.push_back(static_cast<WireType>(header._typeCode));
        }
    }

    const std::vector<WireType>& getWireTypes() const { return _wireTypes; }

private:
    std::vector<WireType> _wireTypes;
};

}

// The elements an UNWIND hands out take one column type whatever form the unwound value is
// written in: a literal, a parameter, a variable or a computed expression. A homogeneous
// list's elements are a column of their own type, and a heterogeneous list's are cells.
class UnwindElementTypeTest : public CallV3Test {
protected:
    void expectUnwound(std::string_view query,
                       const ParameterMap& parameters,
                       WireType expectedType,
                       const Rows& expectedRows) {
        WireTypeRowSink sink;
        runQuery(query, parameters, sink);

        const std::vector<WireType>& wireTypes = sink.getWireTypes();
        ASSERT_EQ(wireTypes.size(), size_t {1}) << query;
        EXPECT_EQ(std::to_underlying(wireTypes.front()), std::to_underlying(expectedType)) << query;

        Rows rows;
        sink.sortedRows(rows);

        Rows sortedExpected = expectedRows;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(rows, sortedExpected) << query;
    }

    void expectUnwound(std::string_view query, WireType expectedType, const Rows& expectedRows) {
        expectUnwound(query, ParameterMap {}, expectedType, expectedRows);
    }
};

TEST_F(UnwindElementTypeTest, unwindsIntegers) {
    ParameterMap parameters;
    parameters.insert("v").setInt64(5);
    parameters.insert("l").setList().emplace_back().setInt64(5);

    const Rows rows {{"5"}};

    expectUnwound("UNWIND 5 AS x RETURN x", WireType::INT64, rows);
    expectUnwound("UNWIND [5] AS x RETURN x", WireType::INT64, rows);
    expectUnwound("UNWIND $v AS x RETURN x", parameters, WireType::INT64, rows);
    expectUnwound("UNWIND $l AS x RETURN x", parameters, WireType::INT64, rows);
    expectUnwound("WITH 5 AS y UNWIND y AS x RETURN x", WireType::INT64, rows);
    expectUnwound("WITH [5] AS y UNWIND y AS x RETURN x", WireType::INT64, rows);
}

TEST_F(UnwindElementTypeTest, unwindsFloats) {
    ParameterMap parameters;
    parameters.insert("v").setDouble(2.5);
    parameters.insert("l").setList().emplace_back().setDouble(2.5);

    const Rows rows {{"2.5"}};

    expectUnwound("UNWIND 2.5 AS x RETURN x", WireType::DOUBLE, rows);
    expectUnwound("UNWIND [2.5] AS x RETURN x", WireType::DOUBLE, rows);
    expectUnwound("UNWIND $v AS x RETURN x", parameters, WireType::DOUBLE, rows);
    expectUnwound("UNWIND $l AS x RETURN x", parameters, WireType::DOUBLE, rows);
    expectUnwound("WITH 2.5 AS y UNWIND y AS x RETURN x", WireType::DOUBLE, rows);
    expectUnwound("WITH [2.5] AS y UNWIND y AS x RETURN x", WireType::DOUBLE, rows);
}

TEST_F(UnwindElementTypeTest, unwindsStrings) {
    ParameterMap parameters;
    parameters.insert("v").setString("abc");
    parameters.insert("l").setList().emplace_back().setString("abc");

    const Rows rows {{"abc"}};

    expectUnwound("UNWIND 'abc' AS x RETURN x", WireType::STRING, rows);
    expectUnwound("UNWIND ['abc'] AS x RETURN x", WireType::STRING, rows);
    expectUnwound("UNWIND $v AS x RETURN x", parameters, WireType::STRING, rows);
    expectUnwound("UNWIND $l AS x RETURN x", parameters, WireType::STRING, rows);
    expectUnwound("WITH 'abc' AS y UNWIND y AS x RETURN x", WireType::STRING, rows);
    expectUnwound("WITH ['abc'] AS y UNWIND y AS x RETURN x", WireType::STRING, rows);
}

TEST_F(UnwindElementTypeTest, unwindsBools) {
    ParameterMap parameters;
    parameters.insert("v").setBool(true);
    parameters.insert("l").setList().emplace_back().setBool(true);

    const Rows rows {{"true"}};

    expectUnwound("UNWIND true AS x RETURN x", WireType::BOOL, rows);
    expectUnwound("UNWIND [true] AS x RETURN x", WireType::BOOL, rows);
    expectUnwound("UNWIND $v AS x RETURN x", parameters, WireType::BOOL, rows);
    expectUnwound("UNWIND $l AS x RETURN x", parameters, WireType::BOOL, rows);
    expectUnwound("WITH true AS y UNWIND y AS x RETURN x", WireType::BOOL, rows);
    expectUnwound("WITH [true] AS y UNWIND y AS x RETURN x", WireType::BOOL, rows);
}

TEST_F(UnwindElementTypeTest, unwindsEmbeddings) {
    const std::vector<float> embedding {1.0f, 0.0f};

    ParameterMap parameters;
    parameters.insert("v").setEmbedding(embedding);
    parameters.insert("l").setList().emplace_back().setEmbedding(embedding);

    const Rows rows {{"1, 0"}};

    expectUnwound("UNWIND (1.0, 0.0) AS x RETURN x", WireType::EMBEDDING, rows);
    expectUnwound("UNWIND [(1.0, 0.0)] AS x RETURN x", WireType::EMBEDDING, rows);
    expectUnwound("UNWIND $v AS x RETURN x", parameters, WireType::EMBEDDING, rows);
    expectUnwound("UNWIND $l AS x RETURN x", parameters, WireType::EMBEDDING, rows);
    expectUnwound("WITH (1.0, 0.0) AS y UNWIND y AS x RETURN x", WireType::EMBEDDING, rows);
    expectUnwound("WITH [(1.0, 0.0)] AS y UNWIND y AS x RETURN x", WireType::EMBEDDING, rows);
}

TEST_F(UnwindElementTypeTest, unwindsMaps) {
    ParameterMap parameters;
    parameters.insert("v").setMap()["a"].setInt64(1);
    parameters.insert("l").setList().emplace_back().setMap()["a"].setInt64(1);

    const Rows rows {{"{a: 1}"}};

    expectUnwound("UNWIND {a: 1} AS x RETURN x", WireType::MAP_VIEW, rows);
    expectUnwound("UNWIND [{a: 1}] AS x RETURN x", WireType::MAP_VIEW, rows);
    expectUnwound("UNWIND $v AS x RETURN x", parameters, WireType::MAP_VIEW, rows);
    expectUnwound("UNWIND $l AS x RETURN x", parameters, WireType::MAP_VIEW, rows);
    expectUnwound("WITH {a: 1} AS y UNWIND y AS x RETURN x", WireType::MAP_VIEW, rows);
    expectUnwound("WITH [{a: 1}] AS y UNWIND y AS x RETURN x", WireType::MAP_VIEW, rows);
}

TEST_F(UnwindElementTypeTest, unwindsNestedLists) {
    ParameterMap parameters;
    ParameterValue::List& outer = parameters.insert("l").setList();
    ParameterValue::List& first = outer.emplace_back().setList();
    first.emplace_back().setInt64(1);
    first.emplace_back().setInt64(2);
    outer.emplace_back().setList().emplace_back().setInt64(3);

    const Rows rows {{"1, 2"}, {"3"}};

    expectUnwound("UNWIND [[1, 2], [3]] AS x RETURN x", WireType::LIST_VIEW, rows);
    expectUnwound("UNWIND $l AS x RETURN x", parameters, WireType::LIST_VIEW, rows);
    expectUnwound("WITH [[1, 2], [3]] AS y UNWIND y AS x RETURN x", WireType::LIST_VIEW, rows);
}

TEST_F(UnwindElementTypeTest, unwindsDateTimes) {
    const DateTime dateTime = DateTime::parse("2020-03-04T00:00:00Z").value();

    ParameterMap parameters;
    parameters.insert("v").setDateTime(dateTime);
    parameters.insert("l").setList().emplace_back().setDateTime(dateTime);

    const Rows rows {{"2020-03-04T00:00:00Z"}};

    expectUnwound("UNWIND datetime('2020-03-04T00:00:00Z') AS x RETURN x", WireType::DATE_TIME, rows);
    expectUnwound("UNWIND [datetime('2020-03-04T00:00:00Z')] AS x RETURN x", WireType::DATE_TIME, rows);
    expectUnwound("UNWIND $v AS x RETURN x", parameters, WireType::DATE_TIME, rows);
    expectUnwound("UNWIND $l AS x RETURN x", parameters, WireType::DATE_TIME, rows);
    expectUnwound("WITH datetime('2020-03-04T00:00:00Z') AS y UNWIND y AS x RETURN x", WireType::DATE_TIME, rows);
    expectUnwound("WITH [datetime('2020-03-04T00:00:00Z')] AS y UNWIND y AS x RETURN x", WireType::DATE_TIME, rows);
}

TEST_F(UnwindElementTypeTest, unwindsDurations) {
    const Duration duration {3600000000};

    ParameterMap parameters;
    parameters.insert("v").setDuration(duration);
    parameters.insert("l").setList().emplace_back().setDuration(duration);

    const Rows rows {{"PT1H"}};

    expectUnwound("UNWIND duration({hours: 1}) AS x RETURN x", WireType::DURATION, rows);
    expectUnwound("UNWIND [duration({hours: 1})] AS x RETURN x", WireType::DURATION, rows);
    expectUnwound("UNWIND $v AS x RETURN x", parameters, WireType::DURATION, rows);
    expectUnwound("UNWIND $l AS x RETURN x", parameters, WireType::DURATION, rows);
    expectUnwound("WITH duration({hours: 1}) AS y UNWIND y AS x RETURN x", WireType::DURATION, rows);
    expectUnwound("WITH [duration({hours: 1})] AS y UNWIND y AS x RETURN x", WireType::DURATION, rows);
}

TEST_F(UnwindElementTypeTest, unwindsPoints) {
    const Point point(PointCRS::Cartesian, 1.0, 2.0, 0.0);

    ParameterMap parameters;
    parameters.insert("v").setPoint(point);
    parameters.insert("l").setList().emplace_back().setPoint(point);

    const Rows rows {{"point({x: 1.0, y: 2.0, crs: 'cartesian'})"}};

    expectUnwound("UNWIND point({x: 1.0, y: 2.0}) AS x RETURN x", WireType::POINT, rows);
    expectUnwound("UNWIND [point({x: 1.0, y: 2.0})] AS x RETURN x", WireType::POINT, rows);
    expectUnwound("UNWIND $v AS x RETURN x", parameters, WireType::POINT, rows);
    expectUnwound("UNWIND $l AS x RETURN x", parameters, WireType::POINT, rows);
    expectUnwound("WITH point({x: 1.0, y: 2.0}) AS y UNWIND y AS x RETURN x", WireType::POINT, rows);
    expectUnwound("WITH [point({x: 1.0, y: 2.0})] AS y UNWIND y AS x RETURN x", WireType::POINT, rows);
}

TEST_F(UnwindElementTypeTest, unwindsNodes) {
    const Rows rows {{"0"}, {"1"}};

    expectUnwound("MATCH (n:Founder) UNWIND n AS x RETURN x", WireType::NODE_ID, rows);
    expectUnwound("MATCH (n:Founder) UNWIND [n] AS x RETURN x", WireType::NODE_ID, rows);
    expectUnwound("MATCH (n:Founder) WITH collect(n) AS y UNWIND y AS x RETURN x", WireType::NODE_ID, rows);
}

TEST_F(UnwindElementTypeTest, unwindsPaths) {
    const Rows rows {{"(6), [7], (0)"}};

    expectUnwound("MATCH p = (a)-[:KNOWS_WELL]->(b) WHERE a.name = 'Ghosts' UNWIND p AS x RETURN x", WireType::GRAPH_PATH, rows);
    expectUnwound("MATCH p = (a)-[:KNOWS_WELL]->(b) WHERE a.name = 'Ghosts' UNWIND [p] AS x RETURN x", WireType::GRAPH_PATH, rows);
    expectUnwound("MATCH p = (a)-[:KNOWS_WELL]->(b) WHERE a.name = 'Ghosts' WITH collect(p) AS y UNWIND y AS x RETURN x", WireType::GRAPH_PATH, rows);
}

TEST_F(UnwindElementTypeTest, unwindsAnEmbeddingBesideANull) {
    const std::vector<float> embedding {1.0f, 0.0f};

    ParameterMap parameters;
    ParameterValue::List& list = parameters.insert("l").setList();
    list.emplace_back().setEmbedding(embedding);
    list.emplace_back().setNull();

    const Rows rows {{"1, 0"}, {"null"}};

    expectUnwound("UNWIND [(1.0, 0.0), null] AS x RETURN x", WireType::EMBEDDING, rows);
    expectUnwound("UNWIND $l AS x RETURN x", parameters, WireType::EMBEDDING, rows);
    expectUnwound("WITH [(1.0, 0.0), null] AS y UNWIND y AS x RETURN x", WireType::EMBEDDING, rows);
}

TEST_F(UnwindElementTypeTest, unwindsAMixedListToCells) {
    const std::vector<float> embedding {1.0f, 0.0f};

    ParameterMap parameters;
    ParameterValue::List& list = parameters.insert("l").setList();
    list.emplace_back().setEmbedding(embedding);
    list.emplace_back().setMap()["a"].setInt64(1);
    list.emplace_back().setInt64(1);

    const Rows rows {{"[1, 0]"}, {"{a: 1}"}, {"1"}};

    expectUnwound("UNWIND [(1.0, 0.0), {a: 1}, 1] AS x RETURN x", WireType::CELL_VIEW, rows);
    expectUnwound("UNWIND $l AS x RETURN x", parameters, WireType::CELL_VIEW, rows);
    expectUnwound("WITH [(1.0, 0.0), {a: 1}, 1] AS y UNWIND y AS x RETURN x", WireType::CELL_VIEW, rows);
}
