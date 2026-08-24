#include <gtest/gtest.h>

#include <stdint.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "metadata/PropertyNull.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

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

    if (const auto* plain = dynamic_cast<const ColumnVector<T>*>(column)) {
        const T value = (*plain)[row];
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
        throw std::runtime_error("WithMultiPartTest: unsupported output column type");
    }
}

// Renders every output row as strings, whatever the column types are, so one sink serves
// every projection these queries end on
class RowSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            Row& cells = _rows.emplace_back();
            cells.resize(chunks.size());

            for (size_t column = 0; column < chunks.size(); column++) {
                renderCell(chunks[column], rowIndex, cells[column]);
            }
        }
    }

    void sortedRows(Rows& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

    const Rows& rows() const { return _rows; }

private:
    Rows _rows;
};

void describeRows(const Rows& rows, std::string& out) {
    out.clear();
    for (const Row& row : rows) {
        out += "        {";
        for (size_t cell = 0; cell < row.size(); cell++) {
            out += cell == 0 ? "\"" : ", \"";
            out += row[cell];
            out += "\"";
        }
        out += "},\n";
    }
}

}

// Multi-part queries: a chain of WITH barriers where each part matches over what the one
// before published. These are the shapes a person actually writes - rank then follow up,
// filter an aggregate then aggregate again, narrow then fan back out - so they exercise
// the barrier against real combinations of grouping, cutting, dedup and traversal rather
// than one feature at a time.
class WithMultiPartTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    QueryStatus runQuery(std::string_view query, NLOutputSink* sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              sink);

        return status;
    }

    // The rows the query emits, compared order-independently
    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    // The rows in the order the query emits them, for the queries whose ORDER BY is the
    // point of the test
    void expectRowsInOrder(std::string_view query, const Rows& expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::string actualText;
        describeRows(sink.rows(), actualText);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query << "\ngot:\n" << actualText;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// Rank the interests by how many Persons reach them, keep the top two, then fan back out
// and count the followers of each. Gym is reached three times and Bio, Computers and
// Cooking twice each, so the tie is broken by name and Bio takes the second place.
TEST_F(WithMultiPartTest, ranksThenFansBackOut) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i) "
               "WITH i, count(p) AS fans ORDER BY fans DESC, i.name LIMIT 2 "
               "MATCH (i)<-[:INTERESTED_IN]-(q:Person) "
               "RETURN i.name, count(q)",
               {{"Bio", "2"}, {"Gym", "3"}});
}

// The Person with the most out-edges, then what they are interested in: Remy has four,
// three of which are interests.
TEST_F(WithMultiPartTest, followsUpOnTheTopRankedRow) {
    expectRows("MATCH (p:Person)-->(x) "
               "WITH p, count(x) AS degree ORDER BY degree DESC LIMIT 1 "
               "MATCH (p)-[:INTERESTED_IN]->(i) "
               "RETURN i.name",
               {{"Computers"}, {"Eighties"}, {"Ghosts"}});
}

// Two aggregating barriers, each filtered like a HAVING. The first drops Martina and
// Doruk, who have one interest each, and that changes the second tally: Cooking falls to
// one sharer and Gym from three to two.
TEST_F(WithMultiPartTest, chainsTwoHavings) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i) "
               "WITH p, count(i) AS interests WHERE interests > 1 "
               "MATCH (p)-[:INTERESTED_IN]->(j) "
               "WITH j.name AS interest, count(p) AS sharers WHERE sharers > 1 "
               "RETURN interest, sharers",
               {{"Bio", "2"}, {"Computers", "2"}, {"Gym", "2"}});
}

// Narrow, hop, dedup, hop back, count. The four French Persons reach seven distinct
// interests, and those seven are reached ten times in all.
TEST_F(WithMultiPartTest, narrowsThenFansOutOverDedupedRows) {
    expectRows("MATCH (p:Person) "
               "WITH p WHERE p.isFrench = true "
               "MATCH (p)-[:INTERESTED_IN]->(i) "
               "WITH DISTINCT i "
               "MATCH (i)<-[:INTERESTED_IN]-(q:Person) "
               "WITH q.name AS person "
               "RETURN count(person)",
               {{"10"}});
}

// An edge property published beside the node the edge points at, filtered on, then
// followed one hop further. Three INTERESTED_IN edges are held with expert proficiency,
// and only Ghosts - the one Remy reaches - knows anybody well.
TEST_F(WithMultiPartTest, carriesAnEdgePropertyAcrossAFurtherHop) {
    expectRows("MATCH (p:Person)-[e:INTERESTED_IN]->(i) "
               "WITH p.name AS person, e.proficiency AS level, i WHERE level = 'expert' "
               "MATCH (i)-[:KNOWS_WELL]->(k) "
               "RETURN person, level, k.name",
               {{"Remy", "expert", "Remy"}});
}

// Three barriers ending on a grouped aggregate of the last hop: the four Persons with a
// PhD and how many interests each of them holds.
TEST_F(WithMultiPartTest, groupsTheLastHopAfterThreeBarriers) {
    expectRows("MATCH (p:Person) WITH p WHERE p.hasPhD = true "
               "MATCH (p)-[:INTERESTED_IN]->(i) WITH DISTINCT p, i "
               "WITH p.name AS person, count(i) AS interests "
               "RETURN person, interests",
               {{"Adam", "2"}, {"Luc", "2"}, {"Martina", "1"}, {"Remy", "3"}});
}

// An aggregate republished through a pass-through barrier, filtered there, and ordered by
// in the projection that ends the query
TEST_F(WithMultiPartTest, republishesAnAggregateThroughASecondBarrier) {
    expectRowsInOrder("MATCH (p:Person)-[:INTERESTED_IN]->(i) "
                      "WITH p.name AS person, count(i) AS interests "
                      "WITH person, interests WHERE interests > 1 "
                      "RETURN person, interests ORDER BY interests DESC, person",
                      {{"Remy", "3"},
                       {"Adam", "2"},
                       {"Cyrus", "2"},
                       {"Luc", "2"},
                       {"Maxime", "2"},
                       {"Suhas", "2"}});
}

// Paging a ranking on the barrier: Gym leads with three followers, so skipping it leaves
// the three interests reached twice, in name order.
TEST_F(WithMultiPartTest, pagesARankingOnTheBarrier) {
    expectRowsInOrder("MATCH (p:Person)-[:INTERESTED_IN]->(i) "
                      "WITH i.name AS interest, count(p) AS fans "
                      "ORDER BY fans DESC, interest SKIP 1 LIMIT 3 "
                      "RETURN interest, fans",
                      {{"Bio", "2"}, {"Computers", "2"}, {"Cooking", "2"}});
}

// The common-neighbour shape: a pattern whose two ends are both variables the barrier
// bound. Neither end fans out - the hop out of one of them has to land on the other - and
// Computers is the one interest Remy and Luc share.
TEST_F(WithMultiPartTest, joinsTwoBoundVariablesThroughANewNode) {
    expectRows("MATCH (a:Person), (b:Person) "
               "WITH a, b WHERE a.name = 'Remy' AND b.name = 'Luc' "
               "MATCH (a)-[:INTERESTED_IN]->(i)<-[:INTERESTED_IN]-(b) "
               "RETURN i.name",
               {{"Computers"}});
}

// The same join with nothing between its ends: of the four nodes Remy points at, one is
// the one he knows well.
TEST_F(WithMultiPartTest, joinsTwoBoundVariablesDirectly) {
    expectRows("MATCH (a:Person {name: 'Remy'})-->(b) WITH a, b "
               "MATCH (a)-[:KNOWS_WELL]->(b) "
               "RETURN b.name",
               {{"Adam"}});
}

// The edge type of a closing join has to constrain it: every Person-to-Person edge of
// simpledb is a KNOWS_WELL, and the seventeen edges out of a Person are not.
TEST_F(WithMultiPartTest, constrainsTheEdgeTypeOfAClosingJoin) {
    expectRows("MATCH (a:Person)-->(b) WITH a, b "
               "MATCH (a)-[:KNOWS_WELL]->(b) "
               "RETURN a.name, b.name",
               {{"Adam", "Remy"}, {"Remy", "Adam"}});
}

// A closing join walked without a direction, which reads the edge either way round: Remy
// knows Adam well in both directions, and Ghosts knows him well the other way.
TEST_F(WithMultiPartTest, joinsTwoBoundVariablesUndirected) {
    expectRows("MATCH (a:Person {name: 'Remy'})-->(b) WITH a, b "
               "MATCH (a)-[:KNOWS_WELL]-(b) "
               "RETURN b.name",
               {{"Adam"}, {"Adam"}, {"Ghosts"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
