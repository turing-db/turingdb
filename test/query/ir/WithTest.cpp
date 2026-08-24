#include <gtest/gtest.h>

#include <stdint.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "QueryConfig.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "dataframe/Dataframe.h"
#include "versioning/Change.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using Names = std::vector<std::string>;
using NameCountRow = std::pair<std::string, uint64_t>;
using NameCountRows = std::vector<NameCountRow>;

// The eight Person nodes of simpledb
constexpr size_t personCount = 8;

// Its fifteen INTERESTED_IN edges
constexpr size_t interestedInCount = 15;

// Collects a single nullable string column: a projected property, or the alias of one
class NameSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* names = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[0]);
        ASSERT_NE(names, nullptr);

        const auto& nameRaw = names->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            ASSERT_TRUE(nameRaw[rowIndex].has_value());
            _names.push_back(std::string(*nameRaw[rowIndex]));
        }
    }

    const Names& names() const { return _names; }

    void sortedNames(Names& names) const {
        names = _names;
        std::sort(names.begin(), names.end());
    }

private:
    Names _names;
};

// Collects a single node ID column
class NodeSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(chunks[0]);
        ASSERT_NE(nodeIDs, nullptr);

        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _nodes.push_back((*nodeIDs)[rowIndex].getValue());
        }
    }

    size_t getRowCount() const { return _nodes.size(); }

private:
    std::vector<uint64_t> _nodes;
};

// Collects the (name, count) rows of a grouped count carried past a barrier
class NameCountSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* names = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[0]);
        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[1]);
        ASSERT_NE(names, nullptr);
        ASSERT_NE(counts, nullptr);

        const auto& nameRaw = names->getRaw();
        const auto& countRaw = counts->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            ASSERT_TRUE(nameRaw[rowIndex].has_value());
            _rows.emplace_back(std::string(*nameRaw[rowIndex]), countRaw[rowIndex]);
        }
    }

    void sortedRows(NameCountRows& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    NameCountRows _rows;
};

// A constant a WITH bound comes out as a ColumnConst read at every row
class ConstantNameSink : public NLOutputSink {
public:
    using Row = std::pair<int64_t, std::string>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* constants = dynamic_cast<const ColumnConst<int64_t>*>(chunks[0]);
        const auto* names = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[1]);
        ASSERT_NE(constants, nullptr);
        ASSERT_NE(names, nullptr);

        const auto& nameRaw = names->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            ASSERT_TRUE(nameRaw[rowIndex].has_value());
            _rows.emplace_back((*constants)[rowIndex], std::string(*nameRaw[rowIndex]));
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

// A nullable i64 the barrier read out of the graph, carried along the hop that followed
class ValueNameSink : public NLOutputSink {
public:
    using Row = std::pair<std::optional<int64_t>, std::string>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* values = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[0]);
        const auto* names = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[1]);
        ASSERT_NE(values, nullptr);
        ASSERT_NE(names, nullptr);

        const auto& valueRaw = values->getRaw();
        const auto& nameRaw = names->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            ASSERT_TRUE(nameRaw[rowIndex].has_value());
            _rows.emplace_back(valueRaw[rowIndex], std::string(*nameRaw[rowIndex]));
        }
    }

    void sortedRows(std::vector<Row>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<Row> _rows;
};

}

// Every shape of the WITH barrier, driven from Cypher text through the analyzer,
// DBProgramGenerator, NL lowering and the NL interpreter.
class WithTest : public TuringTest {
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

    void expectNames(std::string_view query, const Names& expected) {
        NameSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Names actual;
        sink.sortedNames(actual);

        Names sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(actual, sortedExpected) << "query: " << query;
    }

    // The names in the order the query emits them, for the ORDER BY tests
    void expectNamesInOrder(std::string_view query, const Names& expected) {
        NameSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        EXPECT_EQ(sink.names(), expected) << "query: " << query;
    }

    void expectCounts(std::string_view query, const Counts& expected) {
        CountSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Counts actual;
        sink.sortedCounts(actual);

        EXPECT_EQ(actual, expected) << "query: " << query;
    }

    void expectNameCounts(std::string_view query, const NameCountRows& expected) {
        NameCountSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        NameCountRows actual;
        sink.sortedRows(actual);

        NameCountRows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(actual, sortedExpected) << "query: " << query;
    }

    // Runs a writing query in its own change and submits it, so a following read sees it
    void applyWrite(std::string_view query) {
        ChangeID changeID;
        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            const auto res = system.newChange(_graphName);
            ASSERT_TRUE(res);
            changeID = res.value()->id();
        }

        NameSink sink;
        QueryStatus writeStatus;
        _interpreter->execute(writeStatus,
                              query,
                              _graphName,
                              CommitHash::head(),
                              changeID,
                              &_env->getMem(),
                              &sink);
        ASSERT_TRUE(writeStatus.isOk()) << "query: " << query << "\nerror: " << writeStatus.getError();

        QueryCallbacks callbacks;
        callbacks.setOnOutputData([](const Dataframe*) {});

        const QueryState submitState(_graphName,
                                     &_env->getMem(),
                                     &_queryConfig,
                                     &callbacks,
                                     CommitHash::head(),
                                     changeID);
        const QueryStatus submitStatus = _env->getDB().query("CHANGE SUBMIT", submitState);
        ASSERT_TRUE(submitStatus.isOk()) << "CHANGE SUBMIT failed";
    }

    // The rows the query emits, rendered as text so one helper serves every projection
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

    // A rejection has to be a rejection: turned away at @param stage with a message
    // naming what is wrong with the query, not by a tripped assertion or invalid IR -
    // both of which reach the caller as a failed status all the same
    void expectRejected(std::string_view query, QueryStatus::Status stage) {
        NameSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_FALSE(status.isOk()) << "query accepted: " << query;

        const std::string& error = status.getError();

        EXPECT_EQ(status.getStatus(), stage)
            << "query: " << query
            << "\nstage: " << QueryStatusDescription::value(status.getStatus())
            << "\nerror: " << error;

        EXPECT_EQ(error.find("Unexpected exception"), std::string::npos)
            << "query: " << query << "\nerror: " << error;
        EXPECT_EQ(error.find("Internal Error"), std::string::npos)
            << "query: " << query << "\nerror: " << error;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

TEST_F(WithTest, passesAVariableThrough) {
    NodeSink sink;
    const QueryStatus status = runQuery("MATCH (n:Person) WITH n RETURN n", &sink);
    ASSERT_TRUE(status.isOk()) << status.getError();

    EXPECT_EQ(sink.getRowCount(), personCount);
}

TEST_F(WithTest, passesAnAliasedPropertyThrough) {
    expectNames("MATCH (n:Person) WITH n.name AS name RETURN name",
                {"Adam", "Cyrus", "Doruk", "Luc", "Martina", "Maxime", "Remy", "Suhas"});
}

TEST_F(WithTest, chainsTwoBarriers) {
    expectNames("MATCH (n:Person) WITH n.name AS name WITH name AS person RETURN person",
                {"Adam", "Cyrus", "Doruk", "Luc", "Martina", "Maxime", "Remy", "Suhas"});
}

TEST_F(WithTest, publishesEveryVariableUnderAWildcard) {
    expectNames("MATCH (n:Person {name: 'Remy'}) WITH * RETURN n.name", {"Remy"});
}

// An edge variable is carried by the traversal that produced it, not by one of its own
TEST_F(WithTest, publishesAnEdgeVariable) {
    expectNames("MATCH (p:Person {name: 'Remy'})-[e:KNOWS_WELL]->(x) WITH e RETURN e.name",
                {"Remy -> Adam"});
}

TEST_F(WithTest, publishesUnwoundRows) {
    expectCounts("UNWIND [1, 2, 3] AS x WITH x AS value RETURN count(value)", {3});
}

// The predicate names the alias rather than the match, which is what makes a WITH's
// WHERE read like a HAVING
TEST_F(WithTest, filtersThePublishedRows) {
    expectNames("MATCH (n:Person) WITH n.name AS name WHERE name = 'Remy' RETURN name", {"Remy"});
}

TEST_F(WithTest, filtersThePublishedRowsOnAConjunction) {
    expectNames("MATCH (n:Person) WITH n.name AS name, n.hasPhD AS phd "
                "WHERE phd = true AND name <> 'Remy' "
                "RETURN name",
                {"Adam", "Luc", "Martina"});
}

// Remy has four out-edges and Adam three; no other Person has more than two
TEST_F(WithTest, filtersAGroupedCountLikeAHaving) {
    expectNameCounts("MATCH (p:Person)-->(x) WITH p.name AS name, count(x) AS c WHERE c > 2 "
                     "RETURN name, c",
                     {{"Adam", 3}, {"Remy", 4}});
}

// Computers, Bio and Cooking are each reached twice and Gym three times
TEST_F(WithTest, filtersAGroupedCountOverTheTargets) {
    expectNameCounts("MATCH (p:Person)-[:INTERESTED_IN]->(i) "
                     "WITH i.name AS interest, count(p) AS fans WHERE fans > 1 "
                     "RETURN interest, fans",
                     {{"Bio", 2}, {"Computers", 2}, {"Cooking", 2}, {"Gym", 3}});
}

// Remy's four out-edges are the most of any Person
TEST_F(WithTest, ordersTheGroupsByTheirAggregate) {
    expectNameCounts("MATCH (p:Person)-->(x) WITH p.name AS name, count(x) AS c "
                     "ORDER BY c DESC LIMIT 1 "
                     "RETURN name, c",
                     {{"Remy", 4}});
}

// The four nodes Remy points at
TEST_F(WithTest, filtersOneOfTwoDerivedColumns) {
    expectNames("MATCH (a:Person)-->(b) WITH a.name AS source, b.name AS target "
                "WHERE source = 'Remy' "
                "RETURN target",
                {"Adam", "Computers", "Eighties", "Ghosts"});
}

TEST_F(WithTest, publishesAScalarAggregate) {
    expectCounts("MATCH (n:Person) WITH count(*) AS total RETURN total", {personCount});
}

TEST_F(WithTest, keepsAScalarAggregatePastItsFilter) {
    expectCounts("MATCH (n:Person) WITH count(*) AS total WHERE total > 3 RETURN total",
                 {personCount});
}

TEST_F(WithTest, dropsAScalarAggregateFailingItsFilter) {
    expectCounts("MATCH (n:Person) WITH count(*) AS total WHERE total > 100 RETURN total", {});
}

// Fifteen INTERESTED_IN edges reach ten distinct interests
TEST_F(WithTest, dedupsThePublishedRows) {
    expectNames("MATCH (p:Person)-[:INTERESTED_IN]->(i) WITH DISTINCT i.name AS interest "
                "RETURN interest",
                {"Animals", "Bio", "Computers", "Cooking", "Eighties",
                 "Ghosts", "Gym", "JiuJitsu", "Padel", "Travel"});
}

// The last two Persons in name order: the suffix the barrier's cut left
TEST_F(WithTest, ordersAndSkipsBeforePublishing) {
    expectNamesInOrder("MATCH (p:Person) WITH p.name AS name ORDER BY name SKIP 6 RETURN name",
                       {"Remy", "Suhas"});
}

// The three Persons whose names come first
TEST_F(WithTest, ordersAndLimitsBeforePublishing) {
    expectNamesInOrder("MATCH (p:Person) WITH p.name AS name ORDER BY name LIMIT 3 RETURN name",
                       {"Adam", "Cyrus", "Doruk"});
}

// The WHERE reads the rows the cut left: of the first three Persons in name order, one
// is named Cyrus
TEST_F(WithTest, filtersTheRowsItsCutLeft) {
    expectNames("MATCH (p:Person) WITH p.name AS name ORDER BY name LIMIT 3 "
                "WHERE name = 'Cyrus' "
                "RETURN name",
                {"Cyrus"});
}

// Remy sorts after the cut, so the same predicate over the whole match would keep a row
// this one must not
TEST_F(WithTest, filtersNothingBackIntoTheRowsItsCutDropped) {
    expectNames("MATCH (p:Person) WITH p.name AS name ORDER BY name LIMIT 3 "
                "WHERE name = 'Remy' "
                "RETURN name",
                {});
}

// Remy is interested in three things
TEST_F(WithTest, continuesTheTraversalFromABoundVariable) {
    expectNames("MATCH (p:Person {name: 'Remy'}) WITH p "
                "MATCH (p)-[:INTERESTED_IN]->(i) "
                "RETURN i.name",
                {"Computers", "Eighties", "Ghosts"});
}

TEST_F(WithTest, carriesAPublishedValueAlongTheHop) {
    ValueNameSink sink;
    const QueryStatus status = runQuery("MATCH (p:Person {name: 'Remy'}) WITH p, p.age AS age "
                                        "MATCH (p)-[:INTERESTED_IN]->(i) "
                                        "RETURN age, i.name",
                                        &sink);
    ASSERT_TRUE(status.isOk()) << status.getError();

    std::vector<ValueNameSink::Row> actual;
    sink.sortedRows(actual);

    const std::vector<ValueNameSink::Row> remyInterests {
        {32, "Computers"}, {32, "Eighties"}, {32, "Ghosts"}};
    EXPECT_EQ(actual, remyInterests);
}

// Adam is the first Person in name order, and he has three out-edges
TEST_F(WithTest, continuesTheTraversalFromACutPrefix) {
    NodeSink sink;
    const QueryStatus status = runQuery("MATCH (p:Person) WITH p ORDER BY p.name LIMIT 1 "
                                        "MATCH (p)-->(x) "
                                        "RETURN x",
                                        &sink);
    ASSERT_TRUE(status.isOk()) << status.getError();

    EXPECT_EQ(sink.getRowCount(), 3u);
}

// Of Remy's three interests, Eighties and Ghosts carry the Exotic label and Computers
// does not
TEST_F(WithTest, appliesALabelConstraintToABoundVariable) {
    expectNames("MATCH (p:Person {name: 'Remy'}) WITH p "
                "MATCH (p)-[:INTERESTED_IN]->(i:Exotic) "
                "RETURN i.name",
                {"Eighties", "Ghosts"});
}

TEST_F(WithTest, filtersTheTraversalAfterTheBarrier) {
    expectNames("MATCH (p:Person) WITH p "
                "MATCH (p)-[:INTERESTED_IN]->(i) WHERE i.name = 'Gym' "
                "RETURN p.name",
                {"Cyrus", "Doruk", "Suhas"});
}

// With no part before it the barrier drives no rows, so the constant stands for every row
// the pattern after it produced
TEST_F(WithTest, broadcastsAConstantBoundBeforeAMatch) {
    ConstantNameSink sink;
    const QueryStatus status = runQuery("WITH 1 AS x MATCH (p:Person) RETURN x, p.name", &sink);
    ASSERT_TRUE(status.isOk()) << status.getError();

    std::vector<ConstantNameSink::Row> actual;
    sink.sortedRows(actual);

    ASSERT_EQ(actual.size(), personCount);
    for (const ConstantNameSink::Row& row : actual) {
        EXPECT_EQ(row.first, 1);
    }
}

TEST_F(WithTest, returnsTheBarrierScopeUnderAWildcard) {
    expectNames("MATCH (p:Person {name: 'Remy'}) WITH p.name AS name RETURN *", {"Remy"});
}

TEST_F(WithTest, barriersAroundATraversal) {
    expectNames("MATCH (p:Person {name: 'Remy'}) WITH p "
                "MATCH (p)-[:INTERESTED_IN]->(i) "
                "WITH i.name AS interest "
                "RETURN interest",
                {"Computers", "Eighties", "Ghosts"});
}

TEST_F(WithTest, continuesTheTraversalFromARenamedVariable) {
    expectNames("MATCH (p:Person {name: 'Remy'}) WITH p AS q "
                "MATCH (q)-[:INTERESTED_IN]->(i) "
                "RETURN i.name",
                {"Computers", "Eighties", "Ghosts"});
}

// Gym is reached by three Persons, so it reaches the barrier three times and leaves it
// once - the hop back out has to run over that one row
TEST_F(WithTest, continuesTheTraversalFromDedupedRows) {
    expectNames("MATCH (p:Person)-[:INTERESTED_IN]->(i) WITH DISTINCT i "
                "MATCH (i)<-[:INTERESTED_IN]-(q) WHERE i.name = 'Gym' "
                "RETURN q.name",
                {"Cyrus", "Doruk", "Suhas"});
}

TEST_F(WithTest, skipsAndLimitsAWindowBeforePublishing) {
    expectNamesInOrder("MATCH (p:Person) WITH p.name AS name ORDER BY name SKIP 2 LIMIT 3 "
                       "RETURN name",
                       {"Doruk", "Luc", "Martina"});
}

// Eight Persons crossed with eight Persons
TEST_F(WithTest, publishesAnAggregateOverACrossProduct) {
    expectCounts("MATCH (a:Person), (b:Person) WITH count(*) AS c RETURN c",
                 {personCount * personCount});
}

// A pattern sharing no variable with the barrier is crossed with the rows it published:
// the one Person the barrier kept, against each of the eight the pattern matches.
TEST_F(WithTest, crossesAPatternWithThePublishedRows) {
    expectCounts("MATCH (p:Person {name: 'Remy'}) WITH p MATCH (q:Person) RETURN count(*)",
                 {personCount});
    expectCounts("MATCH (p:Person) WITH p MATCH (q:Person) RETURN count(*)",
                 {personCount * personCount});
}

// The new edge leaves the node the barrier bound, so Remy knows one more person
TEST_F(WithTest, createsFromABoundVariable) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) WITH p "
               "CREATE (p)-[:KNOWS_WELL]->(:Person {name: 'Zoe'})");

    expectNames("MATCH (p:Person {name: 'Remy'})-[:KNOWS_WELL]->(x) RETURN x.name",
                {"Adam", "Zoe"});
}

TEST_F(WithTest, setsAPropertyOfABoundVariable) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) WITH p SET p.dob = '01/01'");

    expectNames("MATCH (p:Person {name: 'Remy'}) RETURN p.dob", {"01/01"});
}

// Every item of a WITH names a column, so an unaliased expression names none
TEST_F(WithTest, rejectsAnUnaliasedExpression) {
    expectRejected("MATCH (n:Person) WITH n.name RETURN n.name",
                   QueryStatus::Status::ANALYZE_ERROR);
}

TEST_F(WithTest, rejectsAVariableTheBarrierDropped) {
    expectRejected("MATCH (n:Person) WITH n.name AS name RETURN n",
                   QueryStatus::Status::ANALYZE_ERROR);
}

// A name the barrier dropped is free again below it, so a pattern spelling it declares a
// variable of its own rather than joining onto what the name used to hold - which makes
// this the cross product above, not a traversal of the published rows: Remy's name
// against each of the three KNOWS_WELL edges of the graph.
TEST_F(WithTest, crossesAPatternReusingADroppedName) {
    expectRows("MATCH (n:Person {name: 'Remy'}) WITH n.name AS name "
               "MATCH (n)-[:KNOWS_WELL]->(x) "
               "RETURN name, x.name",
               {{"Remy", "Adam"}, {"Remy", "Remy"}, {"Remy", "Remy"}});
}

// An aggregate belongs in the projection the filter reads: a WHERE has no group to reduce
TEST_F(WithTest, rejectsAnAggregateInTheFilter) {
    expectRejected("MATCH (n:Person) WITH n WHERE count(*) > 1 RETURN n",
                   QueryStatus::Status::ANALYZE_ERROR);
}

// count(*) counts the rows of the part it ends, and a barrier publishing nothing but a
// nullable value still published one row per row it read: two of the eight Persons carry
// an age, and count(*) is eight of them either way round the items are written.
TEST_F(WithTest, countsEveryRowPastAValueBarrier) {
    expectCounts("MATCH (n:Person) RETURN count(*)", {personCount});
    expectCounts("MATCH (n:Person) WITH n.age AS age RETURN count(*)", {personCount});
    expectCounts("MATCH (n:Person) WITH n.age AS age, n.name AS person RETURN count(*)",
                 {personCount});
    expectCounts("MATCH (n:Person) WITH n.name AS person, n.age AS age RETURN count(*)",
                 {personCount});
}

// count(x) is the other tally and stays the other tally: it counts the rows in which x is
// not null, which is what the two aged Persons are.
TEST_F(WithTest, countsOnlyTheNonNullRowsOfANamedColumn) {
    expectCounts("MATCH (n:Person) WITH n.age AS age RETURN count(age)", {2});
}

// The same over a hop, where the rows are the fifteen INTERESTED_IN edges rather than the
// nodes the barrier read a value off.
TEST_F(WithTest, countsEveryRowOfAHopPastAValueBarrier) {
    expectCounts("MATCH (n:Person)-[:INTERESTED_IN]->(i) RETURN count(*)", {interestedInCount});
    expectCounts("MATCH (n:Person)-[:INTERESTED_IN]->(i) WITH n.age AS age, i RETURN count(*)",
                 {interestedInCount});
}

// A scope of constants alone is one row repeated once per row the part before it matched,
// so count(*) counts those rows - and one row when nothing came before.
TEST_F(WithTest, countsTheRowsOfAScopeOfConstantsAlone) {
    expectRows("MATCH (n:Person) RETURN 1 AS one, count(*) AS c", {{"1", "8"}});
    expectRows("MATCH (n:Person) WITH 1 AS one RETURN one, count(*) AS c", {{"1", "8"}});
    expectCounts("WITH 1 AS one RETURN count(*)", {1});
}

// A WHERE over a scope of constants alone still cuts rows: the predicate holds for every
// row of the barrier or for none of them.
TEST_F(WithTest, filtersAScopeOfConstantsAlone) {
    expectRows("WITH 1 AS x WHERE x > 100 RETURN x", {});
    expectRows("WITH 1 AS x WHERE x < 100 RETURN x", {{"1"}});
    expectRows("MATCH (p:Person {name: 'Remy'}) WITH 1 AS x WHERE x > 100 RETURN x", {});
    expectRows("MATCH (p:Person {name: 'Remy'}) WITH 1 AS x WHERE x < 100 RETURN x", {{"1"}});
    expectRows("WITH 1 AS x WHERE x > 100 MATCH (p:Person) RETURN x, p.name", {});
}

// Two conjuncts over that scope, so the second cuts what the first left rather than the
// single row the constants started as
TEST_F(WithTest, filtersAScopeOfConstantsAloneTwice) {
    expectRows("WITH 1 AS x WHERE x > 100 AND x < 5 RETURN x", {});
    expectRows("WITH 1 AS x WHERE x > 0 AND x < 5 RETURN x", {{"1"}});
    expectRows("MATCH (p:Person {name: 'Remy'}) WITH 1 AS x WHERE x > 100 AND x < 5 RETURN x", {});
}

// A constant published beside a grouping key groups nothing, whether the projection
// spells it out or a wildcard expands it: Remy's three interests, one row each.
TEST_F(WithTest, groupsBesideAConstantAWildcardExpanded) {
    expectRows("MATCH (p:Person {name: 'Remy'})-[:INTERESTED_IN]->(x) "
               "WITH 1 AS one, x.name AS interest "
               "RETURN *, count(interest)",
               {{"1", "Computers", "1"}, {"1", "Eighties", "1"}, {"1", "Ghosts", "1"}});
}

// Grouping again on an aggregate a barrier published: six interests are reached once,
// three of them twice and Gym three times.
TEST_F(WithTest, groupsOnAPublishedAggregate) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i) "
               "WITH i, count(p) AS fans "
               "WITH fans, count(i) AS kinds "
               "RETURN fans, kinds",
               {{"1", "6"}, {"2", "3"}, {"3", "1"}});
}

// The generator names the entities a pattern leaves anonymous, and those names are its
// own: an alias spelling one of them names the column the barrier published, not the
// anonymous end of the hop that follows.
TEST_F(WithTest, keepsAnAliasApartFromAGeneratedName) {
    expectNames("MATCH (p:Person {name: 'Remy'}) WITH p AS v0 "
                "MATCH (v0)-[:INTERESTED_IN]->() "
                "RETURN v0.name",
                {"Remy", "Remy", "Remy"});
}

// An UNWIND names a new variable, so one naming a variable already in scope is not a
// query the barrier can answer: it is two variables under one name.
TEST_F(WithTest, rejectsAnUnwindRedeclaringABoundVariable) {
    expectRejected("MATCH (n:Person) WITH 1 AS x UNWIND [1, 2] AS x RETURN x",
                   QueryStatus::Status::ANALYZE_ERROR);
}

// A CREATE after a barrier writes one node per row the barrier published, whatever the
// scope it published is made of: eight Persons, so eight Tags.
TEST_F(WithTest, createsOneNodePerRowAConstantsOnlyBarrierPublished) {
    applyWrite("MATCH (p:Person) WITH 1 AS one CREATE (:Tag)");

    expectCounts("MATCH (t:Tag) RETURN count(*)", {personCount});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
