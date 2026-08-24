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

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using Names = std::vector<std::string>;
using Counts = std::vector<uint64_t>;
using NameCountRow = std::pair<std::string, uint64_t>;
using NameCountRows = std::vector<NameCountRow>;
using Int64Values = std::vector<int64_t>;

// The eight Person nodes of simpledb
constexpr size_t personCount = 8;

// Collects a single nullable string column: the shape of a projected property or of the
// alias a WITH gives one
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

// Collects a single non-null ui64 column: what a count reduces to
class CountSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[0]);
        ASSERT_NE(counts, nullptr);

        const auto& countRaw = counts->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _counts.push_back(countRaw[rowIndex]);
        }
    }

    void sortedCounts(Counts& counts) const {
        counts = _counts;
        std::sort(counts.begin(), counts.end());
    }

private:
    Counts _counts;
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

// Collects a constant column beside a nullable string one: a constant a WITH bound holds
// one value standing for every row, so it comes out as a ColumnConst read at each of them
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

// The same pair for a value the barrier read out of the graph: a nullable i64 property
// carried along the hop that followed, beside a name of the row it landed on
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

// WITH is a projection that replaces the scope of the query rather than ending it: the
// statements after it read the columns it publishes and nothing else. These tests drive
// every shape of that barrier - a pass-through, an aggregation filtered like a HAVING, a
// dedup, an ordered cut feeding a further traversal - from Cypher text through the
// analyzer, DBProgramGenerator, NL lowering and the NL interpreter.
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

    // The names in the order the query emits them, for an ORDER BY whose order is the
    // point of the test
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

    // Runs a writing query in its own change and submits it, so a following read sees
    // what it wrote
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

    void expectRejected(std::string_view query) {
        NameSink sink;
        const QueryStatus status = runQuery(query, &sink);
        EXPECT_FALSE(status.isOk()) << "query accepted: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

// The plainest barrier: it publishes the variable it was given and the RETURN reads it
// back, so the rows are the ones the MATCH produced.
TEST_F(WithTest, passesAVariableThrough) {
    NodeSink sink;
    const QueryStatus status = runQuery("MATCH (n:Person) WITH n RETURN n", &sink);
    ASSERT_TRUE(status.isOk()) << status.getError();

    EXPECT_EQ(sink.getRowCount(), personCount);
}

// An aliased property: the column the barrier publishes is the one the projection
// computed, and the alias is the only name the RETURN has for it.
TEST_F(WithTest, passesAnAliasedPropertyThrough) {
    expectNames("MATCH (n:Person) WITH n.name AS name RETURN name",
                {"Adam", "Cyrus", "Doruk", "Luc", "Martina", "Maxime", "Remy", "Suhas"});
}

// Two barriers in a row: the second reads what the first published, and the alias travels
// through both.
TEST_F(WithTest, chainsTwoBarriers) {
    expectNames("MATCH (n:Person) WITH n.name AS name WITH name AS person RETURN person",
                {"Adam", "Cyrus", "Doruk", "Luc", "Martina", "Maxime", "Remy", "Suhas"});
}

// WITH * publishes every variable in scope, so the barrier keeps the match's binding.
TEST_F(WithTest, publishesEveryVariableUnderAWildcard) {
    expectNames("MATCH (n:Person {name: 'Remy'}) WITH * RETURN n.name", {"Remy"});
}

// An edge variable is carried by the traversal that produced it rather than by a variable
// of its own, and the barrier publishes it all the same: the property read after it comes
// off the edge the match bound.
TEST_F(WithTest, publishesAnEdgeVariable) {
    expectNames("MATCH (p:Person {name: 'Remy'})-[e:KNOWS_WELL]->(x) WITH e RETURN e.name",
                {"Remy -> Adam"});
}

// An UNWIND opens a dataflow of its own, and a barrier publishes its rows like any other
TEST_F(WithTest, publishesUnwoundRows) {
    expectCounts("UNWIND [1, 2, 3] AS x WITH x AS value RETURN count(value)", {3});
}

// The WHERE of a WITH filters the rows the projection published, which is what makes it
// read like a HAVING: the predicate names the alias, not the match.
TEST_F(WithTest, filtersThePublishedRows) {
    expectNames("MATCH (n:Person) WITH n.name AS name WHERE name = 'Remy' RETURN name", {"Remy"});
}

// The same filter over two conjuncts, each cutting the published rows in turn
TEST_F(WithTest, filtersThePublishedRowsOnAConjunction) {
    expectNames("MATCH (n:Person) WITH n.name AS name, n.hasPhD AS phd "
                "WHERE phd = true AND name <> 'Remy' "
                "RETURN name",
                {"Adam", "Luc", "Martina"});
}

// A grouped count carried past the barrier and filtered on: Remy has four out-edges and
// Adam three, and no other Person has more than two.
TEST_F(WithTest, filtersAGroupedCountLikeAHaving) {
    expectNameCounts("MATCH (p:Person)-->(x) WITH p.name AS name, count(x) AS c WHERE c > 2 "
                     "RETURN name, c",
                     {{"Adam", 3}, {"Remy", 4}});
}

// The same grouping the other way round: an interest and how many Persons reach it.
// Computers, Bio and Cooking are each reached twice and Gym three times.
TEST_F(WithTest, filtersAGroupedCountOverTheTargets) {
    expectNameCounts("MATCH (p:Person)-[:INTERESTED_IN]->(i) "
                     "WITH i.name AS interest, count(p) AS fans WHERE fans > 1 "
                     "RETURN interest, fans",
                     {{"Bio", 2}, {"Computers", 2}, {"Cooking", 2}, {"Gym", 3}});
}

// The groups a barrier published, ordered by the aggregate that reduced them and cut to
// the first: Remy's four out-edges are the most of any Person.
TEST_F(WithTest, ordersTheGroupsByTheirAggregate) {
    expectNameCounts("MATCH (p:Person)-->(x) WITH p.name AS name, count(x) AS c "
                     "ORDER BY c DESC LIMIT 1 "
                     "RETURN name, c",
                     {{"Remy", 4}});
}

// Two derived columns published together and filtered on one of them: the four nodes
// Remy points at.
TEST_F(WithTest, filtersOneOfTwoDerivedColumns) {
    expectNames("MATCH (a:Person)-->(b) WITH a.name AS source, b.name AS target "
                "WHERE source = 'Remy' "
                "RETURN target",
                {"Adam", "Computers", "Eighties", "Ghosts"});
}

// A scalar aggregate is one row, and the barrier publishes it as any other column: the
// filter then either keeps that row or drops it.
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

// WITH DISTINCT dedups the rows the barrier publishes: fifteen INTERESTED_IN edges reach
// ten distinct interests.
TEST_F(WithTest, dedupsThePublishedRows) {
    expectNames("MATCH (p:Person)-[:INTERESTED_IN]->(i) WITH DISTINCT i.name AS interest "
                "RETURN interest",
                {"Animals", "Bio", "Computers", "Cooking", "Eighties",
                 "Ghosts", "Gym", "JiuJitsu", "Padel", "Travel"});
}

// ORDER BY and SKIP shape the rows before they are published, so the RETURN reads the
// suffix the barrier left: the last two Persons in name order.
TEST_F(WithTest, ordersAndSkipsBeforePublishing) {
    expectNamesInOrder("MATCH (p:Person) WITH p.name AS name ORDER BY name SKIP 6 RETURN name",
                       {"Remy", "Suhas"});
}

// ORDER BY with a LIMIT, the top-K shape: the three Persons whose names come first.
TEST_F(WithTest, ordersAndLimitsBeforePublishing) {
    expectNamesInOrder("MATCH (p:Person) WITH p.name AS name ORDER BY name LIMIT 3 RETURN name",
                       {"Adam", "Cyrus", "Doruk"});
}

// The WHERE of a WITH reads the rows that survived its cut, so it filters the limited
// prefix rather than the whole match: of the first three Persons in name order, one is
// named Cyrus.
TEST_F(WithTest, filtersTheRowsItsCutLeft) {
    expectNames("MATCH (p:Person) WITH p.name AS name ORDER BY name LIMIT 3 "
                "WHERE name = 'Cyrus' "
                "RETURN name",
                {"Cyrus"});
}

// A Person whose name sorts after the cut is gone by the time the filter runs, so the
// same predicate over the whole match would have kept a row this one must not.
TEST_F(WithTest, filtersNothingBackIntoTheRowsItsCutDropped) {
    expectNames("MATCH (p:Person) WITH p.name AS name ORDER BY name LIMIT 3 "
                "WHERE name = 'Remy' "
                "RETURN name",
                {});
}

// A pattern after the barrier joins onto the variable it published rather than scanning:
// Remy is interested in three things.
TEST_F(WithTest, continuesTheTraversalFromABoundVariable) {
    expectNames("MATCH (p:Person {name: 'Remy'}) WITH p "
                "MATCH (p)-[:INTERESTED_IN]->(i) "
                "RETURN i.name",
                {"Computers", "Eighties", "Ghosts"});
}

// The columns beside the joined variable are carried along the hop, so a value the
// barrier published stays row-aligned with the rows the hop produced.
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

// An ordered cut feeding a further traversal: Adam is the first Person in name order, and
// he has three out-edges.
TEST_F(WithTest, continuesTheTraversalFromACutPrefix) {
    NodeSink sink;
    const QueryStatus status = runQuery("MATCH (p:Person) WITH p ORDER BY p.name LIMIT 1 "
                                        "MATCH (p)-->(x) "
                                        "RETURN x",
                                        &sink);
    ASSERT_TRUE(status.isOk()) << status.getError();

    EXPECT_EQ(sink.getRowCount(), 3u);
}

// A label the pattern after the barrier asks for constrains the variable it joined onto,
// so the bound rows are filtered by it: of Remy's three interests, Eighties and Ghosts
// carry the Exotic label and Computers does not.
TEST_F(WithTest, appliesALabelConstraintToABoundVariable) {
    expectNames("MATCH (p:Person {name: 'Remy'}) WITH p "
                "MATCH (p)-[:INTERESTED_IN]->(i:Exotic) "
                "RETURN i.name",
                {"Eighties", "Ghosts"});
}

// A predicate of the MATCH after the barrier cuts the joined rows the ordinary way
TEST_F(WithTest, filtersTheTraversalAfterTheBarrier) {
    expectNames("MATCH (p:Person) WITH p "
                "MATCH (p)-[:INTERESTED_IN]->(i) WHERE i.name = 'Gym' "
                "RETURN p.name",
                {"Cyrus", "Doruk", "Suhas"});
}

// A barrier binding constants alone drives no rows of its own, so the pattern after it
// matches on its own and the constant stands for every row it produced.
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

// The wildcard of a RETURN after a barrier expands to the barrier's scope, which is the
// only thing left in it.
TEST_F(WithTest, returnsTheBarrierScopeUnderAWildcard) {
    expectNames("MATCH (p:Person {name: 'Remy'}) WITH p.name AS name RETURN *", {"Remy"});
}

// A traversal between two barriers: the second publishes a property of the rows the hop
// produced from what the first published.
TEST_F(WithTest, barriersAroundATraversal) {
    expectNames("MATCH (p:Person {name: 'Remy'}) WITH p "
                "MATCH (p)-[:INTERESTED_IN]->(i) "
                "WITH i.name AS interest "
                "RETURN interest",
                {"Computers", "Eighties", "Ghosts"});
}

// An alias renames the variable it publishes, and the pattern after the barrier joins onto
// it under the new name.
TEST_F(WithTest, continuesTheTraversalFromARenamedVariable) {
    expectNames("MATCH (p:Person {name: 'Remy'}) WITH p AS q "
                "MATCH (q)-[:INTERESTED_IN]->(i) "
                "RETURN i.name",
                {"Computers", "Eighties", "Ghosts"});
}

// A dedup feeding a further traversal: Gym is reached by three Persons, so it reaches the
// barrier three times and leaves it once - the hop back out of it must run over that one
// row, giving each of the three Persons once.
TEST_F(WithTest, continuesTheTraversalFromDedupedRows) {
    expectNames("MATCH (p:Person)-[:INTERESTED_IN]->(i) WITH DISTINCT i "
                "MATCH (i)<-[:INTERESTED_IN]-(q) WHERE i.name = 'Gym' "
                "RETURN q.name",
                {"Cyrus", "Doruk", "Suhas"});
}

// SKIP and LIMIT together cut a window out of the ordered rows before publishing them
TEST_F(WithTest, skipsAndLimitsAWindowBeforePublishing) {
    expectNamesInOrder("MATCH (p:Person) WITH p.name AS name ORDER BY name SKIP 2 LIMIT 3 "
                       "RETURN name",
                       {"Doruk", "Luc", "Martina"});
}

// A barrier over a cross product reduces the rows of the product: eight Persons crossed
// with eight Persons.
TEST_F(WithTest, publishesAnAggregateOverACrossProduct) {
    expectCounts("MATCH (a:Person), (b:Person) WITH count(*) AS c RETURN c",
                 {personCount * personCount});
}

// A pattern sharing no variable with the barrier would have to be crossed with the rows it
// published, which the generator does not build yet: it must say so rather than answer
// with the wrong row set.
TEST_F(WithTest, rejectsAPatternCrossedWithThePublishedRows) {
    expectRejected("MATCH (p:Person) WITH p MATCH (q:Person) RETURN p, q");
}

// An update after the barrier writes over the rows it published: the new edge leaves the
// node the barrier bound, so Remy knows one more person than before.
TEST_F(WithTest, createsFromABoundVariable) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) WITH p "
               "CREATE (p)-[:KNOWS_WELL]->(:Person {name: 'Zoe'})");

    expectNames("MATCH (p:Person {name: 'Remy'})-[:KNOWS_WELL]->(x) RETURN x.name",
                {"Adam", "Zoe"});
}

// A property write over the rows the barrier published
TEST_F(WithTest, setsAPropertyOfABoundVariable) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) WITH p SET p.dob = '01/01'");

    expectNames("MATCH (p:Person {name: 'Remy'}) RETURN p.dob", {"01/01"});
}

// Every item of a WITH names a column, so an expression without an alias has no name for
// the statements after it to read.
TEST_F(WithTest, rejectsAnUnaliasedExpression) {
    expectRejected("MATCH (n:Person) WITH n.name RETURN n.name");
}

// The barrier drops what it does not publish, so a variable left out of it is out of
// scope below.
TEST_F(WithTest, rejectsAVariableTheBarrierDropped) {
    expectRejected("MATCH (n:Person) WITH n.name AS name RETURN n");
}

TEST_F(WithTest, rejectsAPatternNamingADroppedVariable) {
    expectRejected("MATCH (n:Person) WITH n.name AS name MATCH (n)-->(x) RETURN name");
}

// An aggregate belongs in the projection the filter reads, not in the filter: there is no
// group for a WHERE to reduce.
TEST_F(WithTest, rejectsAnAggregateInTheFilter) {
    expectRejected("MATCH (n:Person) WITH n WHERE count(*) > 1 RETURN n");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
