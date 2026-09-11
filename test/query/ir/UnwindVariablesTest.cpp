#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

// UNWIND of a list whose elements are read per row: MATCH (n)-->(m) UNWIND [n, m] AS x.
// The list is no longer a value known at plan time, so it is built one cell per row out of
// the column each element rides (db.make_list) and spread by the ordinary db.unwind - two
// rows per input row for a pair, interleaved, with everything else in flight repeated
// beside them.
//
// Remy (0) is the anchor throughout: its four out-edges reach Adam (1), Ghosts (6),
// Computers (2) and Eighties (3), in that order.
class UnwindVariablesTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void runQuery(std::string_view query, StringRowSink& sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();
    }

    void expectRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getRows(), expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// The headline case: each matched pair spreads into the two rows its two nodes are, the
// source ahead of the target, so the four edges out of Remy give eight rows.
TEST_F(UnwindVariablesTest, spreadsAMatchedPairIntoTwoRows) {
    const Rows expected = {{"0"}, {"1"}, {"0"}, {"6"}, {"0"}, {"2"}, {"0"}, {"3"}};
    expectRows("MATCH (n)-->(m) WHERE n.name = 'Remy' UNWIND [n, m] AS x RETURN x", expected);
}

// The unwound cell is a node, not a tagged scalar, so a property reads off it
TEST_F(UnwindVariablesTest, readsAPropertyOfTheUnwoundNode) {
    const Rows expected = {{"Remy"}, {"Adam"}, {"Remy"}, {"Ghosts"},
                           {"Remy"}, {"Computers"}, {"Remy"}, {"Eighties"}};
    expectRows("MATCH (n)-->(m) WHERE n.name = 'Remy' UNWIND [n, m] AS x RETURN x.name", expected);
}

// What was in flight rides through the carry set, repeated once per element, so the pair
// the element came from is still readable beside it
TEST_F(UnwindVariablesTest, repeatsTheMatchedRowBesideEachElement) {
    const Rows expected = {{"Remy", "Adam", "Remy"},
                           {"Remy", "Adam", "Adam"},
                           {"Remy", "Ghosts", "Remy"},
                           {"Remy", "Ghosts", "Ghosts"},
                           {"Remy", "Computers", "Remy"},
                           {"Remy", "Computers", "Computers"},
                           {"Remy", "Eighties", "Remy"},
                           {"Remy", "Eighties", "Eighties"}};
    expectRows("MATCH (n)-->(m) WHERE n.name = 'Remy' UNWIND [n, m] AS x RETURN n.name, m.name, x.name",
               expected);
}

// A singleton list of an edge: the four out-edges of Remy are edges 0 through 3
TEST_F(UnwindVariablesTest, spreadsASingletonListOfAnEdge) {
    const Rows expected = {{"0"}, {"1"}, {"2"}, {"3"}};
    expectRows("MATCH (n)-[e]->(m) WHERE n.name = 'Remy' UNWIND [e] AS x RETURN x", expected);
}

// A variable beside a literal: the literal stands for every row, so it is laid out over
// the rows the variable carries and each of them gets a two-element list
TEST_F(UnwindVariablesTest, spreadsAVariableBesideALiteral) {
    const Rows expected = {{"32"}, {"1"}};
    expectRows("MATCH (n) WHERE n.name = 'Remy' UNWIND [n.age, 1] AS x RETURN x", expected);
}

// Elements of differing types make the list type-erased, so the cells come back as the
// tagged scalars they were put in as: Remy's name then its age
TEST_F(UnwindVariablesTest, spreadsPropertiesOfDifferingTypes) {
    const Rows expected = {{"Remy"}, {"32"}};
    expectRows("MATCH (n) WHERE n.name = 'Remy' UNWIND [n.name, n.age] AS x RETURN x", expected);
}

// A row with no value for one element still contributes it: UNWIND drops the null the
// whole list is, never the null one of its cells holds
TEST_F(UnwindVariablesTest, keepsTheNullOfAnAbsentProperty) {
    const Rows expected = {{"32"}, {"null"}};
    expectRows("MATCH (n) WHERE n.name = 'Remy' UNWIND [n.age, n.height] AS x RETURN x", expected);
}

// One level deeper: the elements are lists of their own, so each cell unwinds into the
// list it is rather than into the nodes inside it
TEST_F(UnwindVariablesTest, spreadsAListOfLists) {
    const Rows expected = {{"0"}, {"0, 0"}};
    expectRows("MATCH (n) WHERE n.name = 'Remy' UNWIND [[n], [n, n]] AS x RETURN x", expected);
}

// The unwound node drives the pattern that follows, as a matched one does
TEST_F(UnwindVariablesTest, walksOutOfTheUnwoundNode) {
    const Rows expected = {{"Adam"}, {"Ghosts"}, {"Computers"}, {"Eighties"}};
    expectRows("MATCH (n) WHERE n.name = 'Remy' UNWIND [n] AS x MATCH (x)-->(y) RETURN y.name", expected);
}

TEST_F(UnwindVariablesTest, countsTheUnwoundRows) {
    const Rows expected = {{"8"}};
    expectRows("MATCH (n)-->(m) WHERE n.name = 'Remy' UNWIND [n, m] AS x RETURN count(x)", expected);
}

// Remy repeats once per edge and each target appears once, so five names survive the dedup
TEST_F(UnwindVariablesTest, dedupsTheUnwoundNames) {
    const Rows expected = {{"Remy"}, {"Adam"}, {"Ghosts"}, {"Computers"}, {"Eighties"}};
    expectRows("MATCH (n)-->(m) WHERE n.name = 'Remy' UNWIND [n, m] AS x RETURN DISTINCT x.name", expected);
}

// The same list as a value rather than a source: one cell per matched row, holding the
// pair instead of spreading it
TEST_F(UnwindVariablesTest, returnsThePairAsOneCell) {
    const Rows expected = {{"0, 1"}, {"0, 6"}, {"0, 2"}, {"0, 3"}};
    expectRows("MATCH (n)-->(m) WHERE n.name = 'Remy' RETURN [n, m] AS pair", expected);
}

// Published through a barrier and unwound on the other side: the list is built before the
// WITH and spread after it, to the rows the pair gave directly
TEST_F(UnwindVariablesTest, spreadsAPairPublishedByAWith) {
    const Rows expected = {{"Remy"}, {"Adam"}, {"Remy"}, {"Ghosts"},
                           {"Remy"}, {"Computers"}, {"Remy"}, {"Eighties"}};
    expectRows("MATCH (n)-->(m) WHERE n.name = 'Remy' WITH [n, m] AS pair UNWIND pair AS x RETURN x.name",
               expected);
}

// The list carries rows rather than standing for all of them, so a reduction over it
// tallies one per matched pair
TEST_F(UnwindVariablesTest, countsThePairOfEveryMatchedRow) {
    const Rows expected = {{"4"}};
    expectRows("MATCH (n)-->(m) WHERE n.name = 'Remy' RETURN count([n, m])", expected);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
