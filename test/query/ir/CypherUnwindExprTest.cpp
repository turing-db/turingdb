#include <gtest/gtest.h>

#include <stddef.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"

#include "DBDialect.h"
#include "DBDialectInterpreter.h"
#include "DBProgramGenerator.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "StorageDialect.h"

#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "iterators/ChunkConfig.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "IRTestRows.h"
#include "TuringException.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// UNWIND of an expression evaluated per row - a property, a variable a WITH published, a
// value a CALL yielded, an arithmetic result - rather than of a literal list known at
// plan time. Each test asserts the rows a query returns, so the whole frontend, the
// lowering and the interpreter are on the path.
class CypherUnwindExprTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);
    }

    void runQuery(std::string_view query, Rows& rows, size_t chunkSize = ChunkConfig::CHUNK_SIZE) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const ProcedureManager* procedures = system.getProcedures();

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        CypherAST ast(procedures, query);

        CypherParser parser(&ast);
        parser.parse(query);

        CypherAnalyzer analyzer(&ast, view);
        analyzer.setV3();
        analyzer.analyze();

        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::db::DB>();
        context.getOrLoadDialect<mlir::nl::NL>();

        mlir::OpBuilder builder(&context);
        mlir::OwningOpRef<mlir::ModuleOp> owningModule = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp module = owningModule.get();

        DBProgramGenerator generator(&module);
        generator.generate(&ast);

        RowSink sink;
        LocalMemory memory;
        DBDialectInterpreter interpreter(module, &view, &sink, &memory, chunkSize);
        interpreter.run();

        rows = sink.rows();
    }

    // The rows in the order the query emits them, which is what an ORDER BY pins
    void expectRowsInOrder(std::string_view query,
                           const Rows& expected,
                           size_t chunkSize = ChunkConfig::CHUNK_SIZE) {
        Rows actual;
        runQuery(query, actual, chunkSize);

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, expected) << "query: " << query << "\ngot:\n" << actualText;
    }

    void expectRows(std::string_view query,
                    const Rows& expected,
                    size_t chunkSize = ChunkConfig::CHUNK_SIZE) {
        Rows actual;
        runQuery(query, actual, chunkSize);
        std::sort(actual.begin(), actual.end());

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    void expectRejected(std::string_view query, std::string_view reason) {
        Rows rows;

        try {
            runQuery(query, rows);
        } catch (const TuringException& error) {
            const std::string message = error.what();
            EXPECT_NE(message.find(reason), std::string::npos)
                << "query: " << query << "\nerror: " << message;
            return;
        }

        ADD_FAILURE() << "query was accepted: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
};

TEST_F(CypherUnwindExprTest, rejectsAScalarLiteral) {
    expectRejected("UNWIND 5 AS x RETURN x", "UNWIND requires a list");
}

TEST_F(CypherUnwindExprTest, rejectsEachScalarLiteralKind) {
    expectRejected("UNWIND 'text' AS x RETURN x", "UNWIND requires a list, not 'String'");
    expectRejected("UNWIND true AS x RETURN x", "UNWIND requires a list, not 'Bool'");
    expectRejected("UNWIND 2.5 AS x RETURN x", "UNWIND requires a list, not 'Double'");
}

TEST_F(CypherUnwindExprTest, rejectsAScalarLiteralBesideAMatch) {
    expectRejected("MATCH (n:Person) WHERE n.name = 'Remy' UNWIND 7 AS x RETURN n.name, x",
                   "UNWIND requires a list");
}

// Unwinding null is the one literal that is no list and still analyzes: Cypher spreads it
// into no row rather than calling it a type error.
TEST_F(CypherUnwindExprTest, unwindsNullToNoRow) {
    expectRows("UNWIND null AS x RETURN x", {});
}

TEST_F(CypherUnwindExprTest, unwindsNullBesideAMatchToNoRow) {
    expectRows("MATCH (n:Person) WHERE n.name = 'Remy' UNWIND null AS x RETURN n.name, x", {});
}

// Only a literal is typed at plan time, so the rule reaches no further: an expression
// evaluating to a scalar still spreads to the single row that value is.
TEST_F(CypherUnwindExprTest, unwindsAConstantExpressionToOneRow) {
    expectRows("UNWIND 2 + 3 AS x RETURN x", {{"5"}});
}

TEST_F(CypherUnwindExprTest, unwindsAPropertyToOneRowPerPresentValue) {
    // A property is no list, so each row spreads to the single value it holds - and a row
    // whose property is absent unwinds to nothing. Only Remy and Adam carry an age.
    expectRows("MATCH (n) UNWIND n.age AS a RETURN n.name, a",
               {{"Remy", "32"}, {"Adam", "32"}});
}

TEST_F(CypherUnwindExprTest, unwindsANodeVariableToItself) {
    expectRows("MATCH (n) WHERE n.name = 'Remy' UNWIND n AS m RETURN m.name", {{"Remy"}});
}

TEST_F(CypherUnwindExprTest, unwindsAnArithmeticExpressionOverAProperty) {
    expectRows("MATCH (n) UNWIND n.age + 1 AS a RETURN n.name, a",
               {{"Remy", "33"}, {"Adam", "33"}});
}

TEST_F(CypherUnwindExprTest, unwindsAFunctionResult) {
    // The call is no literal, so it takes the per-row path even though it reads no row:
    // its one value stands for the single row a constant scope is.
    expectRows("UNWIND toInteger('18') AS d RETURN d", {{"18"}});
}

TEST_F(CypherUnwindExprTest, unwindsAListPublishedByAWith) {
    expectRows("MATCH (i:Interest) WITH collect(i.name) AS names UNWIND names AS x RETURN x",
               {{"Computers"}, {"Eighties"}, {"Bio"}, {"Cooking"}, {"Ghosts"}, {"Padel"},
                {"Animals"}, {"Gym"}, {"Travel"}, {"JiuJitsu"}});
}

TEST_F(CypherUnwindExprTest, unwindsAGroupedListBesideItsKey) {
    // One list per group, so the key repeats once per element of the group's list.
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) "
               "WITH p.name AS person, collect(i.name) AS interests "
               "UNWIND interests AS interest "
               "RETURN person, interest",
               {{"Remy", "Ghosts"}, {"Remy", "Computers"}, {"Remy", "Eighties"},
                {"Adam", "Bio"}, {"Adam", "Cooking"},
                {"Maxime", "Bio"}, {"Maxime", "Padel"},
                {"Luc", "Animals"}, {"Luc", "Computers"},
                {"Martina", "Cooking"},
                {"Suhas", "Gym"}, {"Suhas", "JiuJitsu"},
                {"Cyrus", "Gym"}, {"Cyrus", "Travel"},
                {"Doruk", "Gym"}});
}

TEST_F(CypherUnwindExprTest, keepsTheUnwoundListInScope) {
    // Cypher leaves the list bound after the UNWIND, so the projection can return the
    // whole list beside one of its elements.
    expectRows("MATCH (p:Person) WHERE p.name = 'Adam' "
               "MATCH (p)-[:INTERESTED_IN]->(i:Interest) "
               "WITH collect(i.name) AS interests "
               "UNWIND interests AS interest "
               "RETURN interests, interest",
               {{"[Bio, Cooking]", "Bio"}, {"[Bio, Cooking]", "Cooking"}});
}

TEST_F(CypherUnwindExprTest, unwindsAnEmptyCollectToNoRow) {
    // No row reaches the collect, so its one group holds the empty list and the unwind
    // emits nothing - UNWIND of an empty list.
    expectRows("MATCH (n) WHERE n.name = 'nobody' WITH collect(n.name) AS names "
               "UNWIND names AS x RETURN x",
               {});
}

TEST_F(CypherUnwindExprTest, unwindsACollectedIntegerList) {
    // The tags the collect wrote are the ones the unwind reads back, so every value type
    // it can fold is a path of its own. Only Remy and Adam carry an age.
    expectRows("MATCH (n:Person) WITH collect(n.age) AS ages UNWIND ages AS a RETURN a",
               {{"32"}, {"32"}});
}

TEST_F(CypherUnwindExprTest, unwindsACollectedBooleanList) {
    expectRows("MATCH (n:Person) WITH collect(n.isFrench) AS flags UNWIND flags AS f RETURN f",
               {{"true"}, {"true"}, {"true"}, {"true"},
                {"false"}, {"false"}, {"false"}, {"false"}});
}

TEST_F(CypherUnwindExprTest, unwindsACollectedDoubleList) {
    expectRowsInOrder("UNWIND [1.5, 2.5] AS v WITH collect(v) AS values "
                      "UNWIND values AS x RETURN x",
                      {{"1.500000"}, {"2.500000"}});
}

TEST_F(CypherUnwindExprTest, unwindsACollectedEdgeProperty) {
    // A collect drops the rows whose property is absent, so the eleven edges carrying no
    // proficiency reach neither the list nor the unwind.
    expectRows("MATCH ()-[e:INTERESTED_IN]->() WITH collect(e.proficiency) AS levels "
               "UNWIND levels AS level RETURN level",
               {{"expert"}, {"expert"}, {"expert"}, {"moderate"}});
}

TEST_F(CypherUnwindExprTest, unwindsADistinctCollect) {
    // The collect dedups, so each interest is one element and the unwind emits it once -
    // Computers and the three Gym rows collapse before the expansion rather than after.
    expectRows("MATCH (:Person)-[:INTERESTED_IN]->(i:Interest) "
               "WITH collect(DISTINCT i.name) AS names "
               "UNWIND names AS name RETURN name",
               {{"Ghosts"}, {"Computers"}, {"Eighties"}, {"Bio"}, {"Cooking"}, {"Padel"},
                {"Animals"}, {"Gym"}, {"JiuJitsu"}, {"Travel"}});
}

TEST_F(CypherUnwindExprTest, unwindsEmptyGroupsBesideNonEmptyOnes) {
    // Six of the eight people carry no age, so their group's list is empty and the unwind
    // walks past it to the next cell that holds one.
    expectRows("MATCH (n:Person) WITH n.name AS name, collect(n.age) AS ages "
               "UNWIND ages AS age RETURN name, age",
               {{"Remy", "32"}, {"Adam", "32"}});
}

TEST_F(CypherUnwindExprTest, unwindsACollectedNodeAsANode) {
    // A list of nodes gives up nodes, so the element reads its properties the way the
    // matched variable it was collected from does.
    expectRows("MATCH (n:Person) WITH collect(n) AS people UNWIND people AS person "
               "RETURN person.name",
               {{"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"}, {"Martina"}, {"Suhas"},
                {"Cyrus"}, {"Doruk"}});
}

TEST_F(CypherUnwindExprTest, unwindsACollectedEdgeAsAnEdge) {
    expectRows("MATCH ()-[e:KNOWS_WELL]->() WITH collect(e) AS edges UNWIND edges AS edge "
               "RETURN edge.name, edge.duration",
               {{"Remy -> Adam", "20"}, {"Adam -> Remy", "20"}, {"Ghosts -> Remy", "200"}});
}

TEST_F(CypherUnwindExprTest, traversesFromACollectedNodeAfterUnwinding) {
    // The element is a node, so a later pattern binds it and hops off it - the collect
    // and the expansion leave the variable as good as the one the MATCH bound.
    expectRows("MATCH (p:Person) WHERE p.name = 'Adam' "
               "WITH collect(p) AS people "
               "UNWIND people AS person "
               "MATCH (person)-[:INTERESTED_IN]->(i:Interest) "
               "RETURN person.name, i.name",
               {{"Adam", "Bio"}, {"Adam", "Cooking"}});
}

TEST_F(CypherUnwindExprTest, unwindsADistinctCollectOfNodes) {
    // Ten interests are reached by fifteen edges, so the collect's dedup is what the
    // count behind the expansion reports.
    expectRows("MATCH (:Person)-[:INTERESTED_IN]->(i:Interest) "
               "WITH collect(DISTINCT i) AS interests "
               "UNWIND interests AS interest "
               "RETURN count(interest)",
               {{"10"}});
}

TEST_F(CypherUnwindExprTest, unwindsAGroupedCollectOfNodes) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) "
               "WITH p.name AS person, collect(i) AS interests "
               "UNWIND interests AS interest "
               "RETURN person, interest.name",
               {{"Remy", "Ghosts"}, {"Remy", "Computers"}, {"Remy", "Eighties"},
                {"Adam", "Bio"}, {"Adam", "Cooking"},
                {"Maxime", "Bio"}, {"Maxime", "Padel"},
                {"Luc", "Animals"}, {"Luc", "Computers"},
                {"Martina", "Cooking"},
                {"Suhas", "Gym"}, {"Suhas", "JiuJitsu"},
                {"Cyrus", "Gym"}, {"Cyrus", "Travel"},
                {"Doruk", "Gym"}});
}

TEST_F(CypherUnwindExprTest, reducesOverACollectedElement) {
    // The elements are the integers the collect gathered, so they reduce like any other
    // integer column rather than only being counted.
    expectRows("MATCH ()-[e]->() WITH collect(e.duration) AS durations "
               "UNWIND durations AS duration "
               "RETURN sum(duration), min(duration), max(duration), count(duration)",
               {{"325", "10", "200", "8"}});
}

TEST_F(CypherUnwindExprTest, averagesACollectedElement) {
    expectRows("MATCH (n:Person) WITH collect(n.age) AS ages UNWIND ages AS age "
               "RETURN avg(age)",
               {{"32.000000"}});
}

TEST_F(CypherUnwindExprTest, computesOverACollectedElement) {
    expectRows("MATCH (n:Person) WITH collect(n.age) AS ages UNWIND ages AS age "
               "RETURN age + 1",
               {{"33"}, {"33"}});
}

TEST_F(CypherUnwindExprTest, ordersACollectedElementByItsOwnType) {
    // 200 sorts after 20 rather than between 15 and 20, so the elements are ordered as
    // the integers they are.
    expectRowsInOrder("MATCH ()-[e]->() WITH collect(e.duration) AS durations "
                      "UNWIND durations AS duration "
                      "RETURN duration ORDER BY duration",
                      {{"10"}, {"15"}, {"20"}, {"20"}, {"20"}, {"20"}, {"20"}, {"200"}});
}

TEST_F(CypherUnwindExprTest, recollectsAnUnwoundElement) {
    // The round trip: what a collect gathered comes back out of the expansion as the same
    // values, so a second collect gathers them again.
    expectRows("MATCH (i:Interest) WITH collect(i.name) AS names "
               "UNWIND names AS name "
               "WITH collect(name) AS again "
               "RETURN again",
               {{"[Computers, Eighties, Bio, Cooking, Ghosts, Padel, Animals, Gym, Travel, JiuJitsu]"}});
}

TEST_F(CypherUnwindExprTest, collectsAListIntoAListOfLists) {
    // A list cell collects like any other value, nesting one list inside another.
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) WHERE p.name = 'Adam' "
               "WITH p.name AS person, collect(i.name) AS interests "
               "WITH collect(interests) AS nested "
               "RETURN nested",
               {{"[[Bio, Cooking]]"}});
}

TEST_F(CypherUnwindExprTest, unwindsAListOfListsOneLevel) {
    // The outer list gives up its cells, each still a list of its own.
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) WHERE p.name = 'Adam' "
               "WITH p.name AS person, collect(i.name) AS interests "
               "WITH collect(interests) AS nested "
               "UNWIND nested AS one "
               "RETURN one",
               {{"[Bio, Cooking]"}});
}

TEST_F(CypherUnwindExprTest, unwindsAListOfListsToItsLeaves) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) WHERE p.name = 'Adam' "
               "WITH p.name AS person, collect(i.name) AS interests "
               "WITH collect(interests) AS nested "
               "UNWIND nested AS one UNWIND one AS interest "
               "RETURN interest",
               {{"Bio"}, {"Cooking"}});
}

TEST_F(CypherUnwindExprTest, unwindsEveryGroupsListOutOfANestedCollect) {
    // One inner list per person, gathered into one outer list and drained back down to
    // the fifteen names the groups were built from.
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) "
               "WITH p.name AS person, collect(i.name) AS interests "
               "WITH collect(interests) AS nested "
               "UNWIND nested AS one UNWIND one AS interest "
               "RETURN count(interest)",
               {{"15"}});
}

TEST_F(CypherUnwindExprTest, unwindsANestedNodeListToItsNodes) {
    // The nodes survive both levels: the outer list gives up lists of nodes and the inner
    // one the nodes themselves, so the leaf reads its properties.
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) WHERE p.name = 'Adam' "
               "WITH p.name AS person, collect(i) AS interests "
               "WITH collect(interests) AS nested "
               "UNWIND nested AS one UNWIND one AS interest "
               "RETURN interest.name",
               {{"Bio"}, {"Cooking"}});
}

TEST_F(CypherUnwindExprTest, traversesFromANodeUnwoundOutOfANestedList) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) WHERE p.name = 'Adam' "
               "WITH p.name AS person, collect(i) AS interests "
               "WITH collect(interests) AS nested "
               "UNWIND nested AS one UNWIND one AS interest "
               "MATCH (interest)<-[:INTERESTED_IN]-(fan:Person) "
               "RETURN interest.name, fan.name",
               {{"Bio", "Adam"}, {"Bio", "Maxime"},
                {"Cooking", "Adam"}, {"Cooking", "Martina"}});
}

TEST_F(CypherUnwindExprTest, reducesOverTheLeafOfANestedList) {
    expectRows("MATCH ()-[e]->() WITH collect(e.duration) AS durations "
               "WITH collect(durations) AS nested "
               "UNWIND nested AS one UNWIND one AS duration "
               "RETURN sum(duration)",
               {{"325"}});
}

TEST_F(CypherUnwindExprTest, unwindsThreeLevelsOfCollectedLists) {
    // Nothing caps the nesting: each collect goes one level deeper and each UNWIND one
    // level back out, down to the values the innermost list holds.
    expectRowsInOrder("UNWIND [1, 2] AS v "
                      "WITH collect(v) AS ones "
                      "WITH collect(ones) AS twos "
                      "WITH collect(twos) AS threes "
                      "UNWIND threes AS a UNWIND a AS b UNWIND b AS c "
                      "RETURN c",
                      {{"1"}, {"2"}});
}

TEST_F(CypherUnwindExprTest, collectsAHeterogeneousElement) {
    // The elements share no type, so each is gathered under the one its own tag names.
    expectRows("UNWIND [10, 'a'] AS v RETURN collect(v)", {{"[10, a]"}});
}

TEST_F(CypherUnwindExprTest, collectsANestedListElement) {
    expectRows("UNWIND [[1, 2], [3]] AS l RETURN collect(l)", {{"[[1, 2], [3]]"}});
}

TEST_F(CypherUnwindExprTest, dropsNullsCollectingAHeterogeneousElement) {
    // A cell tagged null is the null Cypher's collect drops, exactly as a typed collect
    // drops an absent property.
    expectRows("UNWIND [1, null, 2] AS v RETURN collect(v)", {{"[1, 2]"}});
}

TEST_F(CypherUnwindExprTest, dedupsAHeterogeneousCollect) {
    expectRows("UNWIND [1, 'a', 1, 'a', 2] AS v RETURN collect(DISTINCT v)", {{"[1, a, 2]"}});
}

TEST_F(CypherUnwindExprTest, roundTripsAHeterogeneousElementThroughACollect) {
    expectRowsInOrder("UNWIND [10, 'a'] AS v WITH collect(v) AS values "
                      "UNWIND values AS x RETURN x",
                      {{"10"}, {"a"}});
}

TEST_F(CypherUnwindExprTest, dedupsEqualListsInACollect) {
    // Two groups build the same list from different rows, so DISTINCT keeps one of it:
    // two list cells are the same value when their elements are, never by sharing a cell.
    const std::string_view collected = "UNWIND [1, 2] AS key "
                                       "MATCH (p:Person) WHERE p.name = 'Adam' "
                                       "MATCH (p)-[:INTERESTED_IN]->(i:Interest) "
                                       "WITH key, collect(i.name) AS interests ";

    expectRows(std::string(collected).append("WITH collect(DISTINCT interests) AS nested "
                                             "UNWIND nested AS one RETURN one"),
               {{"[Bio, Cooking]"}});

    expectRows(std::string(collected).append("WITH collect(interests) AS nested "
                                             "UNWIND nested AS one RETURN one"),
               {{"[Bio, Cooking]"}, {"[Bio, Cooking]"}});
}

TEST_F(CypherUnwindExprTest, reducesOverAListLiteralPublishedByAWith) {
    // The barrier publishes the element type with the list, so a literal read through a
    // variable reduces exactly as the same literal written into the UNWIND does.
    expectRows("WITH [10, 20] AS numbers UNWIND numbers AS x RETURN sum(x)", {{"30"}});
    expectRows("WITH [10, 20] AS numbers UNWIND numbers AS x RETURN x + 1",
               {{"11"}, {"21"}});
}

TEST_F(CypherUnwindExprTest, crossesTheUnwoundElementsWithAFollowingMatch) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Adam' "
               "MATCH (p)-[:INTERESTED_IN]->(i:Interest) "
               "WITH collect(i.name) AS interests "
               "UNWIND interests AS interest "
               "MATCH (n) WHERE n.name = interest "
               "RETURN n.name",
               {{"Bio"}, {"Cooking"}});
}

TEST_F(CypherUnwindExprTest, carriesANodeAndAnEdgePastTheUnwind) {
    // Entity columns ride the carry set as values do, so the node and the edge of each
    // group come back beside the element their own row's cell unwound into.
    expectRows("MATCH (p:Person) WHERE p.name = 'Adam' "
               "MATCH (p)-[e:INTERESTED_IN]->(i:Interest) "
               "WITH p, e, collect(i.name) AS interests "
               "UNWIND interests AS interest "
               "RETURN p.name, e.name, interest",
               {{"Adam", "Adam -> Bio", "Bio"}, {"Adam", "Adam -> Cooking", "Cooking"}});
}

TEST_F(CypherUnwindExprTest, traversesFromANodeCarriedPastTheUnwind) {
    // The carried node is still a node behind the expansion, so a later MATCH hops off it
    // once per element it was replicated for.
    expectRows("MATCH (p:Person) WHERE p.name = 'Adam' "
               "MATCH (p)-[:INTERESTED_IN]->(i:Interest) "
               "WITH p, collect(i.name) AS interests "
               "UNWIND interests AS interest "
               "MATCH (p)-[:KNOWS_WELL]->(other) "
               "RETURN interest, other.name",
               {{"Bio", "Remy"}, {"Cooking", "Remy"}});
}

TEST_F(CypherUnwindExprTest, filtersAFollowingMatchOnAnInlinePropertyConstraint) {
    // The same join written as an inline constraint: the tagged element is compared to the
    // property row by row, exactly as the WHERE form is.
    expectRows("MATCH (p:Person) WHERE p.name = 'Adam' "
               "MATCH (p)-[:INTERESTED_IN]->(i:Interest) "
               "WITH collect(i.name) AS interests "
               "UNWIND interests AS interest "
               "MATCH (n {name: interest}) "
               "RETURN n.name",
               {{"Bio"}, {"Cooking"}});
}

TEST_F(CypherUnwindExprTest, filtersOnTheUnwoundElement) {
    expectRows("MATCH (i:Interest) WITH collect(i.name) AS names "
               "UNWIND names AS x MATCH (n) WHERE n.name = x AND x = 'Bio' RETURN n.name",
               {{"Bio"}});
}

TEST_F(CypherUnwindExprTest, ordersTheUnwoundElements) {
    expectRowsInOrder("MATCH (p:Person) WHERE p.name = 'Remy' "
                      "MATCH (p)-[:INTERESTED_IN]->(i:Interest) "
                      "WITH collect(i.name) AS interests "
                      "UNWIND interests AS interest "
                      "RETURN interest ORDER BY interest",
                      {{"Computers"}, {"Eighties"}, {"Ghosts"}});
}

TEST_F(CypherUnwindExprTest, cutsTheUnwoundElements) {
    expectRowsInOrder("MATCH (p:Person) WHERE p.name = 'Remy' "
                      "MATCH (p)-[:INTERESTED_IN]->(i:Interest) "
                      "WITH collect(i.name) AS interests "
                      "UNWIND interests AS interest "
                      "RETURN interest ORDER BY interest SKIP 1 LIMIT 1",
                      {{"Eighties"}});
}

TEST_F(CypherUnwindExprTest, cutsTheUnwoundElementsWithoutASort) {
    // No sort stands between the cut and the expansion, so the limit bounds the expansion
    // itself: it stops once three elements are out, whatever chunk size it reaches them in.
    const Rows firstThree {{"Computers"}, {"Eighties"}, {"Bio"}};
    const std::string_view query = "MATCH (i:Interest) WITH collect(i.name) AS names "
                                   "UNWIND names AS name RETURN name LIMIT 3";

    expectRowsInOrder(query, firstThree);
    expectRowsInOrder(query, firstThree, /*chunkSize=*/1);
    expectRowsInOrder(query, firstThree, /*chunkSize=*/2);
    expectRowsInOrder(query, firstThree, /*chunkSize=*/4);
}

TEST_F(CypherUnwindExprTest, dedupsTheUnwoundElements) {
    // Computers is collected twice - Remy's and Luc's - so DISTINCT keeps one of it.
    expectRows("MATCH (:Person)-[:INTERESTED_IN]->(i:Interest) "
               "WITH collect(i.name) AS interests "
               "UNWIND interests AS interest "
               "RETURN DISTINCT interest",
               {{"Ghosts"}, {"Computers"}, {"Eighties"}, {"Bio"}, {"Cooking"}, {"Padel"},
                {"Animals"}, {"Gym"}, {"JiuJitsu"}, {"Travel"}});
}

TEST_F(CypherUnwindExprTest, countsTheUnwoundElements) {
    expectRows("MATCH (:Person)-[:INTERESTED_IN]->(i:Interest) "
               "WITH collect(i.name) AS interests "
               "UNWIND interests AS interest "
               "RETURN count(interest)",
               {{"15"}});
}

TEST_F(CypherUnwindExprTest, groupsByTheUnwoundElement) {
    expectRows("MATCH (:Person)-[:INTERESTED_IN]->(i:Interest) "
               "WITH collect(i.name) AS interests "
               "UNWIND interests AS interest "
               "RETURN interest, count(interest)",
               {{"Ghosts", "1"}, {"Computers", "2"}, {"Eighties", "1"}, {"Bio", "2"},
                {"Cooking", "2"}, {"Padel", "1"}, {"Animals", "1"}, {"Gym", "3"},
                {"JiuJitsu", "1"}, {"Travel", "1"}});
}

TEST_F(CypherUnwindExprTest, unwindsALiteralListBesideAnExpressionUnwind) {
    // The literal opens a dataflow of its own and is crossed with the rows; the property
    // expands each of those. Only Remy carries both an age and the two crossed elements.
    expectRows("MATCH (n) WHERE n.name = 'Remy' UNWIND [1, 2] AS i UNWIND n.age AS a "
               "RETURN i, a",
               {{"1", "32"}, {"2", "32"}});
}

TEST_F(CypherUnwindExprTest, chainsTwoExpressionUnwinds) {
    expectRows("MATCH (n) WHERE n.name = 'Remy' UNWIND n.age AS a UNWIND a + 1 AS b "
               "RETURN a, b",
               {{"32", "33"}});
}

TEST_F(CypherUnwindExprTest, unwindsTheSameCollectedListTwice) {
    // Both unwinds read the one list, and the second expands the rows the first produced,
    // so the elements come back crossed with themselves.
    expectRows("MATCH (p:Person) WHERE p.name = 'Adam' "
               "MATCH (p)-[:INTERESTED_IN]->(i:Interest) "
               "WITH collect(i.name) AS interests "
               "UNWIND interests AS a UNWIND interests AS b "
               "RETURN a, b",
               {{"Bio", "Bio"}, {"Bio", "Cooking"},
                {"Cooking", "Bio"}, {"Cooking", "Cooking"}});
}

TEST_F(CypherUnwindExprTest, unwindsTwoListsOfOneProjection) {
    // Two collects of the same projection, each expanded in turn: the second list rides
    // the first expansion's carry set before unwinding over it.
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) WHERE p.name = 'Adam' "
               "WITH collect(p.name) AS people, collect(i.name) AS interests "
               "UNWIND people AS person UNWIND interests AS interest "
               "RETURN person, interest",
               {{"Adam", "Bio"}, {"Adam", "Bio"},
                {"Adam", "Cooking"}, {"Adam", "Cooking"}});
}

TEST_F(CypherUnwindExprTest, unwindsAListLiteralPublishedByAWith) {
    // The list is a literal, but the UNWIND reads it through a variable, so it takes the
    // per-row path over a constant column with nothing in flight to carry.
    expectRowsInOrder("WITH [1, 2, 3] AS numbers UNWIND numbers AS x RETURN x",
                      {{"1"}, {"2"}, {"3"}});
}

TEST_F(CypherUnwindExprTest, unwindsAcrossChunkBoundaries) {
    // One chunk per element, so the loop re-enters its body for every row it emits and
    // the carried key is gathered afresh each time.
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) "
               "WITH p.name AS person, collect(i.name) AS interests "
               "UNWIND interests AS interest "
               "RETURN person, interest",
               {{"Remy", "Ghosts"}, {"Remy", "Computers"}, {"Remy", "Eighties"},
                {"Adam", "Bio"}, {"Adam", "Cooking"},
                {"Maxime", "Bio"}, {"Maxime", "Padel"},
                {"Luc", "Animals"}, {"Luc", "Computers"},
                {"Martina", "Cooking"},
                {"Suhas", "Gym"}, {"Suhas", "JiuJitsu"},
                {"Cyrus", "Gym"}, {"Cyrus", "Travel"},
                {"Doruk", "Gym"}},
               /*chunkSize=*/1);
}

TEST_F(CypherUnwindExprTest, unwindsOneCollectedCellAcrossChunkBoundaries) {
    // A single ungrouped cell holding every interest, cut at sizes that do not divide its
    // ten elements: the expansion resumes inside the cell it was halfway through.
    const Rows interests {{"Computers"}, {"Eighties"}, {"Bio"}, {"Cooking"}, {"Ghosts"},
                          {"Padel"}, {"Animals"}, {"Gym"}, {"Travel"}, {"JiuJitsu"}};
    const std::string_view query = "MATCH (i:Interest) WITH collect(i.name) AS names "
                                   "UNWIND names AS name RETURN name";

    expectRowsInOrder(query, interests, /*chunkSize=*/3);
    expectRowsInOrder(query, interests, /*chunkSize=*/4);
    expectRowsInOrder(query, interests, /*chunkSize=*/7);
}

TEST_F(CypherUnwindExprTest, publishesTheUnwoundElementAtAFollowingBarrier) {
    // A WITH after the UNWIND republishes its elements, so the part behind the barrier
    // reads them as a variable of its own.
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) "
               "WITH p, collect(i.name) AS interests "
               "UNWIND interests AS interest "
               "WITH p, interest WHERE interest = 'Gym' "
               "RETURN p.name, interest",
               {{"Cyrus", "Gym"}, {"Suhas", "Gym"}, {"Doruk", "Gym"}});
}

TEST_F(CypherUnwindExprTest, unwindsANestedListOneLevelDeeper) {
    // The outer UNWIND hands out tagged cells, and a cell that is itself a list unwinds
    // into its own elements - Cypher's UNWIND applied twice.
    expectRowsInOrder("UNWIND [[1, 2], [3]] AS l UNWIND l AS x RETURN x",
                      {{"1"}, {"2"}, {"3"}});
}

TEST_F(CypherUnwindExprTest, dropsANullCellOfAnUnwoundList) {
    expectRowsInOrder("UNWIND [1, null, 2] AS l UNWIND l AS x RETURN x", {{"1"}, {"2"}});
}

TEST_F(CypherUnwindExprTest, unwindsTaggedCellsBesideTheCellTheyCameFrom) {
    // A scalar cell is its own single row; a nested list is one row per element. The
    // outer cell rides the carry set, so it repeats beside each of them.
    expectRowsInOrder("UNWIND [1, 'a', [2, 3]] AS l UNWIND l AS x RETURN l, x",
                      {{"1", "1"}, {"a", "a"}, {"[2, 3]", "2"}, {"[2, 3]", "3"}});
}

TEST_F(CypherUnwindExprTest, sumsAHeterogeneousElement) {
    // Mixed numeric tags are what leaves the list type-erased, and Cypher adds those to a
    // float, so the cells reduce by tag into one.
    expectRows("UNWIND [1, 2.5] AS v RETURN sum(v)", {{"3.500000"}});
}

TEST_F(CypherUnwindExprTest, averagesAHeterogeneousElement) {
    expectRows("UNWIND [1, 2.5] AS v RETURN avg(v)", {{"1.750000"}});
}

TEST_F(CypherUnwindExprTest, sumsTheDistinctCellsOfAHeterogeneousElement) {
    expectRows("UNWIND [1, 2.5, 1] AS v RETURN sum(DISTINCT v)", {{"3.500000"}});
}

TEST_F(CypherUnwindExprTest, sumsAHeterogeneousElementOutOfACollect) {
    expectRows("UNWIND [1, 2.5] AS v WITH collect(v) AS values "
               "UNWIND values AS x RETURN sum(x)",
               {{"3.500000"}});
}

// The distinct siblings of the reduction above, charged once per distinct cell of the
// group: 1, 2.5 and 1 again reduce as the two values they are.
TEST_F(CypherUnwindExprTest, reducesTheDistinctCellsOfAHeterogeneousElementPerGroup) {
    expectRows("UNWIND [1, 2.5, 1] AS v MATCH (n:Person) WHERE n.name = 'Remy' "
               "RETURN n.name, sum(DISTINCT v), avg(DISTINCT v)",
               {{"Remy", "3.500000", "1.750000"}});
}

// min and max name no type a static result column could be, so a tagged cell is no
// argument for them: it is the signature that turns the query away, ahead of the
// reduction that has no fold for the pair.
TEST_F(CypherUnwindExprTest, rejectsMinMaxOverAHeterogeneousElement) {
    expectRejected("UNWIND [10, 'a'] AS v RETURN min(v)", "Invalid arguments for function 'min'");
    expectRejected("MATCH (n) UNWIND [10, 'a'] AS v RETURN n.name, max(v)",
                   "Invalid arguments for function 'max'");
}

TEST_F(CypherUnwindExprTest, sumsAHeterogeneousElementPerGroup) {
    expectRows("UNWIND [1, 2.5] AS v MATCH (n:Person) WHERE n.name = 'Remy' "
               "RETURN n.name, sum(v), avg(v)",
               {{"Remy", "3.500000", "1.750000"}});
}

TEST_F(CypherUnwindExprTest, sumsNoHeterogeneousCellToZero) {
    expectRows("UNWIND [] AS v RETURN sum(v)", {{"0.000000"}});
}

TEST_F(CypherUnwindExprTest, collectsEachTagOfAHeterogeneousElement) {
    // Every cell goes back into the list under the type its own tag names, so a bool
    // stays a bool and a double a double beside whatever they were mixed with.
    expectRows("UNWIND [true, 1] AS v RETURN collect(v)", {{"[true, 1]"}});
    expectRows("UNWIND [1.5, 'a'] AS v RETURN collect(v)", {{"[1.500000, a]"}});
}

TEST_F(CypherUnwindExprTest, averagesTheDistinctCellsOfAHeterogeneousElement) {
    expectRows("UNWIND [1, 2.5, 1] AS v RETURN avg(DISTINCT v)", {{"1.750000"}});
}

TEST_F(CypherUnwindExprTest, unwindsANestedEdgeListToItsEdges) {
    expectRows("MATCH ()-[e:KNOWS_WELL]->() WITH collect(e) AS edges "
               "WITH collect(edges) AS nested "
               "UNWIND nested AS one UNWIND one AS edge "
               "RETURN edge.name",
               {{"Remy -> Adam"}, {"Adam -> Remy"}, {"Ghosts -> Remy"}});
}

TEST_F(CypherUnwindExprTest, rejectsSummingANonNumericCell) {
    // The cells carry a type each, so the check the column types make for a typed
    // reduction is made per row instead - and a string is no number in either.
    expectRejected("UNWIND [10, 'a'] AS v RETURN sum(v)", "requires a numeric column");
}

TEST_F(CypherUnwindExprTest, rejectsAnUnwoundVariableThatIsAlreadyDeclared) {
    expectRejected("MATCH (n) UNWIND n.age AS n RETURN n", "already declared");
}

// The argument is not a list here, so what is rejected is the argument itself: a message
// about the elements of a list would send the reader looking for a list they never wrote
TEST_F(CypherUnwindExprTest, rejectsAnUnrepresentableUnwindArgumentAsTheArgument) {
    expectRejected("UNWIND {a: 1} AS x RETURN x", "UNWIND requires a list, not 'Map'");
}

TEST_F(CypherUnwindExprTest, rejectsAnUnrepresentableListElementAsAnElement) {
    expectRejected("UNWIND [{a: 1}] AS x RETURN x", "as list elements");
}

// An aggregate folds the rows a projection groups, so it names no value an UNWIND could
// spread: the list has to be published by a WITH first
TEST_F(CypherUnwindExprTest, rejectsAnAggregateUnwindArgument) {
    expectRejected("MATCH (n) UNWIND collect(n.name) AS x RETURN x", "aggregate");
    expectRejected("MATCH (n) UNWIND count(n) AS x RETURN x", "aggregate");
    expectRejected("MATCH (n) UNWIND count(n) + 1 AS x RETURN x", "aggregate");
}
