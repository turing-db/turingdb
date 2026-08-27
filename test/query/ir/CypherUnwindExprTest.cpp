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

TEST_F(CypherUnwindExprTest, unwindsAScalarLiteralToOneRow) {
    expectRows("UNWIND 5 AS x RETURN x", {{"5"}});
}

TEST_F(CypherUnwindExprTest, unwindsEachScalarLiteralKindToOneRow) {
    expectRows("UNWIND 'text' AS x RETURN x", {{"text"}});
    expectRows("UNWIND true AS x RETURN x", {{"true"}});
    expectRows("UNWIND 2.5 AS x RETURN x", {{"2.500000"}});
}

TEST_F(CypherUnwindExprTest, unwindsNullToNoRow) {
    expectRows("UNWIND null AS x RETURN x", {});
}

TEST_F(CypherUnwindExprTest, unwindsAConstantExpressionToOneRow) {
    expectRows("UNWIND 2 + 3 AS x RETURN x", {{"5"}});
}

TEST_F(CypherUnwindExprTest, unwindsAScalarLiteralBesideAMatch) {
    // The scalar is one element, so each matched row comes back exactly once beside it.
    expectRows("MATCH (n:Person) WHERE n.name = 'Remy' UNWIND 7 AS x RETURN n.name, x",
               {{"Remy", "7"}});
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

TEST_F(CypherUnwindExprTest, crossesTheUnwoundElementsWithAFollowingMatch) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Adam' "
               "MATCH (p)-[:INTERESTED_IN]->(i:Interest) "
               "WITH collect(i.name) AS interests "
               "UNWIND interests AS interest "
               "MATCH (n) WHERE n.name = interest "
               "RETURN n.name",
               {{"Bio"}, {"Cooking"}});
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

TEST_F(CypherUnwindExprTest, rejectsAnUnwoundVariableThatIsAlreadyDeclared) {
    expectRejected("MATCH (n) UNWIND n.age AS n RETURN n", "already declared");
}
