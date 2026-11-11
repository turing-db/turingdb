#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "llvm/ADT/STLFunctionalExtras.h"
#include "llvm/Support/raw_ostream.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "DBProgramGenerator.h"
#include "NLDialect.h"
#include "StorageDialect.h"

#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"
#include "Graph.h"
#include "QueryStatus.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A region run more than once for one row - a body over the elements of a list, the hop region
// of a walk, a pattern comprehension's body behind its hop - computes what it reads off the row
// alone once per row in front of the op holding it, carried in through the CASE branches and
// hops between. Remy and Adam are the two people aged 32; no other person has an age.
class HoistRepeatedRegionInvariantOpsTest : public WriteQueryTest {
protected:
    // More items than one chunk holds. Item i has v = i % 5 and two targets, one with
    // w = i % 3 and one with w = i % 7.
    static constexpr int64_t ITEM_COUNT = 3 * 65536 + 7;

    struct OpCount {
        size_t _nested {0};
        size_t _total {0};
    };

    void initialize() override {
        WriteQueryTest::initialize();

        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

    // An op in the function's own block runs once per row of the rows it reads, an op
    // nested in a region once per run of that region
    void countDBOps(std::string_view query, llvm::function_ref<bool(mlir::Operation*)> counted, OpCount& count, std::string& program) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const ProcedureManager* procedures = system.getProcedures();
        Graph* graph = system.getGraph(_graphName);

        const FrozenCommitTx transaction = graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        CypherAST ast(procedures, query);

        CypherParser parser(&ast);
        parser.parse(query);

        CypherAnalyzer analyzer(&ast, view);
        analyzer.analyze();

        mlir::OpBuilder builder(&_context);
        mlir::OwningOpRef<mlir::ModuleOp> owningModule = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp module = owningModule.get();

        DBProgramGenerator generator(&module);
        generator.generate(&ast);

        module.walk([counted, &count](mlir::Operation* op) {
            if (!counted(op)) {
                return;
            }

            count._total++;
            if (!mlir::isa<mlir::func::FuncOp>(op->getParentOp())) {
                count._nested++;
            }
        });

        llvm::raw_string_ostream stream(program);
        module.print(stream);
    }

    void nlProgramOf(std::string_view query, std::string& program) {
        RowSink sink;
        const QueryStatus status = runQuery("EXPLAIN (nl) " + std::string(query), &sink);
        ASSERT_TRUE(status.isOk()) << query << "\n" << status.getError();
        ASSERT_EQ(sink.rows().size(), 1u) << query;

        program = sink.rows().front().back();
    }

    // The lines of an nl program for which @param counted holds, and how many of them sit
    // deeper than the body of the first loop, the one over the scanned rows
    static void countNLLines(std::string_view program, llvm::function_ref<bool(std::string_view)> counted, OpCount& count) {
        size_t rowBodyIndent = std::string_view::npos;
        size_t lineStart = 0;

        while (lineStart < program.size()) {
            const size_t lineEnd = std::min(program.find('\n', lineStart), program.size());
            const std::string_view line = program.substr(lineStart, lineEnd - lineStart);
            lineStart = lineEnd + 1;

            const size_t indent = line.find_first_not_of(' ');
            if (indent == std::string_view::npos) {
                continue;
            }

            const std::string_view text = line.substr(indent);
            if (rowBodyIndent == std::string_view::npos && text.starts_with("nl.for ")) {
                rowBodyIndent = indent + 2;
            }

            if (!counted(text)) {
                continue;
            }

            count._total++;
            if (indent > rowBodyIndent) {
                count._nested++;
            }
        }
    }

    // The name an nl program gives the type of @param property, as `%0` in
    // `%0 = nl.get_property_type("age")`
    static void propertyTypeValue(std::string_view program, std::string_view property, std::string& value) {
        const std::string definition = " = nl.get_property_type(\"" + std::string(property) + "\")";
        const size_t definitionPosition = program.find(definition);
        ASSERT_NE(definitionPosition, std::string_view::npos) << property << "\n" << program;

        const size_t valueStart = program.rfind('%', definitionPosition);
        value = program.substr(valueStart, definitionPosition - valueStart);
    }

    void expectDBNodePropertyReads(std::string_view query, llvm::StringRef property, size_t nested, size_t total) {
        const auto readsTheProperty = [property](mlir::Operation* op) {
            mlir::db::GetNodeProperties read = mlir::dyn_cast<mlir::db::GetNodeProperties>(op);
            return read && read.getProperty() == property;
        };

        OpCount count;
        std::string program;
        countDBOps(query, readsTheProperty, count, program);

        EXPECT_EQ(count._nested, nested) << query << "\n" << program;
        EXPECT_EQ(count._total, total) << query << "\n" << program;
    }

    // The same reads in the db program and in the nl program it lowers to
    void expectNodePropertyReads(std::string_view query, llvm::StringRef property, size_t nested, size_t total) {
        expectDBNodePropertyReads(query, property, nested, total);

        std::string nlProgram;
        ASSERT_NO_FATAL_FAILURE(nlProgramOf(query, nlProgram));

        std::string propertyType;
        ASSERT_NO_FATAL_FAILURE(propertyTypeValue(nlProgram, std::string_view(property), propertyType));

        const std::string readSuffix = ", " + propertyType + ")";
        const auto readsTheType = [&readSuffix](std::string_view line) {
            return line.find("nl.get_node_properties(") != std::string_view::npos
                && line.find(readSuffix) != std::string_view::npos;
        };

        OpCount nlCount;
        countNLLines(nlProgram, readsTheType, nlCount);

        EXPECT_EQ(nlCount._nested, nested) << query << "\n" << nlProgram;
        EXPECT_EQ(nlCount._total, total) << query << "\n" << nlProgram;
    }

    template <typename CountedOp>
    void expectDBOps(std::string_view query, size_t nested, size_t total) {
        const auto isCounted = [](mlir::Operation* op) {
            return mlir::isa<CountedOp>(op);
        };

        OpCount count;
        std::string program;
        countDBOps(query, isCounted, count, program);

        EXPECT_EQ(count._nested, nested) << query << "\n" << program;
        EXPECT_EQ(count._total, total) << query << "\n" << program;
    }

    void expectNLLines(std::string_view query, std::string_view lineText, size_t nested, size_t total) {
        std::string program;
        ASSERT_NO_FATAL_FAILURE(nlProgramOf(query, program));

        const auto holdsTheText = [lineText](std::string_view line) {
            return line.find(lineText) != std::string_view::npos;
        };

        OpCount count;
        countNLLines(program, holdsTheText, count);

        EXPECT_EQ(count._nested, nested) << query << "\n" << program;
        EXPECT_EQ(count._total, total) << query << "\n" << program;
    }

    void createItems() {
        applyWrite("UNWIND range(0, " + std::to_string(ITEM_COUNT - 1) + ") AS i "
                   "CREATE (n:Item {i: i, v: i % 5}) "
                   "CREATE (n)-[:R]->(:Target {w: i % 3}) "
                   "CREATE (n)-[:R]->(:Target {w: i % 7})");
    }

    // Prints the first row that differs rather than every row
    void expectManyRows(std::string_view query, Rows expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows rows;
        sink.sortedRows(rows);
        std::sort(expected.begin(), expected.end());

        ASSERT_EQ(rows.size(), expected.size()) << "query: " << query;

        const auto [actualIt, expectedIt] = std::mismatch(rows.begin(), rows.end(), expected.begin());
        if (actualIt == rows.end()) {
            return;
        }

        std::string actualText;
        describeRows(Rows {*actualIt}, actualText);

        std::string expectedText;
        describeRows(Rows {*expectedIt}, expectedText);

        ADD_FAILURE() << "query: " << query << "\nfirst differing row: " << actualText << "expected: " << expectedText;
    }

private:
    mlir::MLIRContext _context;
};

// The age of n is read once per person and carried through the hop to each edge; the age of m
// is read per edge, and the name of n, used after the filter alone, for each match kept
TEST_F(HoistRepeatedRegionInvariantOpsTest, readsTheOuterPropertiesOfAPatternComprehensionOncePerRow) {
    const std::string_view query = "MATCH (n:Person) RETURN [(n)-->(m) WHERE m.age > n.age | n.name]";
    expectNodePropertyReads(query, "age", 1, 2);
    expectNodePropertyReads(query, "name", 1, 1);

    expectRows("MATCH (n:Person) RETURN n.name, [(n)-->(m) WHERE m.name > n.name | n.name]",
               {{"Remy", "[]"}, {"Adam", "[Adam, Adam, Adam]"}, {"Maxime", "[Maxime]"}, {"Luc", "[]"},
                {"Martina", "[]"}, {"Suhas", "[]"}, {"Cyrus", "[Cyrus, Cyrus]"}, {"Doruk", "[Doruk]"}});
}

TEST_F(HoistRepeatedRegionInvariantOpsTest, readsTheOuterPropertiesOfAPatternComprehensionOverTwoHopsOncePerRow) {
    const std::string_view query = "MATCH (n:Person) RETURN [(n)-->(m)-->(k) WHERE k.age > n.age | n.name]";
    expectNodePropertyReads(query, "age", 1, 2);
    expectNodePropertyReads(query, "name", 1, 1);

    expectRows("MATCH (n:Person) RETURN n.name, [(n)-->(m)-->(k) WHERE k.age >= n.age | n.age]",
               {{"Remy", "[32, 32]"}, {"Adam", "[32]"}, {"Maxime", "[]"}, {"Luc", "[]"},
                {"Martina", "[]"}, {"Suhas", "[]"}, {"Cyrus", "[]"}, {"Doruk", "[]"}});
}

TEST_F(HoistRepeatedRegionInvariantOpsTest, readsTheOuterPropertiesOfAPatternComprehensionOverAWalkOncePerRow) {
    const std::string_view query = "MATCH (n:Person) RETURN [(n)-[*1..2]->(m) WHERE m.age > n.age | n.name]";
    expectNodePropertyReads(query, "age", 1, 2);
    expectNodePropertyReads(query, "name", 1, 1);

    expectRows("MATCH (n:Person) RETURN n.name, [(n)-[*1..2]->(m) WHERE m.age >= n.age | n.name]",
               {{"Remy", "[Remy, Remy, Remy]"}, {"Adam", "[Adam, Adam]"}, {"Maxime", "[]"}, {"Luc", "[]"},
                {"Martina", "[]"}, {"Suhas", "[]"}, {"Cyrus", "[]"}, {"Doruk", "[]"}});
}

TEST_F(HoistRepeatedRegionInvariantOpsTest, readsTheOuterPropertyOfACountSubqueryOncePerRow) {
    const std::string_view query = "MATCH (n:Person) RETURN n.name, COUNT { MATCH (n)-->(m) WHERE m.age >= n.age }";
    expectNodePropertyReads(query, "age", 1, 2);

    expectRows(query, {{"Remy", "1"}, {"Adam", "1"}, {"Maxime", "0"}, {"Luc", "0"},
                       {"Martina", "0"}, {"Suhas", "0"}, {"Cyrus", "0"}, {"Doruk", "0"}});
}

TEST_F(HoistRepeatedRegionInvariantOpsTest, readsAnOuterPropertyOnceFromACaseBranchInAComprehension) {
    const std::string_view query = "MATCH (n:Person) RETURN [x IN [1, 2] | CASE WHEN x > 1 THEN n.age END]";
    expectNodePropertyReads(query, "age", 0, 1);

    expectRows("MATCH (n:Person) RETURN n.name, [x IN [1, 2] | CASE WHEN x > 1 THEN n.age END]",
               {{"Remy", "[null, 32]"}, {"Adam", "[null, 32]"}, {"Maxime", "[null, null]"},
                {"Luc", "[null, null]"}, {"Martina", "[null, null]"}, {"Suhas", "[null, null]"},
                {"Cyrus", "[null, null]"}, {"Doruk", "[null, null]"}});
}

TEST_F(HoistRepeatedRegionInvariantOpsTest, readsAnOuterPropertyOnceFromACoalesceArgumentInAComprehension) {
    const std::string_view query = "MATCH (n:Person) RETURN n.name, [x IN [1, 2] | coalesce(CASE WHEN x > 1 THEN x END, n.age)]";
    expectNodePropertyReads(query, "age", 0, 1);

    expectRows(query, {{"Remy", "[32, 2]"}, {"Adam", "[32, 2]"}, {"Maxime", "[null, 2]"}, {"Luc", "[null, 2]"},
                       {"Martina", "[null, 2]"}, {"Suhas", "[null, 2]"}, {"Cyrus", "[null, 2]"},
                       {"Doruk", "[null, 2]"}});
}

// Remy and Adam are 32 and every edge from them lasts 20 or 200, or has no duration
TEST_F(HoistRepeatedRegionInvariantOpsTest, readsTheSeedPropertyOfAWalkOncePerSeed) {
    const std::string_view query = "MATCH p = (a:Person)-[*1..2]->(b) "
                                   "WHERE all(r IN relationships(p) WHERE r.duration > a.age) RETURN b";
    expectNodePropertyReads(query, "age", 0, 1);

    const std::string_view offsetQuery = "MATCH p = (a:Person)-[*1..2]->(b) "
                                         "WHERE all(r IN relationships(p) WHERE r.duration > a.age - 15) "
                                         "RETURN a.name, b.name";
    expectNodePropertyReads(offsetQuery, "age", 0, 1);
    expectDBOps<mlir::db::SubOp>(offsetQuery, 0, 1);

    expectRows(offsetQuery, {{"Remy", "Adam"}, {"Remy", "Ghosts"}, {"Remy", "Eighties"}, {"Remy", "Remy"},
                             {"Remy", "Remy"}, {"Adam", "Remy"}, {"Adam", "Adam"}, {"Adam", "Ghosts"},
                             {"Adam", "Eighties"}});
}

// The count is the same for every element of a person's list
TEST_F(HoistRepeatedRegionInvariantOpsTest, countsASubqueryOncePerRow) {
    const std::string_view query = "MATCH (n:Person) RETURN n.name, [x IN [1, 2] | COUNT { MATCH (n)-->(m) RETURN DISTINCT m }]";
    expectDBOps<mlir::db::CountSubquery>(query, 0, 1);
    expectNLLines(query, "nl.count_subquery_buffer(", 0, 1);

    expectRows(query, {{"Remy", "[4, 4]"}, {"Adam", "[3, 3]"}, {"Maxime", "[2, 2]"}, {"Luc", "[2, 2]"},
                       {"Martina", "[1, 1]"}, {"Suhas", "[2, 2]"}, {"Cyrus", "[2, 2]"}, {"Doruk", "[1, 1]"}});
}

TEST_F(HoistRepeatedRegionInvariantOpsTest, hoistsOutOfNestedComprehensions) {
    const std::string_view caseQuery = "MATCH (n:Person) RETURN n.name, [x IN [1, 2] | [y IN [x] | CASE WHEN y > 1 THEN n.age END]]";
    expectNodePropertyReads(caseQuery, "age", 0, 1);

    expectRows(caseQuery, {{"Remy", "[[null], [32]]"}, {"Adam", "[[null], [32]]"}, {"Maxime", "[[null], [null]]"},
                           {"Luc", "[[null], [null]]"}, {"Martina", "[[null], [null]]"},
                           {"Suhas", "[[null], [null]]"}, {"Cyrus", "[[null], [null]]"},
                           {"Doruk", "[[null], [null]]"}});

    const std::string_view patternQuery = "MATCH (n:Person) RETURN [x IN [1, 2] | [(n)-->(m) WHERE m.age > n.age | n.name]]";
    expectNodePropertyReads(patternQuery, "age", 1, 2);
    expectNodePropertyReads(patternQuery, "name", 1, 1);

    const std::string_view innerQuery = "MATCH (n:Person) RETURN n.name, [(n)-->(m) | [x IN [1, 2] | n.age]]";
    expectNodePropertyReads(innerQuery, "age", 0, 1);

    expectRows(innerQuery, {{"Remy", "[[32, 32], [32, 32], [32, 32], [32, 32]]"},
                            {"Adam", "[[32, 32], [32, 32], [32, 32]]"},
                            {"Maxime", "[[null, null], [null, null]]"}, {"Luc", "[[null, null], [null, null]]"},
                            {"Martina", "[[null, null]]"}, {"Suhas", "[[null, null], [null, null]]"},
                            {"Cyrus", "[[null, null], [null, null]]"}, {"Doruk", "[[null, null]]"}});
}

// Remy and Adam would divide by zero. The age is read once per row, but the division runs
// only on the elements the CASE sends to its branch.
TEST_F(HoistRepeatedRegionInvariantOpsTest, keepsADivisionThatMayRaiseInACaseBranch) {
    const std::string_view query = "MATCH (n:Person) RETURN n.name, [x IN [1, 2] | CASE WHEN x > 5 THEN 10 / (n.age - 32) END]";
    expectNodePropertyReads(query, "age", 0, 1);
    expectDBOps<mlir::db::DivOp>(query, 1, 1);

    expectRows(query, {{"Remy", "[null, null]"}, {"Adam", "[null, null]"}, {"Maxime", "[null, null]"},
                       {"Luc", "[null, null]"}, {"Martina", "[null, null]"}, {"Suhas", "[null, null]"},
                       {"Cyrus", "[null, null]"}, {"Doruk", "[null, null]"}});

    expectError("MATCH (n:Person) RETURN n.name, [x IN [1, 2] | CASE WHEN x > 1 THEN 10 / (n.age - 32) END]",
                "Attempted to divide by zero");
}

// s holds a name, a string, which has no key to read
TEST_F(HoistRepeatedRegionInvariantOpsTest, keepsAKeyReadThatMayRaiseInACaseBranch) {
    const std::string_view query = "MATCH (n:Person) WITH n, [n.name, 1][0] AS s "
                                   "RETURN n.name, [x IN [1, 2] | CASE WHEN x > 5 THEN s.j END]";
    expectDBOps<mlir::db::StaticMapKey>(query, 1, 1);

    expectRows(query, {{"Remy", "[null, null]"}, {"Adam", "[null, null]"}, {"Maxime", "[null, null]"},
                       {"Luc", "[null, null]"}, {"Martina", "[null, null]"}, {"Suhas", "[null, null]"},
                       {"Cyrus", "[null, null]"}, {"Doruk", "[null, null]"}});

    expectError("MATCH (n:Person) WITH n, [n.name, 1][0] AS s "
                "RETURN n.name, [x IN [1, 2] | CASE WHEN x > 1 THEN s.j END]",
                "String");
}

// A node aged 0 with no edge has no hop for the walk to test, so nothing divides by its age
TEST_F(HoistRepeatedRegionInvariantOpsTest, keepsADivisionThatMayRaiseInAWalkHop) {
    applyWrite("CREATE (:Lonely {age: 0})");

    const std::string_view query = "MATCH p = (a:Lonely)-[*1..2]->(b) "
                                   "WHERE all(r IN relationships(p) WHERE r.duration > 10 / a.age) RETURN b";
    expectNodePropertyReads(query, "age", 0, 1);
    expectDBOps<mlir::db::DivOp>(query, 1, 1);
    expectRows(query, {});

    expectError("MATCH p = (a:Person)-[*1..2]->(b) "
                "WHERE all(r IN relationships(p) WHERE r.duration > 10 / (a.age - 32)) RETURN b",
                "Attempted to divide by zero");
}

// A CASE branch and a subquery body each run once for the rows they take, so what they read
// stays where they read it
TEST_F(HoistRepeatedRegionInvariantOpsTest, leavesARegionRunOncePerRow) {
    const std::string_view caseQuery = "MATCH (n:Person) RETURN n.name, CASE WHEN n.name > 'M' THEN n.age END";
    expectDBNodePropertyReads(caseQuery, "age", 1, 1);

    expectRows(caseQuery, {{"Remy", "32"}, {"Adam", "null"}, {"Maxime", "null"}, {"Luc", "null"},
                           {"Martina", "null"}, {"Suhas", "null"}, {"Cyrus", "null"}, {"Doruk", "null"}});

    const std::string_view countQuery = "MATCH (n:Person) RETURN n.name, COUNT { MATCH (n) WHERE n.age > 30 }";
    expectDBNodePropertyReads(countQuery, "age", 1, 1);

    expectRows(countQuery, {{"Remy", "1"}, {"Adam", "1"}, {"Maxime", "0"}, {"Luc", "0"},
                            {"Martina", "0"}, {"Suhas", "0"}, {"Cyrus", "0"}, {"Doruk", "0"}});
}

TEST_F(HoistRepeatedRegionInvariantOpsTest, carriesTheValuesOfEachRowThroughTheHopAcrossChunks) {
    createItems();

    Rows sums;
    for (int64_t item = 0; item < ITEM_COUNT; item++) {
        const int64_t value = item % 5;

        int64_t sum = 0;
        for (const int64_t weight : {item % 3, item % 7}) {
            if (weight < value) {
                sum += item * 10 + weight;
            }
        }

        sums.push_back({std::to_string(item), std::to_string(sum)});
    }

    const std::string_view query = "MATCH (n:Item) RETURN n.i, "
                                   "reduce(s = 0, x IN [(n)-->(m) WHERE m.w < n.v | n.i * 10 + m.w] | s + x)";
    expectNodePropertyReads(query, "v", 0, 1);
    expectManyRows(query, sums);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
