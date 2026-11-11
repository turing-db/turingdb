#include <gtest/gtest.h>

#include <stddef.h>

#include <algorithm>
#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/IR/Verifier.h"
#include "mlir/Parser/Parser.h"
#include "mlir/Pass/PassManager.h"
#include "llvm/Support/raw_ostream.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "DBPasses.h"
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

// A body run once per row reads its input as a run constant, and a product repeats the rows of
// each factor once per row of the other, so a read of the input behind either runs once per row
// the body or the product makes. It runs once per row of the input instead, in front of the op
// holding the body. Remy and Adam are the two people aged 32; no other person has an age.
class HoistThroughProductsAndRowConstantsTest : public WriteQueryTest {
protected:
    struct ReadCount {
        size_t _inBody {0};
        size_t _total {0};
    };

    void initialize() override {
        WriteQueryTest::initialize();

        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

    void generateProgram(std::string_view query, mlir::OwningOpRef<mlir::ModuleOp>& module, std::string& program) {
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
        module = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp moduleOp = module.get();

        DBProgramGenerator generator(&moduleOp);
        generator.generate(&ast);

        llvm::raw_string_ostream stream(program);
        moduleOp.print(stream);
    }

    // The ops @param isCounted picks in the db program of @param query, and how many of them sit
    // in a region of a @param BodyOp
    template <typename BodyOp, typename IsCounted>
    void expectDBOps(std::string_view query, IsCounted isCounted, size_t inBody, size_t total) {
        mlir::OwningOpRef<mlir::ModuleOp> module;
        std::string program;
        ASSERT_NO_FATAL_FAILURE(generateProgram(query, module, program));

        ReadCount count;
        module->walk([&isCounted, &count](mlir::Operation* op) {
            if (!isCounted(op)) {
                return;
            }

            count._total++;
            if (op->getParentOfType<BodyOp>()) {
                count._inBody++;
            }
        });

        EXPECT_EQ(count._inBody, inBody) << query << "\n" << program;
        EXPECT_EQ(count._total, total) << query << "\n" << program;
    }

    template <typename BodyOp>
    void expectDBNodePropertyReads(std::string_view query, llvm::StringRef property, size_t inBody, size_t total) {
        const auto readsTheProperty = [property](mlir::Operation* op) {
            mlir::db::GetNodeProperties read = mlir::dyn_cast<mlir::db::GetNodeProperties>(op);
            return read && read.getProperty() == property;
        };

        expectDBOps<BodyOp>(query, readsTheProperty, inBody, total);
    }

    template <typename CountedOp, typename BodyOp>
    void expectDBOpsOf(std::string_view query, size_t inBody, size_t total) {
        const auto isCounted = [](mlir::Operation* op) {
            return mlir::isa<CountedOp>(op);
        };

        expectDBOps<BodyOp>(query, isCounted, inBody, total);
    }

    void nlProgramOf(std::string_view query, std::string& program) {
        RowSink sink;
        const QueryStatus status = runQuery("EXPLAIN (nl) " + std::string(query), &sink);
        ASSERT_TRUE(status.isOk()) << query << "\n" << status.getError();
        ASSERT_EQ(sink.rows().size(), 1u) << query;

        program = sink.rows().front().back();
    }

    // The reads of @param property in the nl program of @param query, and how many of them sit
    // deeper than the body of the first loop, the one over the scanned rows
    void expectNLNodePropertyReads(std::string_view query, std::string_view property, size_t nested, size_t total) {
        std::string program;
        ASSERT_NO_FATAL_FAILURE(nlProgramOf(query, program));

        const std::string definition = " = nl.get_property_type(\"" + std::string(property) + "\")";
        const size_t definitionPosition = program.find(definition);
        ASSERT_NE(definitionPosition, std::string::npos) << property << "\n" << program;

        const size_t valueStart = program.rfind('%', definitionPosition);
        const std::string readSuffix = ", " + program.substr(valueStart, definitionPosition - valueStart) + ")";

        size_t rowBodyIndent = std::string_view::npos;
        size_t lineStart = 0;
        ReadCount count;

        while (lineStart < program.size()) {
            const size_t lineEnd = std::min(program.find('\n', lineStart), program.size());
            const std::string_view line = std::string_view(program).substr(lineStart, lineEnd - lineStart);
            lineStart = lineEnd + 1;

            const size_t indent = line.find_first_not_of(' ');
            if (indent == std::string_view::npos) {
                continue;
            }

            const std::string_view text = line.substr(indent);
            if (rowBodyIndent == std::string_view::npos && text.starts_with("nl.for ")) {
                rowBodyIndent = indent + 2;
            }

            const bool readsTheProperty = text.find("nl.get_node_properties(") != std::string_view::npos
                                       && text.find(readSuffix) != std::string_view::npos;
            if (!readsTheProperty) {
                continue;
            }

            count._total++;
            if (indent > rowBodyIndent) {
                count._inBody++;
            }
        }

        EXPECT_EQ(count._inBody, nested) << query << "\n" << program;
        EXPECT_EQ(count._total, total) << query << "\n" << program;
    }

private:
    mlir::MLIRContext _context;
};

// The body runs once per person and reads n as a run constant, laid out over every edge
TEST_F(HoistThroughProductsAndRowConstantsTest, readsTheInputPropertyOfABodyRunPerRowOncePerPerson) {
    const std::string_view query = "MATCH (n:Person) RETURN COUNT { MATCH (n)-->(m) WHERE m.age > n.age RETURN DISTINCT m }";
    expectDBNodePropertyReads<mlir::db::CountSubquery>(query, "age", 1, 2);
    expectNLNodePropertyReads(query, "age", 1, 2);
    expectRows(query, {{"0"}, {"0"}, {"0"}, {"0"}, {"0"}, {"0"}, {"0"}, {"0"}});

    const std::string_view matched = "MATCH (n:Person) RETURN n.name, COUNT { MATCH (n)-->(m) WHERE m.age >= n.age RETURN DISTINCT m }";
    expectDBNodePropertyReads<mlir::db::CountSubquery>(matched, "age", 1, 2);

    expectRows(matched, {{"Remy", "1"}, {"Adam", "1"}, {"Maxime", "0"}, {"Luc", "0"},
                         {"Martina", "0"}, {"Suhas", "0"}, {"Cyrus", "0"}, {"Doruk", "0"}});
}

// The product pairs each edge with every interest, and the comparison reads one name of each
TEST_F(HoistThroughProductsAndRowConstantsTest, readsTheInputPropertyBehindAProductOncePerPerson) {
    const std::string_view query = "MATCH (n:Person) RETURN COUNT { MATCH (n)-->(m), (k:Interest) WHERE k.name > n.name }";
    expectDBNodePropertyReads<mlir::db::CountSubquery>(query, "name", 1, 2);
    expectNLNodePropertyReads(query, "name", 1, 2);
    expectDBOpsOf<mlir::db::GtOp, mlir::db::CountSubquery>(query, 1, 1);

    expectRows(query, {{"4"}, {"30"}, {"4"}, {"4"}, {"2"}, {"2"}, {"12"}, {"6"}});
}

// Nothing but the product repeats the rows of n
TEST_F(HoistThroughProductsAndRowConstantsTest, readsTheInputPropertyOnceWhereOnlyTheProductRepeatsIt) {
    const std::string_view query = "MATCH (n:Person) RETURN COUNT { MATCH (n), (k:Interest) WHERE k.name > n.name }";
    expectDBNodePropertyReads<mlir::db::CountSubquery>(query, "name", 1, 2);
    expectNLNodePropertyReads(query, "name", 1, 2);

    expectRows(query, {{"1"}, {"10"}, {"2"}, {"2"}, {"2"}, {"1"}, {"6"}, {"6"}});
}

// The comparison reads only the edges, so it sits in the first factor, in front of the product
TEST_F(HoistThroughProductsAndRowConstantsTest, readsTheInputPropertyOfAFactorOncePerPerson) {
    const std::string_view query = "MATCH (n:Person) RETURN n.name, COUNT { MATCH (n)-->(m), (k:Interest) WHERE m.age >= n.age }";
    expectDBNodePropertyReads<mlir::db::CrossProduct>(query, "age", 1, 2);
    expectNLNodePropertyReads(query, "age", 1, 2);

    expectRows(query, {{"Remy", "10"}, {"Adam", "10"}, {"Maxime", "0"}, {"Luc", "0"},
                       {"Martina", "0"}, {"Suhas", "0"}, {"Cyrus", "0"}, {"Doruk", "0"}});
}

// The age of n is read once per person, in front of the list comprehension, while the sum reads
// x, an element of the list
TEST_F(HoistThroughProductsAndRowConstantsTest, keepsWhatReadsAnElementInTheListBody) {
    const std::string_view query = "MATCH (n:Person) RETURN n.name, "
                                   "[x IN [0, 1] | COUNT { MATCH (n)-->(m), (k:Interest) WHERE m.age >= n.age + x }]";
    expectDBNodePropertyReads<mlir::db::ListComprehension>(query, "age", 1, 2);
    expectDBOpsOf<mlir::db::AddOp, mlir::db::ListComprehension>(query, 1, 1);

    expectRows(query, {{"Remy", "[10, 0]"}, {"Adam", "[10, 0]"}, {"Maxime", "[0, 0]"}, {"Luc", "[0, 0]"},
                       {"Martina", "[0, 0]"}, {"Suhas", "[0, 0]"}, {"Cyrus", "[0, 0]"}, {"Doruk", "[0, 0]"}});
}

// The name of m changes with each edge and the name of k with each interest
TEST_F(HoistThroughProductsAndRowConstantsTest, keepsTheReadsOfEachSideOfAProductInTheBody) {
    const std::string_view query = "MATCH (n:Person) RETURN COUNT { MATCH (n)-->(m), (k:Interest) WHERE k.name > m.name }";
    expectDBNodePropertyReads<mlir::db::CountSubquery>(query, "name", 2, 2);
    expectDBOpsOf<mlir::db::GtOp, mlir::db::CountSubquery>(query, 1, 1);
}

// A node with no edge gives the body no row to divide on
TEST_F(HoistThroughProductsAndRowConstantsTest, keepsADivisionThatMayRaiseInTheBody) {
    applyWrite("CREATE (:Lonely {age: 0, name: 'Lonely'})");

    const std::string_view distinct = "MATCH (n:Lonely) RETURN COUNT { MATCH (n)-->(m) WHERE m.age > 10 / n.age RETURN DISTINCT m }";
    expectDBNodePropertyReads<mlir::db::CountSubquery>(distinct, "age", 1, 2);
    expectDBOpsOf<mlir::db::DivOp, mlir::db::CountSubquery>(distinct, 1, 1);
    expectRows(distinct, {{"0"}});

    const std::string_view product = "MATCH (n:Lonely) RETURN COUNT { MATCH (n)-->(m), (k:Interest) WHERE k.name > n.name + toString(10 / n.age) }";
    expectDBNodePropertyReads<mlir::db::CountSubquery>(product, "age", 0, 1);
    expectDBOpsOf<mlir::db::DivOp, mlir::db::CountSubquery>(product, 1, 1);
    expectRows(product, {{"0"}});

    expectError("MATCH (n:Person) RETURN COUNT { MATCH (n)-->(m) WHERE m.age > 10 / (n.age - 32) RETURN DISTINCT m }",
                "Attempted to divide by zero");
}

// The body runs once per person and reads n as a run constant over its one row
TEST_F(HoistThroughProductsAndRowConstantsTest, leavesABodyRunPerRowWithNoHop) {
    const std::string_view query = "MATCH (n:Person) RETURN n.name, COUNT { MATCH (n) WHERE n.age > 30 RETURN DISTINCT n }";
    expectDBNodePropertyReads<mlir::db::CountSubquery>(query, "age", 1, 1);

    expectRows(query, {{"Remy", "1"}, {"Adam", "1"}, {"Maxime", "0"}, {"Luc", "0"},
                       {"Martina", "0"}, {"Suhas", "0"}, {"Cyrus", "0"}, {"Doruk", "0"}});
}

// A region of a CASE or a coalesce forwarding one of its arguments hands the regions after it the
// column that argument carries in
class HoistForwardedArgumentReadTest : public ::testing::Test {
protected:
    HoistForwardedArgumentReadTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    // The reads of the age left in the body of the list comprehension once the hoist has run
    void expectReadsLeftInTheBody(const char* programText, size_t expected) {
        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&_context));
        ASSERT_TRUE(module);

        mlir::PassManager passManager(&_context);
        passManager.addPass(mlir::db::createHoistRepeatedRegionInvariantOps());
        ASSERT_TRUE(mlir::succeeded(passManager.run(*module)));
        ASSERT_TRUE(mlir::succeeded(mlir::verify(*module)));

        std::string program;
        llvm::raw_string_ostream stream(program);
        module->print(stream);

        size_t inBody = 0;
        size_t total = 0;
        module->walk([&inBody, &total](mlir::db::GetNodeProperties read) {
            total++;
            if (read->getParentOfType<mlir::db::ListComprehension>()) {
                inBody++;
            }
        });

        EXPECT_EQ(inBody, expected) << program;
        EXPECT_EQ(total, 1u) << program;
    }

    mlir::MLIRContext _context;
};

// MATCH (n) RETURN [x IN [1, 2] | CASE WHEN x IS NOT NULL THEN n.age END], the condition
// forwarding the n it is carried
const char* const caseForwardingItsArgument = R"mlir(
!nodes = !db.column<!storage.node_id>
!ages = !db.column<!storage.nullable<i64>>
!bools = !db.column<!storage.bool>
!any = !db.column<none>
!tags = !db.column<ui64>
!lists = !db.column<!storage.list<i64>>

func.func @main() {
  %n = db.scan_nodes() : !nodes
  %xs = db.constant([1, 2])
  %l = db.list_comprehension(%xs, {%n}) {
  ^bb0(%x: !any, %tag: !tags, %nIn: !nodes):
    %c = db.lazy_case({%nIn, %x}) {
    ^bb0(%nCondition: !nodes, %xCondition: !any):
      %held = db.is_not_null %xCondition : (!any) -> !bools
      db.case_yield %held : !bools forwarding(%nCondition : !nodes)
    }, {
    ^bb0(%nForwarded: !nodes, %nValue: !nodes, %xValue: !any):
      %age = db.get_node_properties(%nForwarded, "age") : (!nodes) -> !ages
      db.case_yield %age : !ages
    } : (!nodes, !any) -> !ages
    db.comprehension_yield %tag, %c : !ages
  } : (!lists, !nodes) -> !any
  db.output(%l) : !any
  return
}
)mlir";

// MATCH (n) RETURN [x IN [1, 2] | coalesce(x, n.age)], the first argument forwarding the n it
// is carried
const char* const coalesceForwardingItsArgument = R"mlir(
!nodes = !db.column<!storage.node_id>
!ages = !db.column<!storage.nullable<i64>>
!any = !db.column<none>
!tags = !db.column<ui64>
!lists = !db.column<!storage.list<i64>>

func.func @main() {
  %n = db.scan_nodes() : !nodes
  %xs = db.constant([1, 2])
  %l = db.list_comprehension(%xs, {%n}) {
  ^bb0(%x: !any, %tag: !tags, %nIn: !nodes):
    %c = db.coalesce({%nIn, %x}) {
    ^bb0(%nFirst: !nodes, %xFirst: !any):
      db.case_yield %xFirst : !any forwarding(%nFirst : !nodes)
    }, {
    ^bb0(%nForwarded: !nodes, %nSecond: !nodes, %xSecond: !any):
      %age = db.get_node_properties(%nForwarded, "age") : (!nodes) -> !ages
      db.case_yield %age : !ages
    } : (!nodes, !any) -> !any
    db.comprehension_yield %tag, %c : !any
  } : (!lists, !nodes) -> !any
  db.output(%l) : !any
  return
}
)mlir";

TEST_F(HoistForwardedArgumentReadTest, hoistsAReadOfAnArgumentACaseConditionForwards) {
    expectReadsLeftInTheBody(caseForwardingItsArgument, 0);
}

TEST_F(HoistForwardedArgumentReadTest, hoistsAReadOfAnArgumentACoalesceArgumentForwards) {
    expectReadsLeftInTheBody(coalesceForwardingItsArgument, 0);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
