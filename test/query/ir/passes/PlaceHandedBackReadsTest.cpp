#include <gtest/gtest.h>

#include <stddef.h>

#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
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
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// An OPTIONAL MATCH or a CALL hands back the columns it joins onto, each row repeated for every
// match of it, so a value read off one is read once in front of the op and carried through its
// body, as it is in front of a hop. A body that filters the rows it repeats keeps the read after
// it. Remy has 4 out-edges, Adam 3, Maxime, Luc, Cyrus and Suhas 2 each, Martina and Doruk 1
// each; Remy and Adam are the only people with an age, 32.
class PlaceHandedBackReadsTest : public WriteQueryTest {
protected:
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

    // The reads of @param property in the db program of @param query, and how many of them come
    // before the first @param MarkerOp
    template <typename MarkerOp>
    void expectDBReadsBefore(std::string_view query, llvm::StringRef property, size_t before, size_t total) {
        mlir::OwningOpRef<mlir::ModuleOp> module;
        std::string program;
        ASSERT_NO_FATAL_FAILURE(generateProgram(query, module, program));

        size_t placed = 0;
        size_t reads = 0;
        bool markerSeen = false;
        module->walk<mlir::WalkOrder::PreOrder>([property, &placed, &reads, &markerSeen](mlir::Operation* op) {
            markerSeen = markerSeen || mlir::isa<MarkerOp>(op);

            mlir::db::GetNodeProperties read = mlir::dyn_cast<mlir::db::GetNodeProperties>(op);
            if (!read || read.getProperty() != property) {
                return;
            }

            reads++;
            if (!markerSeen) {
                placed++;
            }
        });

        EXPECT_TRUE(markerSeen) << query << "\n" << program;
        EXPECT_EQ(placed, before) << query << "\n" << program;
        EXPECT_EQ(reads, total) << query << "\n" << program;
    }

    // Every @param SubqueryOp of the db program of @param query carries each column in once
    template <typename SubqueryOp>
    void expectEachColumnCarriedOnce(std::string_view query) {
        mlir::OwningOpRef<mlir::ModuleOp> module;
        std::string program;
        ASSERT_NO_FATAL_FAILURE(generateProgram(query, module, program));

        size_t repeats = 0;
        module->walk([&repeats](SubqueryOp op) {
            const mlir::OperandRange inputs = op->getOperands();
            for (size_t inputIndex = 0; inputIndex < inputs.size(); inputIndex++) {
                if (llvm::find(inputs, inputs[inputIndex]) != inputs.begin() + inputIndex) {
                    repeats++;
                }
            }
        });

        EXPECT_EQ(repeats, 0u) << query << "\n" << program;
    }

private:
    mlir::MLIRContext _context;
};

TEST_F(PlaceHandedBackReadsTest, readsWhatAnOptionalMatchHandsBackInFrontOfIt) {
    const std::string_view query = "MATCH (n:Person) OPTIONAL MATCH (n)-->(m) RETURN n.name, m.name";
    expectDBReadsBefore<mlir::db::OptionalMatch>(query, "name", 1, 2);
    expectRows(query, {{"Remy", "Adam"}, {"Remy", "Ghosts"}, {"Remy", "Computers"}, {"Remy", "Eighties"},
                       {"Adam", "Remy"}, {"Adam", "Bio"}, {"Adam", "Cooking"}, {"Maxime", "Bio"}, {"Maxime", "Padel"},
                       {"Luc", "Animals"}, {"Luc", "Computers"}, {"Martina", "Cooking"}, {"Cyrus", "Gym"},
                       {"Cyrus", "Travel"}, {"Suhas", "Gym"}, {"Suhas", "JiuJitsu"}, {"Doruk", "Gym"}});
}

// The WHERE of an OPTIONAL MATCH drops no person: one it leaves no match for comes back padded
TEST_F(PlaceHandedBackReadsTest, readsWhatAnOptionalMatchHandsBackInFrontOfItsFilter) {
    const std::string_view query = "MATCH (n:Person) OPTIONAL MATCH (n)-->(m) WHERE m.age > 30 RETURN n.name, m.name";
    expectDBReadsBefore<mlir::db::OptionalMatch>(query, "name", 1, 2);
    expectRows(query, {{"Remy", "Adam"}, {"Adam", "Remy"}, {"Maxime", "null"}, {"Luc", "null"}, {"Martina", "null"},
                       {"Cyrus", "null"}, {"Suhas", "null"}, {"Doruk", "null"}});
}

TEST_F(PlaceHandedBackReadsTest, readsWhatACallHandsBackInFrontOfIt) {
    const std::string_view query = "MATCH (n:Person) CALL { WITH n MATCH (n)-->(k) RETURN k } RETURN n.name, k.name";
    expectDBReadsBefore<mlir::db::CallSubquery>(query, "name", 1, 2);
    expectRows(query, {{"Remy", "Adam"}, {"Remy", "Ghosts"}, {"Remy", "Computers"}, {"Remy", "Eighties"},
                       {"Adam", "Remy"}, {"Adam", "Bio"}, {"Adam", "Cooking"}, {"Maxime", "Bio"}, {"Maxime", "Padel"},
                       {"Luc", "Animals"}, {"Luc", "Computers"}, {"Martina", "Cooking"}, {"Cyrus", "Gym"},
                       {"Cyrus", "Travel"}, {"Suhas", "Gym"}, {"Suhas", "JiuJitsu"}, {"Doruk", "Gym"}});
}

// The call hands n back through the OPTIONAL MATCH of its body, which repeats it for each edge
TEST_F(PlaceHandedBackReadsTest, readsWhatACallHandsBackThroughAnOptionalMatchInFrontOfIt) {
    const std::string_view query = "MATCH (n:Person) CALL { WITH n OPTIONAL MATCH (n)-->(k) WHERE k.age > 30 RETURN k } "
                                   "RETURN n.name, k.name";
    expectDBReadsBefore<mlir::db::CallSubquery>(query, "name", 1, 2);
    expectRows(query, {{"Remy", "Adam"}, {"Adam", "Remy"}, {"Maxime", "null"}, {"Luc", "null"}, {"Martina", "null"},
                       {"Cyrus", "null"}, {"Suhas", "null"}, {"Doruk", "null"}});
}

// The body keeps the edges to someone over 30, so n.name is read for the 2 rows it keeps rather
// than for the 8 people
TEST_F(PlaceHandedBackReadsTest, keepsAReadAfterACallWhoseBodyFiltersTheRowsItRepeats) {
    const std::string_view query = "MATCH (n:Person) CALL { WITH n MATCH (n)-->(k) WHERE k.age > 30 RETURN k } "
                                   "RETURN n.name, k.name";
    expectDBReadsBefore<mlir::db::CallSubquery>(query, "name", 0, 2);
    expectRows(query, {{"Remy", "Adam"}, {"Adam", "Remy"}});
}

// The age the body compares and the one returned are read once in front of the OPTIONAL MATCH,
// and carried in once
TEST_F(PlaceHandedBackReadsTest, carriesAValueReadInsideAndAfterAnOptionalMatchInOnce) {
    const std::string_view query = "MATCH (n:Person) OPTIONAL MATCH (n)-->(m) WHERE m.age >= n.age RETURN n.name, n.age, m.name";
    expectDBReadsBefore<mlir::db::OptionalMatch>(query, "age", 1, 2);
    expectEachColumnCarriedOnce<mlir::db::OptionalMatch>(query);
    expectRows(query, {{"Remy", "32", "Adam"}, {"Adam", "32", "Remy"}, {"Maxime", "null", "null"}, {"Luc", "null", "null"},
                       {"Martina", "null", "null"}, {"Cyrus", "null", "null"}, {"Suhas", "null", "null"},
                       {"Doruk", "null", "null"}});
}

// Every use of n.name follows the filter after the OPTIONAL MATCH, which repeats it, so it is read
// for the rows the filter keeps
TEST_F(PlaceHandedBackReadsTest, readsWhatOnlyTheRowsAFilterKeepsUseAfterTheFilter) {
    const std::string_view query = "MATCH (n:Person) OPTIONAL MATCH (n)-->(m) WITH n.name AS nm, m WHERE m.name > 'C' RETURN nm";
    expectDBReadsBefore<mlir::db::FilterOp>(query, "name", 1, 2);
    expectRows(query, {{"Remy"}, {"Remy"}, {"Remy"}, {"Adam"}, {"Adam"}, {"Maxime"}, {"Luc"}, {"Martina"}, {"Cyrus"},
                       {"Cyrus"}, {"Suhas"}, {"Suhas"}, {"Doruk"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
