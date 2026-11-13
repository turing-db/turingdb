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

// A subquery returns what its body yields, after the columns it hands back, and a column nothing
// reads past it leaves the yield, with the reads that only computed it. The body still yields
// a row for each of the rows it made, so the rows stay. Remy has 4 out-edges, Adam 3, Maxime,
// Luc, Cyrus and Suhas 2 each, Martina and Doruk 1 each; Remy and Adam are the only people with
// an age, 32.
class TrimSubqueryYieldTest : public WriteQueryTest {
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

    // The one @param SubqueryOp of the db program of @param query returns @param ownCount columns
    // besides the ones it hands back, and reads @param property inside its body @param bodyReads times
    template <typename SubqueryOp>
    void expectSubquery(std::string_view query, size_t ownCount, llvm::StringRef property, size_t bodyReads) {
        mlir::OwningOpRef<mlir::ModuleOp> module;
        std::string program;
        ASSERT_NO_FATAL_FAILURE(generateProgram(query, module, program));

        size_t subqueryCount = 0;
        size_t ownColumns = 0;
        size_t reads = 0;
        module->walk([property, &subqueryCount, &ownColumns, &reads](mlir::Operation* op) {
            if (mlir::isa<SubqueryOp>(op)) {
                subqueryCount++;
                ownColumns = op->getNumResults() - op->getNumOperands();
            }

            mlir::db::GetNodeProperties read = mlir::dyn_cast<mlir::db::GetNodeProperties>(op);
            if (read && read.getProperty() == property && op->getParentOfType<SubqueryOp>()) {
                reads++;
            }
        });

        ASSERT_EQ(subqueryCount, 1u) << query << "\n" << program;
        EXPECT_EQ(ownColumns, ownCount) << query << "\n" << program;
        EXPECT_EQ(reads, bodyReads) << query << "\n" << program;
    }

private:
    mlir::MLIRContext _context;
};

// Nothing reads k or its name past the call, so the call yields n alone, carried through the hop
TEST_F(TrimSubqueryYieldTest, dropsWhatACallCarryingItsScopeReturnsAndNothingReads) {
    const std::string_view query = "MATCH (n:Person) CALL { WITH n MATCH (n)-->(k) RETURN k, k.name AS x } RETURN n.name";
    expectSubquery<mlir::db::CallSubquery>(query, 0, "name", 0);
    expectRows(query, {{"Remy"}, {"Remy"}, {"Remy"}, {"Remy"}, {"Adam"}, {"Adam"}, {"Adam"}, {"Maxime"}, {"Maxime"},
                       {"Luc"}, {"Luc"}, {"Martina"}, {"Cyrus"}, {"Cyrus"}, {"Suhas"}, {"Suhas"}, {"Doruk"}});
}

// The row a person without a match comes back as is padded whatever the body yields
TEST_F(TrimSubqueryYieldTest, dropsWhatAnOptionalCallReturnsAndNothingReads) {
    const std::string_view query = "MATCH (n:Person) "
                                   "OPTIONAL CALL { WITH n MATCH (n)-->(k) WHERE k.age > 30 RETURN k, k.name AS x } "
                                   "RETURN n.name";
    expectSubquery<mlir::db::CallSubquery>(query, 0, "name", 0);
    expectRows(query, {{"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"}, {"Martina"}, {"Cyrus"}, {"Suhas"}, {"Doruk"}});
}

// An aggregating body runs once per person and hands n back ahead of what it yields: the count
// stays, the oldest age and the read computing it go
TEST_F(TrimSubqueryYieldTest, dropsWhatACallRunPerRowReturnsAndNothingReads) {
    const std::string_view query = "MATCH (n:Person) CALL { WITH n MATCH (n)-->(k) RETURN count(k) AS c, max(k.age) AS oldest } "
                                   "RETURN n.name, c";
    expectSubquery<mlir::db::CallSubquery>(query, 1, "age", 0);
    expectRows(query, {{"Remy", "4"}, {"Adam", "3"}, {"Maxime", "2"}, {"Luc", "2"}, {"Martina", "1"}, {"Cyrus", "2"},
                       {"Suhas", "2"}, {"Doruk", "1"}});
}

// A call returning only what nothing reads keeps a row for each edge
TEST_F(TrimSubqueryYieldTest, keepsTheRowsOfACallWhoseReturnedColumnsNothingReads) {
    const std::string_view counted = "MATCH (n:Person) CALL { WITH n MATCH (n)-->(k) RETURN k, k.name AS x } RETURN count(n)";
    expectSubquery<mlir::db::CallSubquery>(counted, 0, "name", 0);
    expectRows(counted, {{"17"}});

    expectRows("MATCH (n:Person) CALL { WITH n MATCH (n)-->(k) RETURN k } RETURN count(*)", {{"17"}});
    expectRows("MATCH (n:Person) OPTIONAL CALL { WITH n MATCH (n)-->(k) WHERE k.age > 30 RETURN k } RETURN count(*)", {{"8"}});
}

// The optional match pads the same way, by the same rule
TEST_F(TrimSubqueryYieldTest, dropsWhatAnOptionalMatchBindsAndNothingReads) {
    const std::string_view query = "MATCH (n:Person) OPTIONAL MATCH (n)-->(k) WHERE k.age > 30 WITH n, k, k.name AS x RETURN n.name";
    expectSubquery<mlir::db::OptionalMatch>(query, 0, "name", 0);
    expectRows(query, {{"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"}, {"Martina"}, {"Cyrus"}, {"Suhas"}, {"Doruk"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
