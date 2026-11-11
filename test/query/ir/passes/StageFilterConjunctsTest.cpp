#include <gtest/gtest.h>

#include <stddef.h>

#include <memory>
#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "llvm/ADT/STLFunctionalExtras.h"
#include "llvm/ADT/SmallVector.h"
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
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// A costly conjunct runs over the rows the cheap and selective conjuncts of its filter keep,
// behind a filter of its own, and cheap conjuncts share one mask. Each query compiles with the
// graph's counts in the pass context, as the engine runs it: with none, no filter splits.
class StageFilterConjunctsTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);

        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

protected:
    struct Reads {
        size_t _total {0};
        size_t _overFilteredRows {0};
    };

    void generate(std::string_view query, mlir::OwningOpRef<mlir::ModuleOp>& module) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const ProcedureManager* procedures = system.getProcedures();

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        CypherAST ast(procedures, query);

        CypherParser parser(&ast);
        parser.parse(query);

        CypherAnalyzer analyzer(&ast, view);
        analyzer.analyze();

        module = mlir::ModuleOp::create(mlir::UnknownLoc::get(&_context));
        mlir::ModuleOp moduleOp = module.get();

        const mlir::db::DBPassContext passContext {._view = &view};
        DBProgramGenerator generator(&moduleOp, nullptr, passContext);
        generator.generate(&ast);
    }

    static void render(mlir::ModuleOp module, std::string& program) {
        program.clear();
        llvm::raw_string_ostream stream(program);

        mlir::OpPrintingFlags flags;
        flags.assumeVerified();
        module.print(stream, flags);
    }

    // The ops @param selects, and how many of them read a column a filter keeps
    static void countReads(mlir::ModuleOp module, llvm::function_ref<bool(mlir::Operation*)> selects, Reads& reads) {
        module.walk([&selects, &reads](mlir::Operation* op) {
            if (!selects(op)) {
                return;
            }

            reads._total++;
            if (op->getOperand(0).getDefiningOp<mlir::db::FilterOp>()) {
                reads._overFilteredRows++;
            }
        });
    }

    static bool readsProperty(mlir::Operation* op, std::string_view property) {
        mlir::db::GetNodeProperties read = mlir::dyn_cast<mlir::db::GetNodeProperties>(op);
        return read && read.getProperty() == llvm::StringRef(property);
    }

    // The filter whose mask holds @param value, through the `and`s joining it to the others
    static mlir::db::FilterOp maskFilter(mlir::Value value) {
        mlir::Value conjunct = value;
        while (conjunct.hasOneUse()) {
            mlir::Operation* const user = *conjunct.getUsers().begin();
            if (mlir::db::FilterOp filter = mlir::dyn_cast<mlir::db::FilterOp>(user)) {
                return filter;
            } else if (!mlir::isa<mlir::db::AndOp>(user)) {
                return {};
            }

            conjunct = user->getResult(0);
        }

        return {};
    }

    void expectEveryReadOverFilteredRows(std::string_view query, llvm::function_ref<bool(mlir::Operation*)> selects) {
        mlir::OwningOpRef<mlir::ModuleOp> module;
        generate(query, module);

        std::string program;
        render(*module, program);

        Reads reads;
        countReads(*module, selects, reads);

        EXPECT_GT(reads._total, 0u) << query << "\n" << program;
        EXPECT_EQ(reads._overFilteredRows, reads._total) << query << "\n" << program;
    }

    const std::string _graphName {"simpledb"};
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
    mlir::MLIRContext _context;
};

// Of the edges of a person, only the INTERESTED_IN ones need the label of the node they reach
TEST_F(StageFilterConjunctsTest, readsTheFarNodeLabelsOfTheTypedEdgesAlone) {
    expectEveryReadOverFilteredRows("MATCH (a:Person)-[:INTERESTED_IN]-(b:Interest) RETURN count(*)", [](mlir::Operation* op) {
        return mlir::isa<mlir::db::GetNodeLabelSet>(op);
    });
}

// One conjunct compares the ages of a pair, the other its names: the pairs the first keeps
// are the only ones whose names are read
TEST_F(StageFilterConjunctsTest, readsTheSecondPairOfPropertiesOverThePairsTheFirstKeeps) {
    const std::string_view query = "MATCH (a:Person), (b:Person) WHERE abs(a.age - b.age) <= 1 AND a.name > b.name RETURN count(*)";

    mlir::OwningOpRef<mlir::ModuleOp> module;
    generate(query, module);

    std::string program;
    render(*module, program);

    Reads reads;
    countReads(*module, [](mlir::Operation* op) { return mlir::isa<mlir::db::GetNodeProperties>(op); }, reads);

    EXPECT_EQ(reads._total, 4u) << query << "\n" << program;
    EXPECT_EQ(reads._overFilteredRows, 2u) << query << "\n" << program;
}

// A range keeps fewer rows than an inequality, so it runs first, whichever conjunct the query
// writes first, and the name is read over the rows it keeps
TEST_F(StageFilterConjunctsTest, testsTheSelectiveConjunctFirst) {
    const auto readsName = [](mlir::Operation* op) {
        return readsProperty(op, "name");
    };

    expectEveryReadOverFilteredRows("MATCH (n:Person) WHERE n.name <> 'Remy' AND n.age > 30 RETURN count(n)", readsName);
    expectEveryReadOverFilteredRows("MATCH (n:Person) WHERE n.age > 30 AND n.name <> 'Remy' RETURN count(n)", readsName);
}

// The list each row tests membership in is built over the rows the duration test keeps
TEST_F(StageFilterConjunctsTest, buildsTheListOverTheRowsTheCheapTestKeeps) {
    expectEveryReadOverFilteredRows("MATCH (a:Person)-[r]->(b) WHERE r.duration > 15 AND NOT b.name IN [a.name, 'Ghosts'] RETURN count(*)", [](mlir::Operation* op) {
        return readsProperty(op, "name");
    });
}

// Two comparisons of the two ends of a hop cost less than a filter between them
TEST_F(StageFilterConjunctsTest, keepsCheapConjunctsInOneMask) {
    const std::string_view query = "MATCH (a:Person)-[:KNOWS_WELL]->(b:Person) WHERE a <> b AND id(a) < id(b) RETURN count(*)";

    mlir::OwningOpRef<mlir::ModuleOp> module;
    generate(query, module);

    std::string program;
    render(*module, program);

    llvm::SmallVector<mlir::Value> inequalities;
    llvm::SmallVector<mlir::Value> comparisons;
    module->walk([&inequalities, &comparisons](mlir::Operation* op) {
        if (mlir::isa<mlir::db::NeqOp>(op)) {
            inequalities.push_back(op->getResult(0));
        } else if (mlir::isa<mlir::db::LtOp>(op)) {
            comparisons.push_back(op->getResult(0));
        }
    });

    ASSERT_EQ(inequalities.size(), 1u) << query << "\n" << program;
    ASSERT_EQ(comparisons.size(), 1u) << query << "\n" << program;

    const mlir::db::FilterOp filter = maskFilter(inequalities.front());
    ASSERT_TRUE(filter) << query << "\n" << program;
    EXPECT_EQ(maskFilter(comparisons.front()), filter) << query << "\n" << program;
}
