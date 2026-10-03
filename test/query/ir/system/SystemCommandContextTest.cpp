#include <gtest/gtest.h>

#include <stddef.h>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"

#include "DBDialect.h"
#include "DBLowering.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "NLInterpreter.h"
#include "NLOutputSink.h"
#include "StorageDialect.h"

#include "Graph.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringException.h"
#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr std::string_view missingFacilityMessage = "which this session has not opened";

class NullSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const>, size_t, size_t) override {}
};

}

// A system command acts through the session's facilities - the accessor it calls,
// the change it writes - which reach the executor from the interpreter, and only
// from the one the server runs. NLInterpreter builds its executor without them, so
// every harness on that path (the mlir sample tool, the lowering tests) runs these
// commands against nothing: they have to say so rather than dereference it.
class SystemCommandContextTest : public TuringTest {
public:
    void initialize() override {
        _graph = Graph::create();
        _transaction.emplace(_graph->openTransaction());
        _reader.emplace(_transaction->readGraph());
    }

protected:
    void runWithoutFacilities(const char* programText) {
        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::db::DB>();
        context.getOrLoadDialect<mlir::nl::NL>();

        mlir::OwningOpRef<mlir::ModuleOp> dbModule
            = mlir::parseSourceString<mlir::ModuleOp>(programText, mlir::ParserConfig(&context));
        ASSERT_TRUE(dbModule);

        const mlir::func::FuncOp dbFunction = dbModule->lookupSymbol<mlir::func::FuncOp>("main");
        ASSERT_TRUE(dbFunction);

        const GraphView& view = _reader->getView();

        mlir::OwningOpRef<mlir::ModuleOp> nlModule
            = mlir::ModuleOp::create(mlir::UnknownLoc::get(&context));
        DBLowering lowering(&context, &view);
        lowering.lower(dbFunction, *nlModule);

        NullSink sink;
        LocalMemory memory;
        NLInterpreter interpreter(*nlModule, &view, &sink, &memory);

        // The message is what says the command stopped on the missing facility
        // rather than on anything the lowering or the translation did.
        try {
            interpreter.run();
            ADD_FAILURE() << "the command ran with no facilities to act through:\n" << programText;
        } catch (const TuringException& e) {
            const std::string_view message = e.what();
            EXPECT_NE(message.find(missingFacilityMessage), std::string_view::npos)
                << "stopped on something else: " << message;
        }
    }

    std::unique_ptr<Graph> _graph;
    std::optional<FrozenCommitTx> _transaction;
    std::optional<GraphReader> _reader;
};

TEST_F(SystemCommandContextTest, aCommandNeedingTheAccessorReportsItsAbsence) {
    runWithoutFacilities(R"(
        module {
          func.func @main() {
            %g = db.list_graphs : !db.column<!storage.string>
            db.output(%g) names ["graphName"] : !db.column<!storage.string>
            return
          }
        }
    )");
}

TEST_F(SystemCommandContextTest, aCommandNeedingTheChangeReportsItsAbsence) {
    runWithoutFacilities(R"(
        module {
          func.func @main() {
            db.commit
            return
          }
        }
    )");
}

TEST_F(SystemCommandContextTest, aCommandNeedingThePendingCommitReportsItsAbsence) {
    runWithoutFacilities(R"(
        module {
          func.func @main() {
            db.create_property_index("byName", "name", node)
            return
          }
        }
    )");
}

TEST_F(SystemCommandContextTest, aCommandNeedingTheVectorDatabaseReportsItsAbsence) {
    runWithoutFacilities(R"(
        module {
          func.func @main() {
            %names, %dimensions = db.show_vector_indexes : !db.column<!storage.string>, !db.column<ui64>
            db.output(%names, %dimensions) names ["name", "dimension"]
               : !db.column<!storage.string>, !db.column<ui64>
            return
          }
        }
    )");
}
