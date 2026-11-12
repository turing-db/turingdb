#include <gtest/gtest.h>

#include <stddef.h>

#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/Diagnostics.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/Parser/Parser.h"

#include "DBDialect.h"
#include "NLDialect.h"
#include "StorageDialect.h"

// A vector index holds vectors of at least one float, as every embedding does. The db and
// nl ops that create one refuse a dimension of 0 when they are verified, so IR text can
// not reach the vector library with one.
class VectorIndexDimensionVerifierTest : public ::testing::Test {
protected:
    VectorIndexDimensionVerifierTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

    bool parses(std::string_view program, std::string& diagnostics) {
        diagnostics.clear();
        const mlir::ScopedDiagnosticHandler handler(&_context, [&diagnostics](mlir::Diagnostic& diagnostic) {
            diagnostics += diagnostic.str();
            diagnostics += '\n';
            return mlir::success();
        });

        const mlir::OwningOpRef<mlir::ModuleOp> module =
            mlir::parseSourceString<mlir::ModuleOp>(program, mlir::ParserConfig(&_context));

        return static_cast<bool>(module);
    }

    static void dbProgram(size_t dimension, std::string& program) {
        program = "module {\n"
                  "  func.func @main() {\n"
                  "    %0 = db.create_vector_index(\"v\", " + std::to_string(dimension) + ", euclidean, hnsw) : !db.column<!storage.string>\n"
                  "    db.output(%0) names [\"index\"] : !db.column<!storage.string>\n"
                  "    return\n"
                  "  }\n"
                  "}\n";
    }

    static void nlProgram(size_t dimension, std::string& program) {
        program = "module {\n"
                  "  func.func @main() {\n"
                  "    %0 = nl.create_vector_index(\"v\", " + std::to_string(dimension) + ", euclidean, hnsw) : !nl.chunk<!storage.string>\n"
                  "    nl.output(%0) names [\"index\"] : !nl.chunk<!storage.string>\n"
                  "    return\n"
                  "  }\n"
                  "}\n";
    }

    mlir::MLIRContext _context;
};

TEST_F(VectorIndexDimensionVerifierTest, dbRefusesADimensionOfZero) {
    std::string program;
    std::string diagnostics;

    dbProgram(0, program);
    EXPECT_FALSE(parses(program, diagnostics));
    EXPECT_NE(diagnostics.find("must hold vectors of at least one dimension"), std::string::npos) << diagnostics;

    dbProgram(1, program);
    EXPECT_TRUE(parses(program, diagnostics)) << diagnostics;
}

TEST_F(VectorIndexDimensionVerifierTest, nlRefusesADimensionOfZero) {
    std::string program;
    std::string diagnostics;

    nlProgram(0, program);
    EXPECT_FALSE(parses(program, diagnostics));
    EXPECT_NE(diagnostics.find("must hold vectors of at least one dimension"), std::string::npos) << diagnostics;

    nlProgram(1, program);
    EXPECT_TRUE(parses(program, diagnostics)) << diagnostics;
}
