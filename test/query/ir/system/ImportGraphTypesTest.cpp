#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/Diagnostics.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"

#include "DBDialect.h"
#include "NLDialect.h"
#include "StorageDialect.h"

namespace {

// The `types` of db.import_graph and nl.import_graph map a property to the type an import
// reads it as where its format spells none: a datetime, duration or point from JSONL, a
// duration from Parquet. Any other entry is malformed IR.
class ImportGraphTypesTest : public ::testing::Test {
protected:
    ImportGraphTypesTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

    bool verifies(std::string_view import) {
        const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
            return mlir::success();
        });

        const std::string text = "module { func.func @main() { " + std::string(import) + " return } }";

        return static_cast<bool>(mlir::parseSourceString<mlir::ModuleOp>(text, mlir::ParserConfig(&_context)));
    }

    mlir::MLIRContext _context;
};

}

TEST_F(ImportGraphTypesTest, acceptsTheTypesEachFormatReads) {
    EXPECT_TRUE(verifies(R"(%g = db.import_graph jsonl("/data/c.jsonl") as "c"
        types {at = !storage.point, founded = !storage.datetime, span = !storage.duration} : !db.column<!storage.string>)"));
    EXPECT_TRUE(verifies(R"(%g = db.import_graph parquet("/data/c") as "c"
        types {span = !storage.duration} : !db.column<!storage.string>)"));
    EXPECT_TRUE(verifies(R"(%g = nl.import_graph jsonl("/data/c.jsonl") as "c"
        types {at = !storage.point, founded = !storage.datetime, span = !storage.duration} : !nl.chunk<!storage.string>)"));
    EXPECT_TRUE(verifies(R"(%g = nl.import_graph parquet("/data/c") as "c"
        types {span = !storage.duration} : !nl.chunk<!storage.string>)"));
}

TEST_F(ImportGraphTypesTest, rejectsAnEntryThatIsNoType) {
    EXPECT_FALSE(verifies(R"(%g = db.import_graph jsonl("/data/c.jsonl") as "c" types {at} : !db.column<!storage.string>)"));
    EXPECT_FALSE(verifies(R"(%g = db.import_graph jsonl("/data/c.jsonl") as "c" types {at = 1 : i64} : !db.column<!storage.string>)"));
    EXPECT_FALSE(verifies(R"(%g = nl.import_graph jsonl("/data/c.jsonl") as "c" types {at} : !nl.chunk<!storage.string>)"));
}

TEST_F(ImportGraphTypesTest, rejectsATypeTheFormatSpellsItself) {
    EXPECT_FALSE(verifies(R"(%g = db.import_graph jsonl("/data/c.jsonl") as "c" types {at = f64} : !db.column<!storage.string>)"));
    EXPECT_FALSE(verifies(R"(%g = nl.import_graph jsonl("/data/c.jsonl") as "c" types {at = f64} : !nl.chunk<!storage.string>)"));
}

TEST_F(ImportGraphTypesTest, rejectsATypeTheFormatDoesNotRead) {
    EXPECT_FALSE(verifies(R"(%g = db.import_graph parquet("/data/c") as "c" types {g = !storage.point} : !db.column<!storage.string>)"));
    EXPECT_FALSE(verifies(R"(%g = db.import_graph parquet("/data/c") as "c" types {g = !storage.datetime} : !db.column<!storage.string>)"));
    EXPECT_FALSE(verifies(R"(%g = db.import_graph gml("/data/c.gml") as "c" types {g = !storage.duration} : !db.column<!storage.string>)"));
    EXPECT_FALSE(verifies(R"(%g = nl.import_graph parquet("/data/c") as "c" types {g = !storage.point} : !nl.chunk<!storage.string>)"));
    EXPECT_FALSE(verifies(R"(%g = nl.import_graph gml("/data/c.gml") as "c" types {g = !storage.duration} : !nl.chunk<!storage.string>)"));
}
