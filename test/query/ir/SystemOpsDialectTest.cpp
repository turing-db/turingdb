#include <gtest/gtest.h>

#include <stdint.h>
#include <fstream>
#include <set>
#include <sstream>
#include <string>
#include <string_view>

#include "llvm/Support/raw_ostream.h"
#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/Diagnostics.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OperationSupport.h"
#include "mlir/Parser/Parser.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "NLDialect.h"
#include "StorageDialect.h"
#include "StorageEnums.h"

using namespace db;

namespace {

// The assembly of the system-command family: each op spells its parameters as
// attributes, several of them enum keywords, so the printer and the parser have to
// be inverses and every keyword has to be one the parser knows. Both samples spell
// the whole family, so round-tripping them covers all of it at once and a command
// added to the samples is covered without editing this file.
class SystemOpsDialectTest : public ::testing::Test {
protected:
    SystemOpsDialectTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

    void SetUp() override {
        std::string dbText;
        readSample("system_commands.mlir", dbText);

        _dbModule = parse(dbText);
    }

    void readSample(std::string_view fileName, std::string& text) {
        const std::string path = std::string(MLIR_SAMPLES_DIR) + "/" + std::string(fileName);

        std::ifstream file(path);
        ASSERT_TRUE(file.is_open()) << "cannot open the sample '" << path << "'";

        std::stringstream buffer;
        buffer << file.rdbuf();
        text = buffer.str();
    }

    // A null module on failure. The diagnostics are swallowed so the deliberately
    // malformed programs below do not print to the test log.
    mlir::OwningOpRef<mlir::ModuleOp> parse(std::string_view text) {
        const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
            return mlir::success();
        });

        return mlir::parseSourceString<mlir::ModuleOp>(text, mlir::ParserConfig(&_context));
    }

    static void printModule(mlir::ModuleOp module, std::string& text) {
        text.clear();

        llvm::raw_string_ostream stream(text);
        module->print(stream, mlir::OpPrintingFlags());
    }

    // Printing a sample and parsing the result back must land on the same text, so
    // the comparison starts from the printed form rather than the file's own
    // spelling, which wraps its longer lines by hand.
    void expectRoundTrip(std::string_view sampleName) {
        std::string text;
        readSample(sampleName, text);

        mlir::OwningOpRef<mlir::ModuleOp> module = parse(text);
        ASSERT_TRUE(module);

        std::string printed;
        printModule(*module, printed);

        mlir::OwningOpRef<mlir::ModuleOp> reparsed = parse(printed);
        ASSERT_TRUE(reparsed) << "the printed form does not parse back:\n" << printed;

        std::string reprinted;
        printModule(*reparsed, reprinted);

        EXPECT_EQ(printed, reprinted);
    }

    // The cases of one enum that the db sample actually spells, as their integer
    // values, gathered from every command carrying it.
    template <typename OpType, typename Reader>
    void collectSpelledCases(Reader reader, std::set<uint64_t>& cases) {
        _dbModule->walk([&](OpType op) {
            cases.insert(static_cast<uint64_t>(reader(op)));
        });
    }

    void expectEveryCaseSpelled(const std::set<uint64_t>& spelled, unsigned maxValue) {
        for (uint64_t value = 0; value <= maxValue; value++) {
            EXPECT_TRUE(spelled.contains(value))
                << "no command in the sample spells case " << value
                << ", so its keyword is never parsed and its lowering never checked";
        }
    }

    mlir::MLIRContext _context;
    mlir::OwningOpRef<mlir::ModuleOp> _dbModule;
};

}

TEST_F(SystemOpsDialectTest, theDbSampleRoundTripsThroughTextualForm) {
    expectRoundTrip("system_commands.mlir");
}

TEST_F(SystemOpsDialectTest, theNlSampleRoundTripsThroughTextualForm) {
    expectRoundTrip("system_commands.nl.mlir");
}

TEST_F(SystemOpsDialectTest, everyChangeOperationIsSpelled) {
    std::set<uint64_t> spelled;
    collectSpelledCases<mlir::db::ChangeCommand>(
        [](mlir::db::ChangeCommand op) { return op.getChangeOperation(); },
        spelled);

    expectEveryCaseSpelled(spelled, mlir::storage::getMaxEnumValForChangeOperation());
}

TEST_F(SystemOpsDialectTest, everyGraphImportFormatIsSpelled) {
    std::set<uint64_t> spelled;
    collectSpelledCases<mlir::db::ImportGraph>(
        [](mlir::db::ImportGraph op) { return op.getFormat(); },
        spelled);

    expectEveryCaseSpelled(spelled, mlir::storage::getMaxEnumValForGraphImportFormat());
}

TEST_F(SystemOpsDialectTest, everyS3TransferDirectionIsSpelled) {
    std::set<uint64_t> spelled;
    collectSpelledCases<mlir::db::S3Transfer>(
        [](mlir::db::S3Transfer op) { return op.getDirection(); },
        spelled);

    expectEveryCaseSpelled(spelled, mlir::storage::getMaxEnumValForS3TransferDirection());
}

TEST_F(SystemOpsDialectTest, everyVectorMetricIsSpelled) {
    std::set<uint64_t> spelled;
    collectSpelledCases<mlir::db::CreateVectorIndex>(
        [](mlir::db::CreateVectorIndex op) { return op.getMetric(); },
        spelled);

    expectEveryCaseSpelled(spelled, mlir::storage::getMaxEnumValForVectorMetric());
}

TEST_F(SystemOpsDialectTest, everyVectorIndexKindIsSpelled) {
    std::set<uint64_t> spelled;
    collectSpelledCases<mlir::db::CreateVectorIndex>(
        [](mlir::db::CreateVectorIndex op) { return op.getIndexKind(); },
        spelled);

    expectEveryCaseSpelled(spelled, mlir::storage::getMaxEnumValForVectorIndexKind());
}

TEST_F(SystemOpsDialectTest, everyIndexedEntityIsSpelled) {
    std::set<uint64_t> spelled;
    collectSpelledCases<mlir::db::CreatePropertyIndex>(
        [](mlir::db::CreatePropertyIndex op) { return op.getEntity(); },
        spelled);

    expectEveryCaseSpelled(spelled, mlir::storage::getMaxEnumValForIndexedEntity());
}

TEST_F(SystemOpsDialectTest, rejectsAnUnknownChangeOperation) {
    EXPECT_FALSE(parse(R"(
        module {
          func.func @main() {
            %ids = db.change rebase : !db.column<!storage.change_id>
            return
          }
        }
    )"));
}

TEST_F(SystemOpsDialectTest, rejectsAnUnknownImportFormat) {
    EXPECT_FALSE(parse(R"(
        module {
          func.func @main() {
            %g = db.import_graph avro("/data/social.avro") as "social" : !db.column<!storage.string>
            return
          }
        }
    )"));
}

TEST_F(SystemOpsDialectTest, rejectsAnUnknownS3TransferDirection) {
    EXPECT_FALSE(parse(R"(
        module {
          func.func @main() {
            db.s3_transfer sync("bucket", "graphs/", "", "graphs")
            return
          }
        }
    )"));
}

// The change IDs a change command reports are their own element type, not an
// integer and not a string, so a result column of anything else is rejected.
TEST_F(SystemOpsDialectTest, rejectsChangeResultsThatAreNotChangeIDs) {
    EXPECT_FALSE(parse(R"(
        module {
          func.func @main() {
            %ids = db.change new : !db.column<!storage.string>
            return
          }
        }
    )"));
}

TEST_F(SystemOpsDialectTest, rejectsACreateVectorIndexMissingItsIndexKind) {
    EXPECT_FALSE(parse(R"(
        module {
          func.func @main() {
            %i = db.create_vector_index("vectors", 4, euclidean) : !db.column<!storage.string>
            return
          }
        }
    )"));
}

// A dimension is a count, so it is unsigned end to end: a negative one fails to
// parse rather than reaching the interpreter as a huge size_t.
TEST_F(SystemOpsDialectTest, rejectsANegativeVectorIndexDimension) {
    EXPECT_FALSE(parse(R"(
        module {
          func.func @main() {
            %i = db.create_vector_index("vectors", -4, euclidean, flat) : !db.column<!storage.string>
            return
          }
        }
    )"));
}
