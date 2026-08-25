#include <gtest/gtest.h>

#include <stddef.h>
#include <fstream>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "llvm/ADT/StringRef.h"
#include "llvm/Support/raw_ostream.h"
#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/Location.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OperationSupport.h"
#include "mlir/Parser/Parser.h"

#include "DBDialect.h"
#include "DBLowering.h"
#include "NLDialect.h"
#include "StorageDialect.h"

using namespace db;

namespace {

// The two system-command samples are a pair: the nl one states it is the DBLowering
// output of the db one, which nothing enforced - the samples regression parses each
// file alone - so the pair had drifted. Lowering the db sample here pins the whole
// family's db -> nl mapping, which one hand-written name table drives.
class SystemCommandLoweringTest : public ::testing::Test {
protected:
    SystemCommandLoweringTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

    void SetUp() override {
        std::string dbText;
        readSample("system_commands.mlir", dbText);

        std::string nlText;
        readSample("system_commands.nl.mlir", nlText);

        const mlir::ParserConfig parserConfig(&_context);
        _dbModule = mlir::parseSourceString<mlir::ModuleOp>(dbText, parserConfig);
        _nlModule = mlir::parseSourceString<mlir::ModuleOp>(nlText, parserConfig);
    }

    void readSample(std::string_view fileName, std::string& text) {
        const std::string path = std::string(MLIR_SAMPLES_DIR) + "/" + std::string(fileName);

        std::ifstream file(path);
        ASSERT_TRUE(file.is_open()) << "cannot open the sample '" << path << "'";

        std::stringstream buffer;
        buffer << file.rdbuf();
        text = buffer.str();
    }

    static void collectFunctionNames(mlir::ModuleOp module, std::vector<std::string>& names) {
        for (mlir::func::FuncOp function : module.getOps<mlir::func::FuncOp>()) {
            names.emplace_back(function.getSymName());
        }
    }

    static void printFunction(mlir::func::FuncOp function, std::string& text) {
        text.clear();

        llvm::raw_string_ostream stream(text);
        function->print(stream, mlir::OpPrintingFlags());
    }

    // The nl function the db one lowers to, printed. Each goes into a module of its
    // own so the printed SSA names start at %0 the way the sample spells them; a
    // system command reads no property, so the lowering has no schema to resolve one
    // against and needs no graph.
    void lowerFunction(mlir::func::FuncOp dbFunction, std::string& text) {
        mlir::OwningOpRef<mlir::ModuleOp> target
            = mlir::ModuleOp::create(mlir::UnknownLoc::get(&_context));

        DBLowering lowering(&_context, nullptr);

        printFunction(lowering.lower(dbFunction, *target), text);
    }

    mlir::MLIRContext _context;
    mlir::OwningOpRef<mlir::ModuleOp> _dbModule;
    mlir::OwningOpRef<mlir::ModuleOp> _nlModule;
};

}

TEST_F(SystemCommandLoweringTest, theSamplesHoldTheSameCommandsInTheSameOrder) {
    ASSERT_TRUE(_dbModule);
    ASSERT_TRUE(_nlModule);

    std::vector<std::string> dbNames;
    collectFunctionNames(*_dbModule, dbNames);

    std::vector<std::string> nlNames;
    collectFunctionNames(*_nlModule, nlNames);

    EXPECT_FALSE(dbNames.empty());
    EXPECT_EQ(dbNames, nlNames);
}

TEST_F(SystemCommandLoweringTest, everyCommandLowersToTheNlSample) {
    ASSERT_TRUE(_dbModule);
    ASSERT_TRUE(_nlModule);

    size_t checked = 0;
    std::string lowered;
    std::string expected;

    for (mlir::func::FuncOp dbFunction : _dbModule->getOps<mlir::func::FuncOp>()) {
        const llvm::StringRef name = dbFunction.getSymName();

        lowerFunction(dbFunction, lowered);

        const mlir::func::FuncOp nlFunction = _nlModule->lookupSymbol<mlir::func::FuncOp>(name);
        if (!nlFunction) {
            ADD_FAILURE() << "the nl sample has no @" << name.str() << ", which lowers to:\n"
                          << lowered;
            continue;
        }

        printFunction(nlFunction, expected);

        EXPECT_EQ(lowered, expected) << "@" << name.str() << " lowers to something else than the "
                                        "nl sample spells";
        checked++;
    }

    EXPECT_GT(checked, 0u);
}
