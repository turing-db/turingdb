#include <gtest/gtest.h>

#include <string>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"
#include "llvm/Support/raw_ostream.h"

#include "DBDialect.h"
#include "StorageAttributes.h"
#include "StorageDialect.h"
#include "StorageTypes.h"

namespace {

class ParameterAttributeTest : public ::testing::Test {
protected:
    ParameterAttributeTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::db::DB>();
    }

    // Parses one db program and reports whether it verified
    bool parses(const char* programText) {
        const mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(programText,
                                                                                                mlir::ParserConfig(&_context));

        return static_cast<bool>(module);
    }

    // The text a program prints back as, empty when it does not parse
    std::string roundTrip(const char* programText) {
        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(programText,
                                                                                                mlir::ParserConfig(&_context));
        if (!module) {
            return {};
        }

        std::string printed;
        llvm::raw_string_ostream stream(printed);
        module->print(stream);

        return printed;
    }

    mlir::MLIRContext _context;
};

constexpr const char* PARAMETER_TEXT = "#storage.parameter<\"label\"> : !storage.label_id";

constexpr const char* attributeOnAFunction = R"mlir(
func.func @main() attributes {p = #storage.parameter<"label"> : !storage.label_id} {
  return
}
)mlir";

constexpr const char* parameterLabelScan = R"mlir(
func.func @main() {
  %0 = db.scan_nodes_by_label([#storage.parameter<"label"> : !storage.label_id]) : !db.column<!storage.node_id>
  return
}
)mlir";

constexpr const char* mixedLabelScan = R"mlir(
func.func @main() {
  %0 = db.scan_nodes_by_label(["Person", #storage.parameter<"extra"> : !storage.label_id]) : !db.column<!storage.node_id>
  return
}
)mlir";

constexpr const char* parameterInAnAlternative = R"mlir(
func.func @main() {
  %0 = db.scan_nodes() : !db.column<!storage.node_id>
  %1 = db.get_node_label_set(%0) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %2 = db.check_label_constraint(%1, [["Person", #storage.parameter<"extra"> : !storage.label_id], ["Interest"]]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  return
}
)mlir";

constexpr const char* integerLabelScan = R"mlir(
func.func @main() {
  %0 = db.scan_nodes_by_label([42 : i64]) : !db.column<!storage.node_id>
  return
}
)mlir";

constexpr const char* integerTypedParameterLabelScan = R"mlir(
func.func @main() {
  %0 = db.scan_nodes_by_label([#storage.parameter<"label"> : i64]) : !db.column<!storage.node_id>
  return
}
)mlir";

constexpr const char* untypedParameterLabelScan = R"mlir(
func.func @main() {
  %0 = db.scan_nodes_by_label([#storage.parameter<"label">]) : !db.column<!storage.node_id>
  return
}
)mlir";

constexpr const char* stringTypedParameterInAnAlternative = R"mlir(
func.func @main() {
  %0 = db.scan_nodes() : !db.column<!storage.node_id>
  %1 = db.get_node_label_set(%0) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %2 = db.check_label_constraint(%1, [["Person", #storage.parameter<"extra"> : !storage.string], ["Interest"]]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  return
}
)mlir";

}

TEST_F(ParameterAttributeTest, carriesANameAndAType) {
    const mlir::Type labelType = mlir::storage::LabelIDType::get(&_context);
    const mlir::storage::ParameterAttr parameter = mlir::storage::ParameterAttr::get(&_context, "label", labelType);

    EXPECT_EQ(parameter.getName(), "label");
    EXPECT_EQ(parameter.getType(), labelType);
    EXPECT_TRUE(mlir::isa<mlir::TypedAttr>(parameter));
}

TEST_F(ParameterAttributeTest, isUniquedByNameAndType) {
    const mlir::Type labelType = mlir::storage::LabelIDType::get(&_context);
    const mlir::Type stringType = mlir::storage::StringType::get(&_context);

    const mlir::storage::ParameterAttr label = mlir::storage::ParameterAttr::get(&_context, "label", labelType);

    EXPECT_EQ(label, mlir::storage::ParameterAttr::get(&_context, "label", labelType));
    EXPECT_NE(label, mlir::storage::ParameterAttr::get(&_context, "other", labelType));
    EXPECT_NE(label, mlir::storage::ParameterAttr::get(&_context, "label", stringType));
}

TEST_F(ParameterAttributeTest, roundTripsThroughText) {
    const std::string printed = roundTrip(attributeOnAFunction);

    ASSERT_FALSE(printed.empty());
    EXPECT_NE(printed.find(PARAMETER_TEXT), std::string::npos) << printed;
}

TEST_F(ParameterAttributeTest, aLabelScanTakesAParameter) {
    EXPECT_TRUE(parses(parameterLabelScan));
}

TEST_F(ParameterAttributeTest, aLabelScanMixesNamesAndParameters) {
    const std::string printed = roundTrip(mixedLabelScan);

    ASSERT_FALSE(printed.empty());
    EXPECT_NE(printed.find("[\"Person\", #storage.parameter<\"extra\"> : !storage.label_id]"), std::string::npos) << printed;
}

TEST_F(ParameterAttributeTest, aLabelConstraintTakesAParameterInAnAlternative) {
    const std::string printed = roundTrip(parameterInAnAlternative);

    ASSERT_FALSE(printed.empty());
    EXPECT_NE(printed.find("[[\"Person\", #storage.parameter<\"extra\"> : !storage.label_id], [\"Interest\"]]"), std::string::npos) << printed;
}

TEST_F(ParameterAttributeTest, aLabelScanRejectsAnInteger) {
    EXPECT_FALSE(parses(integerLabelScan));
}

TEST_F(ParameterAttributeTest, aLabelScanRejectsAParameterOfAnotherType) {
    EXPECT_FALSE(parses(integerTypedParameterLabelScan));
}

TEST_F(ParameterAttributeTest, aLabelScanRejectsAnUntypedParameter) {
    EXPECT_FALSE(parses(untypedParameterLabelScan));
}

TEST_F(ParameterAttributeTest, aLabelConstraintRejectsAParameterOfAnotherType) {
    EXPECT_FALSE(parses(stringTypedParameterInAnAlternative));
}
