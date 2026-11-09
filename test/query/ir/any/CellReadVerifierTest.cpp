#include <gtest/gtest.h>

#include <array>
#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/Diagnostics.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/Parser/Parser.h"

#include "NLDialect.h"
#include "StorageDialect.h"

namespace {

constexpr std::array<std::string_view, 12> PLAIN_SCALARS {
    "i64",
    "ui64",
    "f64",
    "i1",
    "!storage.bool",
    "!storage.string",
    "!storage.datetime",
    "!storage.duration",
    "!storage.label_id",
    "!storage.edge_type_id",
    "!storage.property_type_id",
    "!storage.value_type",
};

constexpr std::array<std::string_view, 9> READ_AS_CELLS {
    "!storage.nullable<i64>",
    "!storage.nullable<!storage.label_id>",
    "!storage.nullable<none>",
    "!storage.node_id",
    "!storage.edge_id",
    "!storage.list<i64>",
    "!storage.map",
    "!storage.any",
    "!storage.nullable<!storage.any>",
};

std::string substituted(std::string_view text, std::string_view placeholder, std::string_view element) {
    std::string result(text);

    for (size_t position = result.find(placeholder); position != std::string::npos; position = result.find(placeholder, position)) {
        result.replace(position, placeholder.size(), element);
        position += element.size();
    }

    return result;
}

constexpr std::string_view TO_ANY = R"mlir(
func.func @main(%v: !nl.chunk<$V>) {
  %r = nl.to_any %v : (!nl.chunk<$V>) -> !nl.chunk<!storage.any>
  return
}
)mlir";

constexpr std::string_view CASE = R"mlir(
func.func @main(%c: !nl.chunk<i1>, %v: !nl.chunk<$V>, %d: !nl.chunk<$D>) {
  %r = nl.case({%c}, {%v}, %d) : (!nl.chunk<i1>, !nl.chunk<$V>, !nl.chunk<$D>) -> !nl.chunk<$R>
  return
}
)mlir";

constexpr std::string_view CASE_MERGE = R"mlir(
func.func @main(%c: !nl.chunk<i1>, %v: !nl.chunk<$V>, %d: !nl.chunk<$D>) {
  %r = nl.case_merge({%c}, {%v}, %d) : (!nl.chunk<i1>, !nl.chunk<$V>, !nl.chunk<$D>) -> !nl.chunk<$R>
  return
}
)mlir";

constexpr std::string_view REDUCE = R"mlir(
func.func @main(%xs: !nl.chunk<!storage.list<i64>>, %init: !nl.chunk<$I>, %v: !nl.chunk<$V>) {
  %r = nl.reduce(%xs, %init, {%v}) {
  ^bb0(%x: !nl.chunk<!storage.nullable<i64>>, %acc: !nl.chunk<$R>, %vIn: !nl.chunk<$V>):
    nl.reduce_yield %vIn : !nl.chunk<$V>
  } : (!nl.chunk<!storage.list<i64>>, !nl.chunk<$I>, !nl.chunk<$V>) -> !nl.chunk<$R>
  return
}
)mlir";

constexpr std::string_view ALL_REDUCE = R"mlir(
func.func @main(%xs: !nl.chunk<!storage.list<i64>>, %init: !nl.chunk<$I>, %v: !nl.chunk<$V>,
                %h: !nl.chunk<!storage.nullable<i1>>) {
  %r = nl.all_reduce(%xs, %init, {%v, %h}) {
  ^bb0(%x: !nl.chunk<!storage.nullable<i64>>, %acc: !nl.chunk<$R>, %vIn: !nl.chunk<$V>,
       %hIn: !nl.chunk<!storage.nullable<i1>>):
    nl.reduce_yield %vIn : !nl.chunk<$V>
  } predicate {
  ^bb0(%x: !nl.chunk<!storage.nullable<i64>>, %acc: !nl.chunk<$R>, %vIn: !nl.chunk<$V>,
       %hIn: !nl.chunk<!storage.nullable<i1>>):
    nl.all_reduce_yield %hIn : !nl.chunk<!storage.nullable<i1>>
  } : (!nl.chunk<!storage.list<i64>>, !nl.chunk<$I>, !nl.chunk<$V>, !nl.chunk<!storage.nullable<i1>>)
    -> !nl.chunk<!storage.nullable<i1>>
  return
}
)mlir";

constexpr std::string_view MAKE_LIST = R"mlir(
func.func @main(%v: !nl.chunk<$V>) {
  %r = nl.make_list(%v) : (!nl.chunk<$V>) -> !nl.chunk<!storage.list<!storage.any>>
  return
}
)mlir";

constexpr std::string_view MAKE_MAP = R"mlir(
func.func @main(%v: !nl.chunk<$V>) {
  %r = nl.make_map(%v) keys ["a"] : !nl.chunk<$V>
  return
}
)mlir";

constexpr std::string_view LIST_COMPREHENSION = R"mlir(
func.func @main(%xs: !nl.chunk<!storage.list<i64>>, %v: !nl.chunk<$V>) {
  %r = nl.list_comprehension(%xs, {%v}) {
  ^bb0(%x: !nl.chunk<!storage.nullable<i64>>, %tag: !nl.chunk<ui64>, %vIn: !nl.chunk<$V>):
    nl.comprehension_yield %tag, %vIn : !nl.chunk<$V>
  } : (!nl.chunk<!storage.list<i64>>, !nl.chunk<$V>) -> !nl.chunk<!storage.nullable<!storage.list<!storage.any>>>
  return
}
)mlir";

constexpr std::string_view ROW_LISTS_COLLECT = R"mlir(
func.func @main(%v: !nl.chunk<$V>) {
  %state, %tag = nl.row_lists_buffer()
  nl.row_lists_collect %state, %v : !nl.chunk<$V>
  return
}
)mlir";

constexpr std::array<std::string_view, 4> READS_A_CELL_FROM_EVERY_OPERAND {
    MAKE_LIST,
    MAKE_MAP,
    LIST_COMPREHENSION,
    ROW_LISTS_COLLECT,
};

}

// An op that reads a tagged cell out of a chunk reads it out of an entity, a path, a list, a
// map, a tagged cell or a nullable value: the executor has no read of a cell out of a plain
// scalar chunk, so lowering reads that through a nullable first, and the verifier turns away
// an op that does not.
class CellReadVerifierTest : public ::testing::Test {
protected:
    CellReadVerifierTest() {
        _context.getOrLoadDialect<mlir::func::FuncDialect>();
        _context.getOrLoadDialect<mlir::storage::Storage>();
        _context.getOrLoadDialect<mlir::nl::NL>();
    }

    bool verifies(const std::string& program) {
        const mlir::ScopedDiagnosticHandler handler(&_context, [](mlir::Diagnostic&) {
            return mlir::success();
        });

        const mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(program, mlir::ParserConfig(&_context));

        return static_cast<bool>(module);
    }

    bool verifiesWith(std::string_view program, std::string_view value) {
        return verifies(substituted(program, "$V", value));
    }

    bool caseVerifiesWith(std::string_view program, std::string_view value, std::string_view defaultValue, std::string_view result) {
        const std::string valued = substituted(program, "$V", value);
        const std::string defaulted = substituted(valued, "$D", defaultValue);

        return verifies(substituted(defaulted, "$R", result));
    }

    bool foldVerifiesWith(std::string_view program, std::string_view initial, std::string_view value, std::string_view accumulator) {
        const std::string initialized = substituted(program, "$I", initial);
        const std::string valued = substituted(initialized, "$V", value);

        return verifies(substituted(valued, "$R", accumulator));
    }

    mlir::MLIRContext _context;
};

TEST_F(CellReadVerifierTest, toAnyReadsACellChunk) {
    for (const std::string_view element : READ_AS_CELLS) {
        EXPECT_TRUE(verifiesWith(TO_ANY, element)) << element;
    }

    for (const std::string_view element : PLAIN_SCALARS) {
        EXPECT_FALSE(verifiesWith(TO_ANY, element)) << element;
    }
}

TEST_F(CellReadVerifierTest, caseWritesTaggedCellsFromCellChunks) {
    constexpr std::string_view nullableString = "!storage.nullable<!storage.string>";

    for (const std::string_view program : {CASE, CASE_MERGE}) {
        for (const std::string_view element : READ_AS_CELLS) {
            EXPECT_TRUE(caseVerifiesWith(program, element, nullableString, "!storage.any")) << program << element;
            EXPECT_TRUE(caseVerifiesWith(program, nullableString, element, "!storage.any")) << program << element;
        }

        for (const std::string_view element : PLAIN_SCALARS) {
            EXPECT_FALSE(caseVerifiesWith(program, element, nullableString, "!storage.any")) << program << element;
            EXPECT_FALSE(caseVerifiesWith(program, nullableString, element, "!storage.any")) << program << element;
        }
    }
}

TEST_F(CellReadVerifierTest, caseWritesAValueColumnFromAPlainValue) {
    for (const std::string_view program : {CASE, CASE_MERGE}) {
        EXPECT_TRUE(caseVerifiesWith(program, "i64", "!storage.nullable<i64>", "!storage.nullable<i64>")) << program;
        EXPECT_TRUE(caseVerifiesWith(program, "!storage.nullable<i64>", "i64", "!storage.nullable<i64>")) << program;
    }
}

TEST_F(CellReadVerifierTest, foldWritesATaggedCellAccumulatorFromCellChunks) {
    constexpr std::string_view nullableString = "!storage.nullable<!storage.string>";

    for (const std::string_view program : {REDUCE, ALL_REDUCE}) {
        for (const std::string_view element : READ_AS_CELLS) {
            EXPECT_TRUE(foldVerifiesWith(program, element, nullableString, "!storage.any")) << program << element;
            EXPECT_TRUE(foldVerifiesWith(program, nullableString, element, "!storage.any")) << program << element;
        }

        for (const std::string_view element : PLAIN_SCALARS) {
            EXPECT_FALSE(foldVerifiesWith(program, element, nullableString, "!storage.any")) << program << element;
            EXPECT_FALSE(foldVerifiesWith(program, nullableString, element, "!storage.any")) << program << element;
        }
    }
}

TEST_F(CellReadVerifierTest, foldWritesAValueAccumulatorFromAPlainValue) {
    for (const std::string_view program : {REDUCE, ALL_REDUCE}) {
        EXPECT_TRUE(foldVerifiesWith(program, "i64", "!storage.nullable<i64>", "!storage.nullable<i64>")) << program;
        EXPECT_TRUE(foldVerifiesWith(program, "!storage.nullable<i64>", "i64", "!storage.nullable<i64>")) << program;
    }
}

TEST_F(CellReadVerifierTest, containerBuildsReadCellChunks) {
    for (const std::string_view program : READS_A_CELL_FROM_EVERY_OPERAND) {
        for (const std::string_view element : READ_AS_CELLS) {
            EXPECT_TRUE(verifiesWith(program, element)) << program << element;
        }

        for (const std::string_view element : PLAIN_SCALARS) {
            EXPECT_FALSE(verifiesWith(program, element)) << program << element;
        }
    }
}
