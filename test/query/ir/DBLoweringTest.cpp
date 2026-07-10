#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"

#include "Graph.h"
#include "JobSystem.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnMask.h"
#include "columns/ColumnOptMask.h"
#include "columns/ColumnOptVector.h"
#include "datapart/EdgeRecord.h"
#include "iterators/ChunkConfig.h"
#include "metadata/PropertyType.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"

#include "DBDialect.h"
#include "DBLowering.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "StorageDialect.h"
#include "NLInterpreter.h"
#include "NLOps.h"
#include "NLOutputSink.h"

#include "SimpleGraph.h"

#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

// Collects every output chunk into one accumulated value vector per column.
// All test programs output node ID chunks only.
class CollectingNodeSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        if (_columns.empty()) {
            _columns.resize(chunks.size());
        }

        ASSERT_EQ(chunks.size(), _columns.size());

        for (size_t columnIndex = 0; columnIndex < chunks.size(); columnIndex++) {
            const auto* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(chunks[columnIndex]);
            ASSERT_NE(nodeIDs, nullptr);

            // Only rows [offset, offset + rowCount) are part of the result: the
            // prefix before offset is what a SKIP dropped, the tail after is what
            // a LIMIT clamped off.
            for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
                _columns[columnIndex].push_back((*nodeIDs)[rowIndex].getValue());
            }
        }
    }

    // Fills rows zipped from the columns and sorted: chunk order depends on
    // datapart-major iteration, so tests compare order-independently
    void sortedRows(std::vector<std::vector<uint64_t>>& rows) const {
        orderedRows(rows);
        std::sort(rows.begin(), rows.end());
    }

    // Fills rows zipped from the columns in emission order, unsorted: a sort test
    // observes the order the program emitted, not a re-sorted copy.
    void orderedRows(std::vector<std::vector<uint64_t>>& rows) const {
        rows.clear();
        const size_t rowCount = _columns.empty() ? 0 : _columns.front().size();

        for (size_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            std::vector<uint64_t> row;
            for (const std::vector<uint64_t>& column : _columns) {
                row.push_back(column[rowIndex]);
            }
            rows.push_back(row);
        }
    }

private:
    std::vector<std::vector<uint64_t>> _columns;
};

// Collects (node ID, nullable int64 property) rows. Output programs that read
// an Int64 property emit a node ID chunk and a !storage.nullable<i64> value chunk.
class CollectingNodeIntPropSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(chunks[0]);
        const auto* values = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[1]);
        ASSERT_NE(nodeIDs, nullptr);
        ASSERT_NE(values, nullptr);
        ASSERT_EQ(nodeIDs->size(), values->size());

        const auto& idRaw = nodeIDs->getRaw();
        const auto& valueRaw = values->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.push_back({idRaw[rowIndex].getValue(), valueRaw[rowIndex]});
        }
    }

    void sortedRows(std::vector<std::pair<uint64_t, std::optional<int64_t>>>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

    // The rows in emission order, unsorted: a sort test observes the program's
    // own order rather than a re-sorted copy.
    void orderedRows(std::vector<std::pair<uint64_t, std::optional<int64_t>>>& rows) const {
        rows = _rows;
    }

private:
    std::vector<std::pair<uint64_t, std::optional<int64_t>>> _rows;
};

// Collects the single-row unsigned-i64 result a COUNT emits: one non-nullable
// !nl.chunk<ui64> with one row. Captures every value seen, so a test can assert
// both that exactly one row came out and what its tally is.
class CollectingCountSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* values = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[0]);
        ASSERT_NE(values, nullptr);

        const auto& raw = values->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _values.push_back(raw[rowIndex]);
        }
    }

    const std::vector<uint64_t>& getValues() const { return _values; }

private:
    std::vector<uint64_t> _values;
};

// Collects the single-row nullable int64 result a SUM/MIN/MAX over an Int64 column
// emits: one !nl.chunk<!storage.nullable<i64>> with one row (present or null).
class CollectingOptInt64Sink : public NLOutputSink {
public:
    using OptInt64Values = std::vector<std::optional<int64_t>>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* values = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[0]);
        ASSERT_NE(values, nullptr);

        const auto& raw = values->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _values.push_back(raw[rowIndex]);
        }
    }

    const OptInt64Values& getValues() const { return _values; }

private:
    OptInt64Values _values;
};

// Collects the single-row nullable double result an AVG emits: one
// !nl.chunk<!storage.nullable<f64>> with one row.
class CollectingOptDoubleSink : public NLOutputSink {
public:
    using OptDoubleValues = std::vector<std::optional<double>>;

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* values = dynamic_cast<const ColumnOptVector<double>*>(chunks[0]);
        ASSERT_NE(values, nullptr);

        const auto& raw = values->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _values.push_back(raw[rowIndex]);
        }
    }

    const OptDoubleValues& getValues() const { return _values; }

private:
    OptDoubleValues _values;
};

// Collects (node ID, nullable string property) rows. The value column is a
// !storage.nullable<!storage.string> chunk, storage's ColumnOptVector<string_view>.
class CollectingNodeStringPropSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(chunks[0]);
        const auto* values = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[1]);
        ASSERT_NE(nodeIDs, nullptr);
        ASSERT_NE(values, nullptr);
        ASSERT_EQ(nodeIDs->size(), values->size());

        const auto& idRaw = nodeIDs->getRaw();
        const auto& valueRaw = values->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            std::optional<std::string> value;
            if (valueRaw[rowIndex]) {
                value = std::string(*valueRaw[rowIndex]);
            }
            _rows.push_back({idRaw[rowIndex].getValue(), value});
        }
    }

    void sortedRows(std::vector<std::pair<uint64_t, std::optional<std::string>>>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<std::pair<uint64_t, std::optional<std::string>>> _rows;
};

// Collects (node ID, nullable embedding property) rows. The value column is a
// !storage.nullable<!storage.embedding> chunk, storage's ColumnOptVector<span<float>>;
// each span is copied out since it points into the live graph storage.
class CollectingNodeEmbeddingPropSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(chunks[0]);
        const auto* values = dynamic_cast<const ColumnOptVector<std::span<const float>>*>(chunks[1]);
        ASSERT_NE(nodeIDs, nullptr);
        ASSERT_NE(values, nullptr);
        ASSERT_EQ(nodeIDs->size(), values->size());

        const auto& idRaw = nodeIDs->getRaw();
        const auto& valueRaw = values->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            std::optional<std::vector<float>> value;
            if (valueRaw[rowIndex]) {
                value = std::vector<float>(valueRaw[rowIndex]->begin(), valueRaw[rowIndex]->end());
            }
            _rows.push_back({idRaw[rowIndex].getValue(), value});
        }
    }

    void sortedRows(std::vector<std::pair<uint64_t, std::optional<std::vector<float>>>>& rows) const {
        // Sort a size_t permutation rather than std::sort-ing the rows directly:
        // GCC 13's __insertion_sort emits a spurious -Wmaybe-uninitialized when it
        // moves a std::pair holding an std::optional<std::vector<float>>, and that
        // middle-end warning cannot be silenced by a diagnostic pragma (its location
        // is inside the std header). Ordering an index permutation never moves the
        // heavy element, sidestepping the false positive; the result is identical.
        std::vector<size_t> order(_rows.size());
        for (size_t index = 0; index < order.size(); index++) {
            order[index] = index;
        }

        std::sort(order.begin(), order.end(), [this](size_t left, size_t right) {
            return _rows[left] < _rows[right];
        });

        rows.clear();
        rows.reserve(_rows.size());
        for (const size_t index : order) {
            rows.push_back(_rows[index]);
        }
    }

private:
    std::vector<std::pair<uint64_t, std::optional<std::vector<float>>>> _rows;
};

// Collects (edge ID, nullable int64 property) rows. Programs that read an Int64
// edge property emit an edge ID chunk and a !storage.nullable<i64> value chunk.
class CollectingEdgeIntPropSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* edgeIDs = dynamic_cast<const ColumnEdgeIDs*>(chunks[0]);
        const auto* values = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[1]);
        ASSERT_NE(edgeIDs, nullptr);
        ASSERT_NE(values, nullptr);
        ASSERT_EQ(edgeIDs->size(), values->size());

        const auto& idRaw = edgeIDs->getRaw();
        const auto& valueRaw = values->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.push_back({idRaw[rowIndex].getValue(), valueRaw[rowIndex]});
        }
    }

    void sortedRows(std::vector<std::pair<uint64_t, std::optional<int64_t>>>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<std::pair<uint64_t, std::optional<int64_t>>> _rows;
};

// Collects constant result rows: each output column is a ColumnConst<T> - the
// single broadcast value a db.constant lowers to - so a constant program emits
// exactly one row, one value per column. Every column must be a ColumnConst<T>.
template <typename T>
class CollectingConstSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            std::vector<T> row;
            for (const Column* chunk : chunks) {
                const auto* values = dynamic_cast<const ColumnConst<T>*>(chunk);
                ASSERT_NE(values, nullptr);
                row.push_back((*values)[rowIndex]);
            }
            _rows.push_back(row);
        }
    }

    const std::vector<std::vector<T>>& rows() const { return _rows; }

private:
    std::vector<std::vector<T>> _rows;
};

// RETURN 30: one constant projected straight to output. It touches no graph -
// the constant is materialized at function scope and emitted as a single row.
constexpr const char* singleConstantProgram = R"mlir(
func.func @main() {
  %c = db.constant(30 : i64)
  db.output(%c) : !db.column<i64>
  return
}
)mlir";

// RETURN 10, 20, 30: several constants projected together as one row.
constexpr const char* multipleConstantsProgram = R"mlir(
func.func @main() {
  %a = db.constant(10 : i64)
  %b = db.constant(20 : i64)
  %c = db.constant(30 : i64)
  db.output(%a, %b, %c) : !db.column<i64>, !db.column<i64>, !db.column<i64>
  return
}
)mlir";

// RETURN 2.5: a single double constant, exercising the f64 value type.
constexpr const char* doubleConstantProgram = R"mlir(
func.func @main() {
  %c = db.constant(2.5 : f64)
  db.output(%c) : !db.column<f64>
  return
}
)mlir";

// Collects one row of the four supported constant value types, each a ColumnConst
// of its primitive, in the fixed projection order (Int64, UInt64, Double, Bool).
// String and embedding are not representable constants, so they are not covered.
class CollectingAllTypesConstSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 4u);
        ASSERT_EQ(rowCount, 1u);

        const auto* ints = dynamic_cast<const ColumnConst<int64_t>*>(chunks[0]);
        const auto* uints = dynamic_cast<const ColumnConst<uint64_t>*>(chunks[1]);
        const auto* doubles = dynamic_cast<const ColumnConst<double>*>(chunks[2]);
        const auto* bools = dynamic_cast<const ColumnConst<CustomBool>*>(chunks[3]);
        ASSERT_NE(ints, nullptr);
        ASSERT_NE(uints, nullptr);
        ASSERT_NE(doubles, nullptr);
        ASSERT_NE(bools, nullptr);

        _seen = true;
        _int = (*ints)[offset];
        _uint = (*uints)[offset];
        _double = (*doubles)[offset];
        _bool = static_cast<bool>((*bools)[offset]);
    }

    bool seen() const { return _seen; }
    int64_t getInt() const { return _int; }
    uint64_t getUint() const { return _uint; }
    double getDouble() const { return _double; }
    bool getBool() const { return _bool; }

private:
    bool _seen {false};
    int64_t _int {0};
    uint64_t _uint {0};
    double _double {0.0};
    bool _bool {false};
};

class CollectingMaskSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* mask = dynamic_cast<const ColumnMask*>(chunks[0]);
        ASSERT_NE(mask, nullptr);

        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _values.push_back((*mask)[rowIndex]);
        }
    }

    const std::vector<bool>& values() const { return _values; }

private:
    std::vector<bool> _values;
};

class CollectingNodeBoolSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(chunks[0]);
        const auto* values = dynamic_cast<const ColumnOptMask*>(chunks[1]);
        ASSERT_NE(nodeIDs, nullptr);
        ASSERT_NE(values, nullptr);
        ASSERT_EQ(nodeIDs->size(), values->size());

        const auto& idRaw = nodeIDs->getRaw();
        const auto& valueRaw = values->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            std::optional<bool> value;
            if (valueRaw[rowIndex].has_value()) {
                value = static_cast<bool>(*valueRaw[rowIndex]);
            }
            _rows.push_back({idRaw[rowIndex].getValue(), value});
        }
    }

    void sortedRows(std::vector<std::pair<uint64_t, std::optional<bool>>>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<std::pair<uint64_t, std::optional<bool>>> _rows;
};

// RETURN 30, 7, 2.5, true: one constant of each supported value type projected
// together as a single row.
constexpr const char* allTypesConstantsProgram = R"mlir(
func.func @main() {
  %i = db.constant(30 : i64)
  %u = db.constant(7 : ui64)
  %f = db.constant(2.5 : f64)
  %b = db.constant(true)
  db.output(%i, %u, %f, %b) : !db.column<i64>, !db.column<ui64>, !db.column<f64>, !db.column<i1>
  return
}
)mlir";

// RETURN 10 + 20: two constants added, a single-row result at function scope.
constexpr const char* addConstantsProgram = R"mlir(
func.func @main() {
  %x = db.constant(10 : i64)
  %y = db.constant(20 : i64)
  %s = db.add %x, %y : (!db.column<i64>, !db.column<i64>) -> !db.column<i64>
  db.output(%s) : !db.column<i64>
  return
}
)mlir";

// RETURN 10 + 2.5: a mixed-type add promoting to a double result.
constexpr const char* addPromotesProgram = R"mlir(
func.func @main() {
  %x = db.constant(10 : i64)
  %y = db.constant(2.5 : f64)
  %s = db.add %x, %y : (!db.column<i64>, !db.column<f64>) -> !db.column<f64>
  db.output(%s) : !db.column<f64>
  return
}
)mlir";

// MATCH (a) RETURN a, a.score + 10 to ensure nulls are propagated
constexpr const char* addPropertyConstantProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>
  %k = db.constant(10 : i64)
  %sum = db.add %score, %k : (!db.column<none>, !db.column<i64>) -> !db.column<none>
  db.output(%a, %sum) : !db.column<!storage.node_id>, !db.column<none>
  return
}
)mlir";

// MATCH (a) RETURN a, a.score + a.score
constexpr const char* addTwoPropertiesProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>
  %score2 = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>
  %sum = db.add %score, %score2 : (!db.column<none>, !db.column<none>) -> !db.column<none>
  db.output(%a, %sum) : !db.column<!storage.node_id>, !db.column<none>
  return
}
)mlir";

// RETURN 30 - 20: two constants subtracted, a single-row result at function scope.
constexpr const char* subConstantsProgram = R"mlir(
func.func @main() {
  %x = db.constant(30 : i64)
  %y = db.constant(20 : i64)
  %s = db.sub %x, %y : (!db.column<i64>, !db.column<i64>) -> !db.column<i64>
  db.output(%s) : !db.column<i64>
  return
}
)mlir";

// RETURN 10 - 2.5: a mixed-type sub promoting to a double result.
constexpr const char* subPromotesProgram = R"mlir(
func.func @main() {
  %x = db.constant(10 : i64)
  %y = db.constant(2.5 : f64)
  %s = db.sub %x, %y : (!db.column<i64>, !db.column<f64>) -> !db.column<f64>
  db.output(%s) : !db.column<f64>
  return
}
)mlir";

// MATCH (a) RETURN a, a.score - 10 to ensure nulls are propagated
constexpr const char* subPropertyConstantProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>
  %k = db.constant(10 : i64)
  %diff = db.sub %score, %k : (!db.column<none>, !db.column<i64>) -> !db.column<none>
  db.output(%a, %diff) : !db.column<!storage.node_id>, !db.column<none>
  return
}
)mlir";

// MATCH (a) RETURN a, a.score - a.score
constexpr const char* subTwoPropertiesProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>
  %score2 = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>
  %diff = db.sub %score, %score2 : (!db.column<none>, !db.column<none>) -> !db.column<none>
  db.output(%a, %diff) : !db.column<!storage.node_id>, !db.column<none>
  return
}
)mlir";

// RETURN 30 * 20: two constants multiplied, a single-row result at function scope.
constexpr const char* mulConstantsProgram = R"mlir(
func.func @main() {
  %x = db.constant(30 : i64)
  %y = db.constant(20 : i64)
  %s = db.mul %x, %y : (!db.column<i64>, !db.column<i64>) -> !db.column<i64>
  db.output(%s) : !db.column<i64>
  return
}
)mlir";

// RETURN 10 * 2.5: a mixed-type mul promoting to a double result.
constexpr const char* mulPromotesProgram = R"mlir(
func.func @main() {
  %x = db.constant(10 : i64)
  %y = db.constant(2.5 : f64)
  %s = db.mul %x, %y : (!db.column<i64>, !db.column<f64>) -> !db.column<f64>
  db.output(%s) : !db.column<f64>
  return
}
)mlir";

// MATCH (a) RETURN a, a.score * 10 to ensure nulls are propagated
constexpr const char* mulPropertyConstantProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>
  %k = db.constant(10 : i64)
  %prod = db.mul %score, %k : (!db.column<none>, !db.column<i64>) -> !db.column<none>
  db.output(%a, %prod) : !db.column<!storage.node_id>, !db.column<none>
  return
}
)mlir";

// MATCH (a) RETURN a, a.score * a.score
constexpr const char* mulTwoPropertiesProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>
  %score2 = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>
  %prod = db.mul %score, %score2 : (!db.column<none>, !db.column<none>) -> !db.column<none>
  db.output(%a, %prod) : !db.column<!storage.node_id>, !db.column<none>
  return
}
)mlir";

// RETURN 10 = 20
constexpr const char* eqConstantsFalseProgram = R"mlir(
func.func @main() {
  %x = db.constant(10 : i64)
  %y = db.constant(20 : i64)
  %r = db.eq %x, %y : (!db.column<i64>, !db.column<i64>) -> !db.column<!storage.bool>
  db.output(%r) : !db.column<!storage.bool>
  return
}
)mlir";

// RETURN 10 = 10
constexpr const char* eqConstantsTrueProgram = R"mlir(
func.func @main() {
  %x = db.constant(10 : i64)
  %y = db.constant(10 : i64)
  %r = db.eq %x, %y : (!db.column<i64>, !db.column<i64>) -> !db.column<!storage.bool>
  db.output(%r) : !db.column<!storage.bool>
  return
}
)mlir";

// MATCH (a) RETURN a = a
constexpr const char* eqSelfProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %r = db.eq %a, %a : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> !db.column<!storage.bool>
  db.output(%r) : !db.column<!storage.bool>
  return
}
)mlir";

// MATCH (a) RETURN a, a.score = 200
constexpr const char* eqPropertyConstantProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>
  %k = db.constant(200 : i64)
  %r = db.eq %score, %k : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
  db.output(%a, %r) : !db.column<!storage.node_id>, !db.column<!storage.bool>
  return
}
)mlir";

// RETURN (10 = 10) AND (10 = 20)
constexpr const char* andConstantsFalseProgram = R"mlir(
func.func @main() {
  %x = db.constant(10 : i64)
  %y = db.constant(20 : i64)
  %t = db.eq %x, %x : (!db.column<i64>, !db.column<i64>) -> !db.column<!storage.bool>
  %f = db.eq %x, %y : (!db.column<i64>, !db.column<i64>) -> !db.column<!storage.bool>
  %r = db.and %t, %f : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  db.output(%r) : !db.column<!storage.bool>
  return
}
)mlir";

// RETURN (10 = 10) AND (20 = 20)
constexpr const char* andConstantsTrueProgram = R"mlir(
func.func @main() {
  %x = db.constant(10 : i64)
  %y = db.constant(20 : i64)
  %t1 = db.eq %x, %x : (!db.column<i64>, !db.column<i64>) -> !db.column<!storage.bool>
  %t2 = db.eq %y, %y : (!db.column<i64>, !db.column<i64>) -> !db.column<!storage.bool>
  %r = db.and %t1, %t2 : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  db.output(%r) : !db.column<!storage.bool>
  return
}
)mlir";

// MATCH (a) RETURN a, (a.score = 200) AND (a.score = 200)
constexpr const char* andPropertySelfProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>
  %k = db.constant(200 : i64)
  %p = db.eq %score, %k : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
  %r = db.and %p, %p : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  db.output(%a, %r) : !db.column<!storage.node_id>, !db.column<!storage.bool>
  return
}
)mlir";

// MATCH (a) RETURN a, (a.score = 200) AND (a = 1)
constexpr const char* andNullShortCircuitProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>
  %k200 = db.constant(200 : i64)
  %p = db.eq %score, %k200 : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
  %k1 = db.constant(1 : i64)
  %q = db.eq %a, %k1 : (!db.column<!storage.node_id>, !db.column<i64>) -> !db.column<!storage.bool>
  %r = db.and %p, %q : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  db.output(%a, %r) : !db.column<!storage.node_id>, !db.column<!storage.bool>
  return
}
)mlir";

// RETURN (10 = 10) OR (10 = 20)
constexpr const char* orConstantsTrueProgram = R"mlir(
func.func @main() {
  %x = db.constant(10 : i64)
  %y = db.constant(20 : i64)
  %t = db.eq %x, %x : (!db.column<i64>, !db.column<i64>) -> !db.column<!storage.bool>
  %f = db.eq %x, %y : (!db.column<i64>, !db.column<i64>) -> !db.column<!storage.bool>
  %r = db.or %t, %f : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  db.output(%r) : !db.column<!storage.bool>
  return
}
)mlir";

// RETURN (10 = 20) OR (20 = 10)
constexpr const char* orConstantsFalseProgram = R"mlir(
func.func @main() {
  %x = db.constant(10 : i64)
  %y = db.constant(20 : i64)
  %f1 = db.eq %x, %y : (!db.column<i64>, !db.column<i64>) -> !db.column<!storage.bool>
  %f2 = db.eq %y, %x : (!db.column<i64>, !db.column<i64>) -> !db.column<!storage.bool>
  %r = db.or %f1, %f2 : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  db.output(%r) : !db.column<!storage.bool>
  return
}
)mlir";

// MATCH (a) RETURN a, (a.score = 200) OR (a.score = 200)
constexpr const char* orPropertySelfProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>
  %k = db.constant(200 : i64)
  %p = db.eq %score, %k : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
  %r = db.or %p, %p : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  db.output(%a, %r) : !db.column<!storage.node_id>, !db.column<!storage.bool>
  return
}
)mlir";

// MATCH (a) RETURN a, (a.score = 200) OR (a = 2)
constexpr const char* orNullShortCircuitProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>
  %k200 = db.constant(200 : i64)
  %p = db.eq %score, %k200 : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
  %k2 = db.constant(2 : i64)
  %q = db.eq %a, %k2 : (!db.column<!storage.node_id>, !db.column<i64>) -> !db.column<!storage.bool>
  %r = db.or %p, %q : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  db.output(%a, %r) : !db.column<!storage.node_id>, !db.column<!storage.bool>
  return
}
)mlir";

// Scan all nodes and output them
constexpr const char* scanProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  db.output(%a) : !db.column<!storage.node_id>
  return
}
)mlir";

// Scan only the nodes carrying the "Person" label
constexpr const char* scanByLabelPersonProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  db.output(%a) : !db.column<!storage.node_id>
  return
}
)mlir";

// Scan only the nodes carrying the "City" label
constexpr const char* scanByLabelCityProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["City"]) : !db.column<!storage.node_id>
  db.output(%a) : !db.column<!storage.node_id>
  return
}
)mlir";

// Scan only the nodes carrying both the "Person" and "City" labels
constexpr const char* scanByLabelPersonAndCityProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Person", "City"]) : !db.column<!storage.node_id>
  db.output(%a) : !db.column<!storage.node_id>
  return
}
)mlir";

// Scan by a label no node was ever created with; the result is empty
constexpr const char* scanByLabelUnknownProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes_by_label(["Robot"]) : !db.column<!storage.node_id>
  db.output(%a) : !db.column<!storage.node_id>
  return
}
)mlir";

// One hop along out-edges, outputting (source, target) pairs
constexpr const char* oneHopOutProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %srcs, %eids, %etypes, %b = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%srcs, %b) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// One hop along in-edges, outputting (source, target) pairs. The writer fills
// the source side and the target side is gathered from the input, so the
// output pairs are the same edge set as the out-edges hop
constexpr const char* oneHopInProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %srcs, %eids, %etypes, %b = db.get_in_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%srcs, %b) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// Two hops a->b->c carrying a through the second hop, outputting (a, c) pairs
constexpr const char* twoHopProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %a1, %e0, %et0, %b = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %b2, %e1, %et1, %c, %a2 = db.get_out_edges(%b, {%a1}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%a2, %c) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// The in-edges mirror of twoHopProgram: two predecessor hops a<-b<-c carrying a
// through the second hop, outputting (a, c) pairs. Each hop's source side (the
// predecessor it discovers) feeds the next hop, while the input side (b, then
// the carried a) is gathered along, exercising the carry set through in-edges.
constexpr const char* twoHopInProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %b, %e0, %et0, %a1 = db.get_in_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %c, %e1, %et1, %b2, %a2 = db.get_in_edges(%b, {%a1}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%a2, %c) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// Scan all nodes and output each node with its "score" property, which some
// nodes lack (those come back null, none are dropped)
constexpr const char* nodePropertiesProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%a, %score) : !db.column<!storage.node_id>, !db.column<none>
  return
}
)mlir";

// Read the string "name" property of each node
constexpr const char* nodeNameProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %name = db.get_node_properties(%a, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%a, %name) : !db.column<!storage.node_id>, !db.column<none>
  return
}
)mlir";

// Read the embedding "vec" property of each node
constexpr const char* nodeVecProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %vec = db.get_node_properties(%a, "vec") : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%a, %vec) : !db.column<!storage.node_id>, !db.column<none>
  return
}
)mlir";

// Walk every out-edge and read its "weight" property, which some edges lack
// (those come back null). Exercises the edge side of the property fetch end to
// end: the db op, its lowering, the edge branch of translation and the EdgeID
// executor handler.
constexpr const char* edgePropertiesProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %srcs, %eids, %etypes, %b = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %weight = db.get_edge_properties(%eids, "weight") : (!db.column<!storage.edge_id>) -> !db.column<none>
  db.output(%eids, %weight) : !db.column<!storage.edge_id>, !db.column<none>
  return
}
)mlir";

// MATCH (a), (b) RETURN a, b: two disconnected scans crossed, |nodes|^2 rows
constexpr const char* crossProductScansProgram = R"mlir(
func.func @main() {
  %0:2 = db.cross_product factor {
    %a = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield %a : !db.column<!storage.node_id>
  } factor {
    %b = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield %b : !db.column<!storage.node_id>
  }
  db.output(%0#0, %0#1) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a)->(b), (c)->(d) RETURN a, b, c, d: each factor walks one hop and
// yields the (source, target) of its edge; the product crosses every edge of
// one factor with every edge of the other, |edges|^2 rows
constexpr const char* crossProductHopsProgram = R"mlir(
func.func @main() {
  %0:4 = db.cross_product factor {
    %a = db.scan_nodes() : !db.column<!storage.node_id>
    %asrc, %ae, %aet, %b = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
    db.yield %asrc, %b : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  } factor {
    %c = db.scan_nodes() : !db.column<!storage.node_id>
    %csrc, %ce, %cet, %d = db.get_out_edges(%c, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
    db.yield %csrc, %d : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  }
  db.output(%0#0, %0#1, %0#2, %0#3) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a)->(b)->(c), (d) RETURN a, b, c, d: an asymmetric product. The left
// factor is a two-hop traversal carrying `a` through the second hop (a 3-deep
// loop nest yielding three row-aligned columns), the right factor a bare scan
// (1-deep). The cross sits at the deepest point of the left nest, so this
// exercises rooting a shallow factor inside a deep one and crossing a carried
// column. Result: every two-hop path (a, b, c) crossed with every node d.
constexpr const char* crossProductTwoHopAndScanProgram = R"mlir(
func.func @main() {
  %0:4 = db.cross_product factor {
    %a = db.scan_nodes() : !db.column<!storage.node_id>
    %a1, %e0, %et0, %b = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
    %b2, %e1, %et1, %c, %a2 = db.get_out_edges(%b, {%a1}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
    db.yield %a2, %b2, %c : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>
  } factor {
    %d = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield %d : !db.column<!storage.node_id>
  }
  db.output(%0#0, %0#1, %0#2, %0#3) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a), (b) RETURN a, b.score: the inner factor fetches a node property
// inside the factor, so the crossed value column is a nullable value chunk -
// the outer node IDs are block-repeated and the inner scores tiled together
constexpr const char* crossProductNodePropertyProgram = R"mlir(
func.func @main() {
  %0:3 = db.cross_product factor {
    %a = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield %a : !db.column<!storage.node_id>
  } factor {
    %b = db.scan_nodes() : !db.column<!storage.node_id>
    %score = db.get_node_properties(%b, "score") : (!db.column<!storage.node_id>) -> !db.column<none>
    db.yield %b, %score : !db.column<!storage.node_id>, !db.column<none>
  }
  db.output(%0#0, %0#2) : !db.column<!storage.node_id>, !db.column<none>
  return
}
)mlir";

// MATCH (a), (b), (c) RETURN a, b, c: a three-way product. The db dialect has no
// n-ary cross_product, so the third factor is expressed by nesting - the outer
// product's left factor is itself a db.cross_product crossing (a) with (b), and
// the outer product crosses that pair with (c). Lowering nests the inner
// product's loops inside the outer product's, so the result is every (a, b, c)
// triple over the node set, |nodes|^3 rows.
constexpr const char* nestedCrossProductScansProgram = R"mlir(
func.func @main() {
  %0:3 = db.cross_product factor {
    %1:2 = db.cross_product factor {
      %a = db.scan_nodes() : !db.column<!storage.node_id>
      db.yield %a : !db.column<!storage.node_id>
    } factor {
      %b = db.scan_nodes() : !db.column<!storage.node_id>
      db.yield %b : !db.column<!storage.node_id>
    }
    db.yield %1#0, %1#1 : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  } factor {
    %c = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield %c : !db.column<!storage.node_id>
  }
  db.output(%0#0, %0#1, %0#2) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// Counts appendChunks calls and the total rows emitted, without materializing
// them. Used to prove a limited run emits a clamped prefix (a partial final
// chunk) and stops early - fewer calls than the unlimited run. Also tracks the
// widest chunk handed over - the materialized column size before the rowCount
// clamp - to prove a limited cross product caps the product it builds.
class CountingSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        _calls++;
        _totalRows += rowCount;

        if (!chunks.empty()) {
            _widestChunk = std::max(_widestChunk, chunks.front()->size());
        }
    }

    size_t getCalls() const { return _calls; }
    size_t getTotalRows() const { return _totalRows; }
    size_t getWidestChunk() const { return _widestChunk; }

private:
    size_t _calls {0};
    size_t _totalRows {0};
    size_t _widestChunk {0};
};

// MATCH (a) RETURN a LIMIT count: a scan capped by db.limit.
std::string limitScanProgram(uint64_t count) {
    return std::string("func.func @main() {\n"
                       "  %a = db.scan_nodes() : !db.column<!storage.node_id>\n"
                       "  %la = db.limit(%a) count ")
           + std::to_string(count)
           + " : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>\n"
             "  db.output(%la) : !db.column<!storage.node_id>\n"
             "  return\n"
             "}\n";
}

// MATCH (a)->(b)->(c) RETURN c LIMIT count: a two-hop traversal (a three-deep
// loop nest) capped by db.limit, so the break must unwind the whole nest.
std::string limitTwoHopProgram(uint64_t count) {
    return std::string("func.func @main() {\n"
                       "  %a = db.scan_nodes() : !db.column<!storage.node_id>\n"
                       "  %a1, %e0, %et0, %b = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)\n"
                       "  %b2, %e1, %et1, %c, %a2 = db.get_out_edges(%b, {%a1}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)\n"
                       "  %lc = db.limit(%c) count ")
           + std::to_string(count)
           + " : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>\n"
             "  db.output(%lc) : !db.column<!storage.node_id>\n"
             "  return\n"
             "}\n";
}

// MATCH (a) RETURN a, a.score LIMIT count: a property fetch result is a trailing
// output column, so the prefix emit must clamp the node IDs and the nullable
// value column together. The representative is the first column, the node IDs.
std::string limitNodePropertyProgram(uint64_t count) {
    return std::string("func.func @main() {\n"
                       "  %a = db.scan_nodes() : !db.column<!storage.node_id>\n"
                       "  %score = db.get_node_properties(%a, \"score\") : (!db.column<!storage.node_id>) -> !db.column<none>\n"
                       "  %la, %lscore = db.limit(%a, %score) count ")
           + std::to_string(count)
           + " : (!db.column<!storage.node_id>, !db.column<none>) -> (!db.column<!storage.node_id>, !db.column<none>)\n"
             "  db.output(%la, %lscore) : !db.column<!storage.node_id>, !db.column<none>\n"
             "  return\n"
             "}\n";
}

// MATCH (a) RETURN a.score LIMIT count: the property fetch result is the only
// output column, so it is also db.limit's representative - an op result produced
// in the loop body, not a loop variable, exercising that ownerBlock path.
std::string limitOnlyPropertyProgram(uint64_t count) {
    return std::string("func.func @main() {\n"
                       "  %a = db.scan_nodes() : !db.column<!storage.node_id>\n"
                       "  %score = db.get_node_properties(%a, \"score\") : (!db.column<!storage.node_id>) -> !db.column<none>\n"
                       "  %lscore = db.limit(%score) count ")
           + std::to_string(count)
           + " : (!db.column<none>) -> !db.column<none>\n"
             "  db.output(%lscore) : !db.column<none>\n"
             "  return\n"
             "}\n";
}

// MATCH (a), (b) RETURN a, b LIMIT count: a cross product capped by db.limit.
// The representative is the post-cross-product column, so the count is right.
std::string limitCrossProductProgram(uint64_t count) {
    return std::string("func.func @main() {\n"
                       "  %0:2 = db.cross_product factor {\n"
                       "    %a = db.scan_nodes() : !db.column<!storage.node_id>\n"
                       "    db.yield %a : !db.column<!storage.node_id>\n"
                       "  } factor {\n"
                       "    %b = db.scan_nodes() : !db.column<!storage.node_id>\n"
                       "    db.yield %b : !db.column<!storage.node_id>\n"
                       "  }\n"
                       "  %la, %lb = db.limit(%0#0, %0#1) count ")
           + std::to_string(count)
           + " : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)\n"
             "  db.output(%la, %lb) : !db.column<!storage.node_id>, !db.column<!storage.node_id>\n"
             "  return\n"
             "}\n";
}

// MATCH (a) WITH a LIMIT count MATCH (a)-->(b) RETURN b: a mid-query (chained)
// limit. db.limit feeds db.get_out_edges, not db.output, so the limit bounds the
// intermediate `a` cardinality while `b` fans out unbounded. Only the scan (a's
// producer) carries the handle; the edge loop is a consumer.
std::string chainedLimitProgram(uint64_t count) {
    return std::string("func.func @main() {\n"
                       "  %a = db.scan_nodes() : !db.column<!storage.node_id>\n"
                       "  %la = db.limit(%a) count ")
           + std::to_string(count)
           + " : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>\n"
             "  %a1, %e0, %et0, %b = db.get_out_edges(%la, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)\n"
             "  db.output(%b) : !db.column<!storage.node_id>\n"
             "  return\n"
             "}\n";
}

// MATCH (a) WITH a LIMIT outer MATCH (a)-->(b) WITH b LIMIT inner RETURN b: two
// independent limits. The first bounds the scan, the second bounds the expansion;
// each gets its own handle, and the shared scan loop is claimed by the outer one.
std::string twoLimitProgram(uint64_t outerCount, uint64_t innerCount) {
    return std::string("func.func @main() {\n"
                       "  %a = db.scan_nodes() : !db.column<!storage.node_id>\n"
                       "  %la = db.limit(%a) count ")
           + std::to_string(outerCount)
           + " : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>\n"
             "  %a1, %e0, %et0, %b = db.get_out_edges(%la, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)\n"
             "  %lb = db.limit(%b) count "
           + std::to_string(innerCount)
           + " : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>\n"
             "  db.output(%lb) : !db.column<!storage.node_id>\n"
             "  return\n"
             "}\n";
}

// MATCH (a) RETURN a SKIP count: a scan whose first `count` rows are dropped.
std::string skipScanProgram(uint64_t count) {
    return std::string("func.func @main() {\n"
                       "  %a = db.scan_nodes() : !db.column<!storage.node_id>\n"
                       "  %sa = db.skip(%a) count ")
           + std::to_string(count)
           + " : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>\n"
             "  db.output(%sa) : !db.column<!storage.node_id>\n"
             "  return\n"
             "}\n";
}

// MATCH (a)->(b)->(c) RETURN c SKIP count: a two-hop traversal (a three-deep loop
// nest) whose first `count` result rows are dropped. The skip sits in the
// innermost body and never gates a loop, so the whole nest runs to exhaustion.
std::string skipTwoHopProgram(uint64_t count) {
    return std::string("func.func @main() {\n"
                       "  %a = db.scan_nodes() : !db.column<!storage.node_id>\n"
                       "  %a1, %e0, %et0, %b = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)\n"
                       "  %b2, %e1, %et1, %c, %a2 = db.get_out_edges(%b, {%a1}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)\n"
                       "  %sc = db.skip(%c) count ")
           + std::to_string(count)
           + " : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>\n"
             "  db.output(%sc) : !db.column<!storage.node_id>\n"
             "  return\n"
             "}\n";
}

// MATCH (a) RETURN a, a.score SKIP count: a property fetch result is a trailing
// output column, so the suffix copy must lift the node IDs and the nullable value
// column together. The representative is the first column, the node IDs.
std::string skipNodePropertyProgram(uint64_t count) {
    return std::string("func.func @main() {\n"
                       "  %a = db.scan_nodes() : !db.column<!storage.node_id>\n"
                       "  %score = db.get_node_properties(%a, \"score\") : (!db.column<!storage.node_id>) -> !db.column<none>\n"
                       "  %sa, %sscore = db.skip(%a, %score) count ")
           + std::to_string(count)
           + " : (!db.column<!storage.node_id>, !db.column<none>) -> (!db.column<!storage.node_id>, !db.column<none>)\n"
             "  db.output(%sa, %sscore) : !db.column<!storage.node_id>, !db.column<none>\n"
             "  return\n"
             "}\n";
}

// MATCH (a) RETURN a SKIP skipCount LIMIT limitCount: a skip stacked under a limit,
// the page of a paginated scan. db.skip drops the first skipCount rows and db.limit
// then keeps the next limitCount. The limit governs the scan's early-exit (it claims
// the producing loop); the skip drops the prefix in front of it.
std::string skipThenLimitProgram(uint64_t skipCount, uint64_t limitCount) {
    return std::string("func.func @main() {\n"
                       "  %a = db.scan_nodes() : !db.column<!storage.node_id>\n"
                       "  %sa = db.skip(%a) count ")
           + std::to_string(skipCount)
           + " : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>\n"
             "  %la = db.limit(%sa) count "
           + std::to_string(limitCount)
           + " : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>\n"
             "  db.output(%la) : !db.column<!storage.node_id>\n"
             "  return\n"
             "}\n";
}

// MATCH (a), (b) RETURN a, b SKIP count: a cross product whose first `count` pairs
// are dropped. The representative is the post-cross-product column, so the count
// is right; the cross product is built in full (a skip cannot cap the build).
std::string skipCrossProductProgram(uint64_t count) {
    return std::string("func.func @main() {\n"
                       "  %0:2 = db.cross_product factor {\n"
                       "    %a = db.scan_nodes() : !db.column<!storage.node_id>\n"
                       "    db.yield %a : !db.column<!storage.node_id>\n"
                       "  } factor {\n"
                       "    %b = db.scan_nodes() : !db.column<!storage.node_id>\n"
                       "    db.yield %b : !db.column<!storage.node_id>\n"
                       "  }\n"
                       "  %sa, %sb = db.skip(%0#0, %0#1) count ")
           + std::to_string(count)
           + " : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)\n"
             "  db.output(%sa, %sb) : !db.column<!storage.node_id>, !db.column<!storage.node_id>\n"
             "  return\n"
             "}\n";
}

// MATCH (a) WITH a SKIP count MATCH (a)-->(b) RETURN b: a mid-query (chained) skip.
// db.skip feeds db.get_out_edges, not db.output, so the skip drops the first `count`
// intermediate `a`s and each surviving `a` then fans out all its out-edges. The scan
// (a's producer) holds the skip; the edge loop is a consumer, built after the
// truncate and limit/skip-oblivious.
std::string chainedSkipProgram(uint64_t count) {
    return std::string("func.func @main() {\n"
                       "  %a = db.scan_nodes() : !db.column<!storage.node_id>\n"
                       "  %sa = db.skip(%a) count ")
           + std::to_string(count)
           + " : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>\n"
             "  %a1, %e0, %et0, %b = db.get_out_edges(%sa, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)\n"
             "  db.output(%b) : !db.column<!storage.node_id>\n"
             "  return\n"
             "}\n";
}

// MATCH (a) RETURN a ORDER BY a DESC: scan every node, then sort the single node
// ID column by itself, descending. The output order is fully determined by the
// sort, independent of the scan's chunking.
constexpr const char* sortNodesDescProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %sa = db.sort(%a) keys [0] ascending [false] : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%sa) : !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a) RETURN a, a.score ORDER BY a.score ASC: read each node's score, then
// sort the (node, score) rows by score ascending. A node without a score sorts
// last (null is greatest).
constexpr const char* sortByScoreAscProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>
  %sa, %sscore = db.sort(%a, %score) keys [1] ascending [true] : (!db.column<!storage.node_id>, !db.column<none>) -> (!db.column<!storage.node_id>, !db.column<none>)
  db.output(%sa, %sscore) : !db.column<!storage.node_id>, !db.column<none>
  return
}
)mlir";

// The same, descending: the null score now sorts first (null is greatest, so a
// descending order puts it at the front).
constexpr const char* sortByScoreDescProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>
  %sa, %sscore = db.sort(%a, %score) keys [1] ascending [false] : (!db.column<!storage.node_id>, !db.column<none>) -> (!db.column<!storage.node_id>, !db.column<none>)
  db.output(%sa, %sscore) : !db.column<!storage.node_id>, !db.column<none>
  return
}
)mlir";

// MATCH (a)-->(b) RETURN a, b ORDER BY a ASC, b DESC: one hop, then sort the
// (source, target) edge rows by source ascending and - within a source - target
// descending. A two-key sort whose primary key has real ties.
constexpr const char* sortEdgesMultiKeyProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %srcs, %eids, %etypes, %b = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %ssrc, %stgt = db.sort(%srcs, %b) keys [0, 1] ascending [true, false] : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%ssrc, %stgt) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a) RETURN a ORDER BY a DESC LIMIT count: the db.limit caps the db.sort's
// result, so lowering fuses them into a bounded top-K (no separate nl.limit).
std::string topKNodesDescProgram(uint64_t count) {
    return std::string("func.func @main() {\n"
                       "  %a = db.scan_nodes() : !db.column<!storage.node_id>\n"
                       "  %sa = db.sort(%a) keys [0] ascending [false] : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>\n"
                       "  %la = db.limit(%sa) count ")
           + std::to_string(count)
           + " : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>\n"
             "  db.output(%la) : !db.column<!storage.node_id>\n"
             "  return\n"
             "}\n";
}

// MATCH (a) RETURN a, a.score ORDER BY a.score ASC LIMIT count: top-K over a
// (node, score) projection, sorted by the nullable score ascending.
std::string topKByScoreAscProgram(uint64_t count) {
    return std::string("func.func @main() {\n"
                       "  %a = db.scan_nodes() : !db.column<!storage.node_id>\n"
                       "  %score = db.get_node_properties(%a, \"score\") : (!db.column<!storage.node_id>) -> !db.column<none>\n"
                       "  %sa, %sscore = db.sort(%a, %score) keys [1] ascending [true] : (!db.column<!storage.node_id>, !db.column<none>) -> (!db.column<!storage.node_id>, !db.column<none>)\n"
                       "  %la, %lscore = db.limit(%sa, %sscore) count ")
           + std::to_string(count)
           + " : (!db.column<!storage.node_id>, !db.column<none>) -> (!db.column<!storage.node_id>, !db.column<none>)\n"
             "  db.output(%la, %lscore) : !db.column<!storage.node_id>, !db.column<none>\n"
             "  return\n"
             "}\n";
}

// MATCH (a)-[]->(b) RETURN DISTINCT b: one hop, then dedup the target column.
// Several sources can point at the same target, so b has duplicates DISTINCT drops.
const char* const removeDuplicatesTargetsProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %a1, %e0, %et0, %b = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %ub = db.remove_duplicates(%b) : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%ub) : !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a)-[]->()-[]->(c) RETURN DISTINCT a, c: a two-hop with the origin `a`
// carried alongside, deduped on the whole (a, c) pair. The diamond's two paths
// 0->1->3 and 0->2->3 both yield (0, 3), so DISTINCT collapses them to one row.
const char* const removeDuplicatesTwoHopProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %a1, %e0, %et0, %b = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %b2, %e1, %et1, %c, %a2 = db.get_out_edges(%b, {%a1}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %da, %dc = db.remove_duplicates(%a2, %c) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%da, %dc) : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a)-[]->(b) WITH DISTINCT a MATCH (a)-[]->(c) RETURN c: a mid-query
// (chained) DISTINCT. The first hop's sources repeat once per out-edge; DISTINCT
// collapses them to the unique sources, and the second hop fans out from each once
// - fewer driver rows than the raw (duplicated) source column would give.
const char* const removeDuplicatesChainedProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %a1, %e0, %et0, %b = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %da = db.remove_duplicates(%a1) : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  %a2, %e1, %et1, %c = db.get_out_edges(%da, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%c) : !db.column<!storage.node_id>
  return
}
)mlir";

// MATCH (a)-[]->(b) RETURN DISTINCT b LIMIT count: DISTINCT streams, so the LIMIT
// bounds the producing loops through the ordinary early-exit - no top-K fusion.
std::string removeDuplicatesLimitProgram(uint64_t count) {
    return std::string("func.func @main() {\n"
                       "  %a = db.scan_nodes() : !db.column<!storage.node_id>\n"
                       "  %a1, %e0, %et0, %b = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)\n"
                       "  %ub = db.remove_duplicates(%b) : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>\n"
                       "  %lb = db.limit(%ub) count ")
           + std::to_string(count)
           + " : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>\n"
             "  db.output(%lb) : !db.column<!storage.node_id>\n"
             "  return\n"
             "}\n";
}

// MATCH (a) RETURN DISTINCT a, a.score: dedup on the (node, nullable score) pair,
// exercising the nullable value column's key-append path (an int64 value or a null
// tag) alongside the node ID. A node with no score serializes its null and still
// round-trips.
const char* const removeDuplicatesNodeScoreProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>
  %da, %dscore = db.remove_duplicates(%a, %score) : (!db.column<!storage.node_id>, !db.column<none>) -> (!db.column<!storage.node_id>, !db.column<none>)
  db.output(%da, %dscore) : !db.column<!storage.node_id>, !db.column<none>
  return
}
)mlir";

// MATCH (a) RETURN count(a): scan every node and count them. Node IDs are never
// null, so this is the total node count.
const char* const countNodesProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %n = db.count(%a) : (!db.column<!storage.node_id>) -> !db.column<ui64>
  db.output(%n) : !db.column<ui64>
  return
}
)mlir";

// MATCH (a) RETURN count(a.score): count the non-null scores. A node without the
// property reads null, and count(a.score) does not charge those - so the tally is
// fewer than the node count.
const char* const countScoresProgram = R"mlir(
func.func @main() {
  %a = db.scan_nodes() : !db.column<!storage.node_id>
  %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>
  %n = db.count(%score) : (!db.column<none>) -> !db.column<ui64>
  db.output(%n) : !db.column<ui64>
  return
}
)mlir";

// MATCH (a) RETURN <agg>(a.score): reduce the non-null scores with the given
// aggregate op (db.sum / db.min / db.max / db.avg). The db result value type is
// resolved during lowering (the property is Int64, so sum/min/max stay Int64 and
// avg widens to f64), so the db column types are left as none - the same way the
// property column is spelled.
std::string aggregateScoreProgram(const char* op) {
    return std::string("func.func @main() {\n"
                       "  %a = db.scan_nodes() : !db.column<!storage.node_id>\n"
                       "  %score = db.get_node_properties(%a, \"score\") : (!db.column<!storage.node_id>) -> !db.column<none>\n"
                       "  %r = db.")
           + op
           + "(%score) : (!db.column<none>) -> !db.column<none>\n"
             "  db.output(%r) : !db.column<none>\n"
             "  return\n"
             "}\n";
}

}

class DBLoweringTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    // A diamond: 0 -> {1, 2} -> 3, so the two-hop pair (0, 3) exists twice
    std::unique_ptr<Graph> buildDiamondGraph() {
        auto graph = Graph::create();

        auto change = graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        metadata.getOrCreateLabel("0");
        metadata.getOrCreateEdgeType("0");

        const LabelSet labelset = LabelSet::fromList({0});
        const NodeID nodeA = builder.addNode(labelset);
        const NodeID nodeB = builder.addNode(labelset);
        const NodeID nodeC = builder.addNode(labelset);
        const NodeID nodeD = builder.addNode(labelset);

        builder.addEdge(0, nodeA, nodeB);
        builder.addEdge(0, nodeA, nodeC);
        builder.addEdge(0, nodeB, nodeD);
        builder.addEdge(0, nodeC, nodeD);

        const auto submitResult = change->access().submit(*_jobSystem);
        EXPECT_TRUE(submitResult);

        return graph;
    }

    // A second datapart with nodes 4, 5 and the edge 4 -> 5, growing the node
    // count past the top-K trim threshold in the trims-across-chunks test.
    void addSecondPart(Graph& graph) {
        auto change = graph.newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();

        const LabelSet labelset = LabelSet::fromList({0});
        const NodeID nodeE = builder.addNode(labelset);
        const NodeID nodeF = builder.addNode(labelset);

        builder.addEdge(0, nodeE, nodeF);

        const auto submitResult = change->access().submit(*_jobSystem);
        EXPECT_TRUE(submitResult);
    }

    // A line graph 0 -> 1 -> 2 where nodes 0 and 1 carry "score" (Int64),
    // "name" (String) and "vec" (Embedding) properties and node 2 carries none,
    // so a read of any of them returns null for node 2. The edge 0 -> 1 carries
    // a "weight" (Int64) property and the edge 1 -> 2 carries none, so reading
    // the edge property returns null for that second edge.
    std::unique_ptr<Graph> buildPropertyGraph() {
        auto graph = Graph::create();

        auto change = graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        metadata.getOrCreateLabel("0");
        metadata.getOrCreateEdgeType("0");
        const PropertyTypeID scoreID = metadata.getOrCreatePropertyType("score", ValueType::Int64)._id;
        const PropertyTypeID nameID = metadata.getOrCreatePropertyType("name", ValueType::String)._id;
        const PropertyTypeID vecID = metadata.getOrCreatePropertyType("vec", ValueType::Embedding)._id;
        const PropertyTypeID weightID = metadata.getOrCreatePropertyType("weight", ValueType::Int64)._id;

        const LabelSet labelset = LabelSet::fromList({0});
        const NodeID nodeA = builder.addNode(labelset);
        const NodeID nodeB = builder.addNode(labelset);
        const NodeID nodeC = builder.addNode(labelset);

        // The reference returned by addEdge points into the builder's edge
        // vector, so attach the property before the next addEdge can move it
        const EdgeRecord& edgeAB = builder.addEdge(0, nodeA, nodeB);
        builder.addEdgeProperty<types::Int64>(edgeAB, weightID, 10);

        // The edge 1 -> 2 carries no weight, so an edge property read is null for it
        builder.addEdge(0, nodeB, nodeC);

        builder.addNodeProperty<types::Int64>(nodeA, scoreID, 100);
        builder.addNodeProperty<types::Int64>(nodeB, scoreID, 200);

        builder.addNodeProperty<types::String>(nodeA, nameID, "alice");
        builder.addNodeProperty<types::String>(nodeB, nameID, "bob");

        const std::vector<float> vecA {1.0f, 2.0f};
        const std::vector<float> vecB {3.0f, 4.0f};
        builder.addNodeProperty<types::Embedding>(nodeA, vecID, vecA);
        builder.addNodeProperty<types::Embedding>(nodeB, vecID, vecB);

        // nodeC carries no property, so every property read returns null for it

        const auto submitResult = change->access().submit(*_jobSystem);
        EXPECT_TRUE(submitResult);

        return graph;
    }

    // Four nodes, each with out-degree exactly two, so a chained LIMIT k on the
    // scan keeps k nodes whose expansion is exactly 2*k edges regardless of which
    // k the scan order happens to pick - a count that is order-independent, unlike
    // the diamond's uneven out-degrees.
    std::unique_ptr<Graph> buildRegularOutGraph() {
        auto graph = Graph::create();

        auto change = graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        metadata.getOrCreateLabel("0");
        metadata.getOrCreateEdgeType("0");

        const LabelSet labelset = LabelSet::fromList({0});
        const NodeID node0 = builder.addNode(labelset);
        const NodeID node1 = builder.addNode(labelset);
        const NodeID node2 = builder.addNode(labelset);
        const NodeID node3 = builder.addNode(labelset);

        builder.addEdge(0, node0, node1);
        builder.addEdge(0, node0, node2);
        builder.addEdge(0, node1, node2);
        builder.addEdge(0, node1, node3);
        builder.addEdge(0, node2, node3);
        builder.addEdge(0, node2, node0);
        builder.addEdge(0, node3, node0);
        builder.addEdge(0, node3, node1);

        const auto submitResult = change->access().submit(*_jobSystem);
        EXPECT_TRUE(submitResult);

        return graph;
    }

    // Five nodes over two labels: node 0, 1 are Person; node 2, 4 are City;
    // node 3 carries both. A scan by "Person" is a superset match, so it keeps
    // node 3 too - the (Person), (Person,City) nodes - and a scan by both labels
    // keeps only node 3. No edges: a label scan reads only the node containers.
    std::unique_ptr<Graph> buildLabeledGraph() {
        auto graph = Graph::create();

        auto change = graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        const LabelID personID = metadata.getOrCreateLabel("Person");
        const LabelID cityID = metadata.getOrCreateLabel("City");

        const LabelSet person = LabelSet::fromList({personID});
        const LabelSet city = LabelSet::fromList({cityID});
        const LabelSet personAndCity = LabelSet::fromList({personID, cityID});

        builder.addNode(person);        // node 0
        builder.addNode(person);        // node 1
        builder.addNode(city);          // node 2
        builder.addNode(personAndCity); // node 3
        builder.addNode(city);          // node 4

        const auto submitResult = change->access().submit(*_jobSystem);
        EXPECT_TRUE(submitResult);

        return graph;
    }

    // Parses a db-dialect program, lowers it to nl with DBLowering, and runs
    // the lowered nl function against the graph view. The chunk size is exposed
    // so a test can force a product to span chunk boundaries.
    void runLoweredProgram(const char* programText,
                           const GraphView& view,
                           NLOutputSink& sink,
                           size_t chunkSize = ChunkConfig::CHUNK_SIZE) {
        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::db::DB>();
        context.getOrLoadDialect<mlir::nl::NL>();

        const mlir::ParserConfig parserConfig(&context);
        mlir::OwningOpRef<mlir::ModuleOp> dbModule = mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
        ASSERT_TRUE(dbModule);

        const mlir::func::FuncOp dbFunction = dbModule->lookupSymbol<mlir::func::FuncOp>("main");
        ASSERT_TRUE(dbFunction);

        // Collect the lowered nl function in its own module, then interpret it
        mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&context));
        DBLowering lowering(&context, &view);
        lowering.lower(dbFunction, *nlModule);

        LocalMemory memory;
        NLInterpreter interpreter(*nlModule, &view, &sink, &memory, chunkSize);
        interpreter.run();
    }

    // Fills the node ID set of a graph by lowering and running MATCH (a) RETURN a,
    // so a cross product's expected result is derived from the graph rather than
    // from a hardcoded node count.
    void collectNodeIDs(const GraphView& view, std::vector<uint64_t>& nodes) {
        CollectingNodeSink scanSink;
        runLoweredProgram(scanProgram, view, scanSink);

        std::vector<std::vector<uint64_t>> scanRows;
        scanSink.sortedRows(scanRows);

        nodes.clear();
        for (const std::vector<uint64_t>& row : scanRows) {
            nodes.push_back(row.front());
        }
    }

    // Runs a single-column node scan program and fills nodeIDs with its node IDs,
    // sorted. The storage assigns node IDs grouped by label set, not by insertion
    // order, so the label-scan tests assert the semantics (superset match,
    // conjunction = intersection) against the plain scan rather than hardcoding an
    // ID scheme.
    void collectScan(const char* program, const GraphView& view, std::vector<uint64_t>& nodeIDs) {
        CollectingNodeSink sink;
        runLoweredProgram(program, view, sink);

        std::vector<std::vector<uint64_t>> rows;
        sink.sortedRows(rows);

        for (const std::vector<uint64_t>& row : rows) {
            nodeIDs.push_back(row.front());
        }
    }

    std::unique_ptr<JobSystem> _jobSystem;
};

TEST_F(DBLoweringTest, lowersScanNodes) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeSink sink;
    runLoweredProgram(scanProgram, reader.getView(), sink);

    const std::vector<std::vector<uint64_t>> expected {{0}, {1}, {2}, {3}};
    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, executesScanNodesByLabel) {
    auto graph = buildLabeledGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    std::vector<uint64_t> all;
    std::vector<uint64_t> person;
    std::vector<uint64_t> city;
    std::vector<uint64_t> both;
    collectScan(scanProgram, view, all);
    collectScan(scanByLabelPersonProgram, view, person);
    collectScan(scanByLabelCityProgram, view, city);
    collectScan(scanByLabelPersonAndCityProgram, view, both);

    // Two Person-only, two City-only, one Person+City node.
    EXPECT_EQ(all.size(), 5u);
    EXPECT_EQ(person.size(), 3u);   // the two Person-only plus the Person+City node
    EXPECT_EQ(city.size(), 3u);     // the two City-only plus the Person+City node
    EXPECT_EQ(both.size(), 1u);     // only the Person+City node

    // A label scan is a superset match, so its result is a subset of all nodes.
    EXPECT_TRUE(std::includes(all.begin(), all.end(), person.begin(), person.end()));
    EXPECT_TRUE(std::includes(all.begin(), all.end(), city.begin(), city.end()));

    // The conjunction is exactly the intersection of the single-label scans, and
    // every node has Person or City, so their union is all nodes.
    std::vector<uint64_t> intersection;
    std::set_intersection(person.begin(), person.end(),
                          city.begin(), city.end(),
                          std::back_inserter(intersection));
    EXPECT_EQ(intersection, both);

    std::vector<uint64_t> unionSet;
    std::set_union(person.begin(), person.end(),
                   city.begin(), city.end(),
                   std::back_inserter(unionSet));
    EXPECT_EQ(unionSet, all);
}

TEST_F(DBLoweringTest, executesScanNodesByLabelUnknownIsEmpty) {
    auto graph = buildLabeledGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // "Robot" was never created, so the conjunction is unsatisfiable: no rows.
    std::vector<uint64_t> robots;
    collectScan(scanByLabelUnknownProgram, reader.getView(), robots);
    EXPECT_TRUE(robots.empty());
}

TEST_F(DBLoweringTest, lowersSingleConstant) {
    auto graph = Graph::create();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingConstSink<int64_t> sink;
    runLoweredProgram(singleConstantProgram, reader.getView(), sink);

    const std::vector<std::vector<int64_t>> expected {{30}};
    EXPECT_EQ(sink.rows(), expected);
}

TEST_F(DBLoweringTest, lowersMultipleConstants) {
    auto graph = Graph::create();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingConstSink<int64_t> sink;
    runLoweredProgram(multipleConstantsProgram, reader.getView(), sink);

    // One row of three constant columns, in projection order.
    const std::vector<std::vector<int64_t>> expected {{10, 20, 30}};
    EXPECT_EQ(sink.rows(), expected);
}

TEST_F(DBLoweringTest, lowersDoubleConstant) {
    auto graph = Graph::create();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingConstSink<double> sink;
    runLoweredProgram(doubleConstantProgram, reader.getView(), sink);

    const std::vector<std::vector<double>> expected {{2.5}};
    EXPECT_EQ(sink.rows(), expected);
}

TEST_F(DBLoweringTest, lowersConstantsOfAllSupportedTypes) {
    auto graph = Graph::create();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingAllTypesConstSink sink;
    runLoweredProgram(allTypesConstantsProgram, reader.getView(), sink);

    ASSERT_TRUE(sink.seen());
    EXPECT_EQ(sink.getInt(), 30);
    EXPECT_EQ(sink.getUint(), 7u);
    EXPECT_DOUBLE_EQ(sink.getDouble(), 2.5);
    EXPECT_TRUE(sink.getBool());
}

TEST_F(DBLoweringTest, addsTwoConstants) {
    auto graph = Graph::create();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingConstSink<int64_t> sink;
    runLoweredProgram(addConstantsProgram, reader.getView(), sink);

    const std::vector<std::vector<int64_t>> expected {{30}};
    EXPECT_EQ(sink.rows(), expected);
}

TEST_F(DBLoweringTest, addPromotesMixedTypesToDouble) {
    auto graph = Graph::create();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingConstSink<double> sink;
    runLoweredProgram(addPromotesProgram, reader.getView(), sink);

    const std::vector<std::vector<double>> expected {{12.5}};
    EXPECT_EQ(sink.rows(), expected);
}

TEST_F(DBLoweringTest, addsConstantToNodePropertyBroadcasting) {
    auto graph = buildPropertyGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeIntPropSink sink;
    runLoweredProgram(addPropertyConstantProgram, reader.getView(), sink);

    // The constant 10 is broadcast against each node's score; node 2 has no score,
    // so null + 10 stays null.
    const std::vector<std::pair<uint64_t, std::optional<int64_t>>> expected {
        {0, 110}, {1, 210}, {2, std::nullopt}
    };
    std::vector<std::pair<uint64_t, std::optional<int64_t>>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, addsTwoNodeProperties) {
    auto graph = buildPropertyGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeIntPropSink sink;
    runLoweredProgram(addTwoPropertiesProgram, reader.getView(), sink);

    const std::vector<std::pair<uint64_t, std::optional<int64_t>>> expected {
        {0, 200}, {1, 400}, {2, std::nullopt}
    };
    std::vector<std::pair<uint64_t, std::optional<int64_t>>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, subsTwoConstants) {
    auto graph = Graph::create();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingConstSink<int64_t> sink;
    runLoweredProgram(subConstantsProgram, reader.getView(), sink);

    const std::vector<std::vector<int64_t>> expected {{10}};
    EXPECT_EQ(sink.rows(), expected);
}

TEST_F(DBLoweringTest, subPromotesMixedTypesToDouble) {
    auto graph = Graph::create();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingConstSink<double> sink;
    runLoweredProgram(subPromotesProgram, reader.getView(), sink);

    const std::vector<std::vector<double>> expected {{7.5}};
    EXPECT_EQ(sink.rows(), expected);
}

TEST_F(DBLoweringTest, subsConstantFromNodePropertyBroadcasting) {
    auto graph = buildPropertyGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeIntPropSink sink;
    runLoweredProgram(subPropertyConstantProgram, reader.getView(), sink);

    // The constant 10 is broadcast against each node's score; node 2 has no score,
    // so null - 10 stays null.
    const std::vector<std::pair<uint64_t, std::optional<int64_t>>> expected {
        {0, 90}, {1, 190}, {2, std::nullopt}
    };
    std::vector<std::pair<uint64_t, std::optional<int64_t>>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, subsTwoNodeProperties) {
    auto graph = buildPropertyGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeIntPropSink sink;
    runLoweredProgram(subTwoPropertiesProgram, reader.getView(), sink);

    const std::vector<std::pair<uint64_t, std::optional<int64_t>>> expected {
        {0, 0}, {1, 0}, {2, std::nullopt}
    };
    std::vector<std::pair<uint64_t, std::optional<int64_t>>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, mulsTwoConstants) {
    auto graph = Graph::create();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingConstSink<int64_t> sink;
    runLoweredProgram(mulConstantsProgram, reader.getView(), sink);

    const std::vector<std::vector<int64_t>> expected {{600}};
    EXPECT_EQ(sink.rows(), expected);
}

TEST_F(DBLoweringTest, mulPromotesMixedTypesToDouble) {
    auto graph = Graph::create();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingConstSink<double> sink;
    runLoweredProgram(mulPromotesProgram, reader.getView(), sink);

    const std::vector<std::vector<double>> expected {{25.0}};
    EXPECT_EQ(sink.rows(), expected);
}

TEST_F(DBLoweringTest, mulsConstantByNodePropertyBroadcasting) {
    auto graph = buildPropertyGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeIntPropSink sink;
    runLoweredProgram(mulPropertyConstantProgram, reader.getView(), sink);

    // The constant 10 is broadcast against each node's score; node 2 has no score,
    // so null * 10 stays null.
    const std::vector<std::pair<uint64_t, std::optional<int64_t>>> expected {
        {0, 1000}, {1, 2000}, {2, std::nullopt}
    };
    std::vector<std::pair<uint64_t, std::optional<int64_t>>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, mulsTwoNodeProperties) {
    auto graph = buildPropertyGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeIntPropSink sink;
    runLoweredProgram(mulTwoPropertiesProgram, reader.getView(), sink);

    const std::vector<std::pair<uint64_t, std::optional<int64_t>>> expected {
        {0, 10000}, {1, 40000}, {2, std::nullopt}
    };
    std::vector<std::pair<uint64_t, std::optional<int64_t>>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, eqConstantsFalse) {
    auto graph = Graph::create();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingMaskSink sink;
    runLoweredProgram(eqConstantsFalseProgram, reader.getView(), sink);

    const std::vector<bool> expected {false};
    EXPECT_EQ(sink.values(), expected);
}

TEST_F(DBLoweringTest, eqConstantsTrue) {
    auto graph = Graph::create();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingMaskSink sink;
    runLoweredProgram(eqConstantsTrueProgram, reader.getView(), sink);

    const std::vector<bool> expected {true};
    EXPECT_EQ(sink.values(), expected);
}

TEST_F(DBLoweringTest, eqNodeToItselfIsAllTrue) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingMaskSink sink;
    runLoweredProgram(eqSelfProgram, reader.getView(), sink);

    // The diamond has four nodes, and every node equals itself.
    const std::vector<bool> expected {true, true, true, true};
    EXPECT_EQ(sink.values(), expected);
}

TEST_F(DBLoweringTest, eqNodePropertyToConstantBroadcasting) {
    auto graph = buildPropertyGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeBoolSink sink;
    runLoweredProgram(eqPropertyConstantProgram, reader.getView(), sink);

    // score is 100 / 200 / null, compared against 200: false / true / null.
    const std::vector<std::pair<uint64_t, std::optional<bool>>> expected {
        {0, false}, {1, true}, {2, std::nullopt}
    };
    std::vector<std::pair<uint64_t, std::optional<bool>>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, andConstantsFalse) {
    auto graph = Graph::create();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingMaskSink sink;
    runLoweredProgram(andConstantsFalseProgram, reader.getView(), sink);

    const std::vector<bool> expected {false};
    EXPECT_EQ(sink.values(), expected);
}

TEST_F(DBLoweringTest, andConstantsTrue) {
    auto graph = Graph::create();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingMaskSink sink;
    runLoweredProgram(andConstantsTrueProgram, reader.getView(), sink);

    const std::vector<bool> expected {true};
    EXPECT_EQ(sink.values(), expected);
}

TEST_F(DBLoweringTest, andNullablePropertySelf) {
    auto graph = buildPropertyGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeBoolSink sink;
    runLoweredProgram(andPropertySelfProgram, reader.getView(), sink);

    // (score = 200) is false / true / null; AND with itself preserves each.
    const std::vector<std::pair<uint64_t, std::optional<bool>>> expected {
        {0, false}, {1, true}, {2, std::nullopt}
    };
    std::vector<std::pair<uint64_t, std::optional<bool>>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, andNullWithFalseIsFalse) {
    auto graph = buildPropertyGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeBoolSink sink;
    runLoweredProgram(andNullShortCircuitProgram, reader.getView(), sink);

    // (score = 200) is false / true / null; (a = 1) is false / true / false.
    // Node 2's null AND false short-circuits to false rather than null.
    const std::vector<std::pair<uint64_t, std::optional<bool>>> expected {
        {0, false}, {1, true}, {2, false}
    };
    std::vector<std::pair<uint64_t, std::optional<bool>>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, orConstantsTrue) {
    auto graph = Graph::create();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingMaskSink sink;
    runLoweredProgram(orConstantsTrueProgram, reader.getView(), sink);

    const std::vector<bool> expected {true};
    EXPECT_EQ(sink.values(), expected);
}

TEST_F(DBLoweringTest, orConstantsFalse) {
    auto graph = Graph::create();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingMaskSink sink;
    runLoweredProgram(orConstantsFalseProgram, reader.getView(), sink);

    const std::vector<bool> expected {false};
    EXPECT_EQ(sink.values(), expected);
}

TEST_F(DBLoweringTest, orNullablePropertySelf) {
    auto graph = buildPropertyGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeBoolSink sink;
    runLoweredProgram(orPropertySelfProgram, reader.getView(), sink);

    // (score = 200) is false / true / null; OR with itself preserves each.
    const std::vector<std::pair<uint64_t, std::optional<bool>>> expected {
        {0, false}, {1, true}, {2, std::nullopt}
    };
    std::vector<std::pair<uint64_t, std::optional<bool>>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, orNullWithTrueIsTrue) {
    auto graph = buildPropertyGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeBoolSink sink;
    runLoweredProgram(orNullShortCircuitProgram, reader.getView(), sink);

    // (score = 200) is false / true / null; (a = 2) is false / false / true.
    // Node 2's null OR true short-circuits to true rather than null.
    const std::vector<std::pair<uint64_t, std::optional<bool>>> expected {
        {0, false}, {1, true}, {2, true}
    };
    std::vector<std::pair<uint64_t, std::optional<bool>>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, lowersOneHopOutEdges) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeSink sink;
    runLoweredProgram(oneHopOutProgram, reader.getView(), sink);

    const std::vector<std::vector<uint64_t>> expected {{0, 1}, {0, 2}, {1, 3}, {2, 3}};
    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, lowersOneHopInEdges) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeSink sink;
    runLoweredProgram(oneHopInProgram, reader.getView(), sink);

    const std::vector<std::vector<uint64_t>> expected {{0, 1}, {0, 2}, {1, 3}, {2, 3}};
    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, lowersTwoHopInEdgesWithCarriedColumn) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeSink sink;
    runLoweredProgram(twoHopInProgram, reader.getView(), sink);

    // Both two-hop predecessor chains end at 3 and trace back to 0, one through
    // 1 and one through 2, so each emits (a, c) = (3, 0)
    const std::vector<std::vector<uint64_t>> expected {{3, 0}, {3, 0}};
    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, lowersTwoHopWithCarriedColumn) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeSink sink;
    runLoweredProgram(twoHopProgram, reader.getView(), sink);

    // Both two-hop paths go from 0 to 3, one through 1 and one through 2
    const std::vector<std::vector<uint64_t>> expected {{0, 3}, {0, 3}};
    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, lowersGetNodeProperties) {
    auto graph = buildPropertyGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeIntPropSink sink;
    runLoweredProgram(nodePropertiesProgram, reader.getView(), sink);

    // Every node appears, with its score or null where it has none
    const std::vector<std::pair<uint64_t, std::optional<int64_t>>> expected {
        {0, 100}, {1, 200}, {2, std::nullopt}
    };
    std::vector<std::pair<uint64_t, std::optional<int64_t>>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, lowersGetNodeStringProperty) {
    auto graph = buildPropertyGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeStringPropSink sink;
    runLoweredProgram(nodeNameProgram, reader.getView(), sink);

    const std::vector<std::pair<uint64_t, std::optional<std::string>>> expected {
        {0, std::string("alice")}, {1, std::string("bob")}, {2, std::nullopt}
    };
    std::vector<std::pair<uint64_t, std::optional<std::string>>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, lowersGetNodeEmbeddingProperty) {
    auto graph = buildPropertyGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeEmbeddingPropSink sink;
    runLoweredProgram(nodeVecProgram, reader.getView(), sink);

    const std::vector<std::pair<uint64_t, std::optional<std::vector<float>>>> expected {
        {0, std::vector<float>{1.0f, 2.0f}}, {1, std::vector<float>{3.0f, 4.0f}}, {2, std::nullopt}
    };
    std::vector<std::pair<uint64_t, std::optional<std::vector<float>>>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, lowersGetEdgeProperties) {
    auto graph = buildPropertyGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingEdgeIntPropSink sink;
    runLoweredProgram(edgePropertiesProgram, reader.getView(), sink);

    // Each out-edge appears with its weight, or null where the edge has none:
    // the edge 0 -> 1 (id 0) carries weight 10, the edge 1 -> 2 (id 1) carries none
    const std::vector<std::pair<uint64_t, std::optional<int64_t>>> expected {
        {0, 10}, {1, std::nullopt}
    };
    std::vector<std::pair<uint64_t, std::optional<int64_t>>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, executesCrossProductOfScans) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeSink sink;
    runLoweredProgram(crossProductScansProgram, reader.getView(), sink);

    // Every node crossed with every node: the 16 (a, b) pairs over four nodes
    std::vector<std::vector<uint64_t>> expected;
    for (uint64_t a = 0; a < 4; a++) {
        for (uint64_t b = 0; b < 4; b++) {
            expected.push_back({a, b});
        }
    }
    std::sort(expected.begin(), expected.end());

    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, executesCrossProductSpanningChunks) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // A chunk of three over four nodes splits each scan into two chunks, so the
    // product must cross rows that land in different chunks - still all 16 pairs
    CollectingNodeSink sink;
    runLoweredProgram(crossProductScansProgram, reader.getView(), sink, /*chunkSize=*/3);

    std::vector<std::vector<uint64_t>> expected;
    for (uint64_t a = 0; a < 4; a++) {
        for (uint64_t b = 0; b < 4; b++) {
            expected.push_back({a, b});
        }
    }
    std::sort(expected.begin(), expected.end());

    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, executesCrossProductOfHops) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeSink sink;
    runLoweredProgram(crossProductHopsProgram, reader.getView(), sink);

    // Every edge crossed with every edge: each row is (outer src, outer tgt,
    // inner src, inner tgt) for the four diamond edges, so 16 rows
    const std::vector<std::pair<uint64_t, uint64_t>> edges {{0, 1}, {0, 2}, {1, 3}, {2, 3}};
    std::vector<std::vector<uint64_t>> expected;
    for (const std::pair<uint64_t, uint64_t>& outer : edges) {
        for (const std::pair<uint64_t, uint64_t>& inner : edges) {
            expected.push_back({outer.first, outer.second, inner.first, inner.second});
        }
    }
    std::sort(expected.begin(), expected.end());

    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, executesCrossProductOfTwoHopAndScan) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeSink sink;
    runLoweredProgram(crossProductTwoHopAndScanProgram, reader.getView(), sink);

    // The diamond's two-hop paths (a, b, c) are 0->1->3 and 0->2->3; cross each
    // with every node d in {0, 1, 2, 3}, so 2 * 4 = 8 rows of (a, b, c, d)
    const std::vector<std::vector<uint64_t>> twoHopPaths {{0, 1, 3}, {0, 2, 3}};
    std::vector<std::vector<uint64_t>> expected;
    for (const std::vector<uint64_t>& path : twoHopPaths) {
        for (uint64_t d = 0; d < 4; d++) {
            expected.push_back({path[0], path[1], path[2], d});
        }
    }
    std::sort(expected.begin(), expected.end());

    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, executesCrossProductOfNodeProperty) {
    auto graph = buildPropertyGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // A node ID crossed with a nullable property column: node 2 has no score,
    // so its value comes back null and is broadcast like any other value
    CollectingNodeIntPropSink sink;
    runLoweredProgram(crossProductNodePropertyProgram, reader.getView(), sink);

    const std::vector<std::optional<int64_t>> scores {100, 200, std::nullopt};
    std::vector<std::pair<uint64_t, std::optional<int64_t>>> expected;
    for (uint64_t a = 0; a < 3; a++) {
        for (const std::optional<int64_t>& score : scores) {
            expected.push_back({a, score});
        }
    }
    std::sort(expected.begin(), expected.end());

    std::vector<std::pair<uint64_t, std::optional<int64_t>>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, executesNestedCrossProductOnSimpleGraph) {
    // MATCH (a), (b), (c) RETURN a, b, c on the shared simpledb graph: a
    // three-way product the db dialect models as a cross_product whose left
    // factor is itself a cross_product. Executed, it is the whole node set
    // crossed with itself three times, so |nodes|^3 rows.
    auto graph = Graph::create();
    SimpleGraph::createSimpleGraph(graph.get());

    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    std::vector<uint64_t> nodes;
    collectNodeIDs(reader.getView(), nodes);
    ASSERT_FALSE(nodes.empty());

    // Every (a, b, c) triple over the node set.
    std::vector<std::vector<uint64_t>> expected;
    for (const uint64_t a : nodes) {
        for (const uint64_t b : nodes) {
            for (const uint64_t c : nodes) {
                expected.push_back({a, b, c});
            }
        }
    }
    std::sort(expected.begin(), expected.end());

    CollectingNodeSink sink;
    runLoweredProgram(nestedCrossProductScansProgram, reader.getView(), sink);

    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows.size(), nodes.size() * nodes.size() * nodes.size());
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, executesNestedCrossProductSpanningChunks) {
    // The same three-way product on simpledb, but a small chunk splits each scan
    // into several chunks, so the inner product re-runs once per (a, b) chunk
    // pair and the outer product crosses that materialized result with each c
    // chunk. The row set is unchanged - still every (a, b, c) triple.
    auto graph = Graph::create();
    SimpleGraph::createSimpleGraph(graph.get());

    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    std::vector<uint64_t> nodes;
    collectNodeIDs(reader.getView(), nodes);
    ASSERT_FALSE(nodes.empty());

    std::vector<std::vector<uint64_t>> expected;
    for (const uint64_t a : nodes) {
        for (const uint64_t b : nodes) {
            for (const uint64_t c : nodes) {
                expected.push_back({a, b, c});
            }
        }
    }
    std::sort(expected.begin(), expected.end());

    CollectingNodeSink sink;
    runLoweredProgram(nestedCrossProductScansProgram, reader.getView(), sink, /*chunkSize=*/3);

    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, lowersCrossProductToNestedLoops) {
    // DBLowering takes a view, but the scans program reads no property, so the
    // lowered structure does not depend on the graph contents.
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    mlir::MLIRContext context;
    context.getOrLoadDialect<mlir::func::FuncDialect>();
    context.getOrLoadDialect<mlir::storage::Storage>();
    context.getOrLoadDialect<mlir::db::DB>();
    context.getOrLoadDialect<mlir::nl::NL>();
    context.getOrLoadDialect<mlir::storage::Storage>();

    const mlir::ParserConfig parserConfig(&context);
    mlir::OwningOpRef<mlir::ModuleOp> dbModule = mlir::parseSourceString<mlir::ModuleOp>(crossProductScansProgram, parserConfig);
    ASSERT_TRUE(dbModule);

    const mlir::func::FuncOp dbFunction = dbModule->lookupSymbol<mlir::func::FuncOp>("main");
    ASSERT_TRUE(dbFunction);

    mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&context));
    DBLowering lowering(&context, &reader.getView());
    lowering.lower(dbFunction, *nlModule);

    // The product becomes a loop nest, not two sibling loops: exactly one
    // nl.cross_product and exactly two nl.for loops (one scan per factor).
    size_t crossProductCount = 0;
    size_t forCount = 0;
    mlir::nl::CrossProduct cross;
    nlModule->walk([&](mlir::Operation* operation) {
        if (mlir::nl::CrossProduct found = mlir::dyn_cast<mlir::nl::CrossProduct>(operation)) {
            cross = found;
            crossProductCount++;
        } else if (mlir::isa<mlir::nl::For>(operation)) {
            forCount++;
        }
    });

    EXPECT_EQ(crossProductCount, 1u);
    EXPECT_EQ(forCount, 2u);
    ASSERT_TRUE(cross);

    // One column contributed by each factor, two results (outer ++ inner).
    EXPECT_EQ(cross.getOuterColumns().size(), 1u);
    EXPECT_EQ(cross.getInnerColumns().size(), 1u);
    EXPECT_EQ(cross.getResults().size(), 2u);

    // The cross sits in the inner loop body, itself nested in the outer loop
    // body - so the inner factor re-runs once per outer chunk.
    auto innerFor = mlir::dyn_cast<mlir::nl::For>(cross->getParentOp());
    ASSERT_TRUE(innerFor);
    auto outerFor = mlir::dyn_cast<mlir::nl::For>(innerFor->getParentOp());
    ASSERT_TRUE(outerFor);

    // The crossed columns are exactly those two loops' chunk variables: the
    // outer column is the outer loop's chunk, the inner column the inner's.
    EXPECT_EQ(cross.getOuterColumns()[0], outerFor.getBody()->getArgument(0));
    EXPECT_EQ(cross.getInnerColumns()[0], innerFor.getBody()->getArgument(0));
}

TEST_F(DBLoweringTest, limitsScanToFewerThanNodeCount) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeSink sink;
    const std::string program = limitScanProgram(3);
    runLoweredProgram(program.c_str(), reader.getView(), sink);

    // Four nodes, LIMIT 3: exactly three rows survive.
    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows.size(), 3u);
}

TEST_F(DBLoweringTest, limitEmitsAllRowsWhenItExceedsNodeCount) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeSink sink;
    const std::string program = limitScanProgram(10);
    runLoweredProgram(program.c_str(), reader.getView(), sink);

    // LIMIT exceeds the four nodes, so every node is emitted, none dropped.
    const std::vector<std::vector<uint64_t>> expected {{0}, {1}, {2}, {3}};
    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, limitZeroEmitsNothing) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CountingSink sink;
    const std::string program = limitScanProgram(0);
    runLoweredProgram(program.c_str(), reader.getView(), sink);

    // LIMIT 0: the loop guard is false on entry, so nothing is scanned or emitted.
    EXPECT_EQ(sink.getCalls(), 0u);
    EXPECT_EQ(sink.getTotalRows(), 0u);
}

TEST_F(DBLoweringTest, limitEmitsPrefixAcrossChunkBoundary) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Four nodes in chunks of two, LIMIT 3: the first chunk emits its two rows,
    // the second emits a one-row prefix of its two, then the loop breaks - three
    // rows over two appendChunks calls, proving the clamped prefix emit.
    CountingSink sink;
    const std::string program = limitScanProgram(3);
    runLoweredProgram(program.c_str(), reader.getView(), sink, /*chunkSize=*/2);

    EXPECT_EQ(sink.getCalls(), 2u);
    EXPECT_EQ(sink.getTotalRows(), 3u);
}

TEST_F(DBLoweringTest, limitTerminatesScanEarly) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Unlimited, the scan fills both chunks of two over four nodes: two calls.
    CountingSink unlimited;
    runLoweredProgram(scanProgram, reader.getView(), unlimited, /*chunkSize=*/2);
    EXPECT_EQ(unlimited.getCalls(), 2u);
    EXPECT_EQ(unlimited.getTotalRows(), 4u);

    // LIMIT 2 is spent after the first chunk, so the loop breaks before filling
    // the second: one call, two rows - fewer chunks than the unlimited run.
    CountingSink limited;
    const std::string program = limitScanProgram(2);
    runLoweredProgram(program.c_str(), reader.getView(), limited, /*chunkSize=*/2);
    EXPECT_EQ(limited.getCalls(), 1u);
    EXPECT_EQ(limited.getTotalRows(), 2u);
    EXPECT_LT(limited.getCalls(), unlimited.getCalls());
}

TEST_F(DBLoweringTest, limitExceedingCountSpansMultipleChunks) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // LIMIT 10 over four nodes in chunks of two: the budget never runs out, so
    // both full chunks are emitted across the boundary and the loop runs to
    // exhaustion - two calls, four rows, the limit never clamping a chunk.
    CountingSink sink;
    const std::string program = limitScanProgram(10);
    runLoweredProgram(program.c_str(), reader.getView(), sink, /*chunkSize=*/2);

    EXPECT_EQ(sink.getCalls(), 2u);
    EXPECT_EQ(sink.getTotalRows(), 4u);
}

TEST_F(DBLoweringTest, limitsTwoHopTraversal) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // The diamond's two two-hop paths both end at node 3. LIMIT 1 keeps one, so
    // a single (c) row, which is node 3 either way - order-independent.
    CollectingNodeSink sink;
    const std::string program = limitTwoHopProgram(1);
    runLoweredProgram(program.c_str(), reader.getView(), sink);

    const std::vector<std::vector<uint64_t>> expected {{3}};
    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, limitsCrossProduct) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Four nodes crossed with four is sixteen pairs; LIMIT 5 keeps five. The
    // representative is the post-cross-product column, so the count is right.
    CountingSink sink;
    const std::string program = limitCrossProductProgram(5);
    runLoweredProgram(program.c_str(), reader.getView(), sink);

    EXPECT_EQ(sink.getTotalRows(), 5u);
}

TEST_F(DBLoweringTest, limitedCrossProductBuildsOnlyTheBudgetPrefix) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Four nodes crossed with four is sixteen pairs; both scans fit in one chunk,
    // so the product is one cross product. Without the cap it would broadcast all
    // sixteen rows into its output columns and let nl.output emit only the first
    // five; with the cap it builds just the five-row prefix the limit can emit.
    // The widest chunk handed to the sink is the materialized column size, so it
    // is five (the budget), not sixteen (the full product).
    CountingSink sink;
    const std::string program = limitCrossProductProgram(5);
    runLoweredProgram(program.c_str(), reader.getView(), sink);

    EXPECT_EQ(sink.getTotalRows(), 5u);
    EXPECT_EQ(sink.getWidestChunk(), 5u);
}

TEST_F(DBLoweringTest, lowersLimitToHandleUpdateAndLoopOperands) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    mlir::MLIRContext context;
    context.getOrLoadDialect<mlir::func::FuncDialect>();
    context.getOrLoadDialect<mlir::storage::Storage>();
    context.getOrLoadDialect<mlir::db::DB>();
    context.getOrLoadDialect<mlir::nl::NL>();

    const mlir::ParserConfig parserConfig(&context);
    const std::string programText = limitTwoHopProgram(3);
    mlir::OwningOpRef<mlir::ModuleOp> dbModule = mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
    ASSERT_TRUE(dbModule);

    const mlir::func::FuncOp dbFunction = dbModule->lookupSymbol<mlir::func::FuncOp>("main");
    ASSERT_TRUE(dbFunction);

    mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&context));
    DBLowering lowering(&context, &reader.getView());
    lowering.lower(dbFunction, *nlModule);

    // A terminal LIMIT: the limit feeds db.output directly, so foldTruncatesIntoOutputs
    // folds the truncate into a limit-bearing output. Result: one hoisted nl.limit,
    // one nl.limit_update, NO nl.limit_truncate (folded away), a three-deep loop
    // nest all carrying the handle, and an nl.output that carries it too.
    size_t limitCount = 0;
    size_t forCount = 0;
    size_t forsWithLimit = 0;
    size_t updateCount = 0;
    size_t truncateCount = 0;
    mlir::nl::Limit limitOp;
    mlir::nl::LimitUpdate updateOp;
    mlir::nl::Output outputOp;

    nlModule->walk([&](mlir::Operation* operation) {
        if (mlir::nl::Limit found = mlir::dyn_cast<mlir::nl::Limit>(operation)) {
            limitOp = found;
            limitCount++;
        } else if (mlir::nl::For forOp = mlir::dyn_cast<mlir::nl::For>(operation)) {
            forCount++;
            if (forOp.getLimit()) {
                forsWithLimit++;
            }
        } else if (mlir::nl::LimitUpdate found = mlir::dyn_cast<mlir::nl::LimitUpdate>(operation)) {
            updateOp = found;
            updateCount++;
        } else if (mlir::isa<mlir::nl::LimitTruncate>(operation)) {
            truncateCount++;
        } else if (mlir::nl::Output found = mlir::dyn_cast<mlir::nl::Output>(operation)) {
            outputOp = found;
        }
    });

    EXPECT_EQ(limitCount, 1u);
    EXPECT_EQ(forCount, 3u);
    EXPECT_EQ(forsWithLimit, 3u);
    EXPECT_EQ(updateCount, 1u);
    EXPECT_EQ(truncateCount, 0u);
    ASSERT_TRUE(limitOp);
    ASSERT_TRUE(updateOp);
    ASSERT_TRUE(outputOp);

    // Every for, the update and the folded output all name the one hoisted handle.
    const mlir::Value handle = limitOp.getState();
    nlModule->walk([&](mlir::nl::For forOp) {
        EXPECT_EQ(forOp.getLimit(), handle);
    });
    EXPECT_EQ(updateOp.getState(), handle);
    EXPECT_EQ(outputOp.getLimit(), handle);

    // The update sits in the innermost loop body, the same block as the output.
    EXPECT_EQ(updateOp->getBlock(), outputOp->getBlock());
    EXPECT_TRUE(mlir::isa<mlir::nl::For>(updateOp->getParentOp()));
}

TEST_F(DBLoweringTest, limitsChainedMidQueryFansOutBeyondBudget) {
    auto graph = buildRegularOutGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // WITH a LIMIT 2 caps the scan to two nodes; each has out-degree two, so the
    // expansion emits four b rows. The downstream fan-out is NOT clamped to the
    // budget - four rows from a LIMIT of two proves the limit bounds the
    // intermediate `a`, not the final output.
    CountingSink sink;
    const std::string program = chainedLimitProgram(2);
    runLoweredProgram(program.c_str(), reader.getView(), sink);

    EXPECT_EQ(sink.getTotalRows(), 4u);
}

TEST_F(DBLoweringTest, lowersChainedLimitToProducerLoopOnly) {
    auto graph = buildRegularOutGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    mlir::MLIRContext context;
    context.getOrLoadDialect<mlir::func::FuncDialect>();
    context.getOrLoadDialect<mlir::storage::Storage>();
    context.getOrLoadDialect<mlir::db::DB>();
    context.getOrLoadDialect<mlir::nl::NL>();

    const mlir::ParserConfig parserConfig(&context);
    const std::string programText = chainedLimitProgram(2);
    mlir::OwningOpRef<mlir::ModuleOp> dbModule = mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
    ASSERT_TRUE(dbModule);

    const mlir::func::FuncOp dbFunction = dbModule->lookupSymbol<mlir::func::FuncOp>("main");
    ASSERT_TRUE(dbFunction);

    mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&context));
    DBLowering lowering(&context, &reader.getView());
    lowering.lower(dbFunction, *nlModule);

    // One handle, one update, one truncate, two loops (scan + edges), but only the
    // scan - the producer of the limited `a` - carries the handle. The edge loop
    // is a consumer and must fan out freely, so it carries none.
    size_t limitCount = 0;
    size_t updateCount = 0;
    size_t truncateCount = 0;
    size_t forCount = 0;
    size_t forsWithLimit = 0;

    nlModule->walk([&](mlir::Operation* operation) {
        if (mlir::isa<mlir::nl::Limit>(operation)) {
            limitCount++;
        } else if (mlir::isa<mlir::nl::LimitUpdate>(operation)) {
            updateCount++;
        } else if (mlir::isa<mlir::nl::LimitTruncate>(operation)) {
            truncateCount++;
        } else if (mlir::nl::For forOp = mlir::dyn_cast<mlir::nl::For>(operation)) {
            forCount++;
            if (forOp.getLimit()) {
                forsWithLimit++;
            }
        }
    });

    EXPECT_EQ(limitCount, 1u);
    EXPECT_EQ(updateCount, 1u);
    EXPECT_EQ(truncateCount, 1u);
    EXPECT_EQ(forCount, 2u);
    EXPECT_EQ(forsWithLimit, 1u);

    // The producing (scan) loop carries the handle; the consuming (edge) loop does
    // not, distinguished by the source op its iterator comes from.
    nlModule->walk([&](mlir::nl::For forOp) {
        mlir::Operation* const iteratorDef = forOp.getIterator().getDefiningOp();
        if (mlir::isa<mlir::nl::ScanNodes>(iteratorDef)) {
            EXPECT_TRUE(forOp.getLimit());
        } else {
            EXPECT_FALSE(forOp.getLimit());
        }
    });
}

TEST_F(DBLoweringTest, limitsTwoIndependentLimits) {
    auto graph = buildRegularOutGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // WITH a LIMIT 2 caps the scan to two nodes (four out-edges between them), then
    // WITH b LIMIT 3 caps the expansion to three. Each budget is independent, so
    // the final output is three rows: min(2*2, 3).
    CountingSink sink;
    const std::string program = twoLimitProgram(2, 3);
    runLoweredProgram(program.c_str(), reader.getView(), sink);

    EXPECT_EQ(sink.getTotalRows(), 3u);
}

TEST_F(DBLoweringTest, lowersTwoLimitsToTwoHandles) {
    auto graph = buildRegularOutGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    mlir::MLIRContext context;
    context.getOrLoadDialect<mlir::func::FuncDialect>();
    context.getOrLoadDialect<mlir::storage::Storage>();
    context.getOrLoadDialect<mlir::db::DB>();
    context.getOrLoadDialect<mlir::nl::NL>();

    const mlir::ParserConfig parserConfig(&context);
    const std::string programText = twoLimitProgram(2, 3);
    mlir::OwningOpRef<mlir::ModuleOp> dbModule = mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
    ASSERT_TRUE(dbModule);

    const mlir::func::FuncOp dbFunction = dbModule->lookupSymbol<mlir::func::FuncOp>("main");
    ASSERT_TRUE(dbFunction);

    mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&context));
    DBLowering lowering(&context, &reader.getView());
    lowering.lower(dbFunction, *nlModule);

    // Two db.limits, so two handles and two updates. Each loop carries a handle:
    // the scan carries the outer (a) limit, the edge loop the inner (b) one - and
    // they are distinct. The inner limit feeds db.output, so its truncate folds
    // into a limit-bearing output; the outer limit feeds the expansion, so its
    // truncate stays. One truncate remains, and the output carries the inner handle.
    size_t limitCount = 0;
    size_t updateCount = 0;
    size_t truncateCount = 0;
    size_t forsWithLimit = 0;
    mlir::Value scanHandle;
    mlir::Value edgeHandle;
    mlir::nl::Output outputOp;

    nlModule->walk([&](mlir::Operation* operation) {
        if (mlir::isa<mlir::nl::Limit>(operation)) {
            limitCount++;
        } else if (mlir::isa<mlir::nl::LimitUpdate>(operation)) {
            updateCount++;
        } else if (mlir::isa<mlir::nl::LimitTruncate>(operation)) {
            truncateCount++;
        } else if (mlir::nl::Output found = mlir::dyn_cast<mlir::nl::Output>(operation)) {
            outputOp = found;
        } else if (mlir::nl::For forOp = mlir::dyn_cast<mlir::nl::For>(operation)) {
            if (forOp.getLimit()) {
                forsWithLimit++;
            }

            mlir::Operation* const iteratorDef = forOp.getIterator().getDefiningOp();
            if (mlir::isa<mlir::nl::ScanNodes>(iteratorDef)) {
                scanHandle = forOp.getLimit();
            } else {
                edgeHandle = forOp.getLimit();
            }
        }
    });

    EXPECT_EQ(limitCount, 2u);
    EXPECT_EQ(updateCount, 2u);
    EXPECT_EQ(truncateCount, 1u);
    EXPECT_EQ(forsWithLimit, 2u);
    ASSERT_TRUE(scanHandle);
    ASSERT_TRUE(edgeHandle);
    EXPECT_NE(scanHandle, edgeHandle);

    // The folded output carries the inner (edge-loop) limit handle.
    ASSERT_TRUE(outputOp);
    EXPECT_EQ(outputOp.getLimit(), edgeHandle);
}

TEST_F(DBLoweringTest, limitsNodePropertyOutput) {
    auto graph = buildPropertyGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Three nodes carry (or lack) a score; LIMIT 2 keeps two (node, score) rows,
    // the node ID column and the nullable value column clamped together.
    CollectingNodeIntPropSink sink;
    const std::string program = limitNodePropertyProgram(2);
    runLoweredProgram(program.c_str(), reader.getView(), sink);

    std::vector<std::pair<uint64_t, std::optional<int64_t>>> rows;
    sink.sortedRows(rows);
    ASSERT_EQ(rows.size(), 2u);

    // Each surviving row pairs a node with its own score (same chunk index), so
    // every emitted pair belongs to the known result set - proving the value
    // column was clamped in step with the node IDs, not misaligned.
    const std::vector<std::pair<uint64_t, std::optional<int64_t>>> all {
        {0, 100}, {1, 200}, {2, std::nullopt}
    };
    for (const std::pair<uint64_t, std::optional<int64_t>>& row : rows) {
        EXPECT_NE(std::find(all.begin(), all.end(), row), all.end());
    }
}

TEST_F(DBLoweringTest, limitsNodePropertyOutputEmitsAllWhenExceedingCount) {
    auto graph = buildPropertyGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // LIMIT exceeds the three nodes, so every (node, score) row survives, with
    // node 2's missing score still coming back null - the limit path does not
    // disturb the property values.
    CollectingNodeIntPropSink sink;
    const std::string program = limitNodePropertyProgram(10);
    runLoweredProgram(program.c_str(), reader.getView(), sink);

    const std::vector<std::pair<uint64_t, std::optional<int64_t>>> expected {
        {0, 100}, {1, 200}, {2, std::nullopt}
    };
    std::vector<std::pair<uint64_t, std::optional<int64_t>>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, limitsWhenPropertyFetchIsRepresentative) {
    auto graph = buildPropertyGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // RETURN a.score makes the property fetch result the sole output column and
    // thus db.limit's representative - an op result, not a loop variable. LIMIT 2
    // over three nodes keeps two rows.
    CountingSink sink;
    const std::string program = limitOnlyPropertyProgram(2);
    runLoweredProgram(program.c_str(), reader.getView(), sink);

    EXPECT_EQ(sink.getTotalRows(), 2u);
}

TEST_F(DBLoweringTest, skipsScanByDroppingPrefix) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeSink sink;
    const std::string program = skipScanProgram(3);
    runLoweredProgram(program.c_str(), reader.getView(), sink);

    // Four nodes, SKIP 3: one row survives (the first three scanned are dropped).
    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows.size(), 1u);
}

TEST_F(DBLoweringTest, skipZeroEmitsAllRows) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeSink sink;
    const std::string program = skipScanProgram(0);
    runLoweredProgram(program.c_str(), reader.getView(), sink);

    // SKIP 0 drops nothing, so every node is emitted.
    const std::vector<std::vector<uint64_t>> expected {{0}, {1}, {2}, {3}};
    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, skipExceedingNodeCountEmitsNothing) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // SKIP 10 over four nodes: the whole scan is dropped, so nothing is emitted.
    // The scan still runs to exhaustion (a skip never early-exits), but every step
    // emits a zero-row suffix.
    CountingSink sink;
    const std::string program = skipScanProgram(10);
    runLoweredProgram(program.c_str(), reader.getView(), sink);

    EXPECT_EQ(sink.getTotalRows(), 0u);
}

TEST_F(DBLoweringTest, skipEmitsSuffixAcrossChunkBoundary) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Four nodes in chunks of two, SKIP 1: the first chunk drops one of its two
    // rows and emits a one-row suffix, the second drops none and emits both - three
    // rows over two appendChunks calls. Unlike a limit, the scan runs to
    // exhaustion: a skip never terminates the loop early.
    CountingSink sink;
    const std::string program = skipScanProgram(1);
    runLoweredProgram(program.c_str(), reader.getView(), sink, /*chunkSize=*/2);

    EXPECT_EQ(sink.getCalls(), 2u);
    EXPECT_EQ(sink.getTotalRows(), 3u);
}

TEST_F(DBLoweringTest, skipsTwoHopTraversal) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // The diamond has two two-hop paths, both ending at node 3. SKIP 1 drops one,
    // leaving a single (c) row, which is node 3 either way - order-independent.
    CollectingNodeSink sink;
    const std::string program = skipTwoHopProgram(1);
    runLoweredProgram(program.c_str(), reader.getView(), sink);

    const std::vector<std::vector<uint64_t>> expected {{3}};
    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, skipsCrossProduct) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Four nodes crossed with four is sixteen pairs; SKIP 5 drops the first five,
    // leaving eleven. The representative is the post-cross-product column, so the
    // count is right.
    CountingSink sink;
    const std::string program = skipCrossProductProgram(5);
    runLoweredProgram(program.c_str(), reader.getView(), sink);

    EXPECT_EQ(sink.getTotalRows(), 11u);
}

TEST_F(DBLoweringTest, skipsNodePropertyOutput) {
    auto graph = buildPropertyGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Three nodes carry (or lack) a score; SKIP 1 drops one, leaving two
    // (node, score) rows, the node ID column and the nullable value column lifted
    // together. Each surviving row pairs a node with its own score, so every
    // emitted pair belongs to the known result set - proving the value column was
    // copied in step with the node IDs, not misaligned.
    CollectingNodeIntPropSink sink;
    const std::string program = skipNodePropertyProgram(1);
    runLoweredProgram(program.c_str(), reader.getView(), sink);

    std::vector<std::pair<uint64_t, std::optional<int64_t>>> rows;
    sink.sortedRows(rows);
    ASSERT_EQ(rows.size(), 2u);

    const std::vector<std::pair<uint64_t, std::optional<int64_t>>> all {
        {0, 100}, {1, 200}, {2, std::nullopt}
    };
    for (const std::pair<uint64_t, std::optional<int64_t>>& row : rows) {
        EXPECT_NE(std::find(all.begin(), all.end(), row), all.end());
    }
}

TEST_F(DBLoweringTest, skipsNodePropertyOutputEmitsAllWhenZero) {
    auto graph = buildPropertyGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // SKIP 0 drops nothing, so every (node, score) row survives, with node 2's
    // missing score still null - the skip path does not disturb the property values.
    CollectingNodeIntPropSink sink;
    const std::string program = skipNodePropertyProgram(0);
    runLoweredProgram(program.c_str(), reader.getView(), sink);

    const std::vector<std::pair<uint64_t, std::optional<int64_t>>> expected {
        {0, 100}, {1, 200}, {2, std::nullopt}
    };
    std::vector<std::pair<uint64_t, std::optional<int64_t>>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, lowersTerminalSkipToFoldedOutput) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    mlir::MLIRContext context;
    context.getOrLoadDialect<mlir::func::FuncDialect>();
    context.getOrLoadDialect<mlir::storage::Storage>();
    context.getOrLoadDialect<mlir::db::DB>();
    context.getOrLoadDialect<mlir::nl::NL>();

    const mlir::ParserConfig parserConfig(&context);
    const std::string programText = skipTwoHopProgram(3);
    mlir::OwningOpRef<mlir::ModuleOp> dbModule = mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
    ASSERT_TRUE(dbModule);

    const mlir::func::FuncOp dbFunction = dbModule->lookupSymbol<mlir::func::FuncOp>("main");
    ASSERT_TRUE(dbFunction);

    mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&context));
    DBLowering lowering(&context, &reader.getView());
    lowering.lower(dbFunction, *nlModule);

    // A terminal SKIP: the skip feeds db.output directly, so foldSkipTruncatesIntoOutputs
    // folds the truncate into a skip-bearing output that emits the surviving suffix
    // in place at an offset. Result: one hoisted nl.skip, one nl.skip_update, NO
    // nl.skip_truncate (folded away), a three-deep loop nest where NO nl.for carries
    // a handle (a skip never gates a loop), and an nl.output that carries the skip
    // handle but no limit.
    size_t skipCount = 0;
    size_t forCount = 0;
    size_t forsWithLimit = 0;
    size_t updateCount = 0;
    size_t truncateCount = 0;
    mlir::nl::Skip skipOp;
    mlir::nl::SkipUpdate updateOp;
    mlir::nl::Output outputOp;

    nlModule->walk([&](mlir::Operation* operation) {
        if (mlir::nl::Skip found = mlir::dyn_cast<mlir::nl::Skip>(operation)) {
            skipOp = found;
            skipCount++;
        } else if (mlir::nl::For forOp = mlir::dyn_cast<mlir::nl::For>(operation)) {
            forCount++;
            if (forOp.getLimit()) {
                forsWithLimit++;
            }
        } else if (mlir::nl::SkipUpdate found = mlir::dyn_cast<mlir::nl::SkipUpdate>(operation)) {
            updateOp = found;
            updateCount++;
        } else if (mlir::isa<mlir::nl::SkipTruncate>(operation)) {
            truncateCount++;
        } else if (mlir::nl::Output found = mlir::dyn_cast<mlir::nl::Output>(operation)) {
            outputOp = found;
        }
    });

    EXPECT_EQ(skipCount, 1u);
    EXPECT_EQ(forCount, 3u);
    EXPECT_EQ(forsWithLimit, 0u);
    EXPECT_EQ(updateCount, 1u);
    EXPECT_EQ(truncateCount, 0u);
    ASSERT_TRUE(skipOp);
    ASSERT_TRUE(updateOp);
    ASSERT_TRUE(outputOp);

    // The update and the folded output both name the one hoisted handle; the output
    // carries the skip handle in its skip operand and no limit.
    const mlir::Value handle = skipOp.getState();
    EXPECT_EQ(updateOp.getState(), handle);
    EXPECT_EQ(outputOp.getSkip(), handle);
    EXPECT_FALSE(outputOp.getLimit());

    // The update sits in the innermost loop body, the same block as the output.
    EXPECT_EQ(updateOp->getBlock(), outputOp->getBlock());
    EXPECT_TRUE(mlir::isa<mlir::nl::For>(updateOp->getParentOp()));
}

TEST_F(DBLoweringTest, skipThenLimitPagesTheScan) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Four nodes, SKIP 1 LIMIT 2: drop the first row, then keep the next two -
    // min(4 - 1, 2) = 2 rows. This exercises the two ops stacked, the skip's suffix
    // copy feeding the limit's representative inside the same loop body.
    CountingSink sink;
    const std::string program = skipThenLimitProgram(1, 2);
    runLoweredProgram(program.c_str(), reader.getView(), sink);

    EXPECT_EQ(sink.getTotalRows(), 2u);
}

TEST_F(DBLoweringTest, skipThenLimitStableAcrossChunkSizes) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // SKIP 1 LIMIT 2 over four nodes is the page [position 1, position 3) of the
    // scan. Chunking only batches the scan, never reorders it, so the page must be
    // the same two rows whatever the chunk size - even when the skip's drop and the
    // limit's cut straddle a chunk boundary (chunkSize 1: drop in chunk 0, then one
    // kept row each in chunks 1 and 2; chunkSize 2: drop one of chunk 0's two rows,
    // emit its suffix, then a one-row prefix of chunk 1). The reference is the
    // result at a chunk size that holds the whole scan in one chunk.
    const std::string program = skipThenLimitProgram(1, 2);

    CollectingNodeSink wholeScanSink;
    runLoweredProgram(program.c_str(), reader.getView(), wholeScanSink, /*chunkSize=*/4);
    std::vector<std::vector<uint64_t>> expected;
    wholeScanSink.sortedRows(expected);
    ASSERT_EQ(expected.size(), 2u);

    for (size_t chunkSize = 1; chunkSize <= 5; chunkSize++) {
        CollectingNodeSink sink;
        runLoweredProgram(program.c_str(), reader.getView(), sink, chunkSize);

        std::vector<std::vector<uint64_t>> rows;
        sink.sortedRows(rows);
        EXPECT_EQ(rows, expected) << "page differs at chunkSize " << chunkSize;
    }
}

TEST_F(DBLoweringTest, skipThenLimitFoldsLimitButKeepsSkipCopy) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    mlir::MLIRContext context;
    context.getOrLoadDialect<mlir::func::FuncDialect>();
    context.getOrLoadDialect<mlir::storage::Storage>();
    context.getOrLoadDialect<mlir::db::DB>();
    context.getOrLoadDialect<mlir::nl::NL>();

    const mlir::ParserConfig parserConfig(&context);
    const std::string programText = skipThenLimitProgram(1, 2);
    mlir::OwningOpRef<mlir::ModuleOp> dbModule = mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
    ASSERT_TRUE(dbModule);

    const mlir::func::FuncOp dbFunction = dbModule->lookupSymbol<mlir::func::FuncOp>("main");
    ASSERT_TRUE(dbFunction);

    mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&context));
    DBLowering lowering(&context, &reader.getView());
    lowering.lower(dbFunction, *nlModule);

    // SKIP 1 LIMIT 2: the skip's nl.skip_truncate feeds nl.limit_update (and, after
    // the limit folds, the output too) - two uses, never one nl.output - so the skip
    // fold's hasOneUse test bails and the skip copy stays. The limit, adjacent to the
    // output, folds. Result: one nl.skip + one surviving nl.skip_truncate, one
    // nl.limit + NO nl.limit_truncate, and an nl.output that carries the limit handle
    // but no skip. The scan loop carries the limit (its early-exit bounds the skip
    // copy's cost).
    size_t skipTruncateCount = 0;
    size_t limitTruncateCount = 0;
    size_t skipCount = 0;
    size_t limitCount = 0;
    size_t forsWithLimit = 0;
    mlir::nl::Limit limitOp;
    mlir::nl::Output outputOp;

    nlModule->walk([&](mlir::Operation* operation) {
        if (mlir::nl::Limit found = mlir::dyn_cast<mlir::nl::Limit>(operation)) {
            limitOp = found;
            limitCount++;
        } else if (mlir::isa<mlir::nl::Skip>(operation)) {
            skipCount++;
        } else if (mlir::isa<mlir::nl::SkipTruncate>(operation)) {
            skipTruncateCount++;
        } else if (mlir::isa<mlir::nl::LimitTruncate>(operation)) {
            limitTruncateCount++;
        } else if (mlir::nl::For forOp = mlir::dyn_cast<mlir::nl::For>(operation)) {
            if (forOp.getLimit()) {
                forsWithLimit++;
            }
        } else if (mlir::nl::Output found = mlir::dyn_cast<mlir::nl::Output>(operation)) {
            outputOp = found;
        }
    });

    EXPECT_EQ(skipCount, 1u);
    EXPECT_EQ(limitCount, 1u);
    EXPECT_EQ(skipTruncateCount, 1u);
    EXPECT_EQ(limitTruncateCount, 0u);
    EXPECT_EQ(forsWithLimit, 1u);
    ASSERT_TRUE(limitOp);
    ASSERT_TRUE(outputOp);

    // The output folded the limit (not the skip): it carries the limit handle and no
    // skip. The skip copy survives upstream, feeding the limit's representative.
    EXPECT_EQ(outputOp.getLimit(), limitOp.getState());
    EXPECT_FALSE(outputOp.getSkip());
}

TEST_F(DBLoweringTest, skipsChainedMidQueryFansOutFromSurvivors) {
    auto graph = buildRegularOutGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // WITH a SKIP 1 drops one of the four nodes; the three survivors each have
    // out-degree two, so the expansion emits six b rows. The downstream fan-out is
    // NOT itself skipped - six rows from a SKIP of one proves the skip bounds the
    // intermediate `a`, with the edge loop a limit/skip-oblivious consumer.
    CountingSink sink;
    const std::string program = chainedSkipProgram(1);
    runLoweredProgram(program.c_str(), reader.getView(), sink);

    EXPECT_EQ(sink.getTotalRows(), 6u);
}

TEST_F(DBLoweringTest, lowersChainedSkipWithConsumerEdgeLoop) {
    auto graph = buildRegularOutGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    mlir::MLIRContext context;
    context.getOrLoadDialect<mlir::func::FuncDialect>();
    context.getOrLoadDialect<mlir::storage::Storage>();
    context.getOrLoadDialect<mlir::db::DB>();
    context.getOrLoadDialect<mlir::nl::NL>();

    const mlir::ParserConfig parserConfig(&context);
    const std::string programText = chainedSkipProgram(1);
    mlir::OwningOpRef<mlir::ModuleOp> dbModule = mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
    ASSERT_TRUE(dbModule);

    const mlir::func::FuncOp dbFunction = dbModule->lookupSymbol<mlir::func::FuncOp>("main");
    ASSERT_TRUE(dbFunction);

    mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&context));
    DBLowering lowering(&context, &reader.getView());
    lowering.lower(dbFunction, *nlModule);

    // One skip, one update, one truncate, two loops (scan + edges) - and NO loop
    // carries a handle (a skip never gates a loop). The truncate stays (no fold for
    // skip), and the edge loop is built inside the scan body, after the truncate,
    // consuming the cut chunk - so it fans out freely.
    size_t skipCount = 0;
    size_t updateCount = 0;
    size_t truncateCount = 0;
    size_t forsWithLimit = 0;
    mlir::nl::SkipTruncate truncateOp;
    mlir::nl::For edgeLoop;

    nlModule->walk([&](mlir::Operation* operation) {
        if (mlir::isa<mlir::nl::Skip>(operation)) {
            skipCount++;
        } else if (mlir::isa<mlir::nl::SkipUpdate>(operation)) {
            updateCount++;
        } else if (mlir::nl::SkipTruncate found = mlir::dyn_cast<mlir::nl::SkipTruncate>(operation)) {
            truncateOp = found;
            truncateCount++;
        } else if (mlir::nl::For forOp = mlir::dyn_cast<mlir::nl::For>(operation)) {
            if (forOp.getLimit()) {
                forsWithLimit++;
            }

            mlir::Operation* const iteratorDef = forOp.getIterator().getDefiningOp();
            if (mlir::isa<mlir::nl::GetOutEdges>(iteratorDef)) {
                edgeLoop = forOp;
            }
        }
    });

    EXPECT_EQ(skipCount, 1u);
    EXPECT_EQ(updateCount, 1u);
    EXPECT_EQ(truncateCount, 1u);
    EXPECT_EQ(forsWithLimit, 0u);
    ASSERT_TRUE(truncateOp);
    ASSERT_TRUE(edgeLoop);

    // The consuming edge loop is nested in the scan body, downstream of the truncate
    // that feeds it - the chained shape, where the cut chunk drives the fan-out.
    EXPECT_EQ(edgeLoop->getBlock(), truncateOp->getBlock());
    EXPECT_TRUE(edgeLoop.getIterator().getDefiningOp<mlir::nl::GetOutEdges>());
}

TEST_F(DBLoweringTest, sortsNodesByIdDescending) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeSink sink;
    runLoweredProgram(sortNodesDescProgram, reader.getView(), sink);

    // Four nodes sorted descending: the emit order is exactly 3, 2, 1, 0,
    // regardless of the order the scan produced them.
    const std::vector<std::vector<uint64_t>> expected {{3}, {2}, {1}, {0}};
    std::vector<std::vector<uint64_t>> rows;
    sink.orderedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, sortByIdDescendingAcrossChunkBoundaries) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // A chunk size of two over four nodes makes the scan collect two chunks and
    // the emit re-chunk into two, so the result proves accumulation across chunks
    // and the chunked emit both preserve the global sorted order.
    CollectingNodeSink sink;
    runLoweredProgram(sortNodesDescProgram, reader.getView(), sink, /*chunkSize=*/2);

    const std::vector<std::vector<uint64_t>> expected {{3}, {2}, {1}, {0}};
    std::vector<std::vector<uint64_t>> rows;
    sink.orderedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, sortsByPropertyAscendingNullsLast) {
    auto graph = buildPropertyGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Scores are 100 (node 0), 200 (node 1) and absent (node 2). Ascending, the
    // null sorts last: (0,100), (1,200), (2,null).
    CollectingNodeIntPropSink sink;
    runLoweredProgram(sortByScoreAscProgram, reader.getView(), sink);

    const std::vector<std::pair<uint64_t, std::optional<int64_t>>> expected {
        {0, 100}, {1, 200}, {2, std::nullopt}
    };
    std::vector<std::pair<uint64_t, std::optional<int64_t>>> rows;
    sink.orderedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, sortsByPropertyDescendingNullsFirst) {
    auto graph = buildPropertyGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Descending, the null score (greatest) leads: (2,null), (1,200), (0,100).
    CollectingNodeIntPropSink sink;
    runLoweredProgram(sortByScoreDescProgram, reader.getView(), sink);

    const std::vector<std::pair<uint64_t, std::optional<int64_t>>> expected {
        {2, std::nullopt}, {1, 200}, {0, 100}
    };
    std::vector<std::pair<uint64_t, std::optional<int64_t>>> rows;
    sink.orderedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, sortsByPropertyAcrossChunkBoundaries) {
    auto graph = buildPropertyGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // chunkSize 1 forces the null-scored node into its own chunk and refills the
    // value buffer over three steps; the sorted result must not depend on it.
    CollectingNodeIntPropSink sink;
    runLoweredProgram(sortByScoreAscProgram, reader.getView(), sink, /*chunkSize=*/1);

    const std::vector<std::pair<uint64_t, std::optional<int64_t>>> expected {
        {0, 100}, {1, 200}, {2, std::nullopt}
    };
    std::vector<std::pair<uint64_t, std::optional<int64_t>>> rows;
    sink.orderedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, sortsByTwoKeys) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // The diamond's edges are (0,1), (0,2), (1,3), (2,3). Sorting by source
    // ascending then target descending groups by source and, within source 0,
    // orders the targets 2 then 1: (0,2), (0,1), (1,3), (2,3).
    CollectingNodeSink sink;
    runLoweredProgram(sortEdgesMultiKeyProgram, reader.getView(), sink);

    const std::vector<std::vector<uint64_t>> expected {{0, 2}, {0, 1}, {1, 3}, {2, 3}};
    std::vector<std::vector<uint64_t>> rows;
    sink.orderedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, sortOfEmptyGraphEmitsNothing) {
    auto graph = Graph::create();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // No rows to accumulate: the emit loop runs zero times, so nothing is output.
    CountingSink sink;
    runLoweredProgram(sortNodesDescProgram, reader.getView(), sink);

    EXPECT_EQ(sink.getCalls(), 0u);
    EXPECT_EQ(sink.getTotalRows(), 0u);
}

TEST_F(DBLoweringTest, lowersSortToBufferCollectAndEmitLoop) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    mlir::MLIRContext context;
    context.getOrLoadDialect<mlir::func::FuncDialect>();
    context.getOrLoadDialect<mlir::storage::Storage>();
    context.getOrLoadDialect<mlir::db::DB>();
    context.getOrLoadDialect<mlir::nl::NL>();

    const mlir::ParserConfig parserConfig(&context);
    mlir::OwningOpRef<mlir::ModuleOp> dbModule = mlir::parseSourceString<mlir::ModuleOp>(sortNodesDescProgram, parserConfig);
    ASSERT_TRUE(dbModule);

    const mlir::func::FuncOp dbFunction = dbModule->lookupSymbol<mlir::func::FuncOp>("main");
    ASSERT_TRUE(dbFunction);

    mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&context));
    DBLowering lowering(&context, &reader.getView());
    lowering.lower(dbFunction, *nlModule);

    // db.sort lowers to one hoisted nl.sort_buffer, one nl.sort_collect in the
    // producing (scan) loop, one nl.sort source op, and two nl.for loops: the
    // producing scan loop and the emit loop over nl.sort.
    size_t bufferCount = 0;
    size_t collectCount = 0;
    size_t sortCount = 0;
    size_t forCount = 0;
    mlir::nl::SortBuffer bufferOp;
    mlir::nl::SortCollect collectOp;
    mlir::nl::Sort sortOp;

    nlModule->walk([&](mlir::Operation* operation) {
        if (mlir::nl::SortBuffer found = mlir::dyn_cast<mlir::nl::SortBuffer>(operation)) {
            bufferOp = found;
            bufferCount++;
        } else if (mlir::nl::SortCollect found = mlir::dyn_cast<mlir::nl::SortCollect>(operation)) {
            collectOp = found;
            collectCount++;
        } else if (mlir::nl::Sort found = mlir::dyn_cast<mlir::nl::Sort>(operation)) {
            sortOp = found;
            sortCount++;
        } else if (mlir::isa<mlir::nl::For>(operation)) {
            forCount++;
        }
    });

    EXPECT_EQ(bufferCount, 1u);
    EXPECT_EQ(collectCount, 1u);
    EXPECT_EQ(sortCount, 1u);
    EXPECT_EQ(forCount, 2u);
    ASSERT_TRUE(bufferOp);
    ASSERT_TRUE(collectOp);
    ASSERT_TRUE(sortOp);

    // The collect, the sort source and the emit loop all name the one hoisted
    // accumulator handle.
    const mlir::Value handle = bufferOp.getState();
    EXPECT_EQ(collectOp.getState(), handle);
    EXPECT_EQ(sortOp.getState(), handle);

    // The single sort key is column 0 (the node IDs), descending.
    ASSERT_EQ(bufferOp.getKeyColumns().size(), 1u);
    EXPECT_EQ(bufferOp.getKeyColumns()[0], 0);
    ASSERT_EQ(bufferOp.getKeyAscending().size(), 1u);
    EXPECT_FALSE(bufferOp.getKeyAscending()[0]);

    // The collect runs inside an nl.for (the producing scan loop); the emit loop
    // iterates the nl.sort iterator.
    EXPECT_TRUE(mlir::isa<mlir::nl::For>(collectOp->getParentOp()));
    EXPECT_TRUE(sortOp.getResult().hasOneUse());
    EXPECT_TRUE(mlir::isa<mlir::nl::For>(*sortOp.getResult().user_begin()));
}

TEST_F(DBLoweringTest, topKNodesByIdDescending) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Four nodes, ORDER BY a DESC LIMIT 2: the two largest IDs, in order: 3, 2.
    CollectingNodeSink sink;
    const std::string program = topKNodesDescProgram(2);
    runLoweredProgram(program.c_str(), reader.getView(), sink);

    const std::vector<std::vector<uint64_t>> expected {{3}, {2}};
    std::vector<std::vector<uint64_t>> rows;
    sink.orderedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, topKTrimsAcrossChunkBoundaries) {
    // Six nodes (two dataparts) so the buffer grows past the 2 * topK = 4 trim
    // threshold; the four-node diamond alone never crosses it, so trimToTopK would
    // never run.
    auto graph = buildDiamondGraph();
    addSecondPart(*graph);

    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // chunkSize 1 over six nodes: the accumulator is fed one row at a time and
    // trims once it overflows the bound, so the top-2 must survive the trimming
    // and still come out 5, 4.
    CollectingNodeSink sink;
    const std::string program = topKNodesDescProgram(2);
    runLoweredProgram(program.c_str(), reader.getView(), sink, /*chunkSize=*/1);

    const std::vector<std::vector<uint64_t>> expected {{5}, {4}};
    std::vector<std::vector<uint64_t>> rows;
    sink.orderedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, topKExceedingInputSortsAll) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // LIMIT 10 over four nodes: the bound exceeds the input, so every node is
    // kept and the result is the full descending sort.
    CollectingNodeSink sink;
    const std::string program = topKNodesDescProgram(10);
    runLoweredProgram(program.c_str(), reader.getView(), sink);

    const std::vector<std::vector<uint64_t>> expected {{3}, {2}, {1}, {0}};
    std::vector<std::vector<uint64_t>> rows;
    sink.orderedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, topKByPropertyAscendingDropsNulls) {
    auto graph = buildPropertyGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Scores 100 (node 0), 200 (node 1), null (node 2). ASC LIMIT 2 keeps the two
    // smallest - 100 then 200 - so the null-scored node falls outside the bound.
    CollectingNodeIntPropSink sink;
    const std::string program = topKByScoreAscProgram(2);
    runLoweredProgram(program.c_str(), reader.getView(), sink);

    const std::vector<std::pair<uint64_t, std::optional<int64_t>>> expected {
        {0, 100}, {1, 200}
    };
    std::vector<std::pair<uint64_t, std::optional<int64_t>>> rows;
    sink.orderedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, topKByPropertyAscendingAcrossChunks) {
    auto graph = buildPropertyGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // chunkSize 1 forces per-row collection and trimming; the top-2 by ascending
    // score must be the same regardless of chunking.
    CollectingNodeIntPropSink sink;
    const std::string program = topKByScoreAscProgram(2);
    runLoweredProgram(program.c_str(), reader.getView(), sink, /*chunkSize=*/1);

    const std::vector<std::pair<uint64_t, std::optional<int64_t>>> expected {
        {0, 100}, {1, 200}
    };
    std::vector<std::pair<uint64_t, std::optional<int64_t>>> rows;
    sink.orderedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, topKZeroEmitsNothing) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // LIMIT 0 fused into the sort: the bounded accumulator keeps no rows, so the
    // emit loop runs zero times.
    CountingSink sink;
    const std::string program = topKNodesDescProgram(0);
    runLoweredProgram(program.c_str(), reader.getView(), sink);

    EXPECT_EQ(sink.getCalls(), 0u);
    EXPECT_EQ(sink.getTotalRows(), 0u);
}

TEST_F(DBLoweringTest, lowersTopKToBoundedSortBufferWithoutLimitOps) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    mlir::MLIRContext context;
    context.getOrLoadDialect<mlir::func::FuncDialect>();
    context.getOrLoadDialect<mlir::storage::Storage>();
    context.getOrLoadDialect<mlir::db::DB>();
    context.getOrLoadDialect<mlir::nl::NL>();

    const mlir::ParserConfig parserConfig(&context);
    const std::string programText = topKNodesDescProgram(2);
    mlir::OwningOpRef<mlir::ModuleOp> dbModule = mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
    ASSERT_TRUE(dbModule);

    const mlir::func::FuncOp dbFunction = dbModule->lookupSymbol<mlir::func::FuncOp>("main");
    ASSERT_TRUE(dbFunction);

    mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&context));
    DBLowering lowering(&context, &reader.getView());
    lowering.lower(dbFunction, *nlModule);

    // The terminal db.limit fuses into the sort: the bound lives on nl.sort_buffer
    // and the streaming limit ops vanish entirely. No nl.limit, no nl.limit_update,
    // no nl.limit_truncate; one bounded nl.sort_buffer; and the scan loop carries
    // no limit handle, since top-K must still scan every row.
    size_t bufferCount = 0;
    size_t limitCount = 0;
    size_t updateCount = 0;
    size_t truncateCount = 0;
    size_t forsWithLimit = 0;
    mlir::nl::SortBuffer bufferOp;

    nlModule->walk([&](mlir::Operation* operation) {
        if (mlir::nl::SortBuffer found = mlir::dyn_cast<mlir::nl::SortBuffer>(operation)) {
            bufferOp = found;
            bufferCount++;
        } else if (mlir::isa<mlir::nl::Limit>(operation)) {
            limitCount++;
        } else if (mlir::isa<mlir::nl::LimitUpdate>(operation)) {
            updateCount++;
        } else if (mlir::isa<mlir::nl::LimitTruncate>(operation)) {
            truncateCount++;
        } else if (mlir::nl::For forOp = mlir::dyn_cast<mlir::nl::For>(operation)) {
            if (forOp.getLimit()) {
                forsWithLimit++;
            }
        }
    });

    EXPECT_EQ(bufferCount, 1u);
    EXPECT_EQ(limitCount, 0u);
    EXPECT_EQ(updateCount, 0u);
    EXPECT_EQ(truncateCount, 0u);
    EXPECT_EQ(forsWithLimit, 0u);

    // The accumulator carries the fused bound as its top-K count.
    ASSERT_TRUE(bufferOp);
    ASSERT_TRUE(bufferOp.getTopK().has_value());
    EXPECT_EQ(*bufferOp.getTopK(), 2u);
}

TEST_F(DBLoweringTest, removesDuplicateTargets) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // The diamond's out-edges point at targets [1, 2, 3, 3] (node 3 twice); DISTINCT
    // drops the repeat, leaving the three distinct targets.
    CollectingNodeSink sink;
    runLoweredProgram(removeDuplicatesTargetsProgram, reader.getView(), sink);

    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    const std::vector<std::vector<uint64_t>> expected {{1}, {2}, {3}};
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, removesDuplicateTargetsAcrossChunks) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // The two edges into node 3 land in different chunks at chunk size 2, so the
    // dedup must span chunks - the seen-set is reset once at function scope, not
    // per chunk.
    CollectingNodeSink sink;
    runLoweredProgram(removeDuplicatesTargetsProgram, reader.getView(), sink, /*chunkSize=*/2);

    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    const std::vector<std::vector<uint64_t>> expected {{1}, {2}, {3}};
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, removesDuplicateTwoHopPairs) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Both two-hop paths 0->1->3 and 0->2->3 give the pair (0, 3), so DISTINCT over
    // the carried origin and the two-hop target collapses them to a single row.
    CollectingNodeSink sink;
    runLoweredProgram(removeDuplicatesTwoHopProgram, reader.getView(), sink);

    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    const std::vector<std::vector<uint64_t>> expected {{0, 3}};
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, removeDuplicatesChainedFansOutFromSurvivors) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // The first hop's sources are [0, 0, 1, 2] (node 0 has out-degree two); DISTINCT
    // collapses them to {0, 1, 2}, and the second hop fans out from those three:
    // 0->1, 0->2, 1->3, 2->3 => c = [1, 2, 3, 3]. Without the dedup the duplicated 0
    // would drive the second hop twice and emit six rows, so four rows proves the
    // downstream traversal fans out from the deduped survivors.
    CollectingNodeSink sink;
    runLoweredProgram(removeDuplicatesChainedProgram, reader.getView(), sink);

    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    const std::vector<std::vector<uint64_t>> expected {{1}, {2}, {3}, {3}};
    EXPECT_EQ(rows, expected);
}

TEST_F(DBLoweringTest, removeDuplicatesLimitEmitsDistinctPrefix) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Three distinct targets exist ({1, 2, 3}); LIMIT 2 over the deduped stream
    // emits exactly two, since the limit charges the deduped survivor count.
    CountingSink sink;
    const std::string program = removeDuplicatesLimitProgram(2);
    runLoweredProgram(program.c_str(), reader.getView(), sink);

    EXPECT_EQ(sink.getTotalRows(), 2u);
}

TEST_F(DBLoweringTest, removeDuplicatesOnNodeAndScore) {
    auto graph = buildPropertyGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Three nodes with scores 100, 200 and null (node 2 carries none). The
    // (node, score) rows are all distinct, so every row survives - but each row's
    // key is built from the nullable score column too, so this exercises the opt
    // key-append path and the null serializing to its tag.
    CollectingNodeIntPropSink sink;
    runLoweredProgram(removeDuplicatesNodeScoreProgram, reader.getView(), sink);

    std::vector<std::pair<uint64_t, std::optional<int64_t>>> rows;
    sink.sortedRows(rows);
    const std::vector<std::pair<uint64_t, std::optional<int64_t>>> expected {
        {0, 100}, {1, 200}, {2, std::nullopt}
    };
    EXPECT_EQ(rows, expected);
}

// DISTINCT ... LIMIT lowers to the streaming path, not a fused top-K: the nl keeps
// its nl.distinct + nl.distinct_filter and a real nl.limit / nl.limit_update, with
// no nl.sort_buffer, and the producing loops carry the limit handle so they
// early-exit once the budget of distinct rows is spent.
TEST_F(DBLoweringTest, lowersRemoveDuplicatesLimitToStreamingFilter) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    mlir::MLIRContext context;
    context.getOrLoadDialect<mlir::func::FuncDialect>();
    context.getOrLoadDialect<mlir::storage::Storage>();
    context.getOrLoadDialect<mlir::db::DB>();
    context.getOrLoadDialect<mlir::nl::NL>();

    const mlir::ParserConfig parserConfig(&context);
    const std::string programText = removeDuplicatesLimitProgram(2);
    mlir::OwningOpRef<mlir::ModuleOp> dbModule = mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
    ASSERT_TRUE(dbModule);

    const mlir::func::FuncOp dbFunction = dbModule->lookupSymbol<mlir::func::FuncOp>("main");
    ASSERT_TRUE(dbFunction);

    mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&context));
    DBLowering lowering(&context, &reader.getView());
    lowering.lower(dbFunction, *nlModule);

    size_t distinctCount = 0;
    size_t filterCount = 0;
    size_t limitCount = 0;
    size_t updateCount = 0;
    size_t sortBufferCount = 0;
    size_t forsWithLimit = 0;

    nlModule->walk([&](mlir::Operation* operation) {
        if (mlir::isa<mlir::nl::Distinct>(operation)) {
            distinctCount++;
        } else if (mlir::isa<mlir::nl::DistinctFilter>(operation)) {
            filterCount++;
        } else if (mlir::isa<mlir::nl::Limit>(operation)) {
            limitCount++;
        } else if (mlir::isa<mlir::nl::LimitUpdate>(operation)) {
            updateCount++;
        } else if (mlir::isa<mlir::nl::SortBuffer>(operation)) {
            sortBufferCount++;
        } else if (mlir::nl::For forOp = mlir::dyn_cast<mlir::nl::For>(operation)) {
            if (forOp.getLimit()) {
                forsWithLimit++;
            }
        }
    });

    EXPECT_EQ(distinctCount, 1u);
    EXPECT_EQ(filterCount, 1u);
    EXPECT_EQ(limitCount, 1u);
    EXPECT_EQ(updateCount, 1u);
    EXPECT_EQ(sortBufferCount, 0u);
    EXPECT_GE(forsWithLimit, 1u);
}

TEST_F(DBLoweringTest, countsNodes) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // The diamond has four nodes, so db.count over the scanned node column emits a
    // single row holding 4.
    CollectingCountSink sink;
    runLoweredProgram(countNodesProgram, reader.getView(), sink);

    const std::vector<uint64_t> expected {4};
    EXPECT_EQ(sink.getValues(), expected);
}

TEST_F(DBLoweringTest, countsNodesAcrossChunks) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // At chunk size 2 the four nodes span two scan chunks; the tally is reset once
    // at function scope, not per chunk, so it still totals 4.
    CollectingCountSink sink;
    runLoweredProgram(countNodesProgram, reader.getView(), sink, /*chunkSize=*/2);

    const std::vector<uint64_t> expected {4};
    EXPECT_EQ(sink.getValues(), expected);
}

TEST_F(DBLoweringTest, countsNonNullScores) {
    auto graph = buildPropertyGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Three nodes carry scores 100, 200 and null (node 2 has none). count(a.score)
    // charges only the present values, so the tally is 2, not 3 - the nullable
    // value chunk's present-value count path.
    CollectingCountSink sink;
    runLoweredProgram(countScoresProgram, reader.getView(), sink);

    const std::vector<uint64_t> expected {2};
    EXPECT_EQ(sink.getValues(), expected);
}

// db.count lowers to the pipeline-breaker shape: a hoisted nl.count, a single
// nl.count_update in the producing loop, and an nl.count_result after it that
// materializes the tally chunk at function scope. Because count collapses to one
// row it opens no emit loop, so there is exactly one nl.for (the producing scan)
// and no nl.sort_buffer.
TEST_F(DBLoweringTest, lowersCountToPipelineBreaker) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    mlir::MLIRContext context;
    context.getOrLoadDialect<mlir::func::FuncDialect>();
    context.getOrLoadDialect<mlir::storage::Storage>();
    context.getOrLoadDialect<mlir::db::DB>();
    context.getOrLoadDialect<mlir::nl::NL>();

    const mlir::ParserConfig parserConfig(&context);
    mlir::OwningOpRef<mlir::ModuleOp> dbModule = mlir::parseSourceString<mlir::ModuleOp>(countNodesProgram, parserConfig);
    ASSERT_TRUE(dbModule);

    const mlir::func::FuncOp dbFunction = dbModule->lookupSymbol<mlir::func::FuncOp>("main");
    ASSERT_TRUE(dbFunction);

    mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&context));
    DBLowering lowering(&context, &reader.getView());
    lowering.lower(dbFunction, *nlModule);

    size_t countCount = 0;
    size_t updateCount = 0;
    size_t resultCount = 0;
    size_t forCount = 0;
    size_t sortBufferCount = 0;

    nlModule->walk([&](mlir::Operation* operation) {
        if (mlir::isa<mlir::nl::Count>(operation)) {
            countCount++;
        } else if (mlir::isa<mlir::nl::CountUpdate>(operation)) {
            updateCount++;
        } else if (mlir::isa<mlir::nl::CountResult>(operation)) {
            resultCount++;
        } else if (mlir::isa<mlir::nl::For>(operation)) {
            forCount++;
        } else if (mlir::isa<mlir::nl::SortBuffer>(operation)) {
            sortBufferCount++;
        }
    });

    EXPECT_EQ(countCount, 1u);
    EXPECT_EQ(updateCount, 1u);
    EXPECT_EQ(resultCount, 1u);
    EXPECT_EQ(forCount, 1u);
    EXPECT_EQ(sortBufferCount, 0u);
}

TEST_F(DBLoweringTest, sumsScores) {
    auto graph = buildPropertyGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Scores 100, 200 and null: sum(a.score) adds the present values to 300 (the
    // null is ignored), through the full db -> nl lowering.
    CollectingOptInt64Sink sink;
    runLoweredProgram(aggregateScoreProgram("sum").c_str(), reader.getView(), sink);

    const std::vector<std::optional<int64_t>> expected {300};
    EXPECT_EQ(sink.getValues(), expected);
}

TEST_F(DBLoweringTest, minsAndMaxsScores) {
    auto graph = buildPropertyGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // min/max(a.score) over 100, 200, null are the smallest and largest present
    // values.
    CollectingOptInt64Sink minSink;
    runLoweredProgram(aggregateScoreProgram("min").c_str(), reader.getView(), minSink);
    EXPECT_EQ(minSink.getValues(), (std::vector<std::optional<int64_t>> {100}));

    CollectingOptInt64Sink maxSink;
    runLoweredProgram(aggregateScoreProgram("max").c_str(), reader.getView(), maxSink);
    EXPECT_EQ(maxSink.getValues(), (std::vector<std::optional<int64_t>> {200}));
}

TEST_F(DBLoweringTest, averagesScores) {
    auto graph = buildPropertyGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // avg(a.score) divides the sum of the present values (300) by their count (2),
    // widening to f64, so the single result row is a present 150.0.
    CollectingOptDoubleSink sink;
    runLoweredProgram(aggregateScoreProgram("avg").c_str(), reader.getView(), sink);

    const std::vector<std::optional<double>> expected {150.0};
    EXPECT_EQ(sink.getValues(), expected);
}

TEST_F(DBLoweringTest, averagesScoresAcrossChunks) {
    auto graph = buildPropertyGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // At chunk size 1 the scores span three scan chunks; the accumulator is reset
    // once at function scope, not per chunk, so avg still totals 150.0.
    CollectingOptDoubleSink sink;
    runLoweredProgram(aggregateScoreProgram("avg").c_str(), reader.getView(), sink, /*chunkSize=*/1);

    const std::vector<std::optional<double>> expected {150.0};
    EXPECT_EQ(sink.getValues(), expected);
}

// A db aggregate op lowers to the pipeline-breaker shape, the same as db.count: a
// hoisted nl.aggregate, a single nl.aggregate_update in the producing loop, and an
// nl.aggregate_result after it that materializes the reduced chunk at function
// scope. It collapses to one row, so there is exactly one nl.for (the producing
// scan) and no nl.sort_buffer.
TEST_F(DBLoweringTest, lowersAggregateToPipelineBreaker) {
    auto graph = buildPropertyGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    mlir::MLIRContext context;
    context.getOrLoadDialect<mlir::func::FuncDialect>();
    context.getOrLoadDialect<mlir::storage::Storage>();
    context.getOrLoadDialect<mlir::db::DB>();
    context.getOrLoadDialect<mlir::nl::NL>();

    const mlir::ParserConfig parserConfig(&context);
    const std::string programText = aggregateScoreProgram("sum");
    mlir::OwningOpRef<mlir::ModuleOp> dbModule = mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
    ASSERT_TRUE(dbModule);

    const mlir::func::FuncOp dbFunction = dbModule->lookupSymbol<mlir::func::FuncOp>("main");
    ASSERT_TRUE(dbFunction);

    mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&context));
    DBLowering lowering(&context, &reader.getView());
    lowering.lower(dbFunction, *nlModule);

    size_t aggregateCount = 0;
    size_t updateCount = 0;
    size_t resultCount = 0;
    size_t forCount = 0;
    size_t sortBufferCount = 0;

    nlModule->walk([&](mlir::Operation* operation) {
        if (mlir::isa<mlir::nl::Aggregate>(operation)) {
            aggregateCount++;
        } else if (mlir::isa<mlir::nl::AggregateUpdate>(operation)) {
            updateCount++;
        } else if (mlir::isa<mlir::nl::AggregateResult>(operation)) {
            resultCount++;
        } else if (mlir::isa<mlir::nl::For>(operation)) {
            forCount++;
        } else if (mlir::isa<mlir::nl::SortBuffer>(operation)) {
            sortBufferCount++;
        }
    });

    EXPECT_EQ(aggregateCount, 1u);
    EXPECT_EQ(updateCount, 1u);
    EXPECT_EQ(resultCount, 1u);
    EXPECT_EQ(forCount, 1u);
    EXPECT_EQ(sortBufferCount, 0u);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
