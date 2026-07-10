#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <span>
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
#include "iterators/ChunkConfig.h"
#include "metadata/PropertyType.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"

#include "IRException.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "NLInterpreter.h"
#include "NLOutputSink.h"
#include "StorageDialect.h"

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

    const std::vector<std::vector<uint64_t>>& getColumns() const { return _columns; }

    // Fills rows zipped from the columns and sorted: chunk order depends on
    // datapart-major iteration, so tests compare order-independently
    void sortedRows(std::vector<std::vector<uint64_t>>& rows) const {
        rows.clear();
        const size_t rowCount = _columns.empty() ? 0 : _columns.front().size();

        for (size_t rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            std::vector<uint64_t> row;
            for (const std::vector<uint64_t>& column : _columns) {
                row.push_back(column[rowIndex]);
            }
            rows.push_back(row);
        }

        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<std::vector<uint64_t>> _columns;
};

// Collects (node ID, nullable int64 property) rows, for programs that read an
// Int64 property: a node ID chunk and a !storage.nullable<i64> value chunk.
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

private:
    std::vector<std::pair<uint64_t, std::optional<int64_t>>> _rows;
};

// Collects the single-row unsigned-i64 result a COUNT emits: one non-nullable
// !nl.chunk<ui64> with one row. Captures every value it sees, so a test can assert
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

// Collects the single-row nullable int64 result a SUM/MIN/MAX over an Int64
// column emits: one !nl.chunk<!storage.nullable<i64>> with one row. Captures the
// value (present or null), so a test can assert both the arity and the reduction.
class CollectingOptInt64Sink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* values = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[0]);
        ASSERT_NE(values, nullptr);

        const auto& raw = values->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _values.push_back(raw[rowIndex]);
        }
    }

    const std::vector<std::optional<int64_t>>& getValues() const { return _values; }

private:
    std::vector<std::optional<int64_t>> _values;
};

// Collects the single-row nullable double result an AVG emits: one
// !nl.chunk<!storage.nullable<f64>> with one row.
class CollectingOptDoubleSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* values = dynamic_cast<const ColumnOptVector<double>*>(chunks[0]);
        ASSERT_NE(values, nullptr);

        const auto& raw = values->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _values.push_back(raw[rowIndex]);
        }
    }

    const std::vector<std::optional<double>>& getValues() const { return _values; }

private:
    std::vector<std::optional<double>> _values;
};

// Collects constant result rows: each output column is a ColumnConst<T> - the
// single broadcast value nl.constant materializes - so a constant program emits
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

// A single constant materialized at function scope and emitted as one row.
constexpr const char* singleConstantProgram = R"mlir(
func.func @main() {
  %c = nl.constant(30 : i64)
  nl.output(%c) : !nl.chunk<i64>
  func.return
}
)mlir";

// Several constants projected together as one row.
constexpr const char* multipleConstantsProgram = R"mlir(
func.func @main() {
  %a = nl.constant(10 : i64)
  %b = nl.constant(20 : i64)
  %c = nl.constant(30 : i64)
  nl.output(%a, %b, %c) : !nl.chunk<i64>, !nl.chunk<i64>, !nl.chunk<i64>
  func.return
}
)mlir";

// A single double constant, exercising the f64 value type.
constexpr const char* doubleConstantProgram = R"mlir(
func.func @main() {
  %c = nl.constant(2.5 : f64)
  nl.output(%c) : !nl.chunk<f64>
  func.return
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

// Collects a single boolean mask column - the non-null result of a comparison, a
// ColumnMask (not a value vector) - as one bool per row.
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

// Collects (node ID, nullable bool) rows: a node ID chunk paired with a
// ColumnOptMask (ColumnOptVector<CustomBool>), the nullable result of a comparison.
// A null (a comparison against a null operand) comes back as an empty optional.
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

// One constant of each supported value type projected together as a single row.
constexpr const char* allTypesConstantsProgram = R"mlir(
func.func @main() {
  %i = nl.constant(30 : i64)
  %u = nl.constant(7 : ui64)
  %f = nl.constant(2.5 : f64)
  %b = nl.constant(true)
  nl.output(%i, %u, %f, %b) : !nl.chunk<i64>, !nl.chunk<ui64>, !nl.chunk<f64>, !nl.chunk<i1>
  func.return
}
)mlir";

// Two constants added at function scope.
constexpr const char* addConstantsProgram = R"mlir(
func.func @main() {
  %x = nl.constant(10 : i64)
  %y = nl.constant(20 : i64)
  %s = nl.add %x, %y : (!nl.chunk<i64>, !nl.chunk<i64>) -> !nl.chunk<i64>
  nl.output(%s) : !nl.chunk<i64>
  func.return
}
)mlir";

// A mixed-type add promoting to a double.
constexpr const char* addPromotesProgram = R"mlir(
func.func @main() {
  %x = nl.constant(10 : i64)
  %y = nl.constant(2.5 : f64)
  %s = nl.add %x, %y : (!nl.chunk<i64>, !nl.chunk<f64>) -> !nl.chunk<f64>
  nl.output(%s) : !nl.chunk<f64>
  func.return
}
)mlir";

// A constant broadcast against a per-node property column, inside the scan loop:
// null + 10 = null for a node without a score.
constexpr const char* addPropertyConstantProgram = R"mlir(
func.func @main() {
  %score = nl.get_property_type("score")
  %k = nl.constant(10 : i64)
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    %v = nl.get_node_properties(%a, %score) : !nl.chunk<!storage.nullable<i64>>
    %sum = nl.add %v, %k : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i64>>
    nl.output(%a, %sum) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<i64>>
  }
  func.return
}
)mlir";

// Two constants subtracted at function scope.
constexpr const char* subConstantsProgram = R"mlir(
func.func @main() {
  %x = nl.constant(30 : i64)
  %y = nl.constant(20 : i64)
  %s = nl.sub %x, %y : (!nl.chunk<i64>, !nl.chunk<i64>) -> !nl.chunk<i64>
  nl.output(%s) : !nl.chunk<i64>
  func.return
}
)mlir";

// A mixed-type sub promoting to a double.
constexpr const char* subPromotesProgram = R"mlir(
func.func @main() {
  %x = nl.constant(10 : i64)
  %y = nl.constant(2.5 : f64)
  %s = nl.sub %x, %y : (!nl.chunk<i64>, !nl.chunk<f64>) -> !nl.chunk<f64>
  nl.output(%s) : !nl.chunk<f64>
  func.return
}
)mlir";

// A constant broadcast against a per-node property column, inside the scan loop:
// null - 10 = null for a node without a score.
constexpr const char* subPropertyConstantProgram = R"mlir(
func.func @main() {
  %score = nl.get_property_type("score")
  %k = nl.constant(10 : i64)
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    %v = nl.get_node_properties(%a, %score) : !nl.chunk<!storage.nullable<i64>>
    %diff = nl.sub %v, %k : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i64>>
    nl.output(%a, %diff) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<i64>>
  }
  func.return
}
)mlir";

// Two constants multiplied at function scope.
constexpr const char* mulConstantsProgram = R"mlir(
func.func @main() {
  %x = nl.constant(30 : i64)
  %y = nl.constant(20 : i64)
  %s = nl.mul %x, %y : (!nl.chunk<i64>, !nl.chunk<i64>) -> !nl.chunk<i64>
  nl.output(%s) : !nl.chunk<i64>
  func.return
}
)mlir";

// A mixed-type mul promoting to a double.
constexpr const char* mulPromotesProgram = R"mlir(
func.func @main() {
  %x = nl.constant(10 : i64)
  %y = nl.constant(2.5 : f64)
  %s = nl.mul %x, %y : (!nl.chunk<i64>, !nl.chunk<f64>) -> !nl.chunk<f64>
  nl.output(%s) : !nl.chunk<f64>
  func.return
}
)mlir";

// A constant broadcast against a per-node property column, inside the scan loop:
// null * 10 = null for a node without a score.
constexpr const char* mulPropertyConstantProgram = R"mlir(
func.func @main() {
  %score = nl.get_property_type("score")
  %k = nl.constant(10 : i64)
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    %v = nl.get_node_properties(%a, %score) : !nl.chunk<!storage.nullable<i64>>
    %prod = nl.mul %v, %k : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i64>>
    nl.output(%a, %prod) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<i64>>
  }
  func.return
}
)mlir";

// RETURN 10 = 20
constexpr const char* eqConstantsFalseProgram = R"mlir(
func.func @main() {
  %x = nl.constant(10 : i64)
  %y = nl.constant(20 : i64)
  %r = nl.eq %x, %y : (!nl.chunk<i64>, !nl.chunk<i64>) -> !nl.chunk<i1>
  nl.output(%r) : !nl.chunk<i1>
  func.return
}
)mlir";

// RETURN 10 = 10
constexpr const char* eqConstantsTrueProgram = R"mlir(
func.func @main() {
  %x = nl.constant(10 : i64)
  %y = nl.constant(10 : i64)
  %r = nl.eq %x, %y : (!nl.chunk<i64>, !nl.chunk<i64>) -> !nl.chunk<i1>
  nl.output(%r) : !nl.chunk<i1>
  func.return
}
)mlir";

// MATCH (n) RETURN n = n
constexpr const char* eqSelfProgram = R"mlir(
func.func @main() {
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    %r = nl.eq %a, %a : (!nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>) -> !nl.chunk<i1>
    nl.output(%r) : !nl.chunk<i1>
  }
  func.return
}
)mlir";

// MATCH (n) RETURN n.score = 200
constexpr const char* eqPropertyConstantProgram = R"mlir(
func.func @main() {
  %score = nl.get_property_type("score")
  %k = nl.constant(200 : i64)
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    %v = nl.get_node_properties(%a, %score) : !nl.chunk<!storage.nullable<i64>>
    %r = nl.eq %v, %k : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i1>>
    nl.output(%a, %r) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<i1>>
  }
  func.return
}
)mlir";

// A non-constant value bound in the OUTER scan loop (a per-node property fetch),
// output from inside the INNER edge loop. This is malformed: %score has node
// cardinality, is not a broadcast constant, and is not row-aligned with the inner
// loop's rows. The db->nl lowering never produces this (it carries such values
// inward as loop variables), so it can only be hand-written; translateOutput must
// reject it, since only a constant may be referenced across scopes.
constexpr const char* outerScopeOutputColumnProgram = R"mlir(
func.func @main() {
  %p = nl.get_property_type("score")
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    %score = nl.get_node_properties(%a, %p) : !nl.chunk<!storage.nullable<i64>>
    %edges = nl.get_out_edges(%a, {})
    nl.for %srcs, %eids, %etypes, %b in %edges : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
      nl.output(%score) : !nl.chunk<!storage.nullable<i64>>
    }
  }
  func.return
}
)mlir";

// Scan all nodes and output them
constexpr const char* scanProgram = R"mlir(
func.func @main() {
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    nl.output(%a) : !nl.chunk<!storage.node_id>
  }
  func.return
}
)mlir";

// One hop along out-edges, outputting (source, target) pairs. The source
// column exercises the gather-by-indices reconstruction.
constexpr const char* oneHopOutProgram = R"mlir(
func.func @main() {
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    %edges = nl.get_out_edges(%a, {})
    nl.for %srcs, %eids, %etypes, %b in %edges : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
      nl.output(%srcs, %b) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>
    }
  }
  func.return
}
)mlir";

// One hop along in-edges: the writer fills the source side and the target
// side is gathered from the input, so the output pairs are the same edge set
constexpr const char* oneHopInProgram = R"mlir(
func.func @main() {
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    %edges = nl.get_in_edges(%a, {})
    nl.for %srcs, %eids, %etypes, %b in %edges : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
      nl.output(%srcs, %b) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>
    }
  }
  func.return
}
)mlir";

// Two hops a->b->c carrying a through the second hop, outputting (a, c) pairs
constexpr const char* twoHopProgram = R"mlir(
func.func @main() {
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    %edges = nl.get_out_edges(%a, {})
    nl.for %srcs, %eids, %etypes, %b in %edges : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
      %hop = nl.get_out_edges(%b, {%srcs}) : !nl.chunk<!storage.node_id>
      nl.for %srcs2, %eids2, %etypes2, %c, %aCarried in %hop : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>> {
        nl.output(%aCarried, %c) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>
      }
    }
  }
  func.return
}
)mlir";

// The in-edges mirror of twoHopProgram: two predecessor hops a<-b<-c carrying a
// through the second hop, outputting (a, c) pairs. Each hop's discovered
// predecessor (%srcs, the source side) feeds the next hop, while the input side
// (a, then the carried a) is gathered along, exercising the carry set through
// in-edges execution.
constexpr const char* twoHopInProgram = R"mlir(
func.func @main() {
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    %edges = nl.get_in_edges(%a, {})
    nl.for %srcs, %eids, %etypes, %aIn in %edges : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
      %hop = nl.get_in_edges(%srcs, {%aIn}) : !nl.chunk<!storage.node_id>
      nl.for %srcs2, %eids2, %etypes2, %bIn, %aCarried in %hop : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>> {
        nl.output(%aCarried, %srcs2) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>
      }
    }
  }
  func.return
}
)mlir";

// Verifier-legal but rejected by the translator: outputs an outer loop
// variable from the inner loop instead of carrying it through the carry set
constexpr const char* crossLoopOutputProgram = R"mlir(
func.func @main() {
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    %edges = nl.get_out_edges(%a, {})
    nl.for %srcs, %eids, %etypes, %b in %edges : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
      nl.output(%a, %b) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>
    }
  }
  func.return
}
)mlir";

// Verifier-legal but rejected by the translator: carries a chunk bound by a
// different loop than the one binding the input chunk
constexpr const char* crossLoopCarryProgram = R"mlir(
func.func @main() {
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    %edges = nl.get_out_edges(%a, {})
    nl.for %srcs, %eids, %etypes, %b in %edges : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
      %hop = nl.get_out_edges(%b, {%a}) : !nl.chunk<!storage.node_id>
      nl.for %srcs2, %eids2, %etypes2, %c, %aCarried in %hop : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>> {
        nl.output(%aCarried, %c) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>
      }
    }
  }
  func.return
}
)mlir";

// Resolve the "score" property once above the loops, then read it per scanned
// node and output (node, score). The value chunk is nullable, so nodes without
// the property still appear, with a null value.
constexpr const char* nodePropertiesProgram = R"mlir(
func.func @main() {
  %score = nl.get_property_type("score")
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    %values = nl.get_node_properties(%a, %score) : !nl.chunk<!storage.nullable<i64>>
    nl.output(%a, %values) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<i64>>
  }
  func.return
}
)mlir";

// Scan all nodes, accumulate them into a sort buffer, then emit them sorted by
// node ID descending. The sort_collect runs in the scan loop; the nl.sort source
// drives the emit loop that outputs the sorted chunks.
constexpr const char* nlSortDescProgram = R"mlir(
func.func @main() {
  %state = nl.sort_buffer keys [0] ascending [false]
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    nl.sort_collect %state, (%a) : !nl.chunk<!storage.node_id>
  }
  %sorted = nl.sort(%state) : !nl.iter<!nl.chunk<!storage.node_id>>
  nl.for %sa in %sorted : !nl.iter<!nl.chunk<!storage.node_id>> {
    nl.output(%sa) : !nl.chunk<!storage.node_id>
  }
  func.return
}
)mlir";

// The same, but the accumulator is bounded to the best 2 rows (a fused top-K):
// nl.sort_buffer carries `limit 2`, so sort_collect keeps only the top 2 by the
// descending node ID and the emit loop yields just those.
constexpr const char* nlTopKDescProgram = R"mlir(
func.func @main() {
  %state = nl.sort_buffer keys [0] ascending [false] limit 2
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    nl.sort_collect %state, (%a) : !nl.chunk<!storage.node_id>
  }
  %sorted = nl.sort(%state) : !nl.iter<!nl.chunk<!storage.node_id>>
  nl.for %sa in %sorted : !nl.iter<!nl.chunk<!storage.node_id>> {
    nl.output(%sa) : !nl.chunk<!storage.node_id>
  }
  func.return
}
)mlir";

// Scan all nodes, walk out-edges, and keep only the distinct targets. The seen-set
// is created once at function scope by nl.distinct and the nl.distinct_filter runs
// in the inner edge loop, so duplicate targets reached from different sources are
// dropped globally, across chunk boundaries.
constexpr const char* nlDistinctTargetsProgram = R"mlir(
func.func @main() {
  %set = nl.distinct
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    %edges = nl.get_out_edges(%a, {})
    nl.for %srcs, %eids, %etypes, %b in %edges : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
      %db = nl.distinct_filter %set, (%b) : !nl.chunk<!storage.node_id>
      nl.output(%db) : !nl.chunk<!storage.node_id>
    }
  }
  func.return
}
)mlir";

// Scan all nodes and count them. The tally is created once at function scope by
// nl.count, incremented per scan chunk by nl.count_update, and - after the loop -
// nl.count_result materializes the single ui64 tally row that the function-scope
// nl.output emits. Node IDs are never null, so this is the total node count.
constexpr const char* nlCountNodesProgram = R"mlir(
func.func @main() {
  %c = nl.count
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    nl.count_update %c, %a : !nl.chunk<!storage.node_id>
  }
  %r = nl.count_result(%c) : !nl.chunk<ui64>
  nl.output(%r) : !nl.chunk<ui64>
  func.return
}
)mlir";

// Scan all nodes, read each one's "score", and count the non-null values -
// Cypher count(a.score). A node without the property contributes a null value
// that nl.count_update does not charge, so the tally is fewer than the node count.
constexpr const char* nlCountScoresProgram = R"mlir(
func.func @main() {
  %score = nl.get_property_type("score")
  %c = nl.count
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    %values = nl.get_node_properties(%a, %score) : !nl.chunk<!storage.nullable<i64>>
    nl.count_update %c, %values : !nl.chunk<!storage.nullable<i64>>
  }
  %r = nl.count_result(%c) : !nl.chunk<ui64>
  nl.output(%r) : !nl.chunk<ui64>
  func.return
}
)mlir";

// Scan all nodes, read each one's "score", and reduce the non-null values with
// the given aggregate - Cypher sum/min/max(a.score). The accumulator is created
// once at function scope, folded per scan chunk by nl.aggregate_update, and - after
// the loop - nl.aggregate_result materializes the single reduced row that the
// function-scope nl.output emits. A null value (node 2 has no score) is ignored.
// sum/min/max keep the Int64 type, so the state and result are !nl...<i64>.
std::string nlIntAggregateScoresProgram(const char* kind) {
    return std::string("func.func @main() {\n"
                       "  %score = nl.get_property_type(\"score\")\n"
                       "  %s = nl.aggregate ")
           + kind
           + " : !nl.aggregate_state<i64>\n"
             "  %nodes = nl.scan_nodes()\n"
             "  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {\n"
             "    %values = nl.get_node_properties(%a, %score) : !nl.chunk<!storage.nullable<i64>>\n"
             "    nl.aggregate_update "
           + kind
           + " %s, %values : !nl.aggregate_state<i64>, !nl.chunk<!storage.nullable<i64>>\n"
             "  }\n"
             "  %r = nl.aggregate_result "
           + kind
           + " (%s) : !nl.aggregate_state<i64> -> !nl.chunk<!storage.nullable<i64>>\n"
             "  nl.output(%r) : !nl.chunk<!storage.nullable<i64>>\n"
             "  func.return\n"
             "}\n";
}

// avg(a.score): the same shape, but avg accumulates in f64 (the running sum) and
// divides by the non-null count, so the state and result are !nl...<f64> while the
// input chunk stays the Int64 property column.
constexpr const char* nlAvgScoresProgram = R"mlir(
func.func @main() {
  %score = nl.get_property_type("score")
  %s = nl.aggregate avg : !nl.aggregate_state<f64>
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    %values = nl.get_node_properties(%a, %score) : !nl.chunk<!storage.nullable<i64>>
    nl.aggregate_update avg %s, %values : !nl.aggregate_state<f64>, !nl.chunk<!storage.nullable<i64>>
  }
  %r = nl.aggregate_result avg (%s) : !nl.aggregate_state<f64> -> !nl.chunk<!storage.nullable<f64>>
  nl.output(%r) : !nl.chunk<!storage.nullable<f64>>
  func.return
}
)mlir";

// Counts appendChunks calls and the total rows emitted, to prove a limited run
// emits a clamped prefix (a partial final chunk) and stops the loop early.
class CountingSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        _calls++;
        _totalRows += rowCount;
    }

    size_t getCalls() const { return _calls; }
    size_t getTotalRows() const { return _totalRows; }

private:
    size_t _calls {0};
    size_t _totalRows {0};
};

// Scan all nodes and output them, capped by a top-level nl.limit. The handle is
// hoisted, threaded onto the loop, charged by limit_update, and the truncate cuts
// each chunk to the prefix the budget allows before the plain output.
std::string nlLimitScanProgram(uint64_t count) {
    return std::string("func.func @main() {\n"
                       "  %h = nl.limit(")
           + std::to_string(count)
           + ")\n"
             "  %nodes = nl.scan_nodes()\n"
             "  nl.for %a in %nodes limit %h : !nl.iter<!nl.chunk<!storage.node_id>> {\n"
             "    nl.limit_update %h, %a : !nl.chunk<!storage.node_id>\n"
             "    %la = nl.limit_truncate %h, (%a) : !nl.chunk<!storage.node_id>\n"
             "    nl.output(%la) : !nl.chunk<!storage.node_id>\n"
             "  }\n"
             "  func.return\n"
             "}\n";
}

// One hop over out-edges, capped by an nl.limit on both loops, so the inner
// break unwinds the outer scan loop too.
std::string nlLimitNestedLoopProgram(uint64_t count) {
    return std::string("func.func @main() {\n"
                       "  %h = nl.limit(")
           + std::to_string(count)
           + ")\n"
             "  %nodes = nl.scan_nodes()\n"
             "  nl.for %a in %nodes limit %h : !nl.iter<!nl.chunk<!storage.node_id>> {\n"
             "    %edges = nl.get_out_edges(%a, {})\n"
             "    nl.for %srcs, %eids, %etypes, %b in %edges limit %h : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {\n"
             "      nl.limit_update %h, %b : !nl.chunk<!storage.node_id>\n"
             "      %lb = nl.limit_truncate %h, (%b) : !nl.chunk<!storage.node_id>\n"
             "      nl.output(%lb) : !nl.chunk<!storage.node_id>\n"
             "    }\n"
             "  }\n"
             "  func.return\n"
             "}\n";
}

// The same one-hop nest, but only the OUTER scan loop carries the limit - the
// inner edge loop is deliberately unbounded. limit_update still charges and the
// truncate copies a zero-row prefix once the budget is spent, so the inner loop
// keeps calling output (with zero rows) if the outer loop fails to halt. This
// makes outer over-driving observable: if the outer loop failed to halt when the
// budget hit zero, it would advance to the remaining nodes and the unbounded inner
// loop would call output again (with zero rows), pushing the call count past one.
// With the outer loop halting correctly, output is called exactly once.
std::string nlLimitOuterLoopOnlyProgram(uint64_t count) {
    return std::string("func.func @main() {\n"
                       "  %h = nl.limit(")
           + std::to_string(count)
           + ")\n"
             "  %nodes = nl.scan_nodes()\n"
             "  nl.for %a in %nodes limit %h : !nl.iter<!nl.chunk<!storage.node_id>> {\n"
             "    %edges = nl.get_out_edges(%a, {})\n"
             "    nl.for %srcs, %eids, %etypes, %b in %edges : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {\n"
             "      nl.limit_update %h, %b : !nl.chunk<!storage.node_id>\n"
             "      %lb = nl.limit_truncate %h, (%b) : !nl.chunk<!storage.node_id>\n"
             "      nl.output(%lb) : !nl.chunk<!storage.node_id>\n"
             "    }\n"
             "  }\n"
             "  func.return\n"
             "}\n";
}

// Scan all nodes and output them, with a top-level nl.skip dropping the first
// `count` rows. The handle is hoisted, charged by skip_update, and the truncate
// lifts each step's surviving suffix to the front before the plain output. Unlike
// a limit, the loop carries NO handle: a skip runs the scan to exhaustion.
std::string nlSkipScanProgram(uint64_t count) {
    return std::string("func.func @main() {\n"
                       "  %h = nl.skip(")
           + std::to_string(count)
           + ")\n"
             "  %nodes = nl.scan_nodes()\n"
             "  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {\n"
             "    nl.skip_update %h, %a : !nl.chunk<!storage.node_id>\n"
             "    %sa = nl.skip_truncate %h, (%a) : !nl.chunk<!storage.node_id>\n"
             "    nl.output(%sa) : !nl.chunk<!storage.node_id>\n"
             "  }\n"
             "  func.return\n"
             "}\n";
}

// The folded terminal-SKIP form that foldSkipTruncatesIntoOutputs produces: no
// nl.skip_truncate, and the nl.output carries the skip handle so it emits the
// surviving suffix in place at offset skipThisStep, with no copy. Same result as
// nlSkipScanProgram - this exercises the copy-free output path.
std::string nlSkipFoldedScanProgram(uint64_t count) {
    return std::string("func.func @main() {\n"
                       "  %h = nl.skip(")
           + std::to_string(count)
           + ")\n"
             "  %nodes = nl.scan_nodes()\n"
             "  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {\n"
             "    nl.skip_update %h, %a : !nl.chunk<!storage.node_id>\n"
             "    nl.output(%a) skip %h : !nl.chunk<!storage.node_id>\n"
             "  }\n"
             "  func.return\n"
             "}\n";
}

}

class NLExecutorTest : public TuringTest {
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

    // A second datapart with nodes 4, 5 and the edge 4 -> 5, exercising the
    // datapart-major iteration of the writers
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

    // A line graph 0 -> 1 -> 2 where nodes 0 and 1 carry a "score" Int64
    // property (100, 200) and node 2 has none, so a property read yields null
    std::unique_ptr<Graph> buildScoredGraph() {
        auto graph = Graph::create();

        auto change = graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        metadata.getOrCreateLabel("0");
        metadata.getOrCreateEdgeType("0");
        const PropertyType scoreType = metadata.getOrCreatePropertyType("score", ValueType::Int64);
        const PropertyTypeID scoreID = scoreType._id;

        const LabelSet labelset = LabelSet::fromList({0});
        const NodeID nodeA = builder.addNode(labelset);
        const NodeID nodeB = builder.addNode(labelset);
        const NodeID nodeC = builder.addNode(labelset);

        builder.addEdge(0, nodeA, nodeB);
        builder.addEdge(0, nodeB, nodeC);

        builder.addNodeProperty<types::Int64>(nodeA, scoreID, 100);
        builder.addNodeProperty<types::Int64>(nodeB, scoreID, 200);
        // nodeC has no "score" property, so a property read returns null for it

        const auto submitResult = change->access().submit(*_jobSystem);
        EXPECT_TRUE(submitResult);

        return graph;
    }

    // Two nodes with a "score" Int64 property declared in the schema but assigned to
    // neither, so every score read is null. Exercises the aggregate identity/empty
    // branches: sum of no non-null value is a present zero, min/max/avg are null. The
    // property is still in the schema, so nl.get_property_type("score") resolves.
    std::unique_ptr<Graph> buildAllNullScoredGraph() {
        auto graph = Graph::create();

        auto change = graph->newChange();
        auto* commitBuilder = change->access().getTip();
        auto& builder = commitBuilder->newBuilder();
        auto& metadata = builder.getMetadata();

        metadata.getOrCreateLabel("0");
        metadata.getOrCreateEdgeType("0");
        metadata.getOrCreatePropertyType("score", ValueType::Int64);

        const LabelSet labelset = LabelSet::fromList({0});
        builder.addNode(labelset);
        builder.addNode(labelset);
        // No node carries a "score", so every property read returns null

        const auto submitResult = change->access().submit(*_jobSystem);
        EXPECT_TRUE(submitResult);

        return graph;
    }

    void runProgram(const char* programText,
                    const GraphView& view,
                    size_t chunkSize,
                    NLOutputSink& sink) {
        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::nl::NL>();

        const mlir::ParserConfig parserConfig(&context);
        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
        ASSERT_TRUE(module);

        LocalMemory memory;
        NLInterpreter interpreter(*module, &view, &sink, &memory, chunkSize);
        interpreter.run();
    }

    // Parses a program that MLIR accepts but the translator must reject
    void expectTranslationFailure(const char* programText) {
        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::nl::NL>();

        const mlir::ParserConfig parserConfig(&context);
        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
        ASSERT_TRUE(module);

        // run() reaches the translator before touching the graph view or sink,
        // so a rejected program surfaces its IRException with neither supplied
        LocalMemory memory;
        NLInterpreter interpreter(*module, nullptr, nullptr, &memory);
        EXPECT_THROW(interpreter.run(), IRException);
    }

    std::unique_ptr<JobSystem> _jobSystem;
};

TEST_F(NLExecutorTest, emptyGraphProducesNoOutput) {
    auto graph = Graph::create();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeSink sink;
    runProgram(scanProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    EXPECT_TRUE(sink.getColumns().empty());
}

TEST_F(NLExecutorTest, scanNodesOutput) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeSink sink;
    runProgram(scanProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    const std::vector<std::vector<uint64_t>> expected {{0}, {1}, {2}, {3}};
    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(NLExecutorTest, constantSingleValue) {
    auto graph = Graph::create();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingConstSink<int64_t> sink;
    runProgram(singleConstantProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    const std::vector<std::vector<int64_t>> expected {{30}};
    EXPECT_EQ(sink.rows(), expected);
}

TEST_F(NLExecutorTest, constantMultipleValues) {
    auto graph = Graph::create();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingConstSink<int64_t> sink;
    runProgram(multipleConstantsProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    // One row of three constant columns, in projection order.
    const std::vector<std::vector<int64_t>> expected {{10, 20, 30}};
    EXPECT_EQ(sink.rows(), expected);
}

TEST_F(NLExecutorTest, constantDoubleValue) {
    auto graph = Graph::create();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingConstSink<double> sink;
    runProgram(doubleConstantProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    const std::vector<std::vector<double>> expected {{2.5}};
    EXPECT_EQ(sink.rows(), expected);
}

TEST_F(NLExecutorTest, constantAllSupportedTypes) {
    auto graph = Graph::create();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingAllTypesConstSink sink;
    runProgram(allTypesConstantsProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    ASSERT_TRUE(sink.seen());
    EXPECT_EQ(sink.getInt(), 30);
    EXPECT_EQ(sink.getUint(), 7u);
    EXPECT_DOUBLE_EQ(sink.getDouble(), 2.5);
    EXPECT_TRUE(sink.getBool());
}

TEST_F(NLExecutorTest, addConstants) {
    auto graph = Graph::create();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingConstSink<int64_t> sink;
    runProgram(addConstantsProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    const std::vector<std::vector<int64_t>> expected {{30}};
    EXPECT_EQ(sink.rows(), expected);
}

TEST_F(NLExecutorTest, addPromotesMixedTypesToDouble) {
    auto graph = Graph::create();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingConstSink<double> sink;
    runProgram(addPromotesProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    const std::vector<std::vector<double>> expected {{12.5}};
    EXPECT_EQ(sink.rows(), expected);
}

TEST_F(NLExecutorTest, addConstantToNodePropertyBroadcasting) {
    auto graph = buildScoredGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeIntPropSink sink;
    runProgram(addPropertyConstantProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    // The constant 10 is broadcast against each node's score; node 2 has no score,
    // so null + 10 stays null.
    const std::vector<std::pair<uint64_t, std::optional<int64_t>>> expected {
        {0, 110}, {1, 210}, {2, std::nullopt}
    };
    std::vector<std::pair<uint64_t, std::optional<int64_t>>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(NLExecutorTest, subConstants) {
    auto graph = Graph::create();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingConstSink<int64_t> sink;
    runProgram(subConstantsProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    const std::vector<std::vector<int64_t>> expected {{10}};
    EXPECT_EQ(sink.rows(), expected);
}

TEST_F(NLExecutorTest, subPromotesMixedTypesToDouble) {
    auto graph = Graph::create();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingConstSink<double> sink;
    runProgram(subPromotesProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    const std::vector<std::vector<double>> expected {{7.5}};
    EXPECT_EQ(sink.rows(), expected);
}

TEST_F(NLExecutorTest, subConstantFromNodePropertyBroadcasting) {
    auto graph = buildScoredGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeIntPropSink sink;
    runProgram(subPropertyConstantProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    // The constant 10 is broadcast against each node's score; node 2 has no score,
    // so null - 10 stays null.
    const std::vector<std::pair<uint64_t, std::optional<int64_t>>> expected {
        {0, 90}, {1, 190}, {2, std::nullopt}
    };
    std::vector<std::pair<uint64_t, std::optional<int64_t>>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(NLExecutorTest, mulConstants) {
    auto graph = Graph::create();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingConstSink<int64_t> sink;
    runProgram(mulConstantsProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    const std::vector<std::vector<int64_t>> expected {{600}};
    EXPECT_EQ(sink.rows(), expected);
}

TEST_F(NLExecutorTest, mulPromotesMixedTypesToDouble) {
    auto graph = Graph::create();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingConstSink<double> sink;
    runProgram(mulPromotesProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    const std::vector<std::vector<double>> expected {{25.0}};
    EXPECT_EQ(sink.rows(), expected);
}

TEST_F(NLExecutorTest, mulConstantByNodePropertyBroadcasting) {
    auto graph = buildScoredGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeIntPropSink sink;
    runProgram(mulPropertyConstantProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    // The constant 10 is broadcast against each node's score; node 2 has no score,
    // so null * 10 stays null.
    const std::vector<std::pair<uint64_t, std::optional<int64_t>>> expected {
        {0, 1000}, {1, 2000}, {2, std::nullopt}
    };
    std::vector<std::pair<uint64_t, std::optional<int64_t>>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(NLExecutorTest, eqConstantsFalse) {
    auto graph = Graph::create();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingMaskSink sink;
    runProgram(eqConstantsFalseProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    const std::vector<bool> expected {false};
    EXPECT_EQ(sink.values(), expected);
}

TEST_F(NLExecutorTest, eqConstantsTrue) {
    auto graph = Graph::create();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingMaskSink sink;
    runProgram(eqConstantsTrueProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    const std::vector<bool> expected {true};
    EXPECT_EQ(sink.values(), expected);
}

TEST_F(NLExecutorTest, eqNodeToItselfIsAllTrue) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingMaskSink sink;
    runProgram(eqSelfProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    // The diamond has four nodes, and every node equals itself.
    const std::vector<bool> expected {true, true, true, true};
    EXPECT_EQ(sink.values(), expected);
}

TEST_F(NLExecutorTest, eqNodePropertyToConstantBroadcasting) {
    auto graph = buildScoredGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeBoolSink sink;
    runProgram(eqPropertyConstantProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    // score is 100 / 200 / null, compared against 200: false / true / null.
    const std::vector<std::pair<uint64_t, std::optional<bool>>> expected {
        {0, false}, {1, true}, {2, std::nullopt}
    };
    std::vector<std::pair<uint64_t, std::optional<bool>>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

// Illustrates review finding #2: translateOutput's isFromOuterScope check accepts
// ANY op-result from an enclosing block, not just a broadcast constant. Here a
// per-node property fetch bound in the outer scan loop is output inside the inner
// edge loop - not row-aligned with the inner loop and not a constant - so the
// translator must reject it. This test currently FAILS: the malformed program is
// wrongly accepted (translation does not throw) and runOutput emits the outer
// column's rows. It passes once the output guard is tightened to admit only a
// constant (or a loop variable / in-block chunk) across scopes.
TEST_F(NLExecutorTest, outputRejectsNonConstantOuterScopeColumn) {
    auto graph = buildScoredGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingOptInt64Sink sink;
    EXPECT_THROW(
        runProgram(outerScopeOutputColumnProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink),
        IRException);
}

TEST_F(NLExecutorTest, getNodeProperties) {
    auto graph = buildScoredGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeIntPropSink sink;
    runProgram(nodePropertiesProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    // Every node appears, with its score or null where it has none
    const std::vector<std::pair<uint64_t, std::optional<int64_t>>> expected {
        {0, 100}, {1, 200}, {2, std::nullopt}
    };
    std::vector<std::pair<uint64_t, std::optional<int64_t>>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(NLExecutorTest, getNodePropertiesSmallChunks) {
    auto graph = buildScoredGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // A chunk size smaller than the node count forces the property fetch to
    // refill its value column over several chunks, with the null-valued node in
    // a later chunk; the result must not depend on the chunking
    CollectingNodeIntPropSink sink;
    runProgram(nodePropertiesProgram, reader.getView(), 2, sink);

    const std::vector<std::pair<uint64_t, std::optional<int64_t>>> expected {
        {0, 100}, {1, 200}, {2, std::nullopt}
    };
    std::vector<std::pair<uint64_t, std::optional<int64_t>>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(NLExecutorTest, oneHopOutEdges) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeSink sink;
    runProgram(oneHopOutProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    const std::vector<std::vector<uint64_t>> expected {{0, 1}, {0, 2}, {1, 3}, {2, 3}};
    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(NLExecutorTest, oneHopInEdges) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeSink sink;
    runProgram(oneHopInProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    const std::vector<std::vector<uint64_t>> expected {{0, 1}, {0, 2}, {1, 3}, {2, 3}};
    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(NLExecutorTest, twoHopWithCarriedColumn) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeSink sink;
    runProgram(twoHopProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    // Both two-hop paths go from 0 to 3, one through 1 and one through 2
    const std::vector<std::vector<uint64_t>> expected {{0, 3}, {0, 3}};
    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(NLExecutorTest, twoHopInWithCarriedColumn) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeSink sink;
    runProgram(twoHopInProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    // Both two-hop predecessor chains end at 3 and trace back to 0, one through
    // 1 and one through 2, so each emits (a, c) = (3, 0)
    const std::vector<std::vector<uint64_t>> expected {{3, 0}, {3, 0}};
    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(NLExecutorTest, oneHopSmallChunksTwoParts) {
    auto graph = buildDiamondGraph();
    addSecondPart(*graph);

    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // A chunk size smaller than the node count forces several steps per loop;
    // the result must not depend on the chunking
    CollectingNodeSink sink;
    runProgram(oneHopOutProgram, reader.getView(), 2, sink);

    const std::vector<std::vector<uint64_t>> expected {{0, 1}, {0, 2}, {1, 3}, {2, 3}, {4, 5}};
    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(NLExecutorTest, limitScanEmitsLimitRows) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeSink sink;
    const std::string program = nlLimitScanProgram(3);
    runProgram(program.c_str(), reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    // Four nodes, LIMIT 3: exactly three rows.
    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows.size(), 3u);
}

TEST_F(NLExecutorTest, limitZeroScansNothing) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // LIMIT 0: the loop guard is false on entry, so the body never runs.
    CountingSink sink;
    const std::string program = nlLimitScanProgram(0);
    runProgram(program.c_str(), reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    EXPECT_EQ(sink.getCalls(), 0u);
    EXPECT_EQ(sink.getTotalRows(), 0u);
}

TEST_F(NLExecutorTest, limitEmitsPrefixAcrossChunks) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // chunkSize 2 over four nodes, LIMIT 3: the first chunk emits two rows, the
    // second a one-row prefix, then the loop breaks - two calls, three rows.
    CountingSink sink;
    const std::string program = nlLimitScanProgram(3);
    runProgram(program.c_str(), reader.getView(), 2, sink);

    EXPECT_EQ(sink.getCalls(), 2u);
    EXPECT_EQ(sink.getTotalRows(), 3u);
}

TEST_F(NLExecutorTest, limitTerminatesLoopEarly) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // LIMIT 2, chunkSize 2: the budget is spent after the first chunk, so the
    // loop breaks without filling the second - one call, two rows.
    CountingSink sink;
    const std::string program = nlLimitScanProgram(2);
    runProgram(program.c_str(), reader.getView(), 2, sink);

    EXPECT_EQ(sink.getCalls(), 1u);
    EXPECT_EQ(sink.getTotalRows(), 2u);
}

TEST_F(NLExecutorTest, limitExceedingCountSpansMultipleChunks) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // LIMIT 10 over four nodes in chunks of two: the budget never runs out, so
    // the loop emits both full chunks across the boundary and runs to exhaustion.
    CountingSink sink;
    const std::string program = nlLimitScanProgram(10);
    runProgram(program.c_str(), reader.getView(), 2, sink);

    EXPECT_EQ(sink.getCalls(), 2u);
    EXPECT_EQ(sink.getTotalRows(), 4u);
}

TEST_F(NLExecutorTest, limitUnwindsLoopNest) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // The diamond has four out-edges; LIMIT 1 on both loops of the nest stops
    // after one row, the inner break unwinding the outer scan loop too. Both
    // loops carry the handle, so the budget is spent in one inner step: exactly
    // one output call of one row. (This asserts correct streaming across the
    // nest; limitHaltsOuterLoopWhenBudgetSpent proves the outer loop halts.)
    CountingSink sink;
    const std::string program = nlLimitNestedLoopProgram(1);
    runProgram(program.c_str(), reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    EXPECT_EQ(sink.getCalls(), 1u);
    EXPECT_EQ(sink.getTotalRows(), 1u);
}

TEST_F(NLExecutorTest, limitHaltsOuterLoopWhenBudgetSpent) {
    auto graph = buildScoredGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Line graph 0 -> 1 -> 2 with chunkSize 1, so each node is its own outer
    // chunk and only the outer loop carries the limit. LIMIT 1: node 0's single
    // edge spends the budget, so the outer loop must halt - one call, one row.
    // Were the outer loop to keep driving, node 1's edge would add a second
    // (zero-row) output call, so getCalls() == 1 is what proves the halt.
    CountingSink sink;
    const std::string program = nlLimitOuterLoopOnlyProgram(1);
    runProgram(program.c_str(), reader.getView(), 1, sink);

    EXPECT_EQ(sink.getCalls(), 1u);
    EXPECT_EQ(sink.getTotalRows(), 1u);
}

TEST_F(NLExecutorTest, skipScanDropsPrefix) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeSink sink;
    const std::string program = nlSkipScanProgram(1);
    runProgram(program.c_str(), reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    // Four nodes, SKIP 1: three rows survive (the first scanned node is dropped).
    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows.size(), 3u);
}

TEST_F(NLExecutorTest, skipZeroEmitsAll) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // SKIP 0: nothing is dropped, so every node survives.
    CollectingNodeSink sink;
    const std::string program = nlSkipScanProgram(0);
    runProgram(program.c_str(), reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    const std::vector<std::vector<uint64_t>> expected {{0}, {1}, {2}, {3}};
    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(NLExecutorTest, skipExceedingCountEmitsNothing) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // SKIP 10 over four nodes: the whole scan is dropped, so no row is emitted.
    CountingSink sink;
    const std::string program = nlSkipScanProgram(10);
    runProgram(program.c_str(), reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    EXPECT_EQ(sink.getTotalRows(), 0u);
}

TEST_F(NLExecutorTest, skipEmitsSuffixAcrossChunks) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // chunkSize 2 over four nodes, SKIP 1: the first chunk drops one of its two
    // rows and emits the one-row suffix, the second chunk drops none and emits
    // both. The scan runs to exhaustion (a skip never early-exits) - two calls,
    // three rows.
    CountingSink sink;
    const std::string program = nlSkipScanProgram(1);
    runProgram(program.c_str(), reader.getView(), 2, sink);

    EXPECT_EQ(sink.getCalls(), 2u);
    EXPECT_EQ(sink.getTotalRows(), 3u);
}

TEST_F(NLExecutorTest, skipFoldedEmitsSuffixInPlace) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // The folded form, chunkSize 2 over nodes {0,1,2,3}, SKIP 1. The first chunk
    // [0,1] emits its suffix in place at offset 1 (node 1), the tail chunk [2,3]
    // passes through whole at offset 0 (nodes 2, 3) - both with no copy. The
    // surviving set is {1,2,3}; were the offset ignored, node 0 would survive and
    // node 1 would be dropped, giving the wrong {0,2,3}.
    CollectingNodeSink sink;
    const std::string program = nlSkipFoldedScanProgram(1);
    runProgram(program.c_str(), reader.getView(), 2, sink);

    const std::vector<std::vector<uint64_t>> expected {{1}, {2}, {3}};
    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    EXPECT_EQ(rows, expected);
}

TEST_F(NLExecutorTest, skipFoldedMatchesTruncated) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // The copy-free folded output must emit exactly what the nl.skip_truncate copy
    // path does, for the same SKIP and chunkSize - the fold is a peephole, not a
    // behaviour change.
    CollectingNodeSink truncatedSink;
    const std::string truncatedProgram = nlSkipScanProgram(1);
    runProgram(truncatedProgram.c_str(), reader.getView(), 2, truncatedSink);

    CollectingNodeSink foldedSink;
    const std::string foldedProgram = nlSkipFoldedScanProgram(1);
    runProgram(foldedProgram.c_str(), reader.getView(), 2, foldedSink);

    std::vector<std::vector<uint64_t>> truncatedRows;
    std::vector<std::vector<uint64_t>> foldedRows;
    truncatedSink.sortedRows(truncatedRows);
    foldedSink.sortedRows(foldedRows);
    EXPECT_EQ(foldedRows, truncatedRows);
}

TEST_F(NLExecutorTest, rejectsCrossLoopOutputColumns) {
    expectTranslationFailure(crossLoopOutputProgram);
}

TEST_F(NLExecutorTest, rejectsCrossLoopCarriedColumns) {
    expectTranslationFailure(crossLoopCarryProgram);
}

TEST_F(NLExecutorTest, sortsScannedNodesDescending) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    CollectingNodeSink sink;
    runProgram(nlSortDescProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    // Four nodes, sorted descending: emitted as exactly 3, 2, 1, 0.
    ASSERT_EQ(sink.getColumns().size(), 1u);
    const std::vector<uint64_t> expected {3, 2, 1, 0};
    EXPECT_EQ(sink.getColumns()[0], expected);
}

TEST_F(NLExecutorTest, sortsScannedNodesDescendingAcrossChunks) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // chunkSize 2 makes the scan collect two chunks into the buffer and the emit
    // re-chunk into two; the global descending order must still be 3, 2, 1, 0.
    CollectingNodeSink sink;
    runProgram(nlSortDescProgram, reader.getView(), 2, sink);

    ASSERT_EQ(sink.getColumns().size(), 1u);
    const std::vector<uint64_t> expected {3, 2, 1, 0};
    EXPECT_EQ(sink.getColumns()[0], expected);
}

TEST_F(NLExecutorTest, topKBoundedAccumulatorKeepsBest) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Four nodes, top-2 descending: the bounded accumulator emits exactly 3, 2.
    CollectingNodeSink sink;
    runProgram(nlTopKDescProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    ASSERT_EQ(sink.getColumns().size(), 1u);
    const std::vector<uint64_t> expected {3, 2};
    EXPECT_EQ(sink.getColumns()[0], expected);
}

TEST_F(NLExecutorTest, topKBoundedAccumulatorTrimsAcrossChunks) {
    // Six nodes (two dataparts) so the buffer grows past the 2 * topK = 4 trim
    // threshold; the four-node diamond alone never crosses it, so trimToTopK would
    // never run.
    auto graph = buildDiamondGraph();
    addSecondPart(*graph);

    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // chunkSize 1 feeds the accumulator one row at a time, so once the sixth row
    // arrives it overflows the bound and trimToTopK runs; the surviving top-2 by
    // descending node ID are 5, 4.
    CollectingNodeSink sink;
    runProgram(nlTopKDescProgram, reader.getView(), 1, sink);

    ASSERT_EQ(sink.getColumns().size(), 1u);
    const std::vector<uint64_t> expected {5, 4};
    EXPECT_EQ(sink.getColumns()[0], expected);
}

TEST_F(NLExecutorTest, distinctFilterDropsDuplicateTargets) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // The diamond's edges point at targets [1, 2, 3, 3] (node 3 from two sources);
    // the filter keeps each target once, so the distinct set is {1, 2, 3}.
    CollectingNodeSink sink;
    runProgram(nlDistinctTargetsProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    ASSERT_EQ(sink.getColumns().size(), 1u);
    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    const std::vector<std::vector<uint64_t>> expected {{1}, {2}, {3}};
    EXPECT_EQ(rows, expected);
}

TEST_F(NLExecutorTest, distinctFilterDedupsAcrossChunks) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // chunkSize 1 feeds one target per step, so the two edges into node 3 arrive in
    // separate steps; the seen-set persists across steps, so 3 is still emitted once.
    CollectingNodeSink sink;
    runProgram(nlDistinctTargetsProgram, reader.getView(), 1, sink);

    ASSERT_EQ(sink.getColumns().size(), 1u);
    std::vector<std::vector<uint64_t>> rows;
    sink.sortedRows(rows);
    const std::vector<std::vector<uint64_t>> expected {{1}, {2}, {3}};
    EXPECT_EQ(rows, expected);
}

TEST_F(NLExecutorTest, countsAllNodes) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // The diamond has four nodes; node IDs are never null, so the count is four,
    // emitted as a single present int64 row.
    CollectingCountSink sink;
    runProgram(nlCountNodesProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    const std::vector<uint64_t> expected {4};
    EXPECT_EQ(sink.getValues(), expected);
}

TEST_F(NLExecutorTest, countsAcrossChunks) {
    auto graph = buildDiamondGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // chunkSize 1 feeds one node per step, so the tally is accumulated across four
    // separate steps; nl.count is reset once at function scope, not per chunk, so
    // the count is still four.
    CollectingCountSink sink;
    runProgram(nlCountNodesProgram, reader.getView(), 1, sink);

    const std::vector<uint64_t> expected {4};
    EXPECT_EQ(sink.getValues(), expected);
}

TEST_F(NLExecutorTest, countsOnlyNonNullValues) {
    auto graph = buildScoredGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Three nodes carry scores 100, 200 and null (node 2 has none). count(a.score)
    // charges only the present values, so the tally is two, not three.
    CollectingCountSink sink;
    runProgram(nlCountScoresProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    const std::vector<uint64_t> expected {2};
    EXPECT_EQ(sink.getValues(), expected);
}

TEST_F(NLExecutorTest, sumsNonNullValues) {
    auto graph = buildScoredGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Scores 100, 200 and null: sum(a.score) adds the present values, so the single
    // result row is a present 300 (the null is ignored).
    CollectingOptInt64Sink sink;
    runProgram(nlIntAggregateScoresProgram("sum").c_str(), reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    const std::vector<std::optional<int64_t>> expected {300};
    EXPECT_EQ(sink.getValues(), expected);
}

TEST_F(NLExecutorTest, minOfNonNullValues) {
    auto graph = buildScoredGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // min(a.score) over 100, 200, null is the smallest present value, 100.
    CollectingOptInt64Sink sink;
    runProgram(nlIntAggregateScoresProgram("min").c_str(), reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    const std::vector<std::optional<int64_t>> expected {100};
    EXPECT_EQ(sink.getValues(), expected);
}

TEST_F(NLExecutorTest, maxOfNonNullValues) {
    auto graph = buildScoredGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // max(a.score) over 100, 200, null is the largest present value, 200.
    CollectingOptInt64Sink sink;
    runProgram(nlIntAggregateScoresProgram("max").c_str(), reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    const std::vector<std::optional<int64_t>> expected {200};
    EXPECT_EQ(sink.getValues(), expected);
}

TEST_F(NLExecutorTest, avgOfNonNullValues) {
    auto graph = buildScoredGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // avg(a.score) divides the sum of the present values (300) by their count (2),
    // ignoring the null, so the single result row is a present 150.0.
    CollectingOptDoubleSink sink;
    runProgram(nlAvgScoresProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    const std::vector<std::optional<double>> expected {150.0};
    EXPECT_EQ(sink.getValues(), expected);
}

TEST_F(NLExecutorTest, sumsAcrossChunks) {
    auto graph = buildScoredGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // chunkSize 1 folds one node per step, so the accumulator is grown across
    // several steps; nl.aggregate resets it once at function scope, not per chunk,
    // so the sum is still 300.
    CollectingOptInt64Sink sink;
    runProgram(nlIntAggregateScoresProgram("sum").c_str(), reader.getView(), 1, sink);

    const std::vector<std::optional<int64_t>> expected {300};
    EXPECT_EQ(sink.getValues(), expected);
}

TEST_F(NLExecutorTest, sumOfAllNullIsZero) {
    auto graph = buildAllNullScoredGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // No node carries a score, so sum folds no value: it stays at its identity, a
    // present 0 (Cypher's sum of an all-null/empty input is 0, never null).
    CollectingOptInt64Sink sink;
    runProgram(nlIntAggregateScoresProgram("sum").c_str(), reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    const std::vector<std::optional<int64_t>> expected {0};
    EXPECT_EQ(sink.getValues(), expected);
}

TEST_F(NLExecutorTest, minMaxOfAllNullIsNull) {
    auto graph = buildAllNullScoredGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // With no non-null value, min/max never seed their accumulator, so each emits a
    // single null row (Cypher's min/max of an all-null/empty input is null).
    CollectingOptInt64Sink minSink;
    runProgram(nlIntAggregateScoresProgram("min").c_str(), reader.getView(), ChunkConfig::CHUNK_SIZE, minSink);
    EXPECT_EQ(minSink.getValues(), (std::vector<std::optional<int64_t>> {std::nullopt}));

    CollectingOptInt64Sink maxSink;
    runProgram(nlIntAggregateScoresProgram("max").c_str(), reader.getView(), ChunkConfig::CHUNK_SIZE, maxSink);
    EXPECT_EQ(maxSink.getValues(), (std::vector<std::optional<int64_t>> {std::nullopt}));
}

TEST_F(NLExecutorTest, avgOfAllNullIsNull) {
    auto graph = buildAllNullScoredGraph();
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // The non-null count is zero, so avg divides nothing and emits a single null row
    // (Cypher's avg of an all-null/empty input is null, not 0).
    CollectingOptDoubleSink sink;
    runProgram(nlAvgScoresProgram, reader.getView(), ChunkConfig::CHUNK_SIZE, sink);

    const std::vector<std::optional<double>> expected {std::nullopt};
    EXPECT_EQ(sink.getValues(), expected);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
