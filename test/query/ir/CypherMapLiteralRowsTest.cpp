#include <gtest/gtest.h>

#include <stdint.h>

#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"

#include "DBDialect.h"
#include "DBDialectInterpreter.h"
#include "DBProgramGenerator.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "NLOutputSink.h"
#include "StorageDialect.h"

#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "iterators/ChunkConfig.h"
#include "map/MapBufferTypeTag.h"
#include "map/MapEntryView.h"
#include "map/MapView.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringException.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using Row = std::vector<std::string>;
using Rows = std::vector<Row>;

std::string renderMap(MapView map);

std::string renderMapValue(const MapEntryView entry) {
    switch (entry.getValueTag()) {
        case MapBufferTypeTag::Int:
            return std::to_string(entry.getValueAs<int64_t>());
        break;

        case MapBufferTypeTag::Bool:
            return static_cast<bool>(entry.getValueAs<CustomBool>()) ? "true" : "false";
        break;

        // Quoted so a zero-length string reads as a value rather than as nothing
        case MapBufferTypeTag::String:
            return "'" + std::string(entry.getValueAs<std::string_view>()) + "'";
        break;

        case MapBufferTypeTag::MapView:
            return renderMap(entry.getValueAs<MapView>());
        break;

        case MapBufferTypeTag::Null:
            return "null";
        break;

        default:
            return "?";
        break;
    }
}

std::string renderMap(const MapView map) {
    std::string rendered = "{";
    for (size_t index = 0; const MapEntryView& entry : map) {
        if (index > 0) {
            rendered += ", ";
        }

        rendered += std::string(entry.getKey()) + ": " + renderMapValue(entry);
        index++;
    }

    return rendered + "}";
}

// Render one output cell, whatever column shape the program emitted: the one map a
// literal holds in every row, the per-row map column a cut reads, or a node ID.
std::string renderCell(const Column* column, size_t row) {
    if (const auto* maps = dynamic_cast<const ColumnConst<MapView>*>(column)) {
        return renderMap((*maps)[row]);
    }

    // A cut reads its rows, so the map is laid out across them as a column of cells
    // rather than the one value it is when nothing but the projection reads it.
    if (const auto* mapRows = dynamic_cast<const ColumnVector<MapView>*>(column)) {
        return renderMap((*mapRows)[row]);
    }

    if (const auto* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(column)) {
        return std::to_string((*nodeIDs)[row].getValue());
    }

    throw TuringException("CypherMapLiteralRowsTest: unsupported output column type");
}

// One row per SimpleGraph node - they are numbered 0 through 17 - each holding the same
// map cell, optionally cut to the first @param rowCount of them.
Rows nodeRowsWithMap(std::string_view mapCell, size_t rowCount) {
    Rows rows;
    for (size_t row = 0; row < rowCount; row++) {
        rows.push_back({std::string(mapCell)});
    }

    return rows;
}

class CollectingRowSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            Row& row = _rows.emplace_back();
            for (const Column* column : chunks) {
                row.push_back(renderCell(column, rowIndex));
            }
        }
    }

    const Rows& rows() const { return _rows; }

private:
    Rows _rows;
};

}

// A map literal projected over matched rows. Without a cut the map stays the single
// ColumnConst<MapView> it is built as; a LIMIT or a SKIP reads its rows, so it is
// broadcast into a ColumnVector<MapView> first - the column a map had no pool for until
// this path was wired, which made every cut over a lone map projection fail to translate.
class CypherMapLiteralRowsTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);
    }

    void runQuery(std::string_view query, Rows& rows, size_t chunkSize = ChunkConfig::CHUNK_SIZE) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const ProcedureManager* procedures = system.getProcedures();

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        CypherAST ast(procedures, query);

        CypherParser parser(&ast);
        parser.parse(query);

        CypherAnalyzer analyzer(&ast, view);
        analyzer.analyze();

        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::db::DB>();
        context.getOrLoadDialect<mlir::nl::NL>();

        mlir::OpBuilder builder(&context);
        mlir::OwningOpRef<mlir::ModuleOp> owningModule = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp module = owningModule.get();

        DBProgramGenerator generator(&module);
        generator.generate(&ast);

        CollectingRowSink sink;
        LocalMemory memory;
        DBDialectInterpreter interpreter(module, &view, &sink, &memory, chunkSize);
        interpreter.run();

        rows = sink.rows();
    }

    void expectRows(std::string_view query,
                    const Rows& expected,
                    size_t chunkSize = ChunkConfig::CHUNK_SIZE) {
        Rows actual;
        runQuery(query, actual, chunkSize);

        EXPECT_EQ(actual, expected) << "query: " << query;
    }

    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
    const std::string _graphName {"simpledb"};
};

TEST_F(CypherMapLiteralRowsTest, MapLiteralOverEveryRow) {
    expectRows("MATCH (n) RETURN {x: 'tag'}", nodeRowsWithMap("{x: 'tag'}", 18));
}

TEST_F(CypherMapLiteralRowsTest, LimitOneOverLoneMapProjection) {
    expectRows("MATCH (n) RETURN {x: 'tag'} LIMIT 1", nodeRowsWithMap("{x: 'tag'}", 1));
}

TEST_F(CypherMapLiteralRowsTest, LimitManyOverLoneMapProjection) {
    expectRows("MATCH (n) RETURN {x: 'tag'} LIMIT 5", nodeRowsWithMap("{x: 'tag'}", 5));
}

TEST_F(CypherMapLiteralRowsTest, SkipOverLoneMapProjection) {
    expectRows("MATCH (n) RETURN {x: 'tag'} SKIP 16", nodeRowsWithMap("{x: 'tag'}", 2));
}

TEST_F(CypherMapLiteralRowsTest, SkipAndLimitOverLoneMapProjection) {
    expectRows("MATCH (n) RETURN {x: 'tag'} SKIP 5 LIMIT 3", nodeRowsWithMap("{x: 'tag'}", 3));
}

// The map is cut beside a column that already carries the rows, so it is the driver's
// cardinality the broadcast lays it out over rather than its own.
TEST_F(CypherMapLiteralRowsTest, LimitOverMapBesideNodeColumn) {
    const Rows expected = {
        {"0", "{x: 'tag'}"},
        {"1", "{x: 'tag'}"},
        {"2", "{x: 'tag'}"},
    };

    expectRows("MATCH (n) RETURN n, {x: 'tag'} LIMIT 3", expected);
}

// A nested map is materialized part-way through filling its parent, so the cut must carry
// the parent's view and the child's alike.
TEST_F(CypherMapLiteralRowsTest, LimitOverNestedMap) {
    expectRows("MATCH (n) RETURN {outer: {inner: 1}, tail: 2} LIMIT 2",
               nodeRowsWithMap("{outer: {inner: 1}, tail: 2}", 2));
}

// A null value is a Null-tagged entry inside a map that is itself present in every row,
// so the column stays a plain ColumnVector<MapView> rather than becoming nullable.
TEST_F(CypherMapLiteralRowsTest, LimitOverMapHoldingNull) {
    expectRows("MATCH (n) RETURN {a: null, b: 1} LIMIT 2",
               nodeRowsWithMap("{a: null, b: 1}", 2));
}

// The cut spans chunk boundaries: with a chunk of 4 rows the scan emits five chunks and
// the broadcast refills the map column on each.
TEST_F(CypherMapLiteralRowsTest, LimitOverMapAcrossChunks) {
    expectRows("MATCH (n) RETURN {x: 'tag'} LIMIT 10", nodeRowsWithMap("{x: 'tag'}", 10), 4);
}
