#include <gtest/gtest.h>

#include <stdint.h>

#include <algorithm>
#include <memory>
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
#include "list/ListElementView.h"
#include "list/ListView.h"
#include "map/MapBufferTypeTag.h"
#include "map/MapEntryView.h"
#include "map/MapView.h"
#include "metadata/DateTime.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringException.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using Rows = std::vector<std::string>;

std::string renderMap(MapView map);

std::string renderList(ListView list);

std::string renderListElement(const ListElementView element) {
    switch (element.getTag()) {
        case ListBufferTypeTag::Int:
            return std::to_string(element.getAs<int64_t>());
        break;

        case ListBufferTypeTag::String:
            return "'" + std::string(element.getAs<std::string_view>()) + "'";
        break;

        case ListBufferTypeTag::Null:
            return "null";
        break;

        default:
            return "?";
        break;
    }
}

std::string renderList(const ListView list) {
    std::string rendered = "[";
    for (size_t index = 0; const ListElementView& element : list) {
        if (index > 0) {
            rendered += ", ";
        }

        rendered += renderListElement(element);
        index++;
    }

    return rendered + "]";
}

std::string renderMapValue(const MapEntryView entry) {
    switch (entry.getValueTag()) {
        case MapBufferTypeTag::Int:
            return std::to_string(entry.getValueAs<int64_t>());
        break;

        case MapBufferTypeTag::UInt:
            return std::to_string(entry.getValueAs<uint64_t>());
        break;

        case MapBufferTypeTag::Bool:
            return static_cast<bool>(entry.getValueAs<CustomBool>()) ? "true" : "false";
        break;

        case MapBufferTypeTag::String:
            return "'" + std::string(entry.getValueAs<std::string_view>()) + "'";
        break;

        case MapBufferTypeTag::ListView:
            return renderList(entry.getValueAs<ListView>());
        break;

        case MapBufferTypeTag::MapView:
            return renderMap(entry.getValueAs<MapView>());
        break;

        case MapBufferTypeTag::Null:
            return "null";
        break;

        case MapBufferTypeTag::NodeID:
            return "node:" + std::to_string(entry.getValueAs<NodeID>().getValue());
        break;

        case MapBufferTypeTag::EdgeID:
            return "edge:" + std::to_string(entry.getValueAs<EdgeID>().getValue());
        break;

        case MapBufferTypeTag::DateTime: {
            std::string formatted;
            DateTime::format(formatted, entry.getValueAs<types::DateTime::Primitive>());
            return formatted;
        }
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

std::string renderCell(const Column* column, size_t row) {
    if (const auto* maps = dynamic_cast<const ColumnConst<MapView>*>(column)) {
        return renderMap((*maps)[row]);
    }

    if (const auto* mapRows = dynamic_cast<const ColumnVector<MapView>*>(column)) {
        return renderMap((*mapRows)[row]);
    }

    if (const auto* edgeIDs = dynamic_cast<const ColumnEdgeIDs*>(column)) {
        return "edge:" + std::to_string((*edgeIDs)[row].getValue());
    }

    throw TuringException("CypherDynamicMapLiteralTest: unsupported output column type");
}

class CollectingMapSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            std::string row;
            for (size_t index = 0; const Column* column : chunks) {
                if (index > 0) {
                    row += " | ";
                }

                row += renderCell(column, rowIndex);
                index++;
            }

            _rows.push_back(row);
        }
    }

    const Rows& rows() const { return _rows; }

private:
    Rows _rows;
};

const Rows personNames = {
    "Adam", "Cyrus", "Doruk", "Luc", "Martina", "Maxime", "Remy", "Suhas",
};

Rows personRows(std::string_view prefix, std::string_view suffix) {
    Rows rows;
    for (const std::string& name : personNames) {
        rows.push_back(std::string(prefix) + "'" + name + "'" + std::string(suffix));
    }

    return rows;
}

// MATCH (n)-->(m) RETURN {name: n.name, age: m.age}: one row per edge. Only Remy and Adam
// have an age, so only the three edges into them carry one.
const Rows edgeNameAgeRows = {
    "{age: 32, name: 'Adam'}",
    "{age: 32, name: 'Ghosts'}",
    "{age: 32, name: 'Remy'}",
    "{age: null, name: 'Adam'}",
    "{age: null, name: 'Adam'}",
    "{age: null, name: 'Cyrus'}",
    "{age: null, name: 'Cyrus'}",
    "{age: null, name: 'Doruk'}",
    "{age: null, name: 'Luc'}",
    "{age: null, name: 'Luc'}",
    "{age: null, name: 'Martina'}",
    "{age: null, name: 'Maxime'}",
    "{age: null, name: 'Maxime'}",
    "{age: null, name: 'Remy'}",
    "{age: null, name: 'Remy'}",
    "{age: null, name: 'Remy'}",
    "{age: null, name: 'Suhas'}",
    "{age: null, name: 'Suhas'}",
};

}

class CypherDynamicMapLiteralTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);
    }

    void runQuery(std::string_view query, Rows& rows, size_t chunkSize) {
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

        CollectingMapSink sink;
        LocalMemory memory;
        DBDialectInterpreter interpreter(module, &view, &sink, &memory, chunkSize);
        interpreter.run();

        rows = sink.rows();
    }

    // Cypher gives no row order without ORDER BY, so rows compare as sorted multisets
    void expectRows(std::string_view query, const Rows& expected, size_t chunkSize = ChunkConfig::CHUNK_SIZE) {
        Rows actual;
        runQuery(query, actual, chunkSize);

        Rows sortedExpected = expected;
        std::sort(actual.begin(), actual.end());
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(actual, sortedExpected) << "query: " << query;
    }

    // A cut without ORDER BY keeps an unspecified subset, so only its size and membership
    // are pinned
    void expectSubsetRows(std::string_view query, const Rows& universe, size_t expectedCount) {
        Rows actual;
        runQuery(query, actual, ChunkConfig::CHUNK_SIZE);

        EXPECT_EQ(actual.size(), expectedCount) << "query: " << query;
        for (const std::string& row : actual) {
            EXPECT_NE(std::find(universe.begin(), universe.end(), row), universe.end())
                << "query: " << query << ", unexpected row: " << row;
        }
    }

    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
    const std::string _graphName {"simpledb"};
};

TEST_F(CypherDynamicMapLiteralTest, PropertiesOfBothEndpoints) {
    expectRows("MATCH (n)-->(m) RETURN {name: n.name, age: m.age}", edgeNameAgeRows);
}

TEST_F(CypherDynamicMapLiteralTest, PropertiesOfBothEndpointsAcrossChunks) {
    expectRows("MATCH (n)-->(m) RETURN {name: n.name, age: m.age}", edgeNameAgeRows, 2);
}

TEST_F(CypherDynamicMapLiteralTest, ConstantValueBesideColumn) {
    expectRows("MATCH (n:Person) RETURN {name: n.name, k: 1}", personRows("{k: 1, name: ", "}"));
}

TEST_F(CypherDynamicMapLiteralTest, KeysOrderedAsInConstantMap) {
    expectRows("MATCH (n:Person) RETURN {z: n.name, a: 1}", personRows("{a: 1, z: ", "}"));
}

TEST_F(CypherDynamicMapLiteralTest, NestedDynamicMap) {
    expectRows("MATCH (n:Person) RETURN {a: {b: n.name}}", personRows("{a: {b: ", "}}"));
}

TEST_F(CypherDynamicMapLiteralTest, ConstantNestedMapBesideColumn) {
    expectRows("MATCH (n:Person) RETURN {a: n.name, b: {c: 1}}", personRows("{a: ", ", b: {c: 1}}"));
}

TEST_F(CypherDynamicMapLiteralTest, DynamicListValue) {
    expectRows("MATCH (n:Person) RETURN {xs: [n.name, 1]}", personRows("{xs: [", ", 1]}"));
}

TEST_F(CypherDynamicMapLiteralTest, LimitOverDynamicMap) {
    expectSubsetRows("MATCH (n:Person) RETURN {name: n.name} LIMIT 3", personRows("{name: ", "}"), 3);
}

TEST_F(CypherDynamicMapLiteralTest, SkipOverDynamicMap) {
    expectSubsetRows("MATCH (n:Person) RETURN {name: n.name} SKIP 5", personRows("{name: ", "}"), 3);
}

TEST_F(CypherDynamicMapLiteralTest, AggregateValue) {
    expectRows("MATCH (n) RETURN {c: count(n)}", {"{c: 18}"});
}

TEST_F(CypherDynamicMapLiteralTest, NodeValue) {
    expectRows("MATCH (n:Person) RETURN {node: n, name: n.name}",
               {
                   "{name: 'Remy', node: node:0}",
                   "{name: 'Adam', node: node:1}",
                   "{name: 'Maxime', node: node:8}",
                   "{name: 'Luc', node: node:9}",
                   "{name: 'Martina', node: node:11}",
                   "{name: 'Suhas', node: node:12}",
                   "{name: 'Cyrus', node: node:15}",
                   "{name: 'Doruk', node: node:17}",
               });
}

// Only Remy and Adam know someone well, each other; the other six friends are the invalid
// ID of an OPTIONAL MATCH miss, which the map holds as null
TEST_F(CypherDynamicMapLiteralTest, UnmatchedNodeValueIsNull) {
    expectRows("MATCH (n:Person) OPTIONAL MATCH (n)-[:KNOWS_WELL]->(m) RETURN {friend: m}",
               {
                   "{friend: node:0}",
                   "{friend: node:1}",
                   "{friend: null}",
                   "{friend: null}",
                   "{friend: null}",
                   "{friend: null}",
                   "{friend: null}",
                   "{friend: null}",
               });
}

TEST_F(CypherDynamicMapLiteralTest, EdgeValueMatchesEdgeColumn) {
    Rows rows;
    runQuery("MATCH (n)-[r]->(m) RETURN r, {rel: r}", rows, ChunkConfig::CHUNK_SIZE);

    ASSERT_EQ(rows.size(), 18u);
    for (const std::string& row : rows) {
        const size_t separator = row.find(" | ");
        ASSERT_NE(separator, std::string::npos) << row;

        const std::string edge = row.substr(0, separator);
        EXPECT_EQ(row.substr(separator + 3), "{rel: " + edge + "}");
    }
}

// Out-degree of each node that has an outgoing edge: Remy 4, Adam 3, Maxime, Luc, Cyrus and
// Suhas 2, Ghosts, Martina and Doruk 1
TEST_F(CypherDynamicMapLiteralTest, AggregateValueBesideGroupingKey) {
    expectRows("MATCH (n)-->(m) WITH n.name AS name, {c: count(m)} AS degree RETURN degree",
               {"{c: 4}", "{c: 3}", "{c: 2}", "{c: 2}", "{c: 2}", "{c: 2}", "{c: 1}", "{c: 1}", "{c: 1}"});
}

// A map holding a grouping key beside an aggregate has to be built over the grouped rows,
// so the key must bind to the column the aggregate yields rather than to the per-row one.
// Codegen binds the per-row column, so the two operands end up bound in different loops and
// lowering rejects the result. Disabled until grouped-aggregate codegen substitutes the key
// inside a container - the same gap RETURN [n.name, count(n)] has had since before maps.
TEST_F(CypherDynamicMapLiteralTest, DISABLED_GroupingKeyBesideAnAggregateInOneMap) {
    Rows expected;
    for (const std::string& name : personNames) {
        expected.push_back("{k: '" + name + "', v: 1}");
    }

    expectRows("MATCH (n:Person) RETURN {k: n.name, v: count(n)}", expected);
}

// A map column keys a DISTINCT by its entries: only Remy and Adam have an age, both 32
TEST_F(CypherDynamicMapLiteralTest, DistinctMapColumn) {
    expectRows("MATCH (n:Person) RETURN DISTINCT {age: n.age}", {"{age: 32}", "{age: null}"});
}

TEST_F(CypherDynamicMapLiteralTest, MapColumnAsGroupingKey) {
    expectRows("MATCH (n:Person) WITH {age: n.age} AS a, count(n) AS c RETURN a", {"{age: 32}", "{age: null}"});
}

TEST_F(CypherDynamicMapLiteralTest, DistinctConstantMap) {
    expectRows("MATCH (n:Person) RETURN DISTINCT {a: 1}", {"{a: 1}"});
}

// A datetime is a value like any other, so a map holds one read per row or from a
// type-erased element, and DISTINCT keys it by the instant it names
TEST_F(CypherDynamicMapLiteralTest, DateTimeValue) {
    expectRows("RETURN {d: datetime('2026-09-23T14:05:00Z')}", {"{d: 2026-09-23T14:05:00Z}"});
}

TEST_F(CypherDynamicMapLiteralTest, DateTimeFromTypeErasedElement) {
    expectRows("UNWIND [datetime('2026-09-23T14:05:00Z'), 1] AS x RETURN {d: x}",
               {"{d: 2026-09-23T14:05:00Z}", "{d: 1}"});
}

TEST_F(CypherDynamicMapLiteralTest, DistinctDateTimeValuesByInstant) {
    expectRows("UNWIND [datetime('2026-09-23T14:05:00Z'), datetime('2026-09-23T16:05:00+02:00')] AS x "
               "RETURN DISTINCT {d: x}",
               {"{d: 2026-09-23T14:05:00Z}"});
}
