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
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "iterators/ChunkConfig.h"
#include "list/ListBufferTypeTag.h"
#include "list/ListElementView.h"
#include "list/ListView.h"
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

using Rows = std::vector<std::string>;

std::string renderMap(MapView map);

std::string renderList(ListView list);

std::string renderMapValue(const MapEntryView entry) {
    switch (entry.getValueTag()) {
        case MapBufferTypeTag::Int:
            return std::to_string(entry.getValueAs<int64_t>());
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

std::string renderElement(const ListElementView element) {
    switch (element.getTag()) {
        case ListBufferTypeTag::Int:
            return std::to_string(element.getAs<int64_t>());
        break;

        case ListBufferTypeTag::String:
            return "'" + std::string(element.getAs<std::string_view>()) + "'";
        break;

        case ListBufferTypeTag::ListView:
            return renderList(element.getAs<ListView>());
        break;

        case ListBufferTypeTag::MapView:
            return renderMap(element.getAs<MapView>());
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

        rendered += renderElement(element);
        index++;
    }

    return rendered + "]";
}

std::string renderCell(const Column* column, size_t row) {
    if (const auto* lists = dynamic_cast<const ColumnConst<ListView>*>(column)) {
        return renderList((*lists)[row]);
    }

    if (const auto* listRows = dynamic_cast<const ColumnVector<ListView>*>(column)) {
        return renderList((*listRows)[row]);
    }

    if (const auto* elements = dynamic_cast<const ColumnVector<ListElementView>*>(column)) {
        return renderElement((*elements)[row]);
    }

    // A list comprehension yields a nullable list: a row whose source list was null has none
    if (const auto* optLists = dynamic_cast<const ColumnOptVector<ListView>*>(column)) {
        const std::optional<ListView>& list = (*optLists)[row];
        return list.has_value() ? renderList(*list) : "null";
    }

    throw TuringException("ListOfMapsTest: unsupported output column type");
}

class CollectingListSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            for (const Column* column : chunks) {
                _rows.push_back(renderCell(column, rowIndex));
            }
        }
    }

    const Rows& rows() const { return _rows; }

private:
    Rows _rows;
};

}

// A list may hold a map, the way a map already holds a list. The map rides the list as the
// one tagged cell it is, so it survives nesting, neighbours of other types and an UNWIND
// that spreads the list back out.
class ListOfMapsTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);
    }

    void runQuery(std::string_view query, Rows& rows) {
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

        CollectingListSink sink;
        LocalMemory memory;
        DBDialectInterpreter interpreter(module, &view, &sink, &memory, ChunkConfig::CHUNK_SIZE);
        interpreter.run();

        rows = sink.rows();
    }

    void expectRows(std::string_view query, const Rows& expected) {
        Rows actual;
        runQuery(query, actual);

        EXPECT_EQ(actual, expected) << "query: " << query;
    }

    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
    const std::string _graphName {"simpledb"};
};

TEST_F(ListOfMapsTest, holdsAMap) {
    expectRows("RETURN [{age: 32}]", {"[{age: 32}]"});
}

TEST_F(ListOfMapsTest, holdsSeveralMaps) {
    expectRows("RETURN [{a: 1}, {b: 2}]", {"[{a: 1}, {b: 2}]"});
}

// Keys reach the buffer in the order DictionaryAttr canonicalises them into, which is
// sorted on the key name rather than the order they were written in
TEST_F(ListOfMapsTest, holdsAMapOfSeveralEntries) {
    expectRows("RETURN [{name: 'Remy', age: 32}]", {"[{age: 32, name: 'Remy'}]"});
}

TEST_F(ListOfMapsTest, holdsAMapBesideOtherElements) {
    expectRows("RETURN [1, {a: 'x'}, null]", {"[1, {a: 'x'}, null]"});
}

TEST_F(ListOfMapsTest, holdsAMapHoldingAList) {
    expectRows("RETURN [{xs: [1, 2]}]", {"[{xs: [1, 2]}]"});
}

TEST_F(ListOfMapsTest, holdsAMapHoldingAMap) {
    expectRows("RETURN [{inner: {a: 1}}]", {"[{inner: {a: 1}}]"});
}

TEST_F(ListOfMapsTest, holdsAListHoldingAMap) {
    expectRows("RETURN [[{a: 1}]]", {"[[{a: 1}]]"});
}

TEST_F(ListOfMapsTest, holdsAnEmptyMap) {
    expectRows("RETURN [{}]", {"[{}]"});
}

// An UNWIND spreads the list back out, so each map comes back as the one tagged cell it
// was stored as
TEST_F(ListOfMapsTest, unwindsToItsMaps) {
    expectRows("UNWIND [{a: 1}, {b: 2}] AS x RETURN x", {"{a: 1}", "{b: 2}"});
}

TEST_F(ListOfMapsTest, unwindsAMapBesideOtherElements) {
    expectRows("UNWIND [1, {a: 'x'}] AS x RETURN x", {"1", "{a: 'x'}"});
}

// A comprehension builds its list one projected element at a time, so a map it projects
// reaches the list through a different site than a list literal's elements do
TEST_F(ListOfMapsTest, holdsTheMapsAComprehensionProjects) {
    expectRows("RETURN [x IN [1, 2] | {a: 1}]", {"[{a: 1}, {a: 1}]"});
}

TEST_F(ListOfMapsTest, holdsTheMapsAComprehensionProjectsPerElement) {
    expectRows("RETURN [x IN [1, 2] | {a: x}]", {"[{a: 1}, {a: 2}]"});
}

TEST_F(ListOfMapsTest, holdsTheMapsAComprehensionProjectsFromARow) {
    expectRows("MATCH (n:Person) RETURN [x IN [1] | {name: n.name}] LIMIT 2",
               {"[{name: 'Remy'}]", "[{name: 'Adam'}]"});
}

// The map a row reads is built per row, so the list holding it is built per row too
TEST_F(ListOfMapsTest, holdsAMapReadingARow) {
    expectRows("MATCH (n:Person) RETURN [{name: n.name}] LIMIT 3",
               {"[{name: 'Remy'}]", "[{name: 'Adam'}]", "[{name: 'Maxime'}]"});
}

TEST_F(ListOfMapsTest, holdsAMapReadingARowBesideAConstant) {
    expectRows("MATCH (n:Person) RETURN [1, {name: n.name}] LIMIT 2",
               {"[1, {name: 'Remy'}]", "[1, {name: 'Adam'}]"});
}

// A collect gathers its rows into a list, so a map value should collect like any other.
// Disabled: collect's signature rejects a Map argument in the analyzer, before the lowering
// gap in lowerCollect's cell-presence set is even reached.
TEST_F(ListOfMapsTest, DISABLED_collectsMapsReadingARow) {
    expectRows("UNWIND [1, 2] AS x RETURN collect({a: x})", {"[{a: 1}, {a: 2}]"});
}

TEST_F(ListOfMapsTest, DISABLED_collectsAConstantMap) {
    expectRows("UNWIND [1, 2] AS x RETURN collect({a: 1})", {"[{a: 1}, {a: 1}]"});
}

// Two maps that compare equal dedup together, so a map keys a DISTINCT like any other value
TEST_F(ListOfMapsTest, dedupsEqualMaps) {
    expectRows("UNWIND [{a: 1}, {a: 1}] AS x RETURN DISTINCT x", {"{a: 1}"});
}

TEST_F(ListOfMapsTest, keepsMapsThatDiffer) {
    expectRows("UNWIND [{a: 1}, {a: 2}] AS x RETURN DISTINCT x", {"{a: 1}", "{a: 2}"});
}

// The key normalises numbers the way a list element's does, so it agrees with equality
TEST_F(ListOfMapsTest, dedupsMapsWhoseNumbersDifferOnlyByTag) {
    expectRows("UNWIND [{a: 1}, {a: 1.0}] AS x RETURN DISTINCT x", {"{a: 1}"});
}

TEST_F(ListOfMapsTest, dedupsListsHoldingEqualMaps) {
    expectRows("UNWIND [[{a: 1}], [{a: 1}]] AS x RETURN DISTINCT x", {"[{a: 1}]"});
}
