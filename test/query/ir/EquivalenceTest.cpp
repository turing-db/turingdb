#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <range/v3/view/zip.hpp>
#include <spdlog/fmt/bundled/format.h>

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

#include "EntityList.h"
#include "Graph.h"
#include "GraphPath.h"
#include "ID.h"
#include "QueryConfig.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "columns/AllowedKinds.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "columns/ColumnVector.h"
#include "dataframe/Dataframe.h"
#include "dataframe/NamedColumn.h"
#include "list/ListElementView.h"
#include "list/ListUtils.h"
#include "list/ListView.h"
#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"
#include "versioning/CommitHash.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

// Columns/dataframes are converted to strings row-wise for simple sorting and comparison
// code, but obviously slow. Since this is just a test and on a small graph (simpledb), I
// think the simplicity of the testing code is more valuable than an efficient
// implementation
using Row = std::vector<std::string>;
using Rows = std::vector<Row>;

std::string valueToString(const std::string& value) {
    return value;
}

std::string valueToString(const std::string_view value) {
    return std::string(value);
}

std::string valueToString(const Path& value) {
    std::string result;

    size_t index = 0;
    for (const auto entity : value | std::views::reverse) {
        if (index % 2 == 0) {
            result += fmt::format("({})", entity.getValue());
        } else {
            result += fmt::format("-[{}]->", entity.getValue());
        }
        index++;
    }

    return result;
}

std::string valueToString(const ValueType value) {
    return std::string(ValueTypeName::value(value));
}

std::string valueToString(const CustomBool value) {
    return value ? "true" : "false";
}

// Fixed 6-decimal rendering so the v2/v3 comparison tolerates float ULP noise
// (e.g. cosine_similarity of identical vectors yields 1.0000001 on one side, 1 on
// the other); fmt's default "{}" would print full precision and flag that as a diff.
std::string valueToString(const double value) {
    return std::to_string(value);
}

template <IntegralType T, int tag>
std::string valueToString(const ID<T, tag> value) {
    return std::to_string(value.getValue());
}

template <int I>
std::string valueToString(const TemplateCommitHash<I> value) {
    return std::to_string(value.get());
}

std::string valueToString(const PropertyNull) {
    return "null";
}

std::string valueToString(const std::span<const float> value) {
    std::string result = "[";

    for (size_t i = 0; i < value.size(); i++) {
        if (i > 0) {
            result += ", ";
        }
        result += std::to_string(value[i]);
    }

    result += "]";
    return result;
}

template <typename T>
std::string valueToString(const T& value) {
    return fmt::format("{}", value);
}

template <typename T>
std::string valueToString(const std::optional<T>& value) {
    if (!value.has_value()) {
        return "null";
    }

    return valueToString(*value);
}

std::string valueToString(const EntityList& value) {
    std::string result = "[";
    size_t index = 0;

    for (const auto& entity : value) {
        if (index++ > 0) {
            result += ", ";
        }

        if (entity._type == EntityType::Node) {
            result += fmt::format("({})", entity._id.getValue());
        } else {
            result += fmt::format("[{}]", entity._id.getValue());
        }
    }

    result += "]";
    return result;
}

std::string valueToString(ListView view);

std::string valueToString(const ListElementView element) {
    const auto writeTyped = []<typename T>(const ListElementView element) -> std::string {
        return valueToString(element.getAs<T>());
    };

    const ListBufferTypeTag tag = element.getTag();
    ListTagDispatcher writer {._tag = tag};

    return writer.execute(writeTyped, element);
}

std::string valueToString(const ListView view) {
    if (view.empty()) {
        return "[]";
    }

    std::string result = "[";
    size_t index = 0;

    for (const ListElementView element : view.elements()) {
        if (index++ > 0) {
            result += ", ";
        }
        result += valueToString(element);
    }

    result += "]";
    return result;
}

struct Stringify {
    std::string& _string;
    size_t _row {0};

    template <typename T>
    void operator()(const ColumnVector<T>* typed) {
        _string = valueToString(typed->at(_row));
    }

    template <typename T>
    void operator()(const ColumnConst<T>* typed) {
        _string = valueToString(typed->at(_row));
    }
};

void renderCell(const Column* column, size_t row, std::string& out) {
    Stringify stringify(out, row);

    using Types = OutputtedTypes;
    using Dispatcher = ColumnSingleDispatcher<Types::Allowed, Stringify, Types::Excluded>;

    Dispatcher::dispatch(column, stringify);
}

void collectPipelineRows(const Dataframe* dataframe, Rows& rows) {
    const Dataframe::NamedColumns& columns = dataframe->cols();
    const size_t rowCount = dataframe->getLogicalRowCount();

    for (size_t row = 0; row < rowCount; row++) {
        Row& cells = rows.emplace_back();
        cells.resize(columns.size());

        for (size_t column = 0; column < columns.size(); column++) {
            renderCell(columns[column]->getColumn(), row, cells[column]);
        }
    }
}

class EquivalenceSink : public NLOutputSink {
public:
    explicit EquivalenceSink(Rows& rows)
        : _rows(rows)
    {
    }

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        for (size_t row = offset; row < offset + rowCount; row++) {
            Row& cells = _rows.emplace_back();
            cells.resize(chunks.size());

            for (size_t column = 0; column < chunks.size(); column++) {
                renderCell(chunks[column], row, cells[column]);
            }
        }
    }

private:
    Rows& _rows;
};

}

class EquivalenceTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);

        _db = &_env->getDB();
    }

    void runViaPipeline(std::string_view query, Rows& rows) {
        QueryCallbacks callbacks;
        callbacks.setOnOutputData([&rows](const Dataframe* dataframe) {
            ASSERT_TRUE(dataframe != nullptr);
            collectPipelineRows(dataframe, rows);
        });

        const QueryState state(_graphName, &_env->getMem(), &_queryConfig, &callbacks);
        const QueryStatus status = _db->query(query, state);
        ASSERT_TRUE(status.isOk()) << "Pipeline query failed: " << query;
    }

    void runViaIR(std::string_view query, Rows& rows) {
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

        EquivalenceSink sink(rows);
        LocalMemory memory;
        DBDialectInterpreter interpreter(module, &view, &sink, &memory);
        interpreter.run();
    }

    void expectRowsEqual(std::string_view query, const Rows& pipelineRows, const Rows& irRows) {
        ASSERT_EQ(pipelineRows.size(), irRows.size()) << "Size mismatch for " << query;

        for (auto [plRow, irRow] : rv::zip(pipelineRows, irRows)) {
            EXPECT_EQ(plRow, irRow) << "Row mismatch for " << query;
        }
    }

    void expectEquivalent(std::string_view query) {
        try {
            Rows pipelineRows;
            runViaPipeline(query, pipelineRows);

            Rows irRows;
            runViaIR(query, irRows);

            std::ranges::sort(pipelineRows);
            std::ranges::sort(irRows);

            expectRowsEqual(query, pipelineRows, irRows);
        } catch (...) {
            spdlog::error("Exception thrown for query: {}.", query);
            throw;
        }
    }

    // The ORDER BY sibling of expectEquivalent: the row order is itself the result being
    // checked, so the rows are compared as the two engines emitted them and neither side
    // is sorted first. The query has to order by a key with no ties, or the engines may
    // break them differently and both still be right
    void expectEquivalentOrdered(std::string_view query) {
        try {
            Rows pipelineRows;
            runViaPipeline(query, pipelineRows);

            Rows irRows;
            runViaIR(query, irRows);

            expectRowsEqual(query, pipelineRows, irRows);
        } catch (...) {
            spdlog::error("Exception thrown for query: {}.", query);
            throw;
        }
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
    Graph* _graph {nullptr};
    QueryConfig _queryConfig;
};

TEST_F(EquivalenceTest, scans) {
    expectEquivalent("MATCH (n) RETURN n");
}

TEST_F(EquivalenceTest, outEdges) {
    expectEquivalent("MATCH (a)-->(b) RETURN a");
    expectEquivalent("MATCH (a)-->(b) RETURN b");
    expectEquivalent("MATCH (a)-->(b) RETURN a, b");

    expectEquivalent("MATCH (a)-->(b)-->(c) RETURN a");
    expectEquivalent("MATCH (a)-->(b)-->(c) RETURN b");
    expectEquivalent("MATCH (a)-->(b)-->(c) RETURN c");
    expectEquivalent("MATCH (a)-->(b)-->(c) RETURN a, b, c");
    expectEquivalent("MATCH (a)-->(b)-->(c) RETURN a, c");

    expectEquivalent("MATCH (a)-->(b)-->(c)-->(d) RETURN a, b, c, d");
    expectEquivalent("MATCH (a)-->(b)-->(c)-->(d) RETURN b, c");
    expectEquivalent("MATCH (a)-->(b)-->(c)-->(d) RETURN a, d");
}

TEST_F(EquivalenceTest, inEdges) {
    expectEquivalent("MATCH (a)<--(b) RETURN a");
    expectEquivalent("MATCH (a)<--(b) RETURN b");
    expectEquivalent("MATCH (a)<--(b) RETURN a, b");

    expectEquivalent("MATCH (a)<--(b)<--(c) RETURN a");
    expectEquivalent("MATCH (a)<--(b)<--(c) RETURN b");
    expectEquivalent("MATCH (a)<--(b)<--(c) RETURN c");
    expectEquivalent("MATCH (a)<--(b)<--(c) RETURN a, b, c");
    expectEquivalent("MATCH (a)<--(b)<--(c) RETURN a, c");

    expectEquivalent("MATCH (a)<--(b)<--(c)<--(d) RETURN a, b, c, d");
    expectEquivalent("MATCH (a)<--(b)<--(c)<--(d) RETURN b, c");
    expectEquivalent("MATCH (a)<--(b)<--(c)<--(d) RETURN a, d");
}

TEST_F(EquivalenceTest, undirectedEdges) {
    expectEquivalent("MATCH (a)--(b) RETURN a");
    expectEquivalent("MATCH (a)--(b) RETURN b");
    expectEquivalent("MATCH (a)--(b) RETURN a, b");

    expectEquivalent("MATCH (a)--(b)--(c) RETURN a");
    expectEquivalent("MATCH (a)--(b)--(c) RETURN b");
    expectEquivalent("MATCH (a)--(b)--(c) RETURN c");
    expectEquivalent("MATCH (a)--(b)--(c) RETURN a, b, c");
    expectEquivalent("MATCH (a)--(b)--(c) RETURN a, c");

    expectEquivalent("MATCH (a)-->(b)--(c) RETURN a, c");
    expectEquivalent("MATCH (a)--(b)-->(c) RETURN a, c");
    expectEquivalent("MATCH (a)<--(b)--(c) RETURN a, c");

    expectEquivalent("MATCH (a {name: 'Remy'})--(b) RETURN b");
    expectEquivalent("MATCH (a:Person)--(b:Interest) RETURN a, b");
    expectEquivalent("MATCH (a)-[:INTERESTED_IN]-(b) RETURN a, b");
}

TEST_F(EquivalenceTest, trees) {
    expectEquivalent("MATCH (a)-->(b), (a)-->(c) RETURN a");
    expectEquivalent("MATCH (a)-->(b), (a)-->(c)-->(d) RETURN a");
    expectEquivalent("MATCH (b)-->(a), (c)-->(a) RETURN c");
    // Disabled due to suspected v2 bug
    // expectEquivalent("MATCH (a)-->(b)-->(c), (b)-->(e) RETURN a");
}

TEST_F(EquivalenceTest, crossProd) {
    expectEquivalent("MATCH (a), (b) RETURN a");
    expectEquivalent("MATCH (a), (b) RETURN b");
    expectEquivalent("MATCH (a), (b) RETURN a, b");

    expectEquivalent("MATCH (a), (b), (c) RETURN b");
    expectEquivalent("MATCH (a), (b), (c) RETURN a, b, c");

    expectEquivalent("MATCH (a)-->(d), (b), (c) RETURN a, b, c, d");

    expectEquivalent("MATCH (a)-->(d), (b)-->(e), (c)-->(f) RETURN a, b, c, d, e, f");
}

TEST_F(EquivalenceTest, eqFilters) {
    expectEquivalent("MATCH (a) WHERE a.age = 32 RETURN a");
    expectEquivalent("MATCH (a), (b) WHERE a.age = 32 RETURN a, b");
    expectEquivalent("MATCH (a), (b) WHERE a.age = b.age RETURN a, b");
    expectEquivalent("MATCH (a), (b) WHERE a.name = a.name RETURN b");

    expectEquivalent("MATCH (a), (b) WHERE a = b RETURN b");
    expectEquivalent("MATCH (a), (b) WHERE a = a RETURN a");
    expectEquivalent("MATCH (a), (b) WHERE a = a RETURN b");

    expectEquivalent("MATCH (a), (b) WHERE a.age = a RETURN b");

    expectEquivalent("MATCH (a), (b) WHERE a = b.age RETURN a, b");

    expectEquivalent("MATCH (a), (b) WHERE a.age = 32 OR a.name = b.name RETURN b");
    expectEquivalent("MATCH (a), (b) WHERE a.age = 32 AND a.name = b.name RETURN b");

    expectEquivalent("MATCH (n)-->(a)-->(m) WHERE n = m RETURN n, a, m");

    expectEquivalent("MATCH ()-[e]->(n) WHERE e = 0 RETURN n");
    expectEquivalent("MATCH (n)-[e]->(m) WHERE e = e RETURN n");

    // Disabled due to suspected v2 bug
    // expectEquivalent("MATCH (n)-->(a)-->(m), (a)-->(b) WHERE b = n RETURN n, a, m, b");

    expectEquivalent("MATCH (n) WHERE NOT n.isFrench RETURN n");

    expectEquivalent("MATCH (n) WHERE n.isFrench RETURN n");

    expectEquivalent("MATCH (n)-->(a) WHERE NOT n.isFrench RETURN n, a");

    expectEquivalent("MATCH (n) WHERE n.age = 16 + 16 RETURN n");
    expectEquivalent("MATCH (n) WHERE n.age = 16 + 16 * 1 RETURN n");
    expectEquivalent("MATCH (n) WHERE n.age = 16 * 2 RETURN n");

    expectEquivalent("MATCH (n)-[e]->(m) WHERE e.duration = 10 + 10 return m");
}

TEST_F(EquivalenceTest, inlineNodePropertyConstraints) {
    // Single string constraint — exactly one person named Remy.
    expectEquivalent("MATCH (n {name: 'Remy'}) RETURN n");

    // Integer constraint — both Remy and Adam have age 32.
    expectEquivalent("MATCH (n {age: 32}) RETURN n");

    // Bool constraint — several Person nodes have isFrench = true.
    expectEquivalent("MATCH (n {isFrench: true}) RETURN n");

    // Multiple constraints ANDed — only Maxime is French with no PhD.
    expectEquivalent("MATCH (n {isFrench: true, hasPhD: false}) RETURN n");

    // Constraint that matches no node in the graph.
    expectEquivalent("MATCH (n {name: 'Nobody'}) RETURN n");

    // Constraint on the source of a traversal.
    expectEquivalent("MATCH (a {name: 'Remy'})-->(b) RETURN b");

    // Constraint on the target of a traversal.
    expectEquivalent("MATCH (a)-->(b {name: 'Bio'}) RETURN a");

    // Constraints on both endpoints of a traversal.
    expectEquivalent("MATCH (a {isFrench: true})-->(b {name: 'Bio'}) RETURN a");
}

TEST_F(EquivalenceTest, inlineEdgePropertyConstraints) {
    // Integer edge property constraint.
    expectEquivalent("MATCH (a)-[e {duration: 20}]->(b) RETURN a, b");

    // String edge property constraint — only expert edges.
    expectEquivalent("MATCH (a)-[e {proficiency: 'expert'}]->(b) RETURN a, b");

    // Edge constraint that matches no edge in the graph.
    expectEquivalent("MATCH (a)-[e {duration: 999}]->(b) RETURN a, b");

    // Combined node and edge constraints.
    expectEquivalent("MATCH (a {name: 'Remy'})-[e {duration: 20}]->(b) RETURN b");
}

TEST_F(EquivalenceTest, arbitraryFilters) {
    expectEquivalent("MATCH (a{age:32})-->(b{age:32}) WHERE a.isFrench RETURN a, b");

    expectEquivalent("MATCH (a {age: 32})-->(b) WHERE a.hasPhD RETURN a, b");
    expectEquivalent("MATCH (a {isFrench: true})-->(b) WHERE a.hasPhD RETURN a, b");
    expectEquivalent("MATCH (a {isFrench: false})-->(b) WHERE NOT a.hasPhD RETURN a, b");

    expectEquivalent("MATCH (a)-[e {duration: 20}]->(b) WHERE a.isFrench RETURN a, b");
    expectEquivalent("MATCH (a)-[e {duration: 20}]->(b) WHERE a.hasPhD RETURN a, b");

    expectEquivalent("MATCH (a {hasPhD: true})-[e {duration: 20}]->(b) WHERE a.isFrench RETURN a, b");
    expectEquivalent("MATCH (a {age: 32})-[e {duration: 20}]->(b) WHERE a.isFrench RETURN a, b");

    expectEquivalent("MATCH (a)-->(b {isReal: true}) WHERE a.isFrench RETURN a, b");
    expectEquivalent("MATCH (a)-->(b {isReal: false}) WHERE a.hasPhD RETURN a, b");

    expectEquivalent("MATCH (a {isFrench: true})-->(b)-->(c) WHERE a.hasPhD RETURN a, c");
    expectEquivalent("MATCH (a {age: 32})-->(b)-->(c) WHERE a.isFrench RETURN a, b, c");

    expectEquivalent("MATCH (a {isFrench: true}), (b {isFrench: false}) RETURN a, b");
    expectEquivalent("MATCH (a {isFrench: true}), (b {hasPhD: false}) RETURN a, b");

    expectEquivalent("MATCH (a {isFrench: true}), (b {isFrench: false}) WHERE a.hasPhD RETURN a, b");
    expectEquivalent("MATCH (a {age: 32}), (b {isFrench: false}) WHERE b.hasPhD RETURN a, b");

    expectEquivalent("MATCH (a {isFrench: true})-->(b), (c {hasPhD: false}) WHERE a.hasPhD RETURN a, b, c");
    expectEquivalent("MATCH (a {age: 32})-->(b), (c {isFrench: false}) WHERE a.isFrench RETURN a, c");
}

TEST_F(EquivalenceTest, labelConstraints) {
    expectEquivalent("MATCH (n:Person) RETURN n");
    expectEquivalent("MATCH (n:Interest) RETURN n");
    expectEquivalent("MATCH (n:Founder) RETURN n");

    expectEquivalent("MATCH (n:Person:Founder) RETURN n");
    expectEquivalent("MATCH (n:Interest:SleepDisturber) RETURN n");

    expectEquivalent("MATCH (n:Person)-->(m) RETURN n");
    expectEquivalent("MATCH (n:Person)-->(m:Interest) RETURN n, m");
    expectEquivalent("MATCH (n)-->(m:Interest) RETURN n, m");

    expectEquivalent("MATCH (n:Person) WHERE n.isFrench RETURN n");
    expectEquivalent("MATCH (n:Person) WHERE n.hasPhD RETURN n");
}

TEST_F(EquivalenceTest, edgeTypeConstraints) {
    expectEquivalent("MATCH (a)-[:KNOWS_WELL]->(b) RETURN a, b");
    expectEquivalent("MATCH (a)-[:INTERESTED_IN]->(b) RETURN a, b");

    // v2 does not support multiple edge types
    // expectEquivalent("MATCH (a)-[:KNOWS_WELL|INTERESTED_IN]->(b) RETURN a, b");

    expectEquivalent("MATCH (a)-[e:KNOWS_WELL]->(b) RETURN a, b");
    expectEquivalent("MATCH (a)-[e:INTERESTED_IN]->(b) RETURN a, b");

    expectEquivalent("MATCH (a {isFrench: true})-[:INTERESTED_IN]->(b) RETURN a, b");

    expectEquivalent("MATCH (a)-[:KNOWS_WELL]->(b)-[:INTERESTED_IN]->(c) RETURN a, b, c");

    expectEquivalent("MATCH (a)-[:INTERESTED_IN]->(b) WHERE a.isFrench RETURN a, b");

    expectEquivalent("MATCH (a)<-[:KNOWS_WELL]-(b) RETURN a, b");
}

TEST_F(EquivalenceTest, constants) {
    expectEquivalent("RETURN 5");
    expectEquivalent("RETURN 5 + 10");
    expectEquivalent("RETURN 5 - 3");
    expectEquivalent("RETURN 5 * 3");
    expectEquivalent("RETURN 10 / 2");
    expectEquivalent("RETURN 9 / 3");

    expectEquivalent("RETURN 'hello'");
    expectEquivalent("RETURN (1, 2, 3)");
    expectEquivalent("RETURN null");
}

TEST_F(EquivalenceTest, divideFilters) {
    expectEquivalent("MATCH (n) WHERE n.age / 2 = 16 RETURN n");

    expectEquivalent("MATCH (n) WHERE n.age / 32 = 1 RETURN n");

    expectEquivalent("MATCH (n)-[e]->(m) WHERE e.duration / 10 = 2 RETURN m");

    expectEquivalent("MATCH (n) WHERE n.age / 4 = 8 RETURN n");

    expectEquivalent("MATCH (n) WHERE n.age / 2 = 8 RETURN n");
}

TEST_F(EquivalenceTest, skip) {
    expectEquivalent("MATCH (n) RETURN n SKIP 100");

    expectEquivalent("MATCH (n) RETURN n SKIP 18");

    expectEquivalent("MATCH (n) RETURN n SKIP 5");
    expectEquivalent("MATCH (n) RETURN n SKIP 1");

    expectEquivalent("MATCH (n) RETURN n SKIP 0");

    expectEquivalent("MATCH (a)-->(b) RETURN a, b SKIP 3");

    expectEquivalent("MATCH (a), (b), (c), (d) RETURN a SKIP 10000");
}

TEST_F(EquivalenceTest, limit) {
    expectEquivalent("MATCH (n) RETURN n LIMIT 100");

    expectEquivalent("MATCH (n) RETURN n LIMIT 18");

    expectEquivalent("MATCH (n) RETURN n LIMIT 5");
    expectEquivalent("MATCH (n) RETURN n LIMIT 1");

    expectEquivalent("MATCH (n) RETURN n LIMIT 0");

    expectEquivalent("MATCH (a)-->(b) RETURN a, b LIMIT 3");

    expectEquivalent("MATCH (a), (b), (c), (d) RETURN a LIMIT 10");
}

// Every node of simpledb has a name and no two share one, so a name key is a total
// order: the engines have no tie to break and must emit the very same sequence
TEST_F(EquivalenceTest, orderBy) {
    expectEquivalentOrdered("MATCH (n) RETURN n.name ORDER BY n.name");
    expectEquivalentOrdered("MATCH (n) RETURN n.name ORDER BY n.name ASC");
    expectEquivalentOrdered("MATCH (n) RETURN n.name ORDER BY n.name DESC");

    expectEquivalentOrdered("MATCH (n:Person) RETURN n.name ORDER BY n.name");
    expectEquivalentOrdered("MATCH (n:Interest) RETURN n.name ORDER BY n.name DESC");

    expectEquivalentOrdered("MATCH (n) WHERE n.age = 32 RETURN n.name ORDER BY n.name");
    expectEquivalentOrdered("MATCH (n) WHERE n.isFrench RETURN n.name ORDER BY n.name DESC");

    // No two edges of simpledb share a source and a target, so the two names order the
    // traversal fully
    expectEquivalentOrdered("MATCH (a)-->(b) RETURN a.name, b.name ORDER BY a.name, b.name");
    expectEquivalentOrdered("MATCH (a)-->(b) RETURN a.name, b.name ORDER BY a.name DESC, b.name");
    expectEquivalentOrdered("MATCH (a)-[:INTERESTED_IN]->(b) RETURN a.name, b.name ORDER BY b.name, a.name");
}

TEST_F(EquivalenceTest, orderBySkipLimit) {
    expectEquivalentOrdered("MATCH (n) RETURN n.name ORDER BY n.name LIMIT 5");
    expectEquivalentOrdered("MATCH (n) RETURN n.name ORDER BY n.name DESC LIMIT 3");
    expectEquivalentOrdered("MATCH (n) RETURN n.name ORDER BY n.name LIMIT 1");
    expectEquivalentOrdered("MATCH (n) RETURN n.name ORDER BY n.name LIMIT 0");
    expectEquivalentOrdered("MATCH (n) RETURN n.name ORDER BY n.name LIMIT 100");

    expectEquivalentOrdered("MATCH (n) RETURN n.name ORDER BY n.name SKIP 5");
    expectEquivalentOrdered("MATCH (n) RETURN n.name ORDER BY n.name DESC SKIP 17");
    expectEquivalentOrdered("MATCH (n) RETURN n.name ORDER BY n.name SKIP 100");

    expectEquivalentOrdered("MATCH (n) RETURN n.name ORDER BY n.name SKIP 2 LIMIT 4");
    expectEquivalentOrdered("MATCH (a)-->(b) RETURN a.name, b.name ORDER BY a.name, b.name SKIP 3 LIMIT 5");
}

TEST_F(EquivalenceTest, neqFilters) {
    expectEquivalent("MATCH (n) WHERE n.age <> 32 RETURN n");
    expectEquivalent("MATCH (n) WHERE n.age <> 0 RETURN n");

    expectEquivalent("MATCH (n) WHERE n.isFrench <> true RETURN n");
    expectEquivalent("MATCH (n) WHERE n.isFrench <> false RETURN n");

    expectEquivalent("MATCH (a), (b) WHERE a <> b RETURN a, b");

    expectEquivalent("MATCH (a), (b) WHERE a.age <> b.age RETURN a, b");

    expectEquivalent("MATCH (n)-[e]->(m) WHERE e.duration <> 20 RETURN m");

    expectEquivalent("MATCH (n) WHERE n.age <> 32 AND n.age > 20 RETURN n");

    expectEquivalent("MATCH (n) WHERE n.age <> 32 OR n.isFrench RETURN n");
}

TEST_F(EquivalenceTest, comparisonFilters) {
    expectEquivalent("MATCH (n) WHERE n.age > 20 RETURN n");

    expectEquivalent("MATCH (n) WHERE n.age > 32 RETURN n");

    expectEquivalent("MATCH (n) WHERE n.age < 50 RETURN n");

    expectEquivalent("MATCH (n) WHERE n.age < 32 RETURN n");

    expectEquivalent("MATCH (n) WHERE n.age >= 32 RETURN n");

    expectEquivalent("MATCH (n) WHERE n.age >= 33 RETURN n");

    expectEquivalent("MATCH (n) WHERE n.age <= 32 RETURN n");

    expectEquivalent("MATCH (n) WHERE n.age <= 31 RETURN n");

    expectEquivalent("MATCH (n) WHERE n.age > 20 AND n.age < 50 RETURN n");

    expectEquivalent("MATCH (n) WHERE n.age / 2 > 10 RETURN n");

    expectEquivalent("MATCH (a), (b) WHERE a.age > b.age RETURN a, b");
    expectEquivalent("MATCH (a), (b) WHERE a.age >= b.age RETURN a, b");

    expectEquivalent("MATCH (n)-[e]->(m) WHERE e.duration > 10 RETURN m");
    expectEquivalent("MATCH (n)-[e]->(m) WHERE e.duration < 100 RETURN m");
    expectEquivalent("MATCH (n)-[e]->(m) WHERE e.duration >= 20 RETURN m");
    expectEquivalent("MATCH (n)-[e]->(m) WHERE e.duration <= 20 RETURN m");
}

TEST_F(EquivalenceTest, wildcardReturn) {
    expectEquivalent("MATCH (n) RETURN *");

    expectEquivalent("MATCH (a)-->(b) RETURN *");
    expectEquivalent("MATCH (a)-->(b)-->(c) RETURN *");
    expectEquivalent("MATCH (a)<--(b) RETURN *");

    expectEquivalent("MATCH (a)-[e]->(b) RETURN *");

    expectEquivalent("MATCH (a), (b) RETURN *");

    expectEquivalent("MATCH (n:Person) RETURN *");
    expectEquivalent("MATCH (n {name: 'Remy'}) RETURN *");
    expectEquivalent("MATCH (n) WHERE n.age = 32 RETURN *");
}

TEST_F(EquivalenceTest, countStar) {
    expectEquivalent("MATCH (n) RETURN count(*)");

    expectEquivalent("MATCH (a)-->(b) RETURN count(*)");
    expectEquivalent("MATCH (a)-->(b)-->(c) RETURN count(*)");
    expectEquivalent("MATCH (a)<--(b) RETURN count(*)");

    expectEquivalent("MATCH (a), (b) RETURN count(*)");

    expectEquivalent("MATCH (n:Person) RETURN count(*)");
    expectEquivalent("MATCH (n {name: 'Remy'}) RETURN count(*)");
    expectEquivalent("MATCH (n) WHERE n.age = 32 RETURN count(*)");

    expectEquivalent("MATCH (a)-[:INTERESTED_IN]->(b) RETURN count(*)");

    expectEquivalent("MATCH (n {name: 'Nobody'}) RETURN count(*)");
}

TEST_F(EquivalenceTest, wildcardAndCountCombination) {
    expectEquivalent("MATCH (a:Person)-->(b:Interest) RETURN *");
    expectEquivalent("MATCH (a {isFrench: true})-->(b) RETURN *");
    expectEquivalent("MATCH (a)-[e {duration: 20}]->(b) RETURN *");
    expectEquivalent("MATCH (a:Person)-->(b), (c:Interest) RETURN *");

    expectEquivalent("MATCH (a:Person)-->(b:Interest) RETURN count(*)");
    expectEquivalent("MATCH (a {isFrench: true})-->(b) RETURN count(*)");
    expectEquivalent("MATCH (a)-[:KNOWS_WELL]->(b) WHERE a.hasPhD RETURN count(*)");
    expectEquivalent("MATCH (a:Person)-->(b)-->(c) WHERE a.isFrench RETURN count(*)");
    expectEquivalent("MATCH (a), (b), (c) RETURN count(*)");
}

TEST_F(EquivalenceTest, labelsAndTypeFunctions) {
    expectEquivalent("MATCH (n) RETURN labels(n)");
    expectEquivalent("MATCH (a:Person) RETURN labels(a)");
    expectEquivalent("MATCH (a:Interest) RETURN labels(a)");
    expectEquivalent("MATCH (n) RETURN n, labels(n)");

    expectEquivalent("MATCH (a)-[e]->(b) RETURN edgeType(e)");
    expectEquivalent("MATCH (a)-[e:INTERESTED_IN]->(b) RETURN edgeType(e)");
    expectEquivalent("MATCH (a)-[e]->(b) RETURN a, edgeType(e), b");

    expectEquivalent("MATCH (n) WHERE labels(n) = 'Interest' RETURN *");
    expectEquivalent("MATCH (n) WHERE labels(n) = 'Person' RETURN *");

    expectEquivalent("MATCH (n) RETURN n, toInteger('42')");
    expectEquivalent("MATCH (n) RETURN count(labels(n))");
}

TEST_F(EquivalenceTest, conversionFunctions) {
    expectEquivalent("MATCH (n) WHERE n.age = toInteger('32') RETURN n.name");
    expectEquivalent("MATCH (n) WHERE n.age > toInteger('30') RETURN n.name");
    expectEquivalent("MATCH (n) WHERE n.isFrench = toBoolean('true') RETURN n.name");

    // DISABLED: Because v2 returns a single row (incorrect) whilst v3 returns |n| (correct)
    //expectEquivalent("MATCH (n) RETURN toInteger('42')");
    //expectEquivalent("MATCH (n) RETURN toFloat('2.5')");
}

TEST_F(EquivalenceTest, embeddingFunctions) {
    expectEquivalent("RETURN cosine_similarity((1.0, 2.0, 3.0), (0.4, 0.3, 0.8))");
    expectEquivalent("RETURN euclidean_distance((0.5, 0.5), (0.5, 0.5))");

    // DISABLED: Because v2 returns a single row (incorrect) whilst v3 returns |n| (correct)
    //expectEquivalent("MATCH (n) RETURN cosine_similarity((1.0, 2.0, 3.0), (0.4, 0.3, 0.8))");
    //expectEquivalent("MATCH (n) RETURN euclidean_distance((1.0, 2.0, 3.0), (0.4, 0.3, 0.8))");
    //expectEquivalent("MATCH (n) RETURN cosine_similarity((0.5, 0.5), (0.5, 0.5))");
    //expectEquivalent("MATCH (n) RETURN euclidean_distance((0.5, 0.5), (0.5, 0.5))");
}

TEST_F(EquivalenceTest, functionsWithLimit) {
    expectEquivalent("MATCH (n) RETURN labels(n) LIMIT 5");
    expectEquivalent("MATCH (n) RETURN labels(n) LIMIT 1");
    expectEquivalent("MATCH (n) RETURN n, labels(n) LIMIT 4");
    expectEquivalent("MATCH (a)-[e]->(b) RETURN edgeType(e) LIMIT 3");
}
