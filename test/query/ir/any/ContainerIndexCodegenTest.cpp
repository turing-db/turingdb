#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"
#include "mlir/Interfaces/SideEffectInterfaces.h"
#include "mlir/Parser/Parser.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"
#include "StorageDialect.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// db.container_index is the bracket whose index is held in a type-erased cell. A typed index
// keeps db.list_index or db.dynamic_map_key, and a null one reads no container at all.
class ContainerIndexCodegenTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());
    }

protected:
    struct IndexOps {
        size_t _containerIndexes {0};
        size_t _listIndexes {0};
        size_t _mapKeys {0};
    };

    void explain(std::string_view query, StringRowSink& sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();
    }

    void countCodegenIndexOps(std::string_view query, IndexOps& ops) {
        StringRowSink sink;
        explain("EXPLAIN (codegen) " + std::string(query), sink);

        const std::string_view codegen = dumpOf(sink, "codegen");
        ops._containerIndexes = occurrences(codegen, "db.container_index");
        ops._listIndexes = occurrences(codegen, "db.list_index");
        ops._mapKeys = occurrences(codegen, "db.dynamic_map_key");
    }

    void expectIndexOps(std::string_view query, size_t containerIndexes, size_t listIndexes, size_t mapKeys) {
        IndexOps ops;
        countCodegenIndexOps(query, ops);

        EXPECT_EQ(ops._containerIndexes, containerIndexes) << "query: " << query;
        EXPECT_EQ(ops._listIndexes, listIndexes) << "query: " << query;
        EXPECT_EQ(ops._mapKeys, mapKeys) << "query: " << query;
    }

    // The nl op the db op lowered to, with the type of its result
    void expectLoweredResult(std::string_view query, std::string_view resultType) {
        StringRowSink sink;
        explain("EXPLAIN (nl) " + std::string(query), sink);

        const std::string_view nl = dumpOf(sink, "nl");
        const size_t at = nl.find("nl.container_index");
        ASSERT_NE(at, std::string_view::npos) << nl;

        const std::string_view line = nl.substr(at, nl.find('\n', at) - at);
        EXPECT_TRUE(line.ends_with(std::string("-> ") + std::string(resultType))) << line;
    }

    static std::string_view dumpOf(const StringRowSink& sink, std::string_view stage) {
        for (const StringRowSink::Row& row : sink.getRows()) {
            if (row.front() == stage) {
                return row.back();
            }
        }

        return {};
    }

    static size_t occurrences(std::string_view text, std::string_view part) {
        size_t count = 0;

        for (size_t at = text.find(part); at != std::string_view::npos; at = text.find(part, at + part.size())) {
            count++;
        }

        return count;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(ContainerIndexCodegenTest, readsAListByAnIndexACellHolds) {
    expectIndexOps("WITH [1, 'a'][0] AS i RETURN [10, 20, 30][i]", 1, 1, 0);
}

TEST_F(ContainerIndexCodegenTest, readsAMapByAnIndexACellHolds) {
    expectIndexOps("WITH ['a', 1][0] AS k, {a: 1} AS m RETURN m[k]", 1, 1, 0);
    expectIndexOps("WITH ['a', 1][0] AS k RETURN {a: 1}[k]", 1, 1, 0);
}

TEST_F(ContainerIndexCodegenTest, readsACellByAnIndexACellHolds) {
    expectIndexOps("UNWIND [[[10, 20], 1], [{a: 'x'}, 'a']] AS p RETURN p[0][p[1]]", 1, 2, 0);
}

TEST_F(ContainerIndexCodegenTest, keepsTheListIndexOfAnIntegerIndex) {
    expectIndexOps("RETURN [10, 20, 30][1]", 0, 1, 0);
    expectIndexOps("MATCH (n:Person) RETURN [1, 2, 3][n.age - 32]", 0, 1, 0);
    expectIndexOps("UNWIND [[1, 2], 'a'] AS v RETURN v[0]", 0, 1, 0);
}

TEST_F(ContainerIndexCodegenTest, keepsTheMapKeyReadOfAStringIndex) {
    expectIndexOps("WITH {a: 1} AS m, 'a' AS k RETURN m[k]", 0, 0, 1);
    expectIndexOps("UNWIND [{a: 1}, 'a'] AS v WITH v, 'a' AS k RETURN v[k]", 0, 0, 1);
    expectIndexOps("WITH 'a' AS k RETURN {a: 1}[k]", 0, 0, 1);
}

TEST_F(ContainerIndexCodegenTest, readsNoContainerByANullIndex) {
    expectIndexOps("RETURN [1, 2][null]", 0, 0, 0);
    expectIndexOps("WITH {a: 1} AS m RETURN m[null]", 0, 0, 0);
}

TEST_F(ContainerIndexCodegenTest, lowersAReadOfATypedListToItsElementType) {
    expectLoweredResult("WITH [1, 'a'][0] AS i RETURN [10, 20, 30][i]", "!nl.chunk<!storage.nullable<i64>>");
    expectLoweredResult("WITH [1, 'a'][0] AS i RETURN ['x', 'y'][i]", "!nl.chunk<!storage.nullable<!storage.string>>");
}

TEST_F(ContainerIndexCodegenTest, lowersAReadOfAListOfNodesToTheNodeColumn) {
    expectLoweredResult("MATCH (p:Person) WITH collect(p) AS l WITH l, [0, 'a'][0] AS i RETURN l[i]",
                        "!nl.chunk<!storage.node_id>");
}

TEST_F(ContainerIndexCodegenTest, lowersAReadOfAMixedListAsAListIndexDoes) {
    expectLoweredResult("WITH [1, 'a'][0] AS i RETURN [1, 'a'][i]", "!nl.chunk<!storage.nullable<!storage.any>>");
}

TEST_F(ContainerIndexCodegenTest, lowersAReadOfAMapOrACellToATaggedCell) {
    expectLoweredResult("WITH ['a', 1][0] AS k, {a: 1} AS m RETURN m[k]", "!nl.chunk<!storage.any>");
    expectLoweredResult("UNWIND [[[10, 20], 1], [{a: 'x'}, 'a']] AS p RETURN p[0][p[1]]", "!nl.chunk<!storage.any>");
}

// db.container_index computes a value per row, so the pattern an equality binds to its
// element is seeded off that element as off a db.list_index's
TEST_F(ContainerIndexCodegenTest, seedsAPatternOffTheReadElement) {
    StringRowSink sink;
    explain("EXPLAIN (db) WITH [0, 1, 6, 99] AS ss UNWIND range(0, 3) AS i WITH ss, [i, 'x'][0] AS j "
            "MATCH (m)-[e]->(n:Person) WHERE n = ss[j] RETURN m.name, n.name",
            sink);

    const std::string_view optimised = dumpOf(sink, "db");
    EXPECT_EQ(occurrences(optimised, "db.fetch_nodes"), 1u) << optimised;
    EXPECT_EQ(occurrences(optimised, "db.scan_nodes"), 0u) << optimised;
}

// The read raises on a row whose index is neither a position nor a key, so an unread one is
// still run
TEST(ContainerIndexEffectTest, isKeptThoughNothingReadsIt) {
    mlir::MLIRContext context;
    context.getOrLoadDialect<mlir::func::FuncDialect>();
    context.getOrLoadDialect<mlir::storage::Storage>();
    context.getOrLoadDialect<mlir::db::DB>();

    constexpr std::string_view PROGRAM = R"mlir(
func.func @main(%list: !db.column<!storage.list<i64>>, %cell: !db.column<!storage.any>) {
  %r = db.container_index %list, %cell : (!db.column<!storage.list<i64>>, !db.column<!storage.any>) -> !db.column<none>
  return
}
)mlir";

    mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(PROGRAM, mlir::ParserConfig(&context));
    ASSERT_TRUE(module);

    mlir::Operation* index = nullptr;
    module->walk([&index](mlir::Operation* op) {
        if (op->getName().getStringRef() == "db.container_index") {
            index = op;
        }
    });
    ASSERT_TRUE(index);

    EXPECT_FALSE(mlir::isSpeculatable(index));
    EXPECT_FALSE(mlir::isOpTriviallyDead(index));
}
