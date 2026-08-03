#include <gtest/gtest.h>

#include <stdint.h>

#include <algorithm>
#include <memory>
#include <ranges>
#include <span>
#include <stdexcept>
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
#include "DBOps.h"
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
#include "TuringException.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using Row = std::vector<uint64_t>;
using Rows = std::vector<Row>;
using Names = std::vector<std::string>;

// Every node of simpledb by name, in ascending byte order - the order an
// ORDER BY n.name must produce. Every node has a name, so no key is null here.
const Names nodeNamesAscending = {
    "Adam", "Animals", "Bio", "Computers", "Cooking", "Cyrus",
    "Doruk", "Eighties", "Ghosts", "Gym", "JiuJitsu", "Luc",
    "Martina", "Maxime", "Padel", "Remy", "Suhas", "Travel",
};

uint64_t readNodeID(const Column* column, size_t row) {
    const auto* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(column);
    if (!nodeIDs) {
        throw std::runtime_error("OrderByTest: expected a node ID output column");
    }

    return (*nodeIDs)[row].getValue();
}

void describeRows(const Rows& rows, std::string& out) {
    out.clear();
    for (const Row& row : rows) {
        out += "        {";
        for (size_t index = 0; index < row.size(); index++) {
            if (index > 0) {
                out += ", ";
            }

            out += std::to_string(row[index]);
        }

        out += "},\n";
    }
}

// Collects the node ID rows a projection emits, in the order the sink sees them. An
// ORDER BY is only correct if that order survives to the output, so - unlike the
// other Cypher output tests - nothing here sorts the rows.
class OrderedNodeSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            Row& row = _rows.emplace_back();
            for (const Column* const column : chunks) {
                row.push_back(readNodeID(column, rowIndex));
            }
        }
    }

    const Rows& rows() const { return _rows; }

private:
    Rows _rows;
};

// The string sibling of OrderedNodeSink: collects a single projected string property
// column, in sink order.
class OrderedNameSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* names = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[0]);
        ASSERT_NE(names, nullptr);

        const auto& nameRaw = names->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            ASSERT_TRUE(nameRaw[rowIndex].has_value());
            _names.push_back(std::string(*nameRaw[rowIndex]));
        }
    }

    const Names& names() const { return _names; }

private:
    Names _names;
};

}

// End-to-end ORDER BY through the MLIR frontend: each query is parsed, analyzed,
// generated into the db dialect by DBProgramGenerator, then lowered and interpreted.
// The assertions are on the row order, which is what the generated db.sort decides.
class OrderByTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);
    }

    // Generates the db dialect program of a query into @param module, without running it
    void generateProgram(std::string_view query,
                         mlir::MLIRContext& context,
                         mlir::OwningOpRef<mlir::ModuleOp>& module) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const ProcedureManager* procedures = system.getProcedures();

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        CypherAST ast(procedures, query);

        CypherParser parser(&ast);
        parser.parse(query);

        CypherAnalyzer analyzer(&ast, view);
        analyzer.analyze();

        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::db::DB>();
        context.getOrLoadDialect<mlir::nl::NL>();

        mlir::OpBuilder builder(&context);
        module = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp moduleOp = module.get();

        DBProgramGenerator generator(&moduleOp);
        generator.generate(&ast);
    }

    void runQuery(std::string_view query, NLOutputSink* sink) {
        mlir::MLIRContext context;
        mlir::OwningOpRef<mlir::ModuleOp> module;
        generateProgram(query, context, module);

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        LocalMemory memory;
        DBDialectInterpreter interpreter(module.get(), &view, sink, &memory);
        interpreter.run();
    }

    void expectNodeRows(std::string_view query, const Rows& expected) {
        OrderedNodeSink sink;
        runQuery(query, &sink);

        std::string description;
        describeRows(sink.rows(), description);

        EXPECT_EQ(sink.rows(), expected)
            << "query: " << query << "\nactual rows:\n" << description;
    }

    void expectNames(std::string_view query, const Names& expected) {
        OrderedNameSink sink;
        runQuery(query, &sink);

        EXPECT_EQ(sink.names(), expected) << "query: " << query;
    }

    // One single-column row per name, holding the ID of the node with that name, so a
    // name order can be expressed as the node order it stands for
    void nodeRowsFor(const Names& names, Rows& rows) {
        rows.clear();
        for (const std::string& name : names) {
            rows.push_back({SimpleGraph::findNodeID(_graph, name).getValue()});
        }
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
};

// The key is the projected column itself, so the sort reorders the projection in
// place: simpledb has 18 nodes, IDs 0 to 17.
TEST_F(OrderByTest, nodesByIDAscending) {
    const Rows expected = {{0}, {1},  {2},  {3},  {4},  {5},  {6},  {7},  {8},
                           {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}, {17}};
    expectNodeRows("MATCH (n) RETURN n ORDER BY n", expected);
}

TEST_F(OrderByTest, nodesByIDDescending) {
    const Rows expected = {{17}, {16}, {15}, {14}, {13}, {12}, {11}, {10}, {9},
                           {8},  {7},  {6},  {5},  {4},  {3},  {2},  {1},  {0}};
    expectNodeRows("MATCH (n) RETURN n ORDER BY n DESC", expected);
}

// The key is a property the projection does not carry, so it is sorted as an extra
// column and dropped from the output: only the node IDs come back, in name order.
TEST_F(OrderByTest, nodesByUnprojectedNameAscending) {
    Rows expected;
    nodeRowsFor(nodeNamesAscending, expected);

    expectNodeRows("MATCH (n) RETURN n ORDER BY n.name", expected);
}

TEST_F(OrderByTest, nodesByUnprojectedNameDescending) {
    Names names = nodeNamesAscending;
    std::ranges::reverse(names);

    Rows expected;
    nodeRowsFor(names, expected);

    expectNodeRows("MATCH (n) RETURN n ORDER BY n.name DESC", expected);
}

// The same query with the pattern variable spelled v0, the name the analyzer gives the
// first variable it has to invent - here the one standing for the ORDER BY key n.name.
// A key column resolved by the name of its declaration finds the user's v0 instead of
// the key's own, and the sort is then keyed on the node column it is meant to reorder,
// so the rows come back in node ID order with no error. Nothing about the query changed
// but the spelling of a variable, so the row order must not change either.
TEST_F(OrderByTest, unprojectedKeyIsNotShadowedByAUserVariable) {
    Rows expected;
    nodeRowsFor(nodeNamesAscending, expected);

    expectNodeRows("MATCH (v0) RETURN v0 ORDER BY v0.name", expected);
}

// A projected property, sorted by itself: the sorted rows carry the string values.
TEST_F(OrderByTest, projectedNamesAscending) {
    expectNames("MATCH (n) RETURN n.name ORDER BY n.name", nodeNamesAscending);
}

// A key naming the alias the projection gives an item: the alias stands for the column
// that item returns, so ordering by it orders by that column.
TEST_F(OrderByTest, projectedAliasAsKey) {
    expectNames("MATCH (n) RETURN n.name AS personName ORDER BY personName", nodeNamesAscending);
}

// The alias names the column the projection already carries, so the sort takes it rather
// than reading the property a second time into a column of its own.
TEST_F(OrderByTest, projectedAliasKeyIsNotDuplicated) {
    mlir::MLIRContext context;
    mlir::OwningOpRef<mlir::ModuleOp> module;
    generateProgram("MATCH (n) RETURN n.name AS personName ORDER BY personName", context, module);

    size_t propertyReads = 0;
    size_t sortCount = 0;

    module->walk([&](mlir::Operation* operation) {
        if (mlir::isa<mlir::db::GetNodeProperties>(operation)) {
            propertyReads++;
        } else if (mlir::db::Sort sortOp = mlir::dyn_cast<mlir::db::Sort>(operation)) {
            sortCount++;

            EXPECT_EQ(sortOp.getColumns().size(), 1u);
            ASSERT_EQ(sortOp.getKeyColumns().size(), 1u);
            EXPECT_EQ(sortOp.getKeyColumns()[0], 0);
        }
    });

    EXPECT_EQ(sortCount, 1u);
    EXPECT_EQ(propertyReads, 1u);
}

// Two keys, the first with real ties: every out-edge row sorted by source ascending
// and - within one source - target descending.
TEST_F(OrderByTest, edgesBySourceThenTargetDescending) {
    const Rows expected = {
        {0, 6}, {0, 3}, {0, 2}, {0, 1},
        {1, 5}, {1, 4}, {1, 0},
        {6, 0},
        {8, 7}, {8, 4},
        {9, 10}, {9, 2},
        {11, 5},
        {12, 16}, {12, 13},
        {15, 14}, {15, 13},
        {17, 13},
    };
    expectNodeRows("MATCH (a)-->(b) RETURN a, b ORDER BY a, b DESC", expected);
}

// ORDER BY ... LIMIT k: the k best rows of the whole order, not of an arbitrary
// prefix - the shape lowering fuses into a bounded top-K.
TEST_F(OrderByTest, orderByThenLimit) {
    const Rows expected = {{17}, {16}, {15}};
    expectNodeRows("MATCH (n) RETURN n ORDER BY n DESC LIMIT 3", expected);
}

// ORDER BY a property ... LIMIT k: the three earliest dobs. Only four nodes carry a
// dob, and a null sorts after every value, so however many null-dob nodes the scan
// runs into first, none of them displaces a real dob from the top three.
TEST_F(OrderByTest, topKByPropertyAscending) {
    Rows expected;
    nodeRowsFor({"Remy", "Adam", "Maxime"}, expected);

    expectNodeRows("MATCH (n) RETURN n ORDER BY n.dob LIMIT 3", expected);
}

// The same shape with the key projected and the order reversed: the last three names.
TEST_F(OrderByTest, topKByProjectedPropertyDescending) {
    const Names expected = {"Travel", "Suhas", "Remy"};

    expectNames("MATCH (n) RETURN n.name ORDER BY n.name DESC LIMIT 3", expected);
}

// ORDER BY ... SKIP m drops the first m rows of the order, not of the scan.
TEST_F(OrderByTest, orderByThenSkip) {
    const Rows expected = {{15}, {16}, {17}};
    expectNodeRows("MATCH (n) RETURN n ORDER BY n SKIP 15", expected);
}

// ORDER BY ... SKIP m LIMIT k: the sort sees every row, then the skip and the limit
// cut a window out of the sorted rows.
TEST_F(OrderByTest, orderByThenSkipLimit) {
    const Rows expected = {{15}, {14}, {13}};
    expectNodeRows("MATCH (n) RETURN n ORDER BY n DESC SKIP 2 LIMIT 3", expected);
}

// The two n.name of RETURN n.name ORDER BY n.name are separate AST nodes, so matching
// the key to the projection by identity would miss and hand the sort a second, equal
// column - one more property read, and one more column buffered for every row. Matching
// by what the expression names keeps the projection the sort's only column.
TEST_F(OrderByTest, projectedPropertyKeyIsNotDuplicated) {
    mlir::MLIRContext context;
    mlir::OwningOpRef<mlir::ModuleOp> module;
    generateProgram("MATCH (n) RETURN n.name ORDER BY n.name", context, module);

    size_t propertyReads = 0;
    size_t sortCount = 0;

    module->walk([&](mlir::Operation* operation) {
        if (mlir::isa<mlir::db::GetNodeProperties>(operation)) {
            propertyReads++;
        } else if (mlir::db::Sort sortOp = mlir::dyn_cast<mlir::db::Sort>(operation)) {
            sortCount++;

            EXPECT_EQ(sortOp.getColumns().size(), 1u);
            ASSERT_EQ(sortOp.getKeyColumns().size(), 1u);
            EXPECT_EQ(sortOp.getKeyColumns()[0], 0);
        }
    });

    EXPECT_EQ(sortCount, 1u);
    EXPECT_EQ(propertyReads, 1u);
}

// A key that reads no row holds the same value in every one of them, so it ties them
// all and orders nothing: the eighteen nodes come back as the scan produced them,
// IDs 0 to 17.
TEST_F(OrderByTest, constantKeyLeavesTheRowsAlone) {
    const Rows expected = {{0}, {1},  {2},  {3},  {4},  {5},  {6},  {7},  {8},
                           {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}, {17}};
    expectNodeRows("MATCH (n) RETURN n ORDER BY 1", expected);
}

// However it is written, a constant key is dropped, and a sort left with no key at all
// is not generated: an integer, a boolean, a string, an expression over literals and
// null all order the same nothing. Generating the sort would key it on a column holding
// the one row the constant is read into, against the many rows of the projection.
TEST_F(OrderByTest, constantKeysGenerateNoSort) {
    const std::vector<std::string> keys = {"1", "true", "'x'", "1 + 1", "-1", "null"};

    for (const std::string& key : keys) {
        const std::string query = "MATCH (n) RETURN n ORDER BY " + key;

        mlir::MLIRContext context;
        mlir::OwningOpRef<mlir::ModuleOp> module;
        generateProgram(query, context, module);

        size_t sortCount = 0;
        module->walk([&](mlir::db::Sort) { sortCount++; });

        EXPECT_EQ(sortCount, 0u) << "query: " << query;
    }
}

// A constant among real keys decides nothing either, so it is dropped and the keys
// around it keep their order of significance: this is ORDER BY n.name DESC.
TEST_F(OrderByTest, constantKeyIsDroppedFromAMultiKeySort) {
    Names expected = nodeNamesAscending;
    std::ranges::reverse(expected);

    expectNames("MATCH (n) RETURN n.name ORDER BY 1, n.name DESC, true", expected);
}

// The same query seen in the generated program: the sort is keyed on the projection's
// only column, once, rather than on three columns of which two never differ.
TEST_F(OrderByTest, constantKeyIsNotGivenToTheSort) {
    mlir::MLIRContext context;
    mlir::OwningOpRef<mlir::ModuleOp> module;
    generateProgram("MATCH (n) RETURN n.name ORDER BY 1, n.name DESC, true", context, module);

    size_t sortCount = 0;

    module->walk([&](mlir::db::Sort sortOp) {
        sortCount++;

        EXPECT_EQ(sortOp.getColumns().size(), 1u);
        ASSERT_EQ(sortOp.getKeyColumns().size(), 1u);
        EXPECT_EQ(sortOp.getKeyColumns()[0], 0);
    });

    EXPECT_EQ(sortCount, 1u);
}

// A key the projection does not carry is rejected after a DISTINCT, but a constant one
// is carried by nothing and asks for nothing: it names no column the dedup dropped, so
// the query stands and returns the distinct rows.
TEST_F(OrderByTest, constantKeyIsAllowedAfterDistinct) {
    OrderedNodeSink sink;
    runQuery("MATCH (a)-->(b) RETURN DISTINCT b ORDER BY 1", &sink);

    // The twelve nodes an edge points at, deduped; their order is the dedup's, which the
    // dropped key leaves untouched and the language does not promise
    Rows rows = sink.rows();
    ASSERT_EQ(rows.size(), 12u);

    std::sort(rows.begin(), rows.end());
    EXPECT_EQ(std::unique(rows.begin(), rows.end()), rows.end());
}

// A container is constant when every element it is written out of is: the map and the
// list here hold literals only, so each of them holds one value in every row and orders
// nothing, exactly as the scalar constants do.
TEST_F(OrderByTest, constantContainerKeysGenerateNoSort) {
    const std::vector<std::string> keys = {"{a: 1}", "{a: 1, b: 'x'}", "[1, 2]", "[]"};

    for (const std::string& key : keys) {
        const std::string query = "MATCH (n) RETURN n.name ORDER BY " + key;

        mlir::MLIRContext context;
        mlir::OwningOpRef<mlir::ModuleOp> module;
        generateProgram(query, context, module);

        size_t sortCount = 0;
        module->walk([&](mlir::db::Sort) { sortCount++; });

        EXPECT_EQ(sortCount, 0u) << "query: " << query;
    }
}

// A map holding a property reads a row through it, so it is not constant and the key is
// not dropped. The generator has no column to read a map into, so the query is rejected
// - which is the point: a key that varies must never be silently discarded, and the
// error names the unsupported map rather than answering in an order nothing decided
TEST_F(OrderByTest, mapKeyReadingARowIsNotDropped) {
    mlir::MLIRContext context;
    mlir::OwningOpRef<mlir::ModuleOp> module;

    EXPECT_THROW(generateProgram("MATCH (n) RETURN n.name ORDER BY {a: n.age}", context, module),
                 TuringException);
}

// The list mirror of the case above, rejected one step earlier: the analyzer does not
// accept a non-literal list element yet, so a list key cannot read a row today. The
// element flags are propagated all the same, so the day it does, the key varies with
// them instead of being taken for a constant
TEST_F(OrderByTest, listKeyReadingARowIsRejected) {
    mlir::MLIRContext context;
    mlir::OwningOpRef<mlir::ModuleOp> module;

    EXPECT_THROW(generateProgram("MATCH (n) RETURN n.name ORDER BY [1, n.age]", context, module),
                 TuringException);
}

// A call over constant arguments answers the same in every row, so it ties them all and
// is dropped like any other constant: this is ORDER BY n.name, keyed once
TEST_F(OrderByTest, constantCallKeyIsDropped) {
    mlir::MLIRContext context;
    mlir::OwningOpRef<mlir::ModuleOp> module;
    generateProgram("MATCH (n) RETURN n.name ORDER BY toInteger('42'), n.name", context, module);

    size_t sortCount = 0;

    module->walk([&](mlir::db::Sort sortOp) {
        sortCount++;

        EXPECT_EQ(sortOp.getColumns().size(), 1u);
        ASSERT_EQ(sortOp.getKeyColumns().size(), 1u);
        EXPECT_EQ(sortOp.getKeyColumns()[0], 0);
    });

    EXPECT_EQ(sortCount, 1u);
}

// A map written in a pattern is pulled out of the expression it was parsed as and
// analyzed as the pattern's properties, so the property equality it stands for is
// unaffected by the map literal analysis the ORDER BY cases above rest on
TEST_F(OrderByTest, patternPropertyMapIsUnaffected) {
    expectNames("MATCH (n {name: 'Remy'}) RETURN n.name", Names {"Remy"});
}

// The mirror case: a key the projection does not carry has no column to be matched to,
// so it is appended and the sort receives one column more than the projection.
TEST_F(OrderByTest, unprojectedKeyIsAppended) {
    mlir::MLIRContext context;
    mlir::OwningOpRef<mlir::ModuleOp> module;
    generateProgram("MATCH (n) RETURN n ORDER BY n.name", context, module);

    size_t sortCount = 0;

    module->walk([&](mlir::db::Sort sortOp) {
        sortCount++;

        EXPECT_EQ(sortOp.getColumns().size(), 2u);
        ASSERT_EQ(sortOp.getKeyColumns().size(), 1u);
        EXPECT_EQ(sortOp.getKeyColumns()[0], 1);
    });

    EXPECT_EQ(sortCount, 1u);
}

// A key most rows do not have: only Remy (0) and Adam (1) carry an age in simpledb, both
// 32, and the other sixteen nodes have none. A null sorts after every value, so the two
// aged nodes come first and the null-keyed rows follow; Remy and Adam tie on 32 and the
// sixteen nulls tie with each other, and a tie keeps the order the rows were collected
// in - the scan order - because the sort is stable.
TEST_F(OrderByTest, nodesByUnprojectedAgeAscending) {
    const Rows expected = {{0}, {1},  {2},  {3},  {4},  {5},  {6},  {7},  {8},
                           {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}, {17}};
    expectNodeRows("MATCH (n) RETURN n ORDER BY n.age", expected);
}

// Reversing the order moves the nulls to the front, since they sort after every value.
// The ties do not reverse with it: a descending sort still keeps tied rows in collected
// order, so the nulls stay in scan order and Remy precedes Adam.
TEST_F(OrderByTest, nodesByUnprojectedAgeDescending) {
    const Rows expected = {{2},  {3},  {4},  {5},  {6},  {7},  {8}, {9}, {10},
                           {11}, {12}, {13}, {14}, {15}, {16}, {17}, {0}, {1}};
    expectNodeRows("MATCH (n) RETURN n ORDER BY n.age DESC", expected);
}

// The key belongs to the other end of the traversal: the projection returns n, so m.age
// is a column the sort carries only to order by, never to output. Just three of the
// eighteen edges point at a node that has an age - Remy -> Adam, Adam -> Remy and
// Ghosts -> Remy, all keyed 32 - so their sources lead, and the fifteen null-keyed rows
// follow in the order the traversal collected them. A source repeats once per out-edge:
// ORDER BY reorders rows, it does not merge them.
TEST_F(OrderByTest, nodesByTargetAgeAscending) {
    const Rows expected = {{0},  {1},  {6},                     // m is Adam or Remy, 32
                           {0},  {0},  {0},  {1},  {1},  {8},   // m has no age from here
                           {8},  {9},  {9},  {11}, {15}, {15},
                           {12}, {12}, {17}};
    expectNodeRows("MATCH (n)-->(m) RETURN n ORDER BY m.age", expected);
}

// A sort over a cross product, keyed on the inner factor alone. The product pairs the
// two nodes aged 32 - Remy (0) and Adam (1) - with the two SleepDisturbers - Ghosts (6)
// and Animals (10) - and the nest produces them outer-first: (0,6), (0,10), (1,6),
// (1,10). Ordering by b alone interleaves those iterations, putting both rows keyed on
// Ghosts ahead of both keyed on Animals, which only a sort that has seen the whole nest
// can do - a sort that ordered each chunk or each outer step on its own could not. The
// two rows sharing a b tie, so they keep the order the nest collected them in.
TEST_F(OrderByTest, crossProductByInnerFactorAscending) {
    const Rows expected = {{0, 6}, {1, 6}, {0, 10}, {1, 10}};
    expectNodeRows("MATCH (a {age: 32}), (b:SleepDisturber) RETURN a, b ORDER BY b", expected);
}

// Keying on both factors leaves no tie, and reversing both turns the nest's output
// inside out: the last row produced is the first emitted.
TEST_F(OrderByTest, crossProductByBothFactorsDescending) {
    const Rows expected = {{1, 10}, {1, 6}, {0, 10}, {0, 6}};
    expectNodeRows("MATCH (a {age: 32}), (b:SleepDisturber) RETURN a, b ORDER BY a DESC, b DESC",
                   expected);
}

// An arithmetic expression as the sort key, over the eight Person nodes paired with
// themselves. Only Remy and Adam have an age, so b.age - a.age is 0 for the four pairs
// drawn from those two and null for the other sixty, null propagating through the
// subtraction. No projected column holds that difference, so it is computed into a
// column of its own and appended to the sort: the four rows keyed 0 lead, in the order
// the nest collected them, and the sixty null-keyed rows follow.
TEST_F(OrderByTest, personPairsByAgeDifference) {
    OrderedNodeSink sink;
    runQuery("MATCH (a:Person), (b:Person) RETURN a, b ORDER BY b.age - a.age", &sink);

    const uint64_t remy = SimpleGraph::findNodeID(_graph, "Remy").getValue();
    const uint64_t adam = SimpleGraph::findNodeID(_graph, "Adam").getValue();

    const Rows& rows = sink.rows();
    ASSERT_EQ(rows.size(), 64u);

    const Rows aged = {{remy, remy}, {remy, adam}, {adam, remy}, {adam, adam}};
    const Rows leading(rows.begin(), rows.begin() + aged.size());
    EXPECT_EQ(leading, aged);

    // Every row after them has an endpoint with no age, which is what nulled its key
    for (size_t rowIndex = aged.size(); rowIndex < rows.size(); rowIndex++) {
        const Row& row = rows[rowIndex];
        const bool sourceAged = row[0] == remy || row[0] == adam;
        const bool targetAged = row[1] == remy || row[1] == adam;

        EXPECT_FALSE(sourceAged && targetAged)
            << "row {" << row[0] << ", " << row[1] << "} has a key of 0 but sorted after a null";
    }
}

// The same match one level deeper: the key is computed, so the two occurrences of
// n.age + 42 are whole trees rather than a single node to recognise. Comparing them
// structurally still finds the projected column, so the sort takes it and the property
// behind it is read once.
TEST_F(OrderByTest, projectedExpressionKeyIsNotDuplicated) {
    mlir::MLIRContext context;
    mlir::OwningOpRef<mlir::ModuleOp> module;
    generateProgram("MATCH (n) RETURN n.age + 42 ORDER BY n.age + 42", context, module);

    size_t propertyReads = 0;
    size_t sortCount = 0;

    module->walk([&](mlir::Operation* operation) {
        if (mlir::isa<mlir::db::GetNodeProperties>(operation)) {
            propertyReads++;
        } else if (mlir::db::Sort sortOp = mlir::dyn_cast<mlir::db::Sort>(operation)) {
            sortCount++;

            EXPECT_EQ(sortOp.getColumns().size(), 1u);
            ASSERT_EQ(sortOp.getKeyColumns().size(), 1u);
            EXPECT_EQ(sortOp.getKeyColumns()[0], 0);
        }
    });

    EXPECT_EQ(sortCount, 1u);
    EXPECT_EQ(propertyReads, 1u);
}
