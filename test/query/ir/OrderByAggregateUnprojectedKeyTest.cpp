#include <gtest/gtest.h>

#include <stdint.h>

#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
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

#include "AnalyzeException.h"
#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

const std::string_view droppedKeyReason =
    "ORDER BY with an aggregate may only order by expressions over the returned columns";

// A group of MATCH (a:Person)-[e]->(b) RETURN a, a.age, count(b.name): the source node,
// the age it is grouped by, and how many targets it has.
using GroupRow = std::tuple<uint64_t, std::optional<int64_t>, uint64_t>;
using GroupRows = std::vector<GroupRow>;

// The eight simpledb Persons with an out-edge, by name ascending - Adam, Cyrus, Doruk,
// Luc, Martina, Maxime, Remy, Suhas - each with its out-degree. Only Remy and Adam carry
// an age. Ghosts has an out-edge too but is no Person, so it is not among the groups.
const GroupRows groupsByName = {
    {1, 32, 3},
    {15, std::nullopt, 2},
    {17, std::nullopt, 1},
    {9, std::nullopt, 2},
    {11, std::nullopt, 1},
    {8, std::nullopt, 2},
    {0, 32, 4},
    {12, std::nullopt, 2},
};

// The same groups by age then by name, the order the compound key asks for: the two aged
// groups first, and the six the age leaves tied ordered by the second key. A null sorts
// after every value, so the ageless groups close the order.
const GroupRows groupsByAgeThenName = {
    {1, 32, 3},
    {0, 32, 4},
    {15, std::nullopt, 2},
    {17, std::nullopt, 1},
    {9, std::nullopt, 2},
    {11, std::nullopt, 1},
    {8, std::nullopt, 2},
    {12, std::nullopt, 2},
};

// A group of MATCH (a)-[e]->(b) RETURN e.duration, count(b.name): the duration grouped on
// and how many edges carry it.
using DurationRow = std::pair<std::optional<int64_t>, uint64_t>;
using DurationRows = std::vector<DurationRow>;

// The five groups the eighteen simpledb edges reduce to, by duration ascending. Ten edges
// carry no duration, and a null sorts after every value, so they close the order.
const DurationRows durationGroupsAscending = {
    {10, 1},
    {15, 1},
    {20, 5},
    {200, 1},
    {std::nullopt, 10},
};

// The same groups by 0 - duration ascending. Negating reverses the four durations while
// leaving the null group last, so this is the order no raw grouping key produces: it is
// only reached by evaluating the key over the grouped column.
const DurationRows durationGroupsByNegation = {
    {200, 1},
    {20, 5},
    {15, 1},
    {10, 1},
    {std::nullopt, 10},
};

// Collects the group rows in the order the sink sees them. An ORDER BY is only correct if
// that order survives to the output, so nothing here sorts what it collected.
class OrderedGroupSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 3u);

        const auto* sources = dynamic_cast<const ColumnNodeIDs*>(chunks[0]);
        ASSERT_NE(sources, nullptr);

        const auto* ages = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[1]);
        ASSERT_NE(ages, nullptr);

        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[2]);
        ASSERT_NE(counts, nullptr);

        const auto& ageRaw = ages->getRaw();
        const auto& countRaw = counts->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back((*sources)[rowIndex].getValue(), ageRaw[rowIndex], countRaw[rowIndex]);
        }
    }

    const GroupRows& rows() const { return _rows; }

private:
    GroupRows _rows;
};

// Collects the (grouping key value, count) rows in the order the sink sees them.
class OrderedDurationSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* durations = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[0]);
        ASSERT_NE(durations, nullptr);

        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[1]);
        ASSERT_NE(counts, nullptr);

        const auto& durationRaw = durations->getRaw();
        const auto& countRaw = counts->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back(durationRaw[rowIndex], countRaw[rowIndex]);
        }
    }

    const DurationRows& rows() const { return _rows; }

private:
    DurationRows _rows;
};

// Reads whatever shape it is handed, for the queries a test only has to see accepted or
// turned away rather than check the rows of.
class AnyShapeSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        _rowCount += rowCount;
    }

    size_t getRowCount() const { return _rowCount; }

private:
    size_t _rowCount {0};
};

}

// An ORDER BY over an aggregating projection sorts the groups, not the matched rows, so a
// key the projection does not carry is only a key at all if it holds one value per group.
// A key reading a returned variable holds one - a.name is fixed by the a every group is
// keyed on - and is read off the grouped column beside the groups it orders. A key reading
// a variable the grouping dropped holds one value per matched row instead, so it lines up
// with nothing the projection emits and the analyzer turns the query away.
class OrderByAggregateUnprojectedKeyTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);
    }

    void runQuery(std::string_view query, NLOutputSink* sink) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const ProcedureManager* procedures = system.getProcedures();

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        CypherAST ast(procedures, query);

        CypherParser parser(&ast);
        parser.parse(query);

        CypherAnalyzer analyzer(&ast, view);
        analyzer.setV3();
        analyzer.analyze();

        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::db::DB>();
        context.getOrLoadDialect<mlir::nl::NL>();

        mlir::OpBuilder builder(&context);
        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::ModuleOp::create(builder.getUnknownLoc());
        mlir::ModuleOp moduleOp = module.get();

        DBProgramGenerator generator(&moduleOp);
        generator.generate(&ast);

        LocalMemory memory;
        DBDialectInterpreter interpreter(moduleOp, &view, sink, &memory);
        interpreter.run();
    }

    void expectRows(std::string_view query, const GroupRows& expected) {
        OrderedGroupSink sink;
        runQuery(query, &sink);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query;
    }

    void expectDurationRows(std::string_view query, const DurationRows& expected) {
        OrderedDurationSink sink;
        runQuery(query, &sink);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query;
    }

    void expectAccepted(std::string_view query) {
        AnyShapeSink sink;
        EXPECT_NO_THROW(runQuery(query, &sink)) << "query: " << query;
    }

    // The query is turned away, and on the dropped key rather than on anything else the
    // analyzer may have to say about it
    void expectRejected(std::string_view query) {
        AnyShapeSink sink;

        try {
            runQuery(query, &sink);
        } catch (const AnalyzeException& error) {
            const std::string message = error.what();
            EXPECT_NE(message.find(droppedKeyReason), std::string::npos)
                << "query: " << query << "\nerror: " << message;
            return;
        }

        ADD_FAILURE() << "query was accepted: " << query;
    }

    // The rows of @param expected from @param first, as many as @param count - the window
    // a SKIP and a LIMIT cut out of an order
    void windowOf(const GroupRows& expected, size_t first, size_t count, GroupRows& rows) {
        rows.assign(expected.begin() + first, expected.begin() + first + count);
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
};

// The key is neither a returned item nor one of the two grouping keys, and it reads the
// returned variable a: one name per group, so the groups have an order to be put in.
TEST_F(OrderByAggregateUnprojectedKeyTest, ordersGroupsByAKeyOverAReturnedVariable) {
    expectRows("MATCH (a:Person)-[e]->(b) RETURN a, a.age, count(b.name) ORDER BY a.name", groupsByName);
}

TEST_F(OrderByAggregateUnprojectedKeyTest, ordersGroupsByAKeyOverAReturnedVariableDescending) {
    const GroupRows expected {groupsByName.rbegin(), groupsByName.rend()};

    expectRows("MATCH (a:Person)-[e]->(b) RETURN a, a.age, count(b.name) ORDER BY a.name DESC", expected);
}

// A bounded sort keeps only the best k groups, so the key is carried by the trim that
// drops the rest as well as by the emit.
TEST_F(OrderByAggregateUnprojectedKeyTest, ordersGroupsByAKeyOverAReturnedVariableThenLimits) {
    GroupRows expected;
    windowOf(groupsByName, 0, 3, expected);

    expectRows("MATCH (a:Person)-[e]->(b) RETURN a, a.age, count(b.name) ORDER BY a.name LIMIT 3", expected);
}

TEST_F(OrderByAggregateUnprojectedKeyTest, ordersGroupsByAKeyOverAReturnedVariableThenSkipsAndLimits) {
    GroupRows expected;
    windowOf(groupsByName, 2, 3, expected);

    expectRows("MATCH (a:Person)-[e]->(b) RETURN a, a.age, count(b.name) ORDER BY a.name SKIP 2 LIMIT 3",
               expected);
}

// The returned grouping key first and the unreturned key second: one key is read off the
// column the projection already carries and the other off a column of its own, and both
// have to hold the same row per group for the second to break the first's ties.
TEST_F(OrderByAggregateUnprojectedKeyTest, breaksTiesOnAReturnedKeyWithAnUnreturnedOne) {
    expectRows("MATCH (a:Person)-[e]->(b) RETURN a, a.age, count(b.name) ORDER BY a.age, a.name",
               groupsByAgeThenName);
}

// A key computing over a returned variable is one value per group just as a property of it
// is, so what the key does with the variable is not what the rule turns on.
TEST_F(OrderByAggregateUnprojectedKeyTest, acceptsAKeyComputedOverAReturnedVariable) {
    expectAccepted("MATCH (a:Person)-[e]->(b) RETURN a, a.age, count(b.name) ORDER BY a.age * 2");
}

// The key is the grouping key itself, which the sort keys on where it already stands: it
// needs no variable of its own, so e is not required to be returned beside it.
TEST_F(OrderByAggregateUnprojectedKeyTest, ordersGroupsByAGroupingKey) {
    expectDurationRows("MATCH (a)-[e]->(b) RETURN e.duration, count(b.name) ORDER BY e.duration",
                       durationGroupsAscending);
}

// A key computing over a grouping key. The group aggregate reduced e.duration to one value
// per group, and the key has to be computed from that column rather than from the ungrouped
// one it was reduced from - which holds one value per edge, and lines up with no group.
TEST_F(OrderByAggregateUnprojectedKeyTest, ordersGroupsByAKeyComputedOverAGroupingKey) {
    expectDurationRows("MATCH (a)-[e]->(b) RETURN e.duration, count(b.name) ORDER BY e.duration + 1",
                       durationGroupsAscending);
}

// Adding one to every key changes no order, so the case above would also pass on a sort
// that ignored the key and used the grouping key column beside it. Negating does reorder
// the groups, so this is the case that pins the key as the thing being evaluated.
TEST_F(OrderByAggregateUnprojectedKeyTest, ordersGroupsByAKeyThatReordersAGroupingKey) {
    expectDurationRows("MATCH (a)-[e]->(b) RETURN e.duration, count(b.name) ORDER BY 0 - e.duration",
                       durationGroupsByNegation);
}

// The grouping key nested deeper than one operator down
TEST_F(OrderByAggregateUnprojectedKeyTest, ordersGroupsByADeeperKeyOverAGroupingKey) {
    expectDurationRows("MATCH (a)-[e]->(b) RETURN e.duration, count(b.name) ORDER BY e.duration * 2 + 1",
                       durationGroupsAscending);
}

// An alias is another spelling of the item it names, so a key computing over one computes
// over that item's grouped column just as spelling the item out does.
TEST_F(OrderByAggregateUnprojectedKeyTest, ordersGroupsByAKeyComputedOverTheAliasOfAGroupingKey) {
    expectDurationRows("MATCH (a)-[e]->(b) RETURN e.duration AS d, count(b.name) ORDER BY 0 - d",
                       durationGroupsByNegation);
}

// A bounded sort keeps only the best k groups, so the computed key is carried by the trim
// that drops the rest as well as by the emit.
TEST_F(OrderByAggregateUnprojectedKeyTest, ordersGroupsByAComputedKeyThenLimits) {
    const DurationRows expected {durationGroupsByNegation.begin(), durationGroupsByNegation.begin() + 3};

    expectDurationRows("MATCH (a)-[e]->(b) RETURN e.duration, count(b.name) ORDER BY 0 - e.duration LIMIT 3",
                       expected);
}

// A constant key holds the same value in every group, so it reads no variable and orders
// nothing.
TEST_F(OrderByAggregateUnprojectedKeyTest, acceptsAConstantKey) {
    expectAccepted("MATCH (a:Person)-[e]->(b) RETURN a, a.age, count(b.name) ORDER BY 1 + 1");
}

// Returning b groups by it too, which keeps it: the same b.name that is no key beside a
// alone is one here, over the finer groups the second key makes.
TEST_F(OrderByAggregateUnprojectedKeyTest, acceptsAKeyOverASecondReturnedVariable) {
    expectAccepted("MATCH (a:Person)-[e]->(b) RETURN a, b, count(e.name) ORDER BY b.name");
}

// The key reads b, which the grouping dropped: b.name is one name per edge - seventeen of
// them - and the projection emits eight groups, so there is no row of the output the key
// belongs to.
TEST_F(OrderByAggregateUnprojectedKeyTest, rejectsAKeyOverADroppedTargetVariable) {
    expectRejected("MATCH (a:Person)-[e]->(b) RETURN a, a.age, count(b.name) ORDER BY b.name");
}

TEST_F(OrderByAggregateUnprojectedKeyTest, rejectsAKeyOverADroppedEdgeVariable) {
    expectRejected("MATCH (a:Person)-[e]->(b) RETURN a, a.age, count(b.name) ORDER BY e.duration");
}

// A bare dropped variable, rather than a property of one: the column it names is the one
// the grouping consumed, so there is none left for the sort to key on.
TEST_F(OrderByAggregateUnprojectedKeyTest, rejectsABareDroppedVariableKey) {
    expectRejected("MATCH (a:Person)-[e]->(b) RETURN a, count(b.name) ORDER BY b");
}

// One usable key does not excuse the other: the query is rejected on the key reading the
// dropped variable, wherever it sits among the keys.
TEST_F(OrderByAggregateUnprojectedKeyTest, rejectsADroppedKeyAmongUsableOnes) {
    expectRejected("MATCH (a:Person)-[e]->(b) RETURN a, a.age, count(b.name) ORDER BY a.name, b.name");
}

// One key reading both a returned variable and a dropped one. The returned half is no
// help: the key is a single column, and it cannot be computed per group.
TEST_F(OrderByAggregateUnprojectedKeyTest, rejectsAKeyMixingAReturnedVariableWithADroppedOne) {
    expectRejected("MATCH (a:Person)-[e]->(b) RETURN a, count(b.name) ORDER BY a.age + b.age");
}

// The line the case above draws: a key computing over the grouping key a.age is one value
// per group, but a.name is a second property of a variable the grouping consumed - two
// Persons of one age have two names, so no value of the group is being asked for.
TEST_F(OrderByAggregateUnprojectedKeyTest, rejectsAKeyOverAVariableOnlyAPropertyOfWhichIsReturned) {
    expectRejected("MATCH (a:Person)-[e]->(b) RETURN a.age, count(b.name) ORDER BY a.name");
}

// Computing over a dropped variable is not saved by the arithmetic around it: b.age is one
// value per edge whether or not one is added to it.
TEST_F(OrderByAggregateUnprojectedKeyTest, rejectsAComputedKeyOverADroppedVariable) {
    expectRejected("MATCH (a:Person)-[e]->(b) RETURN a, count(b.name) ORDER BY b.age + 1");
}

// A projection with no grouping key at all emits one row for the whole match, and it keeps
// no variable: there is nothing a dynamic key could hold one value per row of.
TEST_F(OrderByAggregateUnprojectedKeyTest, rejectsAKeyUnderAScalarAggregate) {
    expectRejected("MATCH (a:Person)-[e]->(b) RETURN count(b.name) ORDER BY a.name");
}

// The rule is the aggregate's: without one every matched row is returned, so an unreturned
// key is read into a column of its own and sorted with the row it belongs to.
TEST_F(OrderByAggregateUnprojectedKeyTest, acceptsADroppedKeyWithoutAnAggregate) {
    expectAccepted("MATCH (a:Person)-[e]->(b) RETURN a ORDER BY b.name");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
