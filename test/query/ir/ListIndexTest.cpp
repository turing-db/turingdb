#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"

#include "llvm/Support/raw_ostream.h"

#include "DBDialect.h"
#include "DBDialectInterpreter.h"
#include "DBLowering.h"
#include "DBProgramGenerator.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "NLOps.h"
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
#include "list/ListBufferTypeTag.h"
#include "list/ListElementView.h"
#include "metadata/PropertyType.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using GroupRows = std::vector<std::pair<std::optional<types::Int64::Primitive>, uint64_t>>;

// The integer an index read produced, or nothing where the position held no value - past
// the end of the list, or a null inside it. getAs reads the cell without checking its
// tag, so a null has to be told from a value before the read, not after.
std::optional<types::Int64::Primitive> elementAsInteger(const std::optional<ListElementView>& element) {
    if (!element.has_value() || element->getTag() == ListBufferTypeTag::Null) {
        return std::nullopt;
    }

    return element->getAs<types::Int64::Primitive>();
}

// Reads the first projected column as one optional integer per row, accepting both shapes
// an index result takes: a constant, when list and position are both literals, and a
// per-row nullable column otherwise.
class ElementSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_FALSE(chunks.empty());

        const Column* column = chunks[0];

        const auto* constElement = dynamic_cast<const ColumnConst<std::optional<ListElementView>>*>(column);
        const auto* vectorElement = dynamic_cast<const ColumnOptVector<ListElementView>*>(column);
        ASSERT_TRUE(constElement || vectorElement);

        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            const std::optional<ListElementView> element =
                constElement ? (*constElement)[rowIndex] : vectorElement->getRaw()[rowIndex];

            _rows.push_back(elementAsInteger(element));
        }
    }

    const std::vector<std::optional<types::Int64::Primitive>>& rows() const { return _rows; }

private:
    std::vector<std::optional<types::Int64::Primitive>> _rows;
};

// Reads the tally an aggregate with no grouping key emits.
class CountSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[0]);
        ASSERT_NE(counts, nullptr);

        const std::vector<uint64_t>& raw = counts->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _values.push_back(raw[rowIndex]);
        }
    }

    const std::vector<uint64_t>& values() const { return _values; }

private:
    std::vector<uint64_t> _values;
};

// Reads the (element, tally) rows a count grouped by an index read emits. A grouped
// aggregate emits its groups in first-seen order, which the language does not promise, so
// the rows are compared sorted.
class GroupedElementCountSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* keys = dynamic_cast<const ColumnOptVector<ListElementView>*>(chunks[0]);
        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[1]);
        ASSERT_NE(keys, nullptr);
        ASSERT_NE(counts, nullptr);

        const std::vector<std::optional<ListElementView>>& keyRaw = keys->getRaw();
        const std::vector<uint64_t>& countRaw = counts->getRaw();

        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back(elementAsInteger(keyRaw[rowIndex]), countRaw[rowIndex]);
        }
    }

    void sortedRows(GroupRows& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    GroupRows _rows;
};

}

class ListIndexTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);
    }

    void generateProgram(std::string_view query,
                         const GraphView& view,
                         const ProcedureManager* procedures,
                         mlir::MLIRContext& context,
                         mlir::OwningOpRef<mlir::ModuleOp>& module) {
        CypherAST ast(procedures, query);

        CypherParser parser(&ast);
        parser.parse(query);

        CypherAnalyzer analyzer(&ast, view);
        analyzer.setV3();
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
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const ProcedureManager* procedures = system.getProcedures();

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        mlir::MLIRContext context;
        mlir::OwningOpRef<mlir::ModuleOp> module;
        generateProgram(query, view, procedures, context, module);

        LocalMemory memory;
        DBDialectInterpreter interpreter(module.get(), &view, sink, &memory);
        interpreter.run();
    }

    std::vector<std::optional<types::Int64::Primitive>> evalElements(std::string_view query) {
        ElementSink sink;
        runQuery(query, &sink);

        return sink.rows();
    }

    std::optional<types::Int64::Primitive> evalElement(std::string_view query) {
        const std::vector<std::optional<types::Int64::Primitive>> rows = evalElements(query);

        EXPECT_EQ(rows.size(), 1u) << "query: " << query;
        return rows.empty() ? std::nullopt : rows.front();
    }

    uint64_t evalCount(std::string_view query) {
        CountSink sink;
        runQuery(query, &sink);

        const std::vector<uint64_t>& values = sink.values();

        EXPECT_EQ(values.size(), 1u) << "query: " << query;
        return values.empty() ? 0 : values.front();
    }

    void evalGroups(std::string_view query, GroupRows& rows) {
        GroupedElementCountSink sink;
        runQuery(query, &sink);
        sink.sortedRows(rows);
    }

    std::string indexResultType(std::string_view query) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const ProcedureManager* procedures = system.getProcedures();

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        mlir::MLIRContext context;
        mlir::OwningOpRef<mlir::ModuleOp> module;
        generateProgram(query, view, procedures, context, module);

        const mlir::func::FuncOp dbFunction = module.get().lookupSymbol<mlir::func::FuncOp>("main");
        EXPECT_TRUE(dbFunction);

        mlir::OwningOpRef<mlir::ModuleOp> nlModule = mlir::ModuleOp::create(mlir::UnknownLoc::get(&context));
        DBLowering lowering(&context, &view);
        lowering.lower(dbFunction, *nlModule);

        mlir::nl::ListIndex index;
        nlModule->walk([&](mlir::nl::ListIndex op) { index = op; });
        EXPECT_TRUE(index);

        std::string printed;
        llvm::raw_string_ostream stream(printed);
        index.getResult().getType().getElementType().print(stream);

        return printed;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
};

TEST_F(ListIndexTest, typesAnIndexAsANullableTaggedScalar) {
    const std::string resultType = indexResultType("MATCH (n) WHERE n.name = 'Remy' RETURN [1, 2, 3][1]");

    EXPECT_NE(resultType.find("nullable"), std::string::npos) << "index result type: " << resultType;
    EXPECT_NE(resultType.find("list_element"), std::string::npos) << "index result type: " << resultType;
}

// A homogeneous list gives the access no more specific type than a mixed one does: the
// element read carries its own tag either way.
TEST_F(ListIndexTest, typesAnIndexIntoAMixedListTheSameWay) {
    const std::string homogeneous = indexResultType("MATCH (n) WHERE n.name = 'Remy' RETURN [1, 2, 3][1]");
    const std::string mixed = indexResultType("MATCH (n) WHERE n.name = 'Remy' RETURN [1, 'a'][1]");

    EXPECT_EQ(homogeneous, mixed);
}

// Out of range reads null, so the result is nullable however the index is written.
TEST_F(ListIndexTest, typesANegativeIndexTheSameWay) {
    const std::string forward = indexResultType("MATCH (n) WHERE n.name = 'Remy' RETURN [1, 2, 3][1]");
    const std::string fromTheEnd = indexResultType("MATCH (n) WHERE n.name = 'Remy' RETURN [1, 2, 3][-1]");

    EXPECT_EQ(forward, fromTheEnd);
}

TEST_F(ListIndexTest, readsTheElementAtAPosition) {
    EXPECT_EQ(evalElement("MATCH (n) WHERE n.name = 'Remy' RETURN [1, 3, 4][2]"), 4);
    EXPECT_EQ(evalElement("MATCH (n) WHERE n.name = 'Remy' RETURN [1, 3, 4][0]"), 1);
}

TEST_F(ListIndexTest, countsANegativePositionFromTheEnd) {
    EXPECT_EQ(evalElement("MATCH (n) WHERE n.name = 'Remy' RETURN [1, 3, 4][-1]"), 4);
    EXPECT_EQ(evalElement("MATCH (n) WHERE n.name = 'Remy' RETURN [1, 3, 4][-3]"), 1);
}

TEST_F(ListIndexTest, readsNullOutsideTheList) {
    EXPECT_EQ(evalElement("MATCH (n) WHERE n.name = 'Remy' RETURN [1, 3, 4][3]"), std::nullopt);
    EXPECT_EQ(evalElement("MATCH (n) WHERE n.name = 'Remy' RETURN [1, 3, 4][9]"), std::nullopt);
    EXPECT_EQ(evalElement("MATCH (n) WHERE n.name = 'Remy' RETURN [1, 3, 4][-4]"), std::nullopt);
}

TEST_F(ListIndexTest, readsAnElementOfAMixedList) {
    EXPECT_EQ(evalElement("MATCH (n) WHERE n.name = 'Remy' RETURN [1, 'a', 4][0]"), 1);
}

// Remy is 32, so every position is past the end: the row's own value drives the read.
TEST_F(ListIndexTest, readsThePositionFromTheRow) {
    EXPECT_EQ(evalElement("MATCH (n) WHERE n.name = 'Remy' RETURN [1, 3, 4][n.age]"), std::nullopt);
}

TEST_F(ListIndexTest, readsNullWhereThePositionIsNull) {
    const std::vector<std::optional<types::Int64::Primitive>> rows =
        evalElements("MATCH (n) RETURN [1, 2, 3][n.age - 32]");

    EXPECT_EQ(rows.size(), 18u);

    size_t present = 0;
    for (const std::optional<types::Int64::Primitive>& row : rows) {
        if (!row.has_value()) {
            continue;
        }

        EXPECT_EQ(*row, 1);
        present++;
    }

    EXPECT_EQ(present, 2u);
}

// Two nodes are 32, so two rows read position 0 and every other row reads null: the
// comparison keeps the two whose element is 1 and drops the nulls with them.
TEST_F(ListIndexTest, filtersOnTheElementAtAPosition) {
    const std::vector<std::optional<types::Int64::Primitive>> matched =
        evalElements("MATCH (n) WHERE [1, 2, 3][n.age - 32] = 1 RETURN [1, 2, 3][n.age - 32]");

    EXPECT_EQ(matched.size(), 2u);
    for (const std::optional<types::Int64::Primitive>& row : matched) {
        EXPECT_EQ(row, 1);
    }

    const std::vector<std::optional<types::Int64::Primitive>> unmatched =
        evalElements("MATCH (n) WHERE [1, 2, 3][n.age - 32] = 9 RETURN [1, 2, 3][n.age - 32]");

    EXPECT_TRUE(unmatched.empty());
}

TEST_F(ListIndexTest, testsWhetherThePositionHeldAnElement) {
    const std::vector<std::optional<types::Int64::Primitive>> absent =
        evalElements("MATCH (n) WHERE [1, 2, 3][n.age - 32] IS NULL RETURN [1, 2, 3][n.age - 32]");

    EXPECT_EQ(absent.size(), 16u);
    for (const std::optional<types::Int64::Primitive>& row : absent) {
        EXPECT_EQ(row, std::nullopt);
    }

    const std::vector<std::optional<types::Int64::Primitive>> present =
        evalElements("MATCH (n) WHERE [1, 2, 3][n.age - 32] IS NOT NULL RETURN [1, 2, 3][n.age - 32]");

    EXPECT_EQ(present.size(), 2u);
}

TEST_F(ListIndexTest, comparesTwoElementReads) {
    const std::vector<std::optional<types::Int64::Primitive>> rows =
        evalElements("MATCH (n) WHERE [1, 2, 3][n.age - 32] = [1, 2, 3][0] RETURN [1, 2, 3][n.age - 32]");

    EXPECT_EQ(rows.size(), 2u);
}

TEST_F(ListIndexTest, comparesAnElementAgainstAString) {
    const std::vector<std::optional<types::Int64::Primitive>> matched =
        evalElements("MATCH (n) WHERE ['a', 'b'][n.age - 32] = 'a' RETURN [1, 2, 3][n.age - 32]");

    EXPECT_EQ(matched.size(), 2u);

    const std::vector<std::optional<types::Int64::Primitive>> unmatched =
        evalElements("MATCH (n) WHERE ['a', 'b'][n.age - 32] = 'z' RETURN [1, 2, 3][n.age - 32]");

    EXPECT_TRUE(unmatched.empty());
}

// Position 1 holds a null inside the list and position 5 is past its end, so two of the
// four rows are null by different routes: ascending puts both after the values, and DESC
// puts both before them.
TEST_F(ListIndexTest, sortsOnTheElementAtAPosition) {
    const std::vector<std::optional<types::Int64::Primitive>> ascending =
        evalElements("UNWIND [0, 1, 2, 5] AS i RETURN [10, null, 30][i] ORDER BY [10, null, 30][i]");

    ASSERT_EQ(ascending.size(), 4u);
    EXPECT_EQ(ascending[0], 10);
    EXPECT_EQ(ascending[1], 30);
    EXPECT_EQ(ascending[2], std::nullopt);
    EXPECT_EQ(ascending[3], std::nullopt);

    const std::vector<std::optional<types::Int64::Primitive>> descending =
        evalElements("UNWIND [0, 1, 2, 5] AS i RETURN [10, null, 30][i] ORDER BY [10, null, 30][i] DESC");

    ASSERT_EQ(descending.size(), 4u);
    EXPECT_EQ(descending[0], std::nullopt);
    EXPECT_EQ(descending[1], std::nullopt);
    EXPECT_EQ(descending[2], 30);
    EXPECT_EQ(descending[3], 10);
}

// The null inside the list and the read past its end are one value to DISTINCT, so the
// four rows dedup to 10, 30 and a single null.
TEST_F(ListIndexTest, dedupsTheElementAtAPosition) {
    const std::vector<std::optional<types::Int64::Primitive>> rows =
        evalElements("UNWIND [0, 1, 2, 5] AS i RETURN DISTINCT [10, null, 30][i]");

    std::vector<std::optional<types::Int64::Primitive>> sorted = rows;
    std::sort(sorted.begin(), sorted.end());

    const std::vector<std::optional<types::Int64::Primitive>> expected = {std::nullopt, 10, 30};
    EXPECT_EQ(sorted, expected);
}

// count(x) charges neither null, so two of the four rows are counted.
TEST_F(ListIndexTest, countsThePresentElements) {
    EXPECT_EQ(evalCount("UNWIND [0, 1, 2, 5] AS i RETURN count([10, null, 30][i])"), 2u);
}

// Position 0 is read twice and the two nulls group together, so the five rows group as
// 10 twice, 30 once and null twice.
TEST_F(ListIndexTest, groupsOnTheElementAtAPosition) {
    GroupRows rows;
    evalGroups("UNWIND [0, 1, 2, 5, 0] AS i RETURN [10, null, 30][i], count(i)", rows);

    const GroupRows expected = {{std::nullopt, 2u}, {10, 2u}, {30, 1u}};
    EXPECT_EQ(rows, expected);
}

TEST_F(ListIndexTest, truncatesRowsCarryingAnElement) {
    const std::vector<std::optional<types::Int64::Primitive>> rows =
        evalElements("UNWIND [0, 1, 2, 5] AS i RETURN [10, null, 30][i] LIMIT 2");

    const std::vector<std::optional<types::Int64::Primitive>> expected = {10, std::nullopt};
    EXPECT_EQ(rows, expected);
}

TEST_F(ListIndexTest, skipsRowsCarryingAnElement) {
    const std::vector<std::optional<types::Int64::Primitive>> rows =
        evalElements("UNWIND [0, 1, 2, 5] AS i RETURN [10, null, 30][i] SKIP 2");

    const std::vector<std::optional<types::Int64::Primitive>> expected = {30, std::nullopt};
    EXPECT_EQ(rows, expected);
}

TEST_F(ListIndexTest, carriesAnElementThroughACrossProduct) {
    const std::vector<std::optional<types::Int64::Primitive>> rows =
        evalElements("UNWIND [0, 2] AS i WITH [10, null, 30][i] AS e "
                     "CALL db.getNodes([0, 1]) YIELD id RETURN e");

    std::vector<std::optional<types::Int64::Primitive>> sorted = rows;
    std::sort(sorted.begin(), sorted.end());

    const std::vector<std::optional<types::Int64::Primitive>> expected = {10, 10, 30, 30};
    EXPECT_EQ(sorted, expected);
}

TEST_F(ListIndexTest, readsAnElementOfAnUnwoundList) {
    const std::vector<std::optional<types::Int64::Primitive>> first =
        evalElements("UNWIND [[1, 2], [3, 4]] AS xs RETURN xs[0]");

    const std::vector<std::optional<types::Int64::Primitive>> expectedFirst = {1, 3};
    EXPECT_EQ(first, expectedFirst);

    const std::vector<std::optional<types::Int64::Primitive>> second =
        evalElements("UNWIND [[1, 2], [3, 4]] AS xs RETURN xs[1]");

    const std::vector<std::optional<types::Int64::Primitive>> expectedSecond = {2, 4};
    EXPECT_EQ(second, expectedSecond);
}

TEST_F(ListIndexTest, readsAnElementOfAnIndexedList) {
    EXPECT_EQ(evalElement("MATCH (n) WHERE n.name = 'Remy' RETURN [[1, 2], [3, 4]][0][1]"), 2);
    EXPECT_EQ(evalElement("MATCH (n) WHERE n.name = 'Remy' RETURN [[1, 2], [3, 4]][1][0]"), 3);
}

TEST_F(ListIndexTest, readsNullWhereTheCellIsNotAList) {
    const std::vector<std::optional<types::Int64::Primitive>> rows =
        evalElements("UNWIND [1, [2, 3]] AS xs RETURN xs[0]");

    const std::vector<std::optional<types::Int64::Primitive>> expected = {std::nullopt, 2};
    EXPECT_EQ(rows, expected);
}

TEST_F(ListIndexTest, readsNullPastTheEndOfAnIndexedList) {
    EXPECT_EQ(evalElement("MATCH (n) WHERE n.name = 'Remy' RETURN [[1, 2], [3, 4]][0][9]"), std::nullopt);
    EXPECT_EQ(evalElement("MATCH (n) WHERE n.name = 'Remy' RETURN [[1, 2], [3, 4]][9][0]"), std::nullopt);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
