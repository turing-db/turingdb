#include <gtest/gtest.h>

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
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "columns/ColumnMask.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "iterators/ChunkConfig.h"
#include "list/ListElementView.h"
#include "list/ListView.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringException.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using Integers = std::vector<int64_t>;
using CountPair = std::pair<uint64_t, uint64_t>;
using CountPairs = std::vector<CountPair>;
using OptionalIntegers = std::vector<std::optional<int64_t>>;
using OptionalDoubles = std::vector<std::optional<double>>;
using Doubles = std::vector<double>;
using Flags = std::vector<bool>;
using NameCountRow = std::pair<std::optional<std::string>, uint64_t>;
using NameCountRows = std::vector<NameCountRow>;
using NameSumRow = std::pair<std::optional<std::string>, std::optional<int64_t>>;
using NameSumRows = std::vector<NameSumRow>;

// Collects the single integer column a count, or an arithmetic expression over one,
// emits. Cypher has one integer type and it is signed, so the sink also records which
// of the two integer columns it read: an expression that runs below zero only has the
// value the language gives it if the column carrying it is signed.
class ScalarIntegerSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* signedValues = dynamic_cast<const ColumnVector<int64_t>*>(chunks[0]);
        const auto* unsignedValues = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[0]);
        ASSERT_TRUE(signedValues || unsignedValues);

        _signedColumn = signedValues != nullptr;

        if (signedValues) {
            const std::vector<int64_t>& raw = signedValues->getRaw();
            for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
                _values.push_back(raw[rowIndex]);
            }
        } else {
            const std::vector<uint64_t>& raw = unsignedValues->getRaw();
            for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
                _values.push_back(static_cast<int64_t>(raw[rowIndex]));
            }
        }
    }

    const Integers& values() const { return _values; }

    bool isSignedColumn() const { return _signedColumn; }

private:
    Integers _values;
    bool _signedColumn {false};
};

// Takes whatever shape a query emits and keeps none of it, for a test whose subject is
// how the engine fails rather than what it returns.
class DiscardingSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
    }
};

// Collects the two count columns a projection of two counts emits, so a test can show
// that each count keeps its own tally.
class ScalarCountPairSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* firsts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[0]);
        const auto* seconds = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[1]);
        ASSERT_NE(firsts, nullptr);
        ASSERT_NE(seconds, nullptr);

        const std::vector<uint64_t>& firstRaw = firsts->getRaw();
        const std::vector<uint64_t>& secondRaw = seconds->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back(firstRaw[rowIndex], secondRaw[rowIndex]);
        }
    }

    const CountPairs& rows() const { return _rows; }

private:
    CountPairs _rows;
};

// Collects the (name, value) rows a grouped expression over a count emits. An expression
// over a tally is a signed integer - the language has no unsigned one - and it is present
// in every group, so the value column is a non-nullable i64 rather than the ui64 the
// tally itself comes out as.
class GroupedIntegerSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* names = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[0]);
        const auto* values = dynamic_cast<const ColumnVector<int64_t>*>(chunks[1]);
        ASSERT_NE(names, nullptr);
        ASSERT_NE(values, nullptr);

        const std::vector<std::optional<std::string_view>>& nameRaw = names->getRaw();
        const std::vector<int64_t>& valueRaw = values->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            std::optional<std::string> name;
            if (nameRaw[rowIndex]) {
                name = std::string(*nameRaw[rowIndex]);
            }

            _rows.emplace_back(name, valueRaw[rowIndex]);
        }
    }

    void sortedRows(NameSumRows& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    NameSumRows _rows;
};

// Collects the double column a count divided by a floating point literal emits: the
// division promotes to a double which is present in every row, so the column is not
// nullable.
class ScalarDoubleSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* values = dynamic_cast<const ColumnVector<double>*>(chunks[0]);
        ASSERT_NE(values, nullptr);

        const std::vector<double>& raw = values->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _values.push_back(raw[rowIndex]);
        }
    }

    const Doubles& values() const { return _values; }

private:
    Doubles _values;
};

// Collects the mask a comparison over a count emits.
class ScalarMaskSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* mask = dynamic_cast<const ColumnMask*>(chunks[0]);
        ASSERT_NE(mask, nullptr);

        const std::vector<ColumnMask::Bool_t>& raw = mask->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _values.push_back(static_cast<bool>(raw[rowIndex]));
        }
    }

    const Flags& values() const { return _values; }

private:
    Flags _values;
};

// Collects the nullable integer column a sum, a min or a max emits: a value reduction
// over no value at all has no value to report, so its result column is nullable.
class ScalarOptionalIntegerSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* values = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[0]);
        ASSERT_NE(values, nullptr);

        const std::vector<std::optional<int64_t>>& raw = values->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _values.push_back(raw[rowIndex]);
        }
    }

    const OptionalIntegers& values() const { return _values; }

private:
    OptionalIntegers _values;
};

// The floating point sibling of ScalarOptionalIntegerSink, for an average.
class ScalarOptionalDoubleSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* values = dynamic_cast<const ColumnOptVector<double>*>(chunks[0]);
        ASSERT_NE(values, nullptr);

        const std::vector<std::optional<double>>& raw = values->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _values.push_back(raw[rowIndex]);
        }
    }

    const OptionalDoubles& values() const { return _values; }

private:
    OptionalDoubles _values;
};

// Collects the single list cell a scalar collect emits, reading its elements as
// integers.
class ScalarIntegerListSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* lists = dynamic_cast<const ColumnVector<ListView>*>(chunks[0]);
        ASSERT_NE(lists, nullptr);

        const std::vector<ListView>& raw = lists->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            for (const ListElementView& element : raw[rowIndex]) {
                _elements.push_back(element.getAs<int64_t>());
            }

            _rowCount++;
        }
    }

    void sortedElements(Integers& elements) const {
        elements = _elements;
        std::sort(elements.begin(), elements.end());
    }

    size_t getRowCount() const { return _rowCount; }

private:
    Integers _elements;
    size_t _rowCount {0};
};

// Collects the (name, count) rows a grouped count emits. A grouped aggregate emits its
// groups in first-seen order, which the language does not promise, so the tests compare
// sorted row sets.
class GroupedCountSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* names = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[0]);
        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks[1]);
        ASSERT_NE(names, nullptr);
        ASSERT_NE(counts, nullptr);

        const std::vector<std::optional<std::string_view>>& nameRaw = names->getRaw();
        const std::vector<uint64_t>& countRaw = counts->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            std::optional<std::string> name;
            if (nameRaw[rowIndex]) {
                name = std::string(*nameRaw[rowIndex]);
            }

            _rows.emplace_back(name, countRaw[rowIndex]);
        }
    }

    void sortedRows(NameCountRows& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    NameCountRows _rows;
};

// The value-reduction sibling of GroupedCountSink: a nullable string key beside the
// nullable integer a grouped sum, min or max reduces to.
class GroupedSumSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 2u);

        const auto* names = dynamic_cast<const ColumnOptVector<std::string_view>*>(chunks[0]);
        const auto* sums = dynamic_cast<const ColumnOptVector<int64_t>*>(chunks[1]);
        ASSERT_NE(names, nullptr);
        ASSERT_NE(sums, nullptr);

        const std::vector<std::optional<std::string_view>>& nameRaw = names->getRaw();
        const std::vector<std::optional<int64_t>>& sumRaw = sums->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            std::optional<std::string> name;
            if (nameRaw[rowIndex]) {
                name = std::string(*nameRaw[rowIndex]);
            }

            _rows.emplace_back(name, sumRaw[rowIndex]);
        }
    }

    void sortedRows(NameSumRows& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    NameSumRows _rows;
};

}

// What sits inside a DISTINCT aggregate and what the aggregate feeds into, end to end
// through the MLIR frontend: each query is parsed, analyzed, generated into the db
// dialect by DBProgramGenerator, then lowered and interpreted against the shared
// simpledb fixture.
class CountDistinctArgumentTest : public TuringTest {
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
        mlir::MLIRContext context;
        mlir::OwningOpRef<mlir::ModuleOp> module;
        generateProgram(query, context, module);

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        LocalMemory memory;
        DBDialectInterpreter interpreter(module.get(), &view, sink, &memory, ChunkConfig::CHUNK_SIZE);
        interpreter.run();
    }

    void expectScalarInteger(std::string_view query, int64_t expected) {
        ScalarIntegerSink sink;
        runQuery(query, &sink);

        EXPECT_EQ(sink.values(), (Integers {expected})) << "query: " << query;
    }

    // A value the language reads as negative only exists in a signed column, so the
    // signedness is asserted beside the value rather than left to the reader
    void expectSignedInteger(std::string_view query, int64_t expected) {
        ScalarIntegerSink sink;
        runQuery(query, &sink);

        EXPECT_TRUE(sink.isSignedColumn()) << "query: " << query;
        EXPECT_EQ(sink.values(), (Integers {expected})) << "query: " << query;
    }

    void expectScalarDouble(std::string_view query, double expected) {
        ScalarDoubleSink sink;
        runQuery(query, &sink);

        EXPECT_EQ(sink.values(), (Doubles {expected})) << "query: " << query;
    }

    void expectScalarFlag(std::string_view query, bool expected) {
        ScalarMaskSink sink;
        runQuery(query, &sink);

        EXPECT_EQ(sink.values(), (Flags {expected})) << "query: " << query;
    }

    void expectScalarOptionalInteger(std::string_view query, int64_t expected) {
        ScalarOptionalIntegerSink sink;
        runQuery(query, &sink);

        EXPECT_EQ(sink.values(), (OptionalIntegers {expected})) << "query: " << query;
    }

    void expectScalarOptionalDouble(std::string_view query, double expected) {
        ScalarOptionalDoubleSink sink;
        runQuery(query, &sink);

        EXPECT_EQ(sink.values(), (OptionalDoubles {expected})) << "query: " << query;
    }

    void expectGroupedCounts(std::string_view query, const NameCountRows& expected) {
        GroupedCountSink sink;
        runQuery(query, &sink);

        NameCountRows rows;
        sink.sortedRows(rows);

        NameCountRows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(rows, sortedExpected) << "query: " << query;
    }

    void expectGroupedIntegers(std::string_view query, const NameSumRows& expected) {
        GroupedIntegerSink sink;
        runQuery(query, &sink);

        NameSumRows rows;
        sink.sortedRows(rows);

        NameSumRows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(rows, sortedExpected) << "query: " << query;
    }

    void expectGroupedSums(std::string_view query, const NameSumRows& expected) {
        GroupedSumSink sink;
        runQuery(query, &sink);

        NameSumRows rows;
        sink.sortedRows(rows);

        NameSumRows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(rows, sortedExpected) << "query: " << query;
    }

    // A query the engine turns away must say what is wrong with the query. A tripped
    // internal assertion is not a rejection: it names a file and a line of the engine,
    // which tells the reader nothing about what they wrote.
    void expectNoInternalError(std::string_view query) {
        DiscardingSink sink;
        try {
            runQuery(query, &sink);
        } catch (const TuringException& error) {
            const std::string_view message = error.what();
            EXPECT_EQ(message.find("Internal Error"), std::string_view::npos) << "query: " << query;
        }
    }

    // The db dialect program of a query, so a test can assert on the op the frontend
    // generated rather than only on the rows it produces
    std::string generatedProgram(std::string_view query) {
        mlir::MLIRContext context;
        mlir::OwningOpRef<mlir::ModuleOp> module;
        generateProgram(query, context, module);

        std::string printed;
        llvm::raw_string_ostream stream(printed);
        module.get().print(stream);

        return printed;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
};

// The eighteen out-edges leave only nine distinct sources, while every edge is its own
// entity, so the DISTINCT collapses the source rows and leaves the edge rows alone.
TEST_F(CountDistinctArgumentTest, countsDistinctSourcesAndEdgesOfAHop) {
    expectScalarInteger("MATCH (a)-->(b) RETURN count(a)", 18);
    expectScalarInteger("MATCH (a)-->(b) RETURN count(DISTINCT a)", 9);
    expectScalarInteger("MATCH (a)-[e]->(b) RETURN count(DISTINCT e)", 18);
}

// Eight of the eighteen edges carry a duration, and those eight hold only four values
// (20, 200, 15 and 10); the two proficiencies present are expert and moderate.
TEST_F(CountDistinctArgumentTest, countsDistinctEdgePropertyValues) {
    expectScalarInteger("MATCH (a)-[e]->(b) RETURN count(e.duration)", 8);
    expectScalarInteger("MATCH (a)-[e]->(b) RETURN count(DISTINCT e.duration)", 4);
    expectScalarInteger("MATCH (a)-[e]->(b) RETURN count(DISTINCT e.proficiency)", 2);
}

// A boolean property takes at most two values, so the distinct count of one is bounded
// by two however many rows carry it: the eight people each have an isFrench and a
// hasPhD, and both flags are taken in both directions.
TEST_F(CountDistinctArgumentTest, countsDistinctBooleanPropertyValues) {
    expectScalarInteger("MATCH (a) RETURN count(a.isFrench)", 8);
    expectScalarInteger("MATCH (a) RETURN count(DISTINCT a.isFrench)", 2);
    expectScalarInteger("MATCH (a) RETURN count(DISTINCT a.hasPhD)", 2);
}

// A string literal is the same value in every row, so the eighteen nodes charge the
// plain count eighteen times and the distinct count once.
TEST_F(CountDistinctArgumentTest, countsOneDistinctValueOfAStringLiteral) {
    expectScalarInteger("MATCH (a) RETURN count('hello')", 18);
    expectScalarInteger("MATCH (a) RETURN count(DISTINCT 'hello')", 1);
}

// Arithmetic in the argument is charged on its result, not on its operands: only Remy
// and Adam carry an age and both are 32, so the two rows the addition has a value in
// hold the one value 33.
TEST_F(CountDistinctArgumentTest, countsDistinctValuesOfArithmeticOnAProperty) {
    expectScalarInteger("MATCH (a) RETURN count(a.age + 1)", 2);
    expectScalarInteger("MATCH (a) RETURN count(DISTINCT a.age + 1)", 1);
}

TEST_F(CountDistinctArgumentTest, namesTheDistinctCountThroughItsAlias) {
    expectScalarInteger("MATCH (a)-->(b) RETURN count(DISTINCT b) AS distinctTargets", 12);

    const std::string program = generatedProgram("MATCH (a)-->(b) RETURN count(DISTINCT b) AS distinctTargets");
    EXPECT_NE(program.find("names [\"distinctTargets\"]"), std::string::npos);
}

// Two distinct counts over two different columns of the same rows: nine sources reach
// twelve targets, so each count must build its own seen-set.
TEST_F(CountDistinctArgumentTest, countsTwoDistinctColumnsInOneProjection) {
    ScalarCountPairSink sink;
    runQuery("MATCH (a)-->(b) RETURN count(DISTINCT a), count(DISTINCT b)", &sink);

    EXPECT_EQ(sink.rows(), (CountPairs {{9, 12}}));
}

// A distinct count beside the plain count of another column: the DISTINCT belongs to
// the aggregate that carries it and must not reach the one beside it.
TEST_F(CountDistinctArgumentTest, countsADistinctColumnBesideThePlainCountOfAnother) {
    ScalarCountPairSink sink;
    runQuery("MATCH (a)-[e]->(b) RETURN count(DISTINCT a), count(b)", &sink);

    EXPECT_EQ(sink.rows(), (CountPairs {{9, 18}}));
}

// The same distinct count twice, told apart by its aliases: both tally the same twelve
// targets, so the second must not be charged against the first's seen-set.
TEST_F(CountDistinctArgumentTest, countsTheSameColumnTwiceThroughTwoAliases) {
    ScalarCountPairSink sink;
    runQuery("MATCH (a)-->(b) RETURN count(DISTINCT b) AS first, count(DISTINCT b) AS second", &sink);

    EXPECT_EQ(sink.rows(), (CountPairs {{12, 12}}));
}

// The twelve distinct targets feeding arithmetic: the aggregate is a value the
// projection computes with, exactly as a plain count is.
TEST_F(CountDistinctArgumentTest, addsMultipliesAndTakesTheRemainderOfADistinctCount) {
    expectScalarInteger("MATCH (a)-->(b) RETURN count(DISTINCT b) + 1", 13);
    expectScalarInteger("MATCH (a)-->(b) RETURN count(DISTINCT b) * 2", 24);
    expectScalarInteger("MATCH (a)-->(b) RETURN count(DISTINCT b) % 5", 2);
}

// Two aggregates as the two operands of one addition: nine sources plus twelve targets,
// and twelve distinct targets plus the eighteen rows they came from.
TEST_F(CountDistinctArgumentTest, addsTwoCountsTogether) {
    expectScalarInteger("MATCH (a)-->(b) RETURN count(DISTINCT a) + count(DISTINCT b)", 21);
    expectScalarInteger("MATCH (a)-->(b) RETURN count(DISTINCT b) + count(b)", 30);
}

TEST_F(CountDistinctArgumentTest, dividesADistinctCountByADouble) {
    expectScalarDouble("MATCH (a)-->(b) RETURN count(DISTINCT b) / 2.0", 6.0);
}

TEST_F(CountDistinctArgumentTest, comparesADistinctCountToALiteral) {
    expectScalarFlag("MATCH (a)-->(b) RETURN count(DISTINCT b) > 1", true);
    expectScalarFlag("MATCH (a)-->(b) RETURN count(DISTINCT b) > 20", false);
}

// The DISTINCT argument is an edge variable under a grouping key: every edge is its own
// entity, so each group's distinct count is its out-degree.
TEST_F(CountDistinctArgumentTest, countsDistinctEdgesPerGroup) {
    const NameCountRows expected {
        {"Adam", 3}, {"Cyrus", 2}, {"Doruk", 1}, {"Ghosts", 1}, {"Luc", 2},
        {"Martina", 1}, {"Maxime", 2}, {"Remy", 4}, {"Suhas", 2},
    };

    expectGroupedCounts("MATCH (a)-[e]->(b) RETURN a.name, count(DISTINCT e)", expected);
}

// openCypher forbids DISTINCT with the wildcard: count(*) tallies rows, which have no
// value to reduce to a distinct set. The plain count(*) is asserted beside it, so the
// rejection is pinned to the pairing rather than to a wildcard the engine cannot count
// or to counts having broken altogether.
TEST_F(CountDistinctArgumentTest, rejectsADistinctCountOverTheWildcard) {
    expectScalarInteger("MATCH (a) RETURN count(*)", 18);

    DiscardingSink sink;
    EXPECT_THROW(runQuery("MATCH (a) RETURN count(DISTINCT *)", &sink), TuringException);
    EXPECT_THROW(runQuery("MATCH (a)-->(b) RETURN a.name, count(DISTINCT *)", &sink), TuringException);
}

// count takes exactly one argument, so neither none nor two of them is a query the
// engine can be asked to run.
TEST_F(CountDistinctArgumentTest, rejectsADistinctCountWithTheWrongArgumentCount) {
    DiscardingSink sink;
    EXPECT_THROW(runQuery("MATCH (a) RETURN count(DISTINCT)", &sink), TuringException);
    EXPECT_THROW(runQuery("MATCH (a)-->(b) RETURN count(DISTINCT a, b)", &sink), TuringException);
}

// An aggregate has no value until its group is complete, so it cannot be the argument of
// another aggregate.
TEST_F(CountDistinctArgumentTest, rejectsADistinctCountOfANestedAggregate) {
    DiscardingSink sink;
    EXPECT_THROW(runQuery("MATCH (a)-->(b) RETURN count(DISTINCT count(b))", &sink), TuringException);
}

// Twelve distinct targets less the eighteen rows they came from is minus six. Cypher has
// one integer type and it is signed, so a count is an ordinary signed integer once it
// takes part in arithmetic.
TEST_F(CountDistinctArgumentTest, subtractsAPlainCountFromADistinctCount) {
    expectSignedInteger("MATCH (a)-->(b) RETURN count(DISTINCT b) - count(b)", -6);
    expectSignedInteger("MATCH (a)-->(b) RETURN count(DISTINCT b) - 20", -8);
}

TEST_F(CountDistinctArgumentTest, negatesADistinctCount) {
    expectSignedInteger("MATCH (a)-->(b) RETURN -count(DISTINCT b)", -12);
}

// The same arithmetic over the same aggregate, one grouping key up: the aggregate is a
// value per group, so a projection may compute with it exactly as the scalar form does.
TEST_F(CountDistinctArgumentTest, computesWithADistinctCountUnderAGroupingKey) {
    const NameSumRows incremented {
        {"Adam", 4}, {"Cyrus", 3}, {"Doruk", 2}, {"Ghosts", 2}, {"Luc", 3},
        {"Martina", 2}, {"Maxime", 3}, {"Remy", 5}, {"Suhas", 3},
    };

    expectGroupedIntegers("MATCH (a)-->(b) RETURN a.name, count(DISTINCT b) + 1", incremented);

    const NameSumRows doubled {
        {"Adam", 6}, {"Cyrus", 4}, {"Doruk", 2}, {"Ghosts", 2}, {"Luc", 4},
        {"Martina", 2}, {"Maxime", 4}, {"Remy", 8}, {"Suhas", 4},
    };

    expectGroupedIntegers("MATCH (a)-->(b) RETURN a.name, count(DISTINCT b) * 2", doubled);
}

// DISTINCT is a modifier of any aggregate, not of count alone: openCypher, Neo4j and SQL
// all take it on a value reduction, where it reduces the set of values rather than the
// bag. The eight durations present hold the four values 20, 200, 15 and 10.
TEST_F(CountDistinctArgumentTest, sumsDistinctValues) {
    expectScalarOptionalInteger("MATCH (a)-[e]->(b) RETURN sum(e.duration)", 325);
    expectScalarOptionalInteger("MATCH (a)-[e]->(b) RETURN sum(DISTINCT e.duration)", 245);
}

TEST_F(CountDistinctArgumentTest, averagesDistinctValues) {
    expectScalarOptionalDouble("MATCH (a)-[e]->(b) RETURN avg(e.duration)", 40.625);
    expectScalarOptionalDouble("MATCH (a)-[e]->(b) RETURN avg(DISTINCT e.duration)", 61.25);
}

// min and max read the same extremes off a set as off the bag it came from, so DISTINCT
// leaves their value alone - but it is still theirs to take.
TEST_F(CountDistinctArgumentTest, takesTheExtremesOfDistinctValues) {
    expectScalarOptionalInteger("MATCH (a)-[e]->(b) RETURN min(DISTINCT e.duration)", 10);
    expectScalarOptionalInteger("MATCH (a)-[e]->(b) RETURN max(DISTINCT e.duration)", 200);
}

TEST_F(CountDistinctArgumentTest, collectsDistinctValues) {
    ScalarIntegerListSink sink;
    runQuery("MATCH (a)-[e]->(b) RETURN collect(DISTINCT e.duration)", &sink);

    Integers elements;
    sink.sortedElements(elements);

    EXPECT_EQ(sink.getRowCount(), 1u);
    EXPECT_EQ(elements, (Integers {10, 15, 20, 200}));
}

// Remy's four out-edges carry the one duration 20 three times, so the distinct sum of
// his group is 20 where the plain sum is 60; a source whose edges carry no duration sums
// to zero, the additive identity.
TEST_F(CountDistinctArgumentTest, sumsDistinctValuesPerGroup) {
    const NameSumRows expected {
        {"Adam", 20}, {"Cyrus", 0}, {"Doruk", 0}, {"Ghosts", 200}, {"Luc", 35},
        {"Martina", 10}, {"Maxime", 0}, {"Remy", 20}, {"Suhas", 0},
    };

    expectGroupedSums("MATCH (a)-[e]->(b) RETURN a.name, sum(DISTINCT e.duration)", expected);
}

// Reading an aggregate's alias back in a later item of the same projection: whether the
// engine computes it or turns it away, what it must not do is trip an internal assertion
// and report a file and a line of its own source.
TEST_F(CountDistinctArgumentTest, doesNotFailInternallyOnADistinctCountAliasReadBack) {
    expectNoInternalError("MATCH (a)-->(b) RETURN count(DISTINCT b) AS distinctTargets, distinctTargets + 1");
}
