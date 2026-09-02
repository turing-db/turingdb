#include <gtest/gtest.h>

#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnMask.h"
#include "metadata/PropertyType.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "TuringException.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using BoolRow = std::vector<bool>;
using BoolRows = std::vector<BoolRow>;

// A predicate over constants alone is the one value it computes, so it reaches the sink as
// a ColumnConst standing for every row, where a predicate evaluated per row comes as a mask.
bool readBool(const Column* column, size_t row) {
    if (const auto* constants = dynamic_cast<const ColumnConst<CustomBool>*>(column)) {
        return static_cast<bool>((*constants)[row]);
    } else if (const auto* mask = dynamic_cast<const ColumnMask*>(column)) {
        return (*mask)[row];
    }

    throw TuringException("ConstantPredicateProjectionTest: unsupported output column type");
}

class CollectingBoolSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            BoolRow& row = _rows.emplace_back();
            for (const Column* column : chunks) {
                row.push_back(readBool(column, rowIndex));
            }
        }
    }

    const BoolRows& getRows() const { return _rows; }

private:
    BoolRows _rows;
};

}

// A projection of a boolean expression over constants alone: the comparison and the
// conjunction are computed once, and the window SKIP 0 LIMIT 1 keeps the single row they
// stand for.
class ConstantPredicateProjectionTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void expectRows(std::string_view query, const BoolRows& expected) {
        CollectingBoolSink sink;

        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        EXPECT_EQ(sink.getRows(), expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(ConstantPredicateProjectionTest, emitsAConstantComparison) {
    const BoolRows expected = {{true}};
    expectRows("RETURN 1 < 2", expected);
}

TEST_F(ConstantPredicateProjectionTest, emitsAConstantConjunction) {
    const BoolRows expected = {{false}};
    expectRows("RETURN true and false", expected);
}

TEST_F(ConstantPredicateProjectionTest, emitsAConstantComparisonUnderAWindow) {
    const BoolRows expected = {{true}};
    expectRows("RETURN 1 < 2 SKIP 0 LIMIT 1", expected);
}

TEST_F(ConstantPredicateProjectionTest, emitsAConstantConjunctionUnderAWindow) {
    const BoolRows expected = {{false}};
    expectRows("RETURN true and false SKIP 0 LIMIT 1", expected);
}

TEST_F(ConstantPredicateProjectionTest, emitsAConstantNegationUnderAWindow) {
    const BoolRows expected = {{false}};
    expectRows("RETURN not true SKIP 0 LIMIT 1", expected);
}

TEST_F(ConstantPredicateProjectionTest, emitsAConstantComparisonUnderALimit) {
    const BoolRows expected = {{true}};
    expectRows("RETURN 1 < 2 LIMIT 1", expected);
}

TEST_F(ConstantPredicateProjectionTest, emitsAConstantComparisonUnderASkip) {
    const BoolRows expected = {{true}};
    expectRows("RETURN 1 < 2 SKIP 0", expected);
}

TEST_F(ConstantPredicateProjectionTest, emitsNothingWhenTheSkipPassesTheOnlyRow) {
    expectRows("RETURN 1 < 2 SKIP 1", {});
}

TEST_F(ConstantPredicateProjectionTest, emitsNothingWhenTheWindowSkipsTheOnlyRow) {
    expectRows("RETURN 1 < 2 SKIP 1 LIMIT 1", {});
}

TEST_F(ConstantPredicateProjectionTest, emitsNothingWhenTheLimitIsZero) {
    expectRows("RETURN true and false SKIP 0 LIMIT 0", {});
}

TEST_F(ConstantPredicateProjectionTest, emitsEveryConstantPredicateColumnOfTheRow) {
    const BoolRows expected = {{true, false}};
    expectRows("RETURN 1 < 2, true and false SKIP 0 LIMIT 1", expected);
}

TEST_F(ConstantPredicateProjectionTest, dedupsAConstantBooleanUnderAWindow) {
    // The dedup caps the projection at one row and the window cuts that row again, so the
    // boolean the LIMIT copies is the one the SKIP emitted.
    const BoolRows expected = {{true}};
    expectRows("RETURN DISTINCT true SKIP 0 LIMIT 1", expected);
}

TEST_F(ConstantPredicateProjectionTest, dedupsAConstantNegationUnderAWindow) {
    const BoolRows expected = {{false}};
    expectRows("RETURN DISTINCT not true SKIP 0 LIMIT 1", expected);
}

// The query test suite's where-not-true-return-not-false case: the constant predicate keeps
// no node, so the projection standing for every row stands for none.
TEST_F(ConstantPredicateProjectionTest, emitsNothingWhenAConstantPredicateExcludesEveryMatchedRow) {
    expectRows("MATCH (n) WHERE NOT TRUE RETURN NOT FALSE", {});
}

// The same projection once the predicate keeps every node of simpledb
TEST_F(ConstantPredicateProjectionTest, emitsTheConstantForEveryMatchedRowWhenTheConstantPredicateKeepsThem) {
    const BoolRows expected(18, {true});
    expectRows("MATCH (n) WHERE NOT FALSE RETURN NOT FALSE", expected);
}

// The other literal under the same two predicates: which constant the projection computes
// leaves untouched how many rows the filter keeps
TEST_F(ConstantPredicateProjectionTest, emitsTheOtherConstantForEveryMatchedRowThePredicateKeeps) {
    const BoolRows expected(18, {false});
    expectRows("MATCH (n) WHERE NOT FALSE RETURN NOT TRUE", expected);
}

TEST_F(ConstantPredicateProjectionTest, emitsNothingWhenTheExcludingPredicateProjectsTheOtherConstant) {
    expectRows("MATCH (n) WHERE NOT TRUE RETURN NOT TRUE", {});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
