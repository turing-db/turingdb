#include <gtest/gtest.h>

#include <stdint.h>

#include <algorithm>
#include <iterator>
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
#include "columns/ColumnVector.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using Integers = std::vector<int64_t>;
using ColumnNames = std::vector<std::string>;

// Collects the column names and the values of the integer column a standalone count, or
// an expression over one, emits - the last column, behind any grouping key - and whether
// that column is signed: a count comes out as a ui64 tally, any arithmetic over it as
// the i64 the language's one integer type is.
class ScalarIntegerSink : public NLOutputSink {
public:
    void setColumnNames(std::span<const std::string_view> names) override {
        _names.assign(names.begin(), names.end());
    }

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_FALSE(chunks.empty());

        const auto* signedValues = dynamic_cast<const ColumnVector<int64_t>*>(chunks.back());
        const auto* unsignedValues = dynamic_cast<const ColumnVector<uint64_t>*>(chunks.back());
        ASSERT_TRUE(signedValues || unsignedValues);

        _signedColumn = signedValues != nullptr;

        if (signedValues) {
            const std::vector<int64_t>& raw = signedValues->getRaw();
            _values.insert(_values.end(), raw.begin() + offset, raw.begin() + offset + rowCount);
        } else {
            const std::vector<uint64_t>& raw = unsignedValues->getRaw();
            std::transform(raw.begin() + offset,
                           raw.begin() + offset + rowCount,
                           std::back_inserter(_values),
                           [](uint64_t value) { return static_cast<int64_t>(value); });
        }
    }

    const ColumnNames& names() const { return _names; }
    const Integers& values() const { return _values; }
    bool isSignedColumn() const { return _signedColumn; }

private:
    ColumnNames _names;
    Integers _values;
    bool _signedColumn {false};
};

}

// The query test suite's count-wildcard-*-empty-input cases on the v3 engine. A RETURN
// with no MATCH in front of it runs over one row of its own, so a count(*) there tallies
// that row rather than answering for an input it never had.
class CountWildcardEmptyInputTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void expectScalar(std::string_view query, const ColumnNames& columnNames, int64_t expected, bool signedColumn) {
        ScalarIntegerSink sink;

        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        EXPECT_EQ(sink.names(), columnNames) << "query: " << query;
        EXPECT_EQ(sink.values(), (Integers {expected})) << "query: " << query;
        EXPECT_EQ(sink.isSignedColumn(), signedColumn) << "query: " << query;
    }

    void expectCount(std::string_view query, const ColumnNames& columnNames, int64_t expected) {
        expectScalar(query, columnNames, expected, false);
    }

    void expectSignedInteger(std::string_view query, const ColumnNames& columnNames, int64_t expected) {
        expectScalar(query, columnNames, expected, true);
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// count-wildcard-empty-input: the tally of the one row a bare RETURN runs over
TEST_F(CountWildcardEmptyInputTest, countsTheOneRowOfABareReturn) {
    expectCount("RETURN COUNT(*)", {"COUNT(*)"}, 1);
}

// The same row under a grouping key: the constant key puts it in one group, whose count
// is that row
TEST_F(CountWildcardEmptyInputTest, countsTheOneRowOfABareReturnUnderAConstantKey) {
    expectCount("RETURN 1 AS x, count(*)", {"x", "count(*)"}, 1);
}

// count-wildcard-divide-limit-empty-input: the division is an expression over the tally,
// so its column is a signed integer, and a LIMIT above the one row leaves it standing
TEST_F(CountWildcardEmptyInputTest, dividesTheCountUnderALimit) {
    expectSignedInteger("RETURN COUNT(*) / 999 LIMIT 2", {"COUNT(*) / 999"}, 0);
}

// count-wildcard-increment-empty-input: every + past the first of each run is a unary
// plus, so the item reads COUNT(*) + 9 + 99
TEST_F(CountWildcardEmptyInputTest, addsThroughRunsOfUnaryPluses) {
    const std::string expression = "++++++++++++++++++++++++++++++++COUNT(*)++++++++9++++++++++++++++++++++++++++++99";
    expectSignedInteger("RETURN " + expression, {expression}, 109);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
