#include <gtest/gtest.h>

#include <stdint.h>

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
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using Constants = std::vector<int64_t>;
using DoubleConstants = std::vector<double>;

// The eighteen nodes of simpledb, which MATCH (n) matches
constexpr size_t nodeCount = 18;

// Collects the one constant column of a projection that names nothing else. The column
// answers the same value at every subscript, so the rows it yields are only the rows the
// sink is handed - which is what these tests are about.
class ConstantSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const ColumnConst<int64_t>* constants = dynamic_cast<const ColumnConst<int64_t>*>(chunks.front());
        ASSERT_NE(constants, nullptr);

        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _constants.push_back((*constants)[rowIndex]);
        }
    }

    const Constants& getConstants() const { return _constants; }

private:
    Constants _constants;
};

// The double sibling of ConstantSink: a power promotes what it computes to f64, so the
// column standing for every row is one of doubles.
class DoubleConstantSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const ColumnConst<double>* constants = dynamic_cast<const ColumnConst<double>*>(chunks.front());
        ASSERT_NE(constants, nullptr);

        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _constants.push_back((*constants)[rowIndex]);
        }
    }

    const DoubleConstants& getConstants() const { return _constants; }

private:
    DoubleConstants _constants;
};

}

// A projection of nothing but a constant has no per-row column to take its row count
// from, so the count has to come from the relation the match left standing: the constant
// stands for each of its rows, one value repeated as many times as the filter kept.
class ConstantOnlyProjectionTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void expectConstants(std::string_view query, const Constants& expected) {
        ConstantSink sink;

        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        EXPECT_EQ(sink.getConstants(), expected) << "query: " << query;
    }

    void expectDoubleConstants(std::string_view query, const DoubleConstants& expected) {
        DoubleConstantSink sink;

        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        const DoubleConstants& actual = sink.getConstants();
        ASSERT_EQ(actual.size(), expected.size()) << "query: " << query;

        for (size_t rowIndex = 0; rowIndex < expected.size(); rowIndex++) {
            EXPECT_DOUBLE_EQ(actual[rowIndex], expected[rowIndex]) << "query: " << query;
        }
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// One node of simpledb is named Remy, so the filter keeps one row and the constant is
// emitted once for it.
TEST_F(ConstantOnlyProjectionTest, emitsTheConstantOncePerMatchedRow) {
    expectConstants("MATCH (n) WHERE n.name = 'Remy' RETURN 5", {5});
}

// No node carries that name, so the filter keeps nothing and the constant is emitted for
// no row at all - a query that matched nothing must not answer with a row of 5.
TEST_F(ConstantOnlyProjectionTest, emitsNothingWhenTheMatchIsEmpty) {
    expectConstants("MATCH (n) WHERE n.name = 'nobody' RETURN 5", {});
}

// An expression over constants alone is a constant column too, so it is sized the same
// way: one row of 42 per matched node.
TEST_F(ConstantOnlyProjectionTest, emitsTheComputedConstantOncePerMatchedRow) {
    expectConstants("MATCH (n) RETURN 40 + 2", Constants(nodeCount, 42));
}

// Every arithmetic operator computes a constant from constants, the ones spelled with a
// symbol of their own included.
TEST_F(ConstantOnlyProjectionTest, emitsAComputedModuloOncePerMatchedRow) {
    expectConstants("MATCH (n) RETURN 40 % 3", Constants(nodeCount, 1));
}

// A power is a constant of the same kind, promoted to a double by the operator rather than
// by what it reads.
TEST_F(ConstantOnlyProjectionTest, emitsAComputedPowerOncePerMatchedRow) {
    expectDoubleConstants("MATCH (n) RETURN 2 ^ 3", DoubleConstants(nodeCount, 8.0));
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
