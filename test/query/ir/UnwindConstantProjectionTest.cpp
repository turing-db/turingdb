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

// The eighteen nodes of simpledb, which MATCH (n) matches
constexpr size_t nodeCount = 18;

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

}

// A projection of constants alone sized by an UNWIND. The driving relation is then a
// column of values rather than of node IDs, and only its row count is read - the same
// count nl.broadcast_constant lays a constant out over.
class UnwindConstantProjectionTest : public TuringTest {
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

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// The list opens one row per element, and the constant stands for each of them.
TEST_F(UnwindConstantProjectionTest, emitsTheConstantOncePerUnwoundElement) {
    expectConstants("UNWIND [1, 2, 3] AS x RETURN 5", Constants(3, 5));
}

// A list whose elements disagree on type unwinds into a type-erased column of tagged
// scalars, which drives the projection the same way: what is read of it is its rows.
TEST_F(UnwindConstantProjectionTest, emitsTheConstantOverAHeterogeneousList) {
    expectConstants("UNWIND [true, 'mixed', 10] AS x RETURN 5", Constants(3, 5));
}

// An UNWIND with incoming rows is crossed with them, so the driver is the product: one
// row of the constant per node per element.
TEST_F(UnwindConstantProjectionTest, emitsTheConstantOverAMatchCrossedWithAnUnwind) {
    expectConstants("MATCH (n) UNWIND [1, 2] AS x RETURN 5", Constants(2 * nodeCount, 5));
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
