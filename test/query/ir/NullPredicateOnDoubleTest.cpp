#include <gtest/gtest.h>

#include <stddef.h>

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "ID.h"
#include "JobSystem.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "metadata/PropertyType.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"
#include "writers/GraphWriter.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

// IS NULL and IS NOT NULL over a Double, whether it comes from a property, an alias or an
// expression computed over one.
//
// Comparing two Doubles for equality stays rejected, on rounding grounds. A null test is
// not such a comparison - there is one value and it either exists or does not - so the two
// rules are independent, and the last case here holds that line.
//
// Two of the four readings carry a `value`, so a null test partitions them.
class NullPredicateOnDoubleTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        buildReadingGraph(system.createGraph(_graphName));
    }

    void expectRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << "query: " << query;
    }

    void expectRejected(std::string_view query, std::string_view reason) {
        StringRowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        EXPECT_EQ(status.getStatus(), QueryStatus::Status::ANALYZE_ERROR) << "query: " << query;

        const std::string error = status.getError();
        EXPECT_NE(error.find(reason), std::string::npos) << "query: " << query << "\nerror: " << error;
    }

    const std::string _graphName = "readings";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;

private:
    void buildReadingGraph(Graph* graph) {
        JobSystem jobSystem;
        jobSystem.init();

        GraphWriter writer(graph, &jobSystem);

        const auto reading = [&](std::string_view name) -> NodeID {
            const NodeID node = writer.addNode({"Reading"});
            writer.addNodeProperty<types::String>(node, "name", types::String::Primitive {name});
            return node;
        };

        writer.addNodeProperty<types::Double>(reading("First"), "value", 1.5);
        writer.addNodeProperty<types::Double>(reading("Second"), "value", -0.25);

        reading("Missing");
        reading("AlsoMissing");

        writer.submit();
        jobSystem.terminate();
    }
};

TEST_F(NullPredicateOnDoubleTest, keepsTheRowsHoldingTheDoubleProperty) {
    const Rows expected {{"First"}, {"Second"}};
    expectRows("MATCH (n:Reading) WHERE n.value IS NOT NULL RETURN n.name", expected);
}

TEST_F(NullPredicateOnDoubleTest, keepsTheRowsMissingTheDoubleProperty) {
    const Rows expected {{"AlsoMissing"}, {"Missing"}};
    expectRows("MATCH (n:Reading) WHERE n.value IS NULL RETURN n.name", expected);
}

TEST_F(NullPredicateOnDoubleTest, testsAComputedDoubleExpressionForNull) {
    const Rows expected {{"First"}, {"Second"}};
    expectRows("MATCH (n:Reading) WITH n, n.value * 2.0 AS scaled "
               "WHERE scaled IS NOT NULL RETURN n.name",
               expected);
}

TEST_F(NullPredicateOnDoubleTest, testsADoubleAliasOfThePropertyForNull) {
    const Rows expected {{"AlsoMissing"}, {"Missing"}};
    expectRows("MATCH (n:Reading) WITH n, n.value AS value "
               "WHERE value IS NULL RETURN n.name",
               expected);
}

// Null-testing a Double does not make comparing two of them acceptable.
TEST_F(NullPredicateOnDoubleTest, stillRejectsTheEqualityOfTwoDoubles) {
    expectRejected("MATCH (n:Reading) WHERE n.value = 1.5 RETURN n.name",
                   "not encouraged due to potential rounding");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
