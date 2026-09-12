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
#include "columns/ColumnVector.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

// Collects the single unsigned tally a count emits. A count is a ui64 column whether the
// engine walked the rows or read the graph's node counts, so a rewritten query that came
// back signed would be a rewritten result type too.
class CountSink : public NLOutputSink {
public:
    void setColumnNames(std::span<const std::string_view> names) override {
    }

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* counts = dynamic_cast<const ColumnVector<uint64_t>*>(chunks.front());
        ASSERT_TRUE(counts);

        const std::vector<uint64_t>& raw = counts->getRaw();
        _values.insert(_values.end(), raw.begin() + offset, raw.begin() + offset + rowCount);
    }

    const std::vector<uint64_t>& values() const { return _values; }

private:
    std::vector<uint64_t> _values;
};

}

// The counts the metadata rewrite answers, run end to end over the shared SimpleGraph
// fixture: 8 Person nodes and 10 Interest ones, 18 in all. The products are the row counts
// the cross products they replace would have walked, which is the point - MATCH (a:Person),
// (b:Person), (c:Person) RETURN count(*) is 512 rows the engine no longer builds.
class CountFromMetadataQueryTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void expectCount(std::string_view query, uint64_t expected) {
        CountSink sink;

        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();
        EXPECT_EQ(sink.values(), (std::vector<uint64_t> {expected})) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(CountFromMetadataQueryTest, countsTheNodesCarryingALabel) {
    expectCount("MATCH (a:Person) RETURN count(*)", 8);
    expectCount("MATCH (a:Interest) RETURN count(*)", 10);
}

TEST_F(CountFromMetadataQueryTest, countsEveryNodeOfAnUnlabelledScan) {
    expectCount("MATCH (a) RETURN count(*)", 18);
}

TEST_F(CountFromMetadataQueryTest, countsTheNodesCarryingEveryLabelOfAConjunction) {
    expectCount("MATCH (a:Person:Founder) RETURN count(*)", 2);
    expectCount("MATCH (a:Person:SoftwareEngineering) RETURN count(*)", 4);
}

TEST_F(CountFromMetadataQueryTest, countsTheScannedNodesThemselves) {
    expectCount("MATCH (a:Person) RETURN count(a)", 8);
}

TEST_F(CountFromMetadataQueryTest, countsTheProductOfTwoScans) {
    expectCount("MATCH (a:Person), (b:Person) RETURN count(*)", 64);
    expectCount("MATCH (a:Person), (b:Interest) RETURN count(*)", 80);
    expectCount("MATCH (a:Person), (b) RETURN count(*)", 144);
}

TEST_F(CountFromMetadataQueryTest, countsTheProductOfThreeScans) {
    expectCount("MATCH (a:Person), (b:Person), (c:Person) RETURN count(*)", 512);
    expectCount("MATCH (a:Person), (b:Interest), (c) RETURN count(*)", 1440);
}

// A label the graph never had leaves the conjunction unsatisfiable, so its scan matches no
// node and every product it stands in counts 0.
TEST_F(CountFromMetadataQueryTest, countsNothingForAnAbsentLabel) {
    expectCount("MATCH (a:Ghost) RETURN count(*)", 0);
    expectCount("MATCH (a:Person), (b:Ghost) RETURN count(*)", 0);
    expectCount("MATCH (a:Person:Ghost) RETURN count(*)", 0);
}

// The shapes the rewrite leaves alone still answer as they did: a filtered scan, a budgeted
// prefix, a hop, and a tally that charges each node once.
TEST_F(CountFromMetadataQueryTest, countsTheShapesTheRewriteLeavesAlone) {
    expectCount("MATCH (a:Person) WHERE a.name = 'Remy' RETURN count(*)", 1);
    expectCount("MATCH (a:Person) WITH a LIMIT 3 RETURN count(*)", 3);
    expectCount("MATCH (a:Person)-[:INTERESTED_IN]->(b) RETURN count(*)", 15);
    expectCount("MATCH (a:Person), (b:Interest) RETURN count(DISTINCT a)", 8);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
