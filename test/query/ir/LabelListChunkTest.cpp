#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

class LabelListChunkTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    QueryStatus runQuery(std::string_view query, NLOutputSink* sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              sink);

        return status;
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    void expectRowCount(std::string_view query, size_t expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        EXPECT_EQ(sink.rows().size(), expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// simpledb holds 18 nodes
TEST_F(LabelListChunkTest, limitsAColumnOfLabels) {
    expectRowCount("MATCH (n) RETURN labels(n) LIMIT 3", 3);
}

TEST_F(LabelListChunkTest, skipsAColumnOfLabels) {
    expectRowCount("MATCH (n) RETURN labels(n) SKIP 1", 17);
}

TEST_F(LabelListChunkTest, dedupsAColumnOfLabels) {
    expectRows("MATCH (p:Person) RETURN DISTINCT labels(p)",
               {{"[Person, SoftwareEngineering, Founder]"},
                {"[Person, Founder, Bioinformatics]"},
                {"[Person, Bioinformatics]"},
                {"[Person, SoftwareEngineering]"},
                {"[Person, Sales]"}});
}

// The eight Persons carry five label sets between them: three write software, two do
// bioinformatics, and the two founders and Doruk stand alone
TEST_F(LabelListChunkTest, groupsOnAColumnOfLabels) {
    expectRows("MATCH (p:Person) RETURN labels(p), count(*)",
               {{"[Person, SoftwareEngineering, Founder]", "1"},
                {"[Person, Founder, Bioinformatics]", "1"},
                {"[Person, Bioinformatics]", "2"},
                {"[Person, SoftwareEngineering]", "3"},
                {"[Person, Sales]", "1"}});
}

// Remy's labels crossed with the ten interests: the one row the WITH published is repeated
// once per interest, carrying its list along
TEST_F(LabelListChunkTest, carriesAColumnOfLabelsAcrossACrossProduct) {
    expectRows("MATCH (p:Person {name: 'Remy'}) WITH labels(p) AS tags "
               "MATCH (i:Interest) RETURN tags, i.name",
               {{"[Person, SoftwareEngineering, Founder]", "Animals"},
                {"[Person, SoftwareEngineering, Founder]", "Bio"},
                {"[Person, SoftwareEngineering, Founder]", "Computers"},
                {"[Person, SoftwareEngineering, Founder]", "Cooking"},
                {"[Person, SoftwareEngineering, Founder]", "Eighties"},
                {"[Person, SoftwareEngineering, Founder]", "Ghosts"},
                {"[Person, SoftwareEngineering, Founder]", "Gym"},
                {"[Person, SoftwareEngineering, Founder]", "JiuJitsu"},
                {"[Person, SoftwareEngineering, Founder]", "Padel"},
                {"[Person, SoftwareEngineering, Founder]", "Travel"}});
}

// A node the pattern missed holds no list, so the grouping key it carries is a null: the
// two Persons who walk a KNOWS_WELL edge share Remy's and Adam's label sets, the six padded
// rows the null one
TEST_F(LabelListChunkTest, groupsOnTheLabelsOfAnOptionalMatch) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "RETURN labels(f), count(*)",
               {{"[Person, Founder, Bioinformatics]", "1"},
                {"[Person, SoftwareEngineering, Founder]", "1"},
                {"null", "6"}});
}

// The list spreads one row per label, which is what returning a list rather than the names
// joined into one string buys
TEST_F(LabelListChunkTest, unwindsAColumnOfLabels) {
    expectRows("MATCH (p:Person {name: 'Remy'}) UNWIND labels(p) AS label RETURN label",
               {{"Person"}, {"SoftwareEngineering"}, {"Founder"}});
}

// collect() drops the rows the pattern missed, so the six Persons with no KNOWS_WELL edge
// contribute nothing and the two that do contribute their friend's labels
TEST_F(LabelListChunkTest, collectsAColumnOfLabels) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) "
               "RETURN count(labels(f))",
               {{"2"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
