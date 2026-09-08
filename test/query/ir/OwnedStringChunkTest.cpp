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

// labels() and edgeType() are the two functions whose rows own the characters they hold,
// where a string property column borrows them from the graph. That makes their chunk a
// nullable of owned strings, an element type every step carrying a chunk on has to know:
// a limit, a skip, a grouping key, a cross product.
class OwnedStringChunkTest : public TuringTest {
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

// simpledb holds 18 nodes and 18 edges
TEST_F(OwnedStringChunkTest, limitsAColumnOfLabels) {
    expectRowCount("MATCH (n) RETURN labels(n) LIMIT 3", 3);
}

TEST_F(OwnedStringChunkTest, skipsAColumnOfLabels) {
    expectRowCount("MATCH (n) RETURN labels(n) SKIP 1", 17);
}

TEST_F(OwnedStringChunkTest, skipsAColumnOfEdgeTypes) {
    expectRowCount("MATCH ()-[e]->() RETURN edgeType(e) SKIP 2", 16);
}

TEST_F(OwnedStringChunkTest, dedupsAColumnOfLabels) {
    expectRows("MATCH (p:Person) RETURN DISTINCT labels(p)",
               {{"Person, SoftwareEngineering, Founder"},
                {"Person, Founder, Bioinformatics"},
                {"Person, Bioinformatics"},
                {"Person, SoftwareEngineering"},
                {"Person, Sales"}});
}

// The eight Persons carry five label sets between them: three write software, two do
// bioinformatics, and the two founders and Doruk stand alone
TEST_F(OwnedStringChunkTest, groupsOnAColumnOfLabels) {
    expectRows("MATCH (p:Person) RETURN labels(p), count(*)",
               {{"Person, SoftwareEngineering, Founder", "1"},
                {"Person, Founder, Bioinformatics", "1"},
                {"Person, Bioinformatics", "2"},
                {"Person, SoftwareEngineering", "3"},
                {"Person, Sales", "1"}});
}

// Remy's labels crossed with the ten interests: the one row the WITH published is repeated
// once per interest, carrying its string along
TEST_F(OwnedStringChunkTest, carriesAColumnOfLabelsAcrossACrossProduct) {
    expectRows("MATCH (p:Person {name: 'Remy'}) WITH labels(p) AS tags "
               "MATCH (i:Interest) RETURN tags, i.name",
               {{"Person, SoftwareEngineering, Founder", "Animals"},
                {"Person, SoftwareEngineering, Founder", "Bio"},
                {"Person, SoftwareEngineering, Founder", "Computers"},
                {"Person, SoftwareEngineering, Founder", "Cooking"},
                {"Person, SoftwareEngineering, Founder", "Eighties"},
                {"Person, SoftwareEngineering, Founder", "Ghosts"},
                {"Person, SoftwareEngineering, Founder", "Gym"},
                {"Person, SoftwareEngineering, Founder", "JiuJitsu"},
                {"Person, SoftwareEngineering, Founder", "Padel"},
                {"Person, SoftwareEngineering, Founder", "Travel"}});
}


// An edge the pattern missed is null, so the grouping key it holds is one too: the two
// edges Remy and Adam walk form the named group, the six padded rows the null one
TEST_F(OwnedStringChunkTest, groupsOnTheEdgeTypeOfAnOptionalMatch) {
    expectRows("MATCH (p:Person) OPTIONAL MATCH (p)-[e:KNOWS_WELL]->(f) "
               "RETURN edgeType(e), count(*)",
               {{"KNOWS_WELL", "2"}, {"null", "6"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
