#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace db;
using namespace turing::test;

using Rows = std::vector<StringRowSink::Row>;

// The pairs of a clause the pattern proves distinct lose their check before any pass fuses
// the hops: two edges typed apart are two edges, so are two whose coincidence needs a node
// no label set of the graph allows, two whose coincidence closes the pattern into a cycle
// no arc of the graph's schema closes, and two whose coincidence closes it into a directed
// cycle over types the data holds no cycle of. The rows are the same with the check and
// without, which the split-clause form, out of the rule's reach, pins.
class ProveDistinctEdgesTest : public CallV3Test {
protected:
    void explain(std::string_view query, std::string& pairs, std::string& program) {
        StringRowSink sink;
        runQuery(std::string("EXPLAIN (edges, db) ") + std::string(query), sink);

        for (const StringRowSink::Row& row : sink.getRows()) {
            if (row.front() == "edges") {
                pairs = row.back();
            } else if (row.front() == "db") {
                program = row.back();
            }
        }
    }

    void expectProven(std::string_view query, std::string_view pairs, std::string_view count) {
        std::string reported;
        std::string program;
        explain(query, reported, program);

        EXPECT_EQ(reported, pairs) << query;
        EXPECT_FALSE(contains(program, "db.check_edge_distinct")) << program;
        EXPECT_FALSE(contains(program, "distinct_from")) << program;

        expectCount(query, count);
    }

    void expectKept(std::string_view query, std::string_view pairs, std::string_view count) {
        std::string reported;
        std::string program;
        explain(query, reported, program);

        EXPECT_EQ(reported, pairs) << query;
        EXPECT_TRUE(contains(program, "db.check_edge_distinct") || contains(program, "distinct_from")) << program;

        expectCount(query, count);
    }

    void expectCount(std::string_view query, std::string_view count) {
        StringRowSink sink;
        runQuery(query, sink);
        EXPECT_EQ(sink.getRows(), (Rows {{std::string(count)}})) << query;
    }

    // Luc reports to Suhas, Suhas to Cyrus and Cyrus to Nour, four engineers of one label
    // set, and Cyrus mentors Luc: neither type closes a cycle on its own, the two together
    // close one
    void addReportingLines() {
        runWrite("CREATE (n:Person:SoftwareEngineering {name: 'Nour'})");
        runWrite("MATCH (luc {name: 'Luc'}), (suhas {name: 'Suhas'}), (cyrus {name: 'Cyrus'}), (nour {name: 'Nour'}) "
                 "CREATE (luc)-[:REPORTS_TO]->(suhas), (suhas)-[:REPORTS_TO]->(cyrus), (cyrus)-[:REPORTS_TO]->(nour), (cyrus)-[:MENTORS]->(luc)");
    }

    static bool contains(std::string_view text, std::string_view part) {
        return text.find(part) != std::string_view::npos;
    }

    static size_t countOf(std::string_view text, std::string_view part) {
        size_t count = 0;
        for (size_t at = text.find(part); at != std::string_view::npos; at = text.find(part, at + part.size())) {
            count++;
        }

        return count;
    }
};

TEST_F(ProveDistinctEdgesTest, provesTwoHopsTypedApart) {
    expectProven("MATCH (a)-[e1:KNOWS_WELL]->(b)-[e2:INTERESTED_IN]->(c) RETURN count(*)",
                 "e2 <> e1: proven by types\n",
                 "8");
}

TEST_F(ProveDistinctEdgesTest, keepsTwoHopsOfOneTypeMeetingAtANode) {
    expectKept("MATCH (a)-[e1:KNOWS_WELL]->(b)<-[e2:KNOWS_WELL]-(c) RETURN count(*)", "e2 <> e1: kept\n", "2");
}

TEST_F(ProveDistinctEdgesTest, keepsAnUntypedHopMeetingATypedOne) {
    expectKept("MATCH (a)-[e1]->(b)<-[e2:INTERESTED_IN]-(c) RETURN count(*)", "e2 <> e1: kept\n", "12");
}

TEST_F(ProveDistinctEdgesTest, namesAnonymousEdgesAsTheDependencyGraphDoes) {
    expectProven("MATCH (a)-[:KNOWS_WELL]->(b)-[:INTERESTED_IN]->(c) RETURN count(*)",
                 "v1' <> v0': proven by types\n",
                 "8");
}

TEST_F(ProveDistinctEdgesTest, provesAWalkTypedApartFromTheHopBeforeIt) {
    expectProven("MATCH (a)-[e1:KNOWS_WELL]->(b)-[e:INTERESTED_IN*1..2]->(c) RETURN count(*)",
                 "e <> e1: proven by types\n",
                 "8");
}

TEST_F(ProveDistinctEdgesTest, provesAHopTypedApartFromTheWalkBeforeIt) {
    expectProven("MATCH (a)-[e:KNOWS_WELL*1..2]->(b)-[f:INTERESTED_IN]->(c) RETURN count(*)",
                 "f <> e: proven by types\n",
                 "15");
}

TEST_F(ProveDistinctEdgesTest, provesCommaPatternsTypedApart) {
    expectProven("MATCH (a)-[e1:KNOWS_WELL]->(b), (c)-[e2:INTERESTED_IN]->(d) RETURN count(*)",
                 "e1 <> e2: proven by types\n",
                 "45");
}

// The check over a chain narrows to the pairs the types leave, and the hop whose pairs are
// all proven takes no distinct_from
TEST_F(ProveDistinctEdgesTest, narrowsACheckToThePairsItKeeps) {
    const std::string_view query = "MATCH (a)-[e1:INTERESTED_IN]->(b)<-[e2:INTERESTED_IN]-(c)-[e3:KNOWS_WELL]->(d) RETURN count(*)";

    std::string pairs;
    std::string program;
    explain(query, pairs, program);

    EXPECT_EQ(pairs, "e2 <> e1: kept\ne3 <> e1: proven by types\ne3 <> e2: proven by types\n");
    EXPECT_FALSE(contains(program, "db.check_edge_distinct")) << program;
    EXPECT_TRUE(contains(program, "db.get_in_edges_by_type(")) << program;
    EXPECT_EQ(countOf(program, "distinct_from"), 1u) << program;

    expectCount(query, "3");
}

// Over a cross product the check narrows to the pair the types keep, which folds into the
// product
TEST_F(ProveDistinctEdgesTest, narrowsACheckOverACrossProduct) {
    const std::string_view query = "MATCH (a)-[e1:KNOWS_WELL]->(b)-[e2:INTERESTED_IN]->(c), (d)-[e3:KNOWS_WELL]->(f) RETURN count(*)";

    std::string pairs;
    std::string program;
    explain(query, pairs, program);

    EXPECT_EQ(pairs, "e2 <> e1: proven by types\ne1 <> e3: kept\ne2 <> e3: proven by types\n");
    EXPECT_FALSE(contains(program, "db.check_edge_distinct(")) << program;
    EXPECT_TRUE(contains(program, "db.cross_product")) << program;
    EXPECT_EQ(countOf(program, "distinct_from"), 1u) << program;

    expectCount(query, "16");
}

TEST_F(ProveDistinctEdgesTest, provesAChainThroughANodeNoLabelSetAllows) {
    expectProven("MATCH (a:Person)-[e1]->(b:Interest)-[e2]->(c) RETURN count(*)", "e2 <> e1: proven by labels\n", "1");
}

TEST_F(ProveDistinctEdgesTest, provesAVWhoseTipsNoLabelSetAllows) {
    expectProven("MATCH (a:Person)-->(b)<--(c:Interest) RETURN count(*)", "v1' <> v0': proven by labels\n", "1");
}

TEST_F(ProveDistinctEdgesTest, keepsAVWhoseTipsOneNodeCanBe) {
    expectKept("MATCH (a:Person)-->(b)<--(c:Founder) RETURN count(*)", "v1' <> v0': kept\n", "3");
}

// Read the other way, the undirected edge leaves b as its source and c as its target, and
// nothing keeps a and c apart
TEST_F(ProveDistinctEdgesTest, keepsAnUndirectedHopTheLabelsLeaveAnOrientation) {
    expectKept("MATCH (a:Person)-[e1]->(b:Interest)-[e2]-(c) RETURN count(*)", "e2 <> e1: kept\n", "13");
}

// Two directed hops in a row share an edge only around a self-loop, and a chain of three
// shares its first and third only around a two-cycle; simpledb has no self-loop, and
// Remy and Adam close the one two-cycle of KNOWS_WELL
TEST_F(ProveDistinctEdgesTest, provesTwoHopsOfOneTypeWithoutASelfLoop) {
    expectProven("MATCH (a)-[e1:KNOWS_WELL]->(b)-[e2:KNOWS_WELL]->(c) RETURN count(*)", "e2 <> e1: proven by schema\n", "3");
}

TEST_F(ProveDistinctEdgesTest, provesTwoUntypedDirectedHopsWithoutASelfLoop) {
    expectProven("MATCH (a)-[e1]->(b)-[e2]->(c) RETURN count(*)", "e2 <> e1: proven by schema\n", "12");
}

TEST_F(ProveDistinctEdgesTest, keepsTheEndsOfAChainATwoCycleCloses) {
    const std::string_view query = "MATCH (a)-[e1:KNOWS_WELL]->(b)-[e2:KNOWS_WELL]->(c)-[e3:KNOWS_WELL]->(d) RETURN count(*)";

    std::string pairs;
    std::string program;
    explain(query, pairs, program);

    EXPECT_EQ(pairs, "e2 <> e1: proven by schema\ne3 <> e1: kept\ne3 <> e2: proven by schema\n");
    EXPECT_EQ(countOf(program, "distinct_from"), 1u) << program;

    expectCount(query, "1");
}

// An interest is interested in nothing, so no INTERESTED_IN chain comes back on itself
TEST_F(ProveDistinctEdgesTest, provesTheEndsOfAChainNoTwoCycleCloses) {
    expectProven("MATCH (a)-[e1:INTERESTED_IN]->(b)-[e2:INTERESTED_IN]->(c)-[e3:INTERESTED_IN]->(d) RETURN count(*)",
                 "e2 <> e1: proven by schema\ne3 <> e1: proven by schema\ne3 <> e2: proven by schema\n",
                 "0");
}

// Remy is interested in Ghosts, which knows Remy well: the two-cycle the ends of this
// chain would share is in the schema, so the pair stays
TEST_F(ProveDistinctEdgesTest, keepsTheEndsOfAChainTwoTypesClose) {
    const std::string_view query = "MATCH (a)-[e1:INTERESTED_IN]->(b)-[e2:KNOWS_WELL]->(c)-[e3:INTERESTED_IN]->(d) RETURN count(*)";

    std::string pairs;
    std::string program;
    explain(query, pairs, program);

    EXPECT_EQ(pairs, "e2 <> e1: proven by types\ne3 <> e1: kept\ne3 <> e2: proven by types\n");

    expectCount(query, "2");
}

// The hop closing the triangle lands beside the node it closes on and an equality after
// the check joins the two; the merge reads it, so every pair of the triangle closes on a
// self-loop and none survives
TEST_F(ProveDistinctEdgesTest, provesEveryPairOfAClosedTriangle) {
    expectProven("MATCH (a)-[e1]->(b)-[e2]->(c)-[e3]->(a) RETURN count(*)",
                 "e1 <> e3: proven by schema\ne2 <> e3: proven by schema\ne2 <> e1: proven by schema\n",
                 "0");
}

// A node an earlier query of the change wrote is in the graph's label sets already, so a
// node carrying both labels takes the proof away
TEST_F(ProveDistinctEdgesTest, readsTheLabelSetsAChangeAdded) {
    StringRowSink sink;
    runWritesInOneChange("CREATE (n:Person:Interest {name: 'both'})",
                         "EXPLAIN (edges) MATCH (a:Person)-[e1]->(b:Interest)-[e2]->(c) RETURN count(*)",
                         sink);

    EXPECT_EQ(sink.getRows(), (Rows {{"edges", "e2 <> e1: kept\n"}}));
}

// The self-loop a change has not committed is in no part, so the schema proves nothing
// while the change holds it
TEST_F(ProveDistinctEdgesTest, keepsTheSchemaProofOutOfAChangeWithPendingEdges) {
    StringRowSink sink;
    runWritesInOneChange("CREATE (n:Person {name: 'loop'})-[:KNOWS_WELL]->(n)",
                         "EXPLAIN (edges) MATCH (a)-[e1:KNOWS_WELL]->(b)-[e2:KNOWS_WELL]->(c) RETURN count(*)",
                         sink);

    EXPECT_EQ(sink.getRows(), (Rows {{"edges", "e2 <> e1: kept\n"}}));
}

// Once the self-loop is committed the schema holds it and the proof is gone; the loop
// walked twice is the one row the check then cuts, so the count stays at 3
TEST_F(ProveDistinctEdgesTest, readsTheSelfLoopACommitAdded) {
    runWrite("CREATE (n:Person {name: 'loop'})-[:KNOWS_WELL]->(n)");

    expectKept("MATCH (a)-[e1:KNOWS_WELL]->(b)-[e2:KNOWS_WELL]->(c) RETURN count(*)", "e2 <> e1: kept\n", "3");
}

// The query's own writes can add a label set before its rows are read, so the labels prove
// nothing in a writing query; the types still do
TEST_F(ProveDistinctEdgesTest, keepsTheLabelProofOutOfAWritingQuery) {
    StringRowSink labelled;
    runWrite("EXPLAIN (edges) MATCH (a:Person)-[e1]->(b:Interest)-[e2]->(c) SET c.seen = true", labelled);
    EXPECT_EQ(labelled.getRows(), (Rows {{"edges", "e2 <> e1: kept\n"}}));

    StringRowSink typed;
    runWrite("EXPLAIN (edges) MATCH (a)-[e1:KNOWS_WELL]->(b)-[e2:INTERESTED_IN]->(c) SET c.seen = true", typed);
    EXPECT_EQ(typed.getRows(), (Rows {{"edges", "e2 <> e1: proven by types\n"}}));
}

// The summary holds the arc from the engineers' label set to itself, so it cannot tell the
// chain's ends apart; the sort of REPORTS_TO can, since no one reports to a subordinate
TEST_F(ProveDistinctEdgesTest, provesTheEndsOfAChainOfATypeWithoutACycle) {
    addReportingLines();

    expectProven("MATCH (a)-[e1:REPORTS_TO]->(b)-[e2:REPORTS_TO]->(c)-[e3:REPORTS_TO]->(d) RETURN count(*)",
                 "e2 <> e1: proven by schema\ne3 <> e1: proven by acyclicity\ne3 <> e2: proven by schema\n",
                 "1");
}

TEST_F(ProveDistinctEdgesTest, provesEveryPairOfALongerChainOfIt) {
    addReportingLines();

    expectProven("MATCH (a)-[e1:REPORTS_TO]->(b)-[e2:REPORTS_TO]->(c)-[e3:REPORTS_TO]->(d)-[e4:REPORTS_TO]->(f) RETURN count(*)",
                 "e2 <> e1: proven by schema\n"
                 "e3 <> e1: proven by acyclicity\ne3 <> e2: proven by schema\n"
                 "e4 <> e1: proven by acyclicity\ne4 <> e2: proven by acyclicity\ne4 <> e3: proven by schema\n",
                 "0");
}

// Cyrus mentors Luc, who reports up to Cyrus: the cycle the ends of this chain would share
// runs over both types, and the two together hold one, so the pair stays and the check
// cuts the one row that walks Luc to Suhas twice
TEST_F(ProveDistinctEdgesTest, keepsTheEndsOfAChainTwoAcyclicTypesCloseTogether) {
    addReportingLines();

    const std::string_view query = "MATCH (a)-[e1:REPORTS_TO]->(b)-[e2:REPORTS_TO]->(c)-[e3:MENTORS]->(d)-[e4:REPORTS_TO]->(f) RETURN count(*)";

    std::string pairs;
    std::string program;
    explain(query, pairs, program);

    EXPECT_EQ(pairs, "e2 <> e1: proven by schema\n"
                     "e3 <> e1: proven by types\ne3 <> e2: proven by types\n"
                     "e4 <> e1: kept\ne4 <> e2: kept\ne4 <> e3: proven by types\n");
    EXPECT_EQ(countOf(program, "distinct_from"), 1u) << program;

    expectCount(query, "0");
}

// Untyped, the chain's ends can share an edge of any type, and KNOWS_WELL closes on Remy
// and Adam
TEST_F(ProveDistinctEdgesTest, keepsTheEndsOfAnUntypedChainSomeTypeCloses) {
    expectKept("MATCH (a)-[e1]->(b)-[e2]->(c)-[e3]->(d) RETURN count(*)",
               "e2 <> e1: proven by schema\ne3 <> e1: kept\ne3 <> e2: proven by schema\n",
               "12");
}

// The edge a change has not committed is in no part, so neither the sort nor the summary
// proves anything while the change holds it
TEST_F(ProveDistinctEdgesTest, keepsTheAcyclicityProofOutOfAChangeWithPendingEdges) {
    addReportingLines();

    StringRowSink sink;
    runWritesInOneChange("MATCH (nour {name: 'Nour'}), (luc {name: 'Luc'}) CREATE (nour)-[:REPORTS_TO]->(luc)",
                         "EXPLAIN (edges) MATCH (a)-[e1:REPORTS_TO]->(b)-[e2:REPORTS_TO]->(c)-[e3:REPORTS_TO]->(d) RETURN count(*)",
                         sink);

    EXPECT_EQ(sink.getRows(), (Rows {{"edges", "e2 <> e1: kept\ne3 <> e1: kept\ne3 <> e2: kept\n"}}));
}

// Once Nour reports to Luc the line is a ring: the sort over the new commit's parts finds
// the cycle, the pair stays, and the four ways around the ring are the rows
TEST_F(ProveDistinctEdgesTest, readsTheCycleACommitAdded) {
    addReportingLines();
    runWrite("MATCH (nour {name: 'Nour'}), (luc {name: 'Luc'}) CREATE (nour)-[:REPORTS_TO]->(luc)");

    expectKept("MATCH (a)-[e1:REPORTS_TO]->(b)-[e2:REPORTS_TO]->(c)-[e3:REPORTS_TO]->(d) RETURN count(*)",
               "e2 <> e1: proven by schema\ne3 <> e1: kept\ne3 <> e2: proven by schema\n",
               "4");
}
