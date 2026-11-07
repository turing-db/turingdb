#include <gtest/gtest.h>

#include <stdint.h>

#include <algorithm>
#include <string>
#include <string_view>

#include "IRTestRows.h"
#include "QueryStatus.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A WHERE testing the labels of a node, the type of an edge or a function of a property
// filters the seeds of a walk or a hop before it. The rows have to come out as they did when
// the walk started from every seed, across chunk boundaries.
class PushDownLabelAndTypeTestsCypherTest : public WriteQueryTest {
protected:
    // More pairs than one chunk holds. Pair i holds item 2i, with v = i % 5, and item 2i + 1,
    // a Marked one with the same v. Each item k leads to a chain of three nodes whose i is k,
    // k + LEAF and k + END: item 2i over an R edge with w = i % 3, item 2i + 1 over an S edge
    // with the same w.
    static constexpr int64_t PAIR_COUNT = 65536 + 7;
    static constexpr int64_t LEAF = 1000000;
    static constexpr int64_t END = 2000000;

    void createItems() {
        applyWrite("UNWIND range(0, " + std::to_string(PAIR_COUNT - 1) + ") AS i "
                   "CREATE (a:Item {i: 2 * i, v: i % 5}) "
                   "CREATE (b:Item:Marked {i: 2 * i + 1, v: i % 5}) "
                   "CREATE (a)-[:R {w: i % 3}]->(:Target {i: 2 * i})"
                   "-[:R]->(:Leaf {i: 2 * i + " + std::to_string(LEAF) + "})"
                   "-[:R]->(:End {i: 2 * i + " + std::to_string(END) + "}) "
                   "CREATE (b)-[:S {w: i % 3}]->(:Target {i: 2 * i + 1})"
                   "-[:R]->(:Leaf {i: 2 * i + 1 + " + std::to_string(LEAF) + "})"
                   "-[:R]->(:End {i: 2 * i + 1 + " + std::to_string(END) + "})");
    }

    static void addItemWalk(int64_t item, Rows& rows) {
        const std::string itemText = std::to_string(item);

        rows.push_back({itemText, itemText});
        rows.push_back({itemText, std::to_string(item + LEAF)});
    }

    static void addTargetWalk(int64_t item, Rows& rows) {
        const std::string itemText = std::to_string(item);

        rows.push_back({itemText, std::to_string(item + LEAF)});
        rows.push_back({itemText, std::to_string(item + END)});
    }

    // Prints the first row that differs rather than every row
    void expectManyRows(std::string_view query, Rows expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows rows;
        sink.sortedRows(rows);
        std::sort(expected.begin(), expected.end());

        ASSERT_EQ(rows.size(), expected.size()) << "query: " << query;

        const auto [actualIt, expectedIt] = std::mismatch(rows.begin(), rows.end(), expected.begin());
        if (actualIt == rows.end()) {
            return;
        }

        std::string actualText;
        describeRows(Rows {*actualIt}, actualText);

        std::string expectedText;
        describeRows(Rows {*expectedIt}, expectedText);

        ADD_FAILURE() << "query: " << query << "\nfirst differing row: " << actualText << "expected: " << expectedText;
    }
};

TEST_F(PushDownLabelAndTypeTestsCypherTest, walksFromTheSeedsALabelTestKeepsAcrossChunks) {
    createItems();

    Rows marked;
    Rows markedOrZero;
    Rows unmarkedAndZero;
    for (int64_t pair = 0; pair < PAIR_COUNT; pair++) {
        const bool zero = pair % 5 == 0;

        addItemWalk(2 * pair + 1, marked);
        addItemWalk(2 * pair + 1, markedOrZero);

        if (zero) {
            addItemWalk(2 * pair, markedOrZero);
            addItemWalk(2 * pair, unmarkedAndZero);
        }
    }

    expectManyRows("MATCH (n:Item) WHERE n:Marked MATCH (n)-[*1..2]->(m) RETURN n.i, m.i", marked);
    expectManyRows("MATCH (n:Item) WHERE n:Marked OR n.v = 0 MATCH (n)-[*1..2]->(m) RETURN n.i, m.i", markedOrZero);
    expectManyRows("MATCH (n:Item) WHERE NOT n:Marked AND n.v = 0 MATCH (n)-[*1..2]->(m) RETURN n.i, m.i", unmarkedAndZero);
}

TEST_F(PushDownLabelAndTypeTestsCypherTest, walksFromTheSeedsATypeTestKeepsAcrossChunks) {
    createItems();

    Rows typed;
    Rows typedOrZero;
    for (int64_t pair = 0; pair < PAIR_COUNT; pair++) {
        addTargetWalk(2 * pair + 1, typed);
        addTargetWalk(2 * pair + 1, typedOrZero);

        if (pair % 3 == 0) {
            addTargetWalk(2 * pair, typedOrZero);
        }
    }

    expectManyRows("MATCH (n:Item)-[r]->(t) WHERE r:S MATCH (t)-[*1..2]->(m) RETURN n.i, m.i", typed);
    expectManyRows("MATCH (n:Item)-[r]->(t) WHERE r:S OR r.w = 0 MATCH (t)-[*1..2]->(m) RETURN n.i, m.i", typedOrZero);
    expectManyRows("MATCH (n:Item)-[r]->(t) WHERE type(r) = 'S' OR r.w = 0 MATCH (t)-[*1..2]->(m) RETURN n.i, m.i", typedOrZero);
}

// Remy and Adam are the founders and nobody is older than 40. Remy reaches Adam, Ghosts,
// Computers and Eighties, then Remy again over Adam and over Ghosts, and Bio and Cooking over
// Adam. Adam reaches Remy, Bio and Cooking, then Adam, Ghosts, Computers and Eighties over Remy.
const Rows walksFromTheFounders {
    {"Remy", "Adam"},
    {"Remy", "Ghosts"},
    {"Remy", "Computers"},
    {"Remy", "Eighties"},
    {"Remy", "Remy"},
    {"Remy", "Remy"},
    {"Remy", "Bio"},
    {"Remy", "Cooking"},
    {"Adam", "Remy"},
    {"Adam", "Bio"},
    {"Adam", "Cooking"},
    {"Adam", "Adam"},
    {"Adam", "Ghosts"},
    {"Adam", "Computers"},
    {"Adam", "Eighties"},
};

TEST_F(PushDownLabelAndTypeTestsCypherTest, walksFromTheSeedsALabelTestKeeps) {
    expectRows("MATCH (n:Person) WHERE n:Founder MATCH (n)-[*1..2]->(m) RETURN n.name, m.name",
               walksFromTheFounders);
    expectRows("MATCH (n:Person) WHERE n:Founder OR n.age > 40 MATCH (n)-[*1..2]->(m) RETURN n.name, m.name",
               walksFromTheFounders);
    expectRows("MATCH (n:Person) WHERE 'Founder' IN labels(n) OR n.age > 40 "
               "MATCH (n)-[*1..2]->(m) RETURN n.name, m.name",
               walksFromTheFounders);
}

// Maxime, Martina, Suhas, Cyrus and Doruk have names longer than 4 letters, and none of them
// reaches a node with an edge out of it. Only Remy and Adam have an age, 32.
TEST_F(PushDownLabelAndTypeTestsCypherTest, walksFromTheSeedsAFunctionOfAPropertyKeeps) {
    expectRows("MATCH (n:Person) WHERE toLower(n.name) = 'remy' MATCH (n)-[*1..2]->(m) RETURN n.name, m.name",
               {{"Remy", "Adam"},
                {"Remy", "Ghosts"},
                {"Remy", "Computers"},
                {"Remy", "Eighties"},
                {"Remy", "Remy"},
                {"Remy", "Remy"},
                {"Remy", "Bio"},
                {"Remy", "Cooking"}});
    expectRows("MATCH (n:Person) WHERE size(n.name) > 4 MATCH (n)-[*1..2]->(m) RETURN n.name, m.name",
               {{"Maxime", "Bio"},
                {"Maxime", "Padel"},
                {"Martina", "Cooking"},
                {"Suhas", "Gym"},
                {"Suhas", "JiuJitsu"},
                {"Cyrus", "Gym"},
                {"Cyrus", "Travel"},
                {"Doruk", "Gym"}});
    expectRows("MATCH (n:Person) WHERE toInteger(n.age) > 3 OR n:Founder MATCH (n)-[*1..2]->(m) RETURN n.name, m.name",
               walksFromTheFounders);
}

// Luc reaches Animals and Computers, and goes no further
TEST_F(PushDownLabelAndTypeTestsCypherTest, walksFromTheSeedsALabelTestInAnAndKeeps) {
    Rows expected = walksFromTheFounders;
    expected.push_back({"Luc", "Animals"});
    expected.push_back({"Luc", "Computers"});

    expectRows("MATCH (n:Person) WHERE (n:Founder AND n.age > 3) OR n.name = 'Luc' "
               "MATCH (n)-[*1..2]->(m) RETURN n.name, m.name",
               expected);
    expectRows("MATCH (n:Person) WHERE n:SoftwareEngineering AND n.isFrench "
               "MATCH (n)-[*1..2]->(m) WHERE m.name <> 'Adam' OR n.name = 'Luc' RETURN n.name, m.name",
               {{"Remy", "Ghosts"},
                {"Remy", "Computers"},
                {"Remy", "Eighties"},
                {"Remy", "Remy"},
                {"Remy", "Remy"},
                {"Remy", "Bio"},
                {"Remy", "Cooking"},
                {"Luc", "Animals"},
                {"Luc", "Computers"}});
}

// None of the six other people reaches a node with an edge out of it
TEST_F(PushDownLabelAndTypeTestsCypherTest, walksFromTheSeedsANegatedLabelTestKeeps) {
    expectRows("MATCH (n:Person) WHERE NOT n:Founder MATCH (n)-[*1..2]->(m) RETURN n.name, m.name",
               {{"Maxime", "Bio"},
                {"Maxime", "Padel"},
                {"Luc", "Animals"},
                {"Luc", "Computers"},
                {"Martina", "Cooking"},
                {"Suhas", "Gym"},
                {"Suhas", "JiuJitsu"},
                {"Cyrus", "Gym"},
                {"Cyrus", "Travel"},
                {"Doruk", "Gym"}});
}

TEST_F(PushDownLabelAndTypeTestsCypherTest, hopsFromTheSeedsALabelTestKeeps) {
    const Rows hopsFromTheFounders {{"Remy", "Adam"},
                                    {"Remy", "Ghosts"},
                                    {"Remy", "Computers"},
                                    {"Remy", "Eighties"},
                                    {"Adam", "Remy"},
                                    {"Adam", "Bio"},
                                    {"Adam", "Cooking"}};

    expectRows("MATCH (n:Person) WHERE n:Founder OR n.age > 40 MATCH (n)-[]->(m) RETURN n.name, m.name", hopsFromTheFounders);
    expectRows("MATCH (n:Person) WHERE n:Founder AND n.age > 3 MATCH (n)-[]->(m) RETURN n.name, m.name", hopsFromTheFounders);
    expectRows("MATCH (n:Person) WHERE n:Founder OR n.age > 40 MATCH (n)-[:INTERESTED_IN]->(m) RETURN n.name, m.name",
               {{"Remy", "Ghosts"},
                {"Remy", "Computers"},
                {"Remy", "Eighties"},
                {"Adam", "Bio"},
                {"Adam", "Cooking"}});
}

// The edges out of a person that are KNOWS_WELL or last more than 15 are Remy's to Adam,
// Ghosts and Eighties, Adam's to Remy and Luc's to Animals. Eighties and Animals have no
// edge out, and Ghosts has one, to Remy. The two KNOWS_WELL edges are the ones reaching a
// founder.
TEST_F(PushDownLabelAndTypeTestsCypherTest, walksFromTheSeedsATypeTestKeeps) {
    Rows overKnowsWell;
    for (const Row& row : walksFromTheFounders) {
        const std::string start = row.front() == "Remy" ? "Adam" : "Remy";
        overKnowsWell.push_back({start, row.front(), row.back()});
    }

    expectRows("MATCH (a:Person)-[r]->(n) WHERE r:KNOWS_WELL MATCH (n)-[*1..2]->(m) RETURN a.name, n.name, m.name",
               overKnowsWell);
    expectRows("MATCH (a:Person)-[r]->(n) WHERE r:KNOWS_WELL OR n:Founder "
               "MATCH (n)-[*1..2]->(m) RETURN a.name, n.name, m.name",
               overKnowsWell);

    Rows overLongEdges = overKnowsWell;
    overLongEdges.push_back({"Remy", "Ghosts", "Remy"});
    overLongEdges.push_back({"Remy", "Ghosts", "Adam"});
    overLongEdges.push_back({"Remy", "Ghosts", "Ghosts"});
    overLongEdges.push_back({"Remy", "Ghosts", "Computers"});
    overLongEdges.push_back({"Remy", "Ghosts", "Eighties"});

    expectRows("MATCH (a:Person)-[r]->(n) WHERE r:KNOWS_WELL OR r.duration > 15 "
               "MATCH (n)-[*1..2]->(m) RETURN a.name, n.name, m.name",
               overLongEdges);
    expectRows("MATCH (a:Person)-[r]->(n) WHERE type(r) = 'KNOWS_WELL' OR r.duration > 15 "
               "MATCH (n)-[*1..2]->(m) RETURN a.name, n.name, m.name",
               overLongEdges);
    expectRows("MATCH (a:Person)-[r]->(n) WHERE NOT r:KNOWS_WELL "
               "MATCH (n)-[*1..2]->(m) RETURN a.name, n.name, m.name",
               {{"Remy", "Ghosts", "Remy"},
                {"Remy", "Ghosts", "Adam"},
                {"Remy", "Ghosts", "Ghosts"},
                {"Remy", "Ghosts", "Computers"},
                {"Remy", "Ghosts", "Eighties"}});
}

// Only the KNOWS_WELL edges between Remy and Adam pass the test of r. Adam's one edge lasting
// more than 15 leads to Remy, and Remy's lead to Adam, Ghosts and Eighties.
TEST_F(PushDownLabelAndTypeTestsCypherTest, walksAQuantifiedPathFromTheSeedsATypeTestKeeps) {
    const Rows overKnowsWell {{"Remy", "Adam", "Remy"}, {"Adam", "Remy", "Adam"}, {"Adam", "Remy", "Ghosts"}, {"Adam", "Remy", "Eighties"}};

    expectRows("MATCH (a:Person)-[r]->(n) WHERE r:KNOWS_WELL "
               "MATCH (n)(()-[e]->() WHERE e.duration > 15){1}(m) RETURN a.name, n.name, m.name",
               overKnowsWell);
    expectRows("MATCH (a:Person)-[r]->(n) WHERE r:KNOWS_WELL OR r.duration > 15 "
               "MATCH (n)(()-[e]->() WHERE r:KNOWS_WELL AND e.duration > 15){1}(m) RETURN a.name, n.name, m.name",
               overKnowsWell);
}

// The edges out of a person that are KNOWS_WELL or last more than 15 lead to Adam, Remy,
// Ghosts, Eighties and Animals. Adam's edges lead to Remy, Bio and Cooking, Remy's to Adam,
// Ghosts, Computers and Eighties, and Ghosts' to Remy.
TEST_F(PushDownLabelAndTypeTestsCypherTest, hopsFromTheSeedsATypeTestKeeps) {
    expectRows("MATCH (a:Person)-[r]->(n) WHERE r:KNOWS_WELL OR r.duration > 15 "
               "MATCH (n)-[]->(m) RETURN a.name, n.name, m.name",
               {{"Remy", "Adam", "Remy"},
                {"Remy", "Adam", "Bio"},
                {"Remy", "Adam", "Cooking"},
                {"Adam", "Remy", "Adam"},
                {"Adam", "Remy", "Ghosts"},
                {"Adam", "Remy", "Computers"},
                {"Adam", "Remy", "Eighties"},
                {"Remy", "Ghosts", "Remy"}});
}

// Ghosts and Gym are the interests whose name starts with a G. Remy is interested in Ghosts,
// which knows Remy, and Gym has no edge of its own.
TEST_F(PushDownLabelAndTypeTestsCypherTest, hopsByTypeAndLabelInEveryOrder) {
    for (const std::string_view query : {
             "MATCH (a:Person)-[r]->(e)-[]->(x) WHERE r:INTERESTED_IN AND e:Interest AND e.name STARTS WITH 'G' RETURN a.name, x.name",
             "MATCH (a:Person)-[r]->(e)-[]->(x) WHERE r:INTERESTED_IN AND e.name STARTS WITH 'G' AND e:Interest RETURN a.name, x.name",
             "MATCH (a:Person)-[r]->(e)-[]->(x) WHERE e:Interest AND r:INTERESTED_IN AND e.name STARTS WITH 'G' RETURN a.name, x.name",
             "MATCH (a:Person)-[r]->(e)-[]->(x) WHERE e:Interest AND e.name STARTS WITH 'G' AND r:INTERESTED_IN RETURN a.name, x.name",
             "MATCH (a:Person)-[r]->(e)-[]->(x) WHERE e.name STARTS WITH 'G' AND r:INTERESTED_IN AND e:Interest RETURN a.name, x.name",
             "MATCH (a:Person)-[r]->(e)-[]->(x) WHERE e.name STARTS WITH 'G' AND e:Interest AND r:INTERESTED_IN RETURN a.name, x.name"}) {
        expectRows(query, {{"Remy", "Remy"}});
    }
}

// Adam, Maxime and Martina are in Bioinformatics. Remy and Adam know each other well, and
// Ghosts, which knows Remy, is no person.
TEST_F(PushDownLabelAndTypeTestsCypherTest, scansTheEdgesOfTheLabelledNodes) {
    expectRows("MATCH (n:Person)-->(m:Interest) WHERE n:Founder RETURN n.name, m.name",
               {{"Remy", "Ghosts"}, {"Remy", "Computers"}, {"Remy", "Eighties"}, {"Adam", "Bio"}, {"Adam", "Cooking"}});
    expectRows("MATCH (n)-->(m:Interest) WHERE n:Bioinformatics RETURN n.name, m.name",
               {{"Adam", "Bio"}, {"Adam", "Cooking"}, {"Maxime", "Bio"}, {"Maxime", "Padel"}, {"Martina", "Cooking"}});
    expectRows("MATCH (x)-->(y:Person)-->(x:Person) RETURN x.name, y.name", {{"Remy", "Adam"}, {"Adam", "Remy"}});
}

// Remy reaches Adam, Ghosts, Computers and Eighties in one hop, and Remy, Bio and Cooking
// through Adam and Remy again through Ghosts in two
TEST_F(PushDownLabelAndTypeTestsCypherTest, walksFromTheNodeScannedByName) {
    const Rows reached {{"Adam"}, {"Ghosts"}, {"Computers"}, {"Eighties"}, {"Remy"}, {"Bio"}, {"Cooking"}, {"Remy"}};

    expectRows("MATCH (n:Person)-[*1..2]->(m) WHERE n.name = 'Remy' AND n:Founder RETURN m.name", reached);
    expectRows("MATCH (n:Person)-[*1..2]->(m) WHERE n:Founder AND n.name = 'Remy' RETURN m.name", reached);
    expectRows("MATCH (n:Person)-[*1..2]->(m) WHERE n.name = 'Luc' AND n:Founder RETURN m.name", {});
}

// Remy is reached from Remy over Adam and over Ghosts and from Adam, and Adam from Remy and
// from Adam over Remy
TEST_F(PushDownLabelAndTypeTestsCypherTest, testsTheWalkEndBelowTheWalk) {
    expectRows("MATCH (n:Person) MATCH (n)-[*1..2]->(m) WHERE m:Founder OR n.age > 40 RETURN n.name, m.name",
               {{"Remy", "Adam"}, {"Remy", "Remy"}, {"Remy", "Remy"}, {"Adam", "Remy"}, {"Adam", "Adam"}});
}

// The two KNOWS_WELL edges out of a person keep every walk from their ends; Remy's edge to
// Ghosts keeps the walks reaching Remy and Adam
TEST_F(PushDownLabelAndTypeTestsCypherTest, testsTheTypeBesideTheWalkEndBelowTheWalk) {
    Rows expected;
    for (const Row& row : walksFromTheFounders) {
        const std::string start = row.front() == "Remy" ? "Adam" : "Remy";
        expected.push_back({start, row.front(), row.back()});
    }

    expected.push_back({"Remy", "Ghosts", "Remy"});
    expected.push_back({"Remy", "Ghosts", "Adam"});

    expectRows("MATCH (a:Person)-[r]->(n) MATCH (n)-[*1..2]->(m) WHERE r:KNOWS_WELL OR m:Founder "
               "RETURN a.name, n.name, m.name",
               expected);
}

// Remy, the one founder in SoftwareEngineering, reaches Adam, Ghosts and Eighties over edges
// lasting more than 15, then Remy over Adam and over Ghosts
TEST_F(PushDownLabelAndTypeTestsCypherTest, testsEachHopOnTheSeedLabels) {
    expectRows("MATCH (n:Person) WHERE n:Founder OR n.age > 40 "
               "MATCH (n)(()-[e]->() WHERE n:SoftwareEngineering AND e.duration > 15){1,2}(m) RETURN n.name, m.name",
               {{"Remy", "Adam"}, {"Remy", "Ghosts"}, {"Remy", "Eighties"}, {"Remy", "Remy"}, {"Remy", "Remy"}});
}
