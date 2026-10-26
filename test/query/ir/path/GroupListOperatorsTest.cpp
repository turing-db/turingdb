#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

// simpledb's KNOWS_WELL edges are Remy -> Adam, Adam -> Remy and Ghosts -> Remy. A walk of
// one or two of them binds x to the node each hop leaves and y to the node it reaches:
//   from Remy:   x [Remy],   y [Adam], c Adam    x [Remy, Adam],   y [Adam, Remy], c Remy
//   from Adam:   x [Adam],   y [Remy], c Remy    x [Adam, Remy],   y [Remy, Adam], c Adam
//   from Ghosts: x [Ghosts], y [Remy], c Remy    x [Ghosts, Remy], y [Remy, Adam], c Adam
class GroupListOperatorsTest : public CallV3Test {
protected:
    void expectSortedRows(const std::string& query, Rows expected) {
        StringRowSink sink;
        runQuery(query, sink);

        Rows rows = sink.getRows();
        std::sort(rows.begin(), rows.end());
        std::sort(expected.begin(), expected.end());

        EXPECT_EQ(rows, expected) << query;
    }
};

TEST_F(GroupListOperatorsTest, filtersOnAGroupEqualToAList) {
    expectSortedRows("MATCH (n) ((x)-[:KNOWS_WELL]->(y)){1,2} (c) WHERE x = [n] RETURN n.name, c.name",
                     {{"Remy", "Adam"},
                      {"Adam", "Remy"},
                      {"Ghosts", "Remy"}});
}

TEST_F(GroupListOperatorsTest, comparesAGroupWithAList) {
    expectSortedRows("MATCH (n) ((x)-[:KNOWS_WELL]->(y)){1,2} (c) RETURN n.name, size(x), y = [c], [n] <> x",
                     {{"Remy", "1", "true", "false"},
                      {"Remy", "2", "false", "true"},
                      {"Adam", "1", "true", "false"},
                      {"Adam", "2", "false", "true"},
                      {"Ghosts", "1", "true", "false"},
                      {"Ghosts", "2", "false", "true"}});
}

TEST_F(GroupListOperatorsTest, concatenatesTwoGroups) {
    expectSortedRows("MATCH (n) ((x)-[:KNOWS_WELL]->(y)){1,2} (c) RETURN n.name, size(x + y), (x + y)[-1].name",
                     {{"Remy", "2", "Adam"},
                      {"Remy", "4", "Remy"},
                      {"Adam", "2", "Remy"},
                      {"Adam", "4", "Adam"},
                      {"Ghosts", "2", "Remy"},
                      {"Ghosts", "4", "Adam"}});
}

TEST_F(GroupListOperatorsTest, appendsANodeToAGroup) {
    expectSortedRows("MATCH (n) ((x)-[:KNOWS_WELL]->(y)){1,2} (c) RETURN n.name, size(x + c), size([c] + x), (x + c)[-1].name",
                     {{"Remy", "2", "2", "Adam"},
                      {"Remy", "3", "3", "Remy"},
                      {"Adam", "2", "2", "Remy"},
                      {"Adam", "3", "3", "Adam"},
                      {"Ghosts", "2", "2", "Remy"},
                      {"Ghosts", "3", "3", "Adam"}});
}

TEST_F(GroupListOperatorsTest, coalescesAGroup) {
    expectSortedRows("MATCH (n) ((x)-[:KNOWS_WELL]->(y)){1,2} (c) RETURN n.name, size(coalesce(y, [])), coalesce(y, []) = y",
                     {{"Remy", "1", "true"},
                      {"Remy", "2", "true"},
                      {"Adam", "1", "true"},
                      {"Adam", "2", "true"},
                      {"Ghosts", "1", "true"},
                      {"Ghosts", "2", "true"}});
}

TEST_F(GroupListOperatorsTest, reducesAGroup) {
    expectSortedRows("MATCH (n) ((x)-[:KNOWS_WELL]->(y)){1,2} (c) "
                     "RETURN reduce(s = 0, v IN y | s + 1), reduce(s = '', v IN x | s + v.name)",
                     {{"1", "Remy"},
                      {"2", "RemyAdam"},
                      {"1", "Adam"},
                      {"2", "AdamRemy"},
                      {"1", "Ghosts"},
                      {"2", "GhostsRemy"}});
}

TEST_F(GroupListOperatorsTest, slicesAGroup) {
    expectSortedRows("MATCH (n) ((x)-[:KNOWS_WELL]->(y)){1,2} (c) RETURN n.name, size(y[0..1]), y[0..1][0].name, size(y[1..])",
                     {{"Remy", "1", "Adam", "0"},
                      {"Remy", "1", "Adam", "1"},
                      {"Adam", "1", "Remy", "0"},
                      {"Adam", "1", "Remy", "1"},
                      {"Ghosts", "1", "Remy", "0"},
                      {"Ghosts", "1", "Remy", "1"}});
}

TEST_F(GroupListOperatorsTest, findsANodeInAGroup) {
    expectSortedRows("MATCH (n) ((x)-[:KNOWS_WELL]->(y)){1,2} (c) WHERE c IN x RETURN n.name, c.name",
                     {{"Remy", "Remy"},
                      {"Adam", "Adam"}});
}

TEST_F(GroupListOperatorsTest, readsListFunctionsOfAGroup) {
    expectSortedRows("MATCH (n) ((x)-[:KNOWS_WELL]->(y)){1,2} (c) "
                     "RETURN head(x).name, last(y).name, size(tail(x)), reverse(x)[0].name",
                     {{"Remy", "Adam", "0", "Remy"},
                      {"Remy", "Remy", "1", "Adam"},
                      {"Adam", "Remy", "0", "Adam"},
                      {"Adam", "Adam", "1", "Remy"},
                      {"Ghosts", "Remy", "0", "Ghosts"},
                      {"Ghosts", "Adam", "1", "Remy"}});
}

TEST_F(GroupListOperatorsTest, concatenatesGroupsAWithPublishes) {
    expectSortedRows("MATCH (n) ((x)-[:KNOWS_WELL]->(y)){1,2} (c) WITH n, x, y RETURN n.name, size(x + y)",
                     {{"Remy", "2"},
                      {"Remy", "4"},
                      {"Adam", "2"},
                      {"Adam", "4"},
                      {"Ghosts", "2"},
                      {"Ghosts", "4"}});
}

TEST_F(GroupListOperatorsTest, concatenatesARelationshipGroup) {
    expectSortedRows("MATCH (n) ((x)-[r:KNOWS_WELL]->(y)){1,2} (c) RETURN n.name, size(r + r)",
                     {{"Remy", "2"},
                      {"Remy", "4"},
                      {"Adam", "2"},
                      {"Adam", "4"},
                      {"Ghosts", "2"},
                      {"Ghosts", "4"}});
}

TEST_F(GroupListOperatorsTest, concatenatesTheRelationshipsOfAWalk) {
    expectSortedRows("MATCH (n)-[e:KNOWS_WELL*1..1]->(c) RETURN n.name, size(e + e)",
                     {{"Remy", "2"},
                      {"Adam", "2"},
                      {"Ghosts", "2"}});
}

TEST_F(GroupListOperatorsTest, appliesListOperatorsToTheRelationshipsOfAWalk) {
    expectSortedRows("MATCH (n)-[e:KNOWS_WELL*1..2]->(c) "
                     "RETURN n.name, size(e[1..]), reduce(s = 0, v IN e | s + 1), size(tail(e)), startNode(head(e)) = n",
                     {{"Remy", "0", "1", "0", "true"},
                      {"Remy", "1", "2", "1", "true"},
                      {"Adam", "0", "1", "0", "true"},
                      {"Adam", "1", "2", "1", "true"},
                      {"Ghosts", "0", "1", "0", "true"},
                      {"Ghosts", "1", "2", "1", "true"}});
}
