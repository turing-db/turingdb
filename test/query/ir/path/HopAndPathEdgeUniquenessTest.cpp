#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace db;
using namespace turing::test;

using Rows = std::vector<StringRowSink::Row>;

// Relationship isomorphism between the fixed hops and the variable-length paths of one
// clause, over the shapes PathEdgeUniquenessTest does not reach. Expected values are an
// enumeration of simpledb's 18 edges under openCypher's rule.
class HopAndPathEdgeUniquenessTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getRows(), expected) << query;
    }
};

TEST_F(HopAndPathEdgeUniquenessTest, excludesThePathFromAHopOfAnotherPattern) {
    expectRows("MATCH (a)-[e*1..2]->(b), (c)-[f]->(d) RETURN count(*)", {{"498"}});
    expectRows("MATCH (a)-[e*1..2]-(b), (c)-[f]-(d) RETURN count(*)", {{"3272"}});
}

TEST_F(HopAndPathEdgeUniquenessTest, excludesAHopFromAPathOfItsOwnType) {
    expectRows("MATCH (a)-[e1:KNOWS_WELL]->(b)-[e:KNOWS_WELL*1..2]->(c) RETURN count(*)", {{"4"}});
    expectRows("MATCH (a)-[e:KNOWS_WELL*1..2]->(b)-[f:KNOWS_WELL]->(c) RETURN count(*)", {{"4"}});
    expectRows("MATCH (a)-[f:KNOWS_WELL]-(b)-[e:KNOWS_WELL*1..2]-(c) RETURN count(*)", {{"12"}});
    expectRows("MATCH (a)-[e:KNOWS_WELL*1..2]-(b)-[f:KNOWS_WELL]-(c) RETURN count(*)", {{"12"}});
}

TEST_F(HopAndPathEdgeUniquenessTest, excludesAHopFromAnIncomingPath) {
    expectRows("MATCH (a)-[e1]->(b)<-[e*1..2]-(c) RETURN count(*)", {{"20"}});
    expectRows("MATCH (a)<-[e*1..2]-(b)<-[f]-(c) RETURN count(*)", {{"24"}});
    expectRows("MATCH (a)<-[e*1..2]-(b)-[f]->(c) RETURN count(*)", {{"46"}});
}

TEST_F(HopAndPathEdgeUniquenessTest, excludesTwoHopsAndTwoPathsFromEachOther) {
    expectRows("MATCH (a)-[e1]->(b)-[e*1..2]->(c)-[e2]->(d)-[f*1..2]->(x) RETURN count(*)", {{"22"}});
    expectRows("MATCH (a)-[e*1..2]->(b)-[e1]->(c)-[f*1..2]->(d)-[e2]->(x) RETURN count(*)", {{"22"}});
    expectRows("MATCH (a)-[e1]-(b)-[e*1..2]-(c)-[e2]-(d)-[f*1..2]-(x) RETURN count(*)", {{"656"}});
}

TEST_F(HopAndPathEdgeUniquenessTest, namesNoEdgeTwiceInAPathOfAHopAndAWalk) {
    expectRows("MATCH p = (a {name: 'Adam'})-[e1]->(b)-[*1..2]-(c) RETURN [r IN relationships(p) | r.name] AS names ORDER BY names",
               {{"Adam -> Bio, Maxime -> Bio"},
                {"Adam -> Bio, Maxime -> Bio, Maxime -> Padel"},
                {"Adam -> Cooking, Martina -> Cooking"},
                {"Adam -> Remy, Ghosts -> Remy"},
                {"Adam -> Remy, Ghosts -> Remy, Remy -> Ghosts"},
                {"Adam -> Remy, Remy -> Adam"},
                {"Adam -> Remy, Remy -> Adam, Adam -> Bio"},
                {"Adam -> Remy, Remy -> Adam, Adam -> Cooking"},
                {"Adam -> Remy, Remy -> Computers"},
                {"Adam -> Remy, Remy -> Computers, Luc -> Computers"},
                {"Adam -> Remy, Remy -> Eighties"},
                {"Adam -> Remy, Remy -> Ghosts"},
                {"Adam -> Remy, Remy -> Ghosts, Ghosts -> Remy"}});
}

TEST_F(HopAndPathEdgeUniquenessTest, measuresThePathsOfAHopAndAWalk) {
    expectRows("MATCH p = (a)-[e1]-(b)-[*1..2]-(c) RETURN count(*), sum(length(p)), sum(size(relationships(p)))",
               {{"170", "446", "446"}});
}

TEST_F(HopAndPathEdgeUniquenessTest, excludesAHopFromAQuantifiedPath) {
    expectRows("MATCH (a)-[e1]->(b)-[e]->{1,2}(c) RETURN count(*)", {{"24"}});
    expectRows("MATCH (a)-[e]-{1,2}(b)-[f]-(c) RETURN count(*)", {{"170"}});
    expectRows("MATCH (a)-[e1]->(b) ((x)-[r]->(y)){1,2} (c) RETURN count(*)", {{"24"}});
}
