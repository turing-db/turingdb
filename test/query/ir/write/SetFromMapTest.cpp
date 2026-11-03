#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class SetFromMapTest : public WriteQueryTest {
protected:
    void commitAfterFailure(std::string_view query) {
        ChangeID changeID;
        openChange(changeID);

        const QueryStatus failed = runWrite(query, changeID);
        ASSERT_FALSE(failed.isOk()) << "query: " << query << "\nexpected it to fail";

        const QueryStatus committed = runWrite("COMMIT", changeID);
        ASSERT_TRUE(committed.isOk()) << "COMMIT failed: " << committed.getError();

        submit(changeID);
    }
};

TEST_F(SetFromMapTest, replacesTheNodePropertiesWithAMapLiteral) {
    applyWrite("MATCH (n:Person {name: 'Remy'}) SET n = {name: 'R', age: 40}");
    expectRows("MATCH (n:Person {name: 'R'}) RETURN n.name, n.age, n.isFrench, n.hasPhD, size(keys(n))",
               {{"R", "40", "null", "null", "2"}});
    expectRows("MATCH (n:Person {name: 'Adam'}) RETURN n.age, n.isFrench", {{"32", "true"}});
}

TEST_F(SetFromMapTest, removesEveryPropertyForAnEmptyMap) {
    applyWrite("MATCH (n:Person {name: 'Remy'}) SET n = {}");
    expectRows("MATCH (n:Person) WHERE n.name IS NULL RETURN size(keys(n))", {{"0"}});
}

TEST_F(SetFromMapTest, removesAPropertyWhoseEntryIsNull) {
    applyWrite("MATCH (n:Person {name: 'Remy'}) SET n = {name: 'Remy', age: null}");
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.age, size(keys(n))", {{"null", "1"}});
}

TEST_F(SetFromMapTest, createsAPropertyAMapLiteralNames) {
    applyWrite("MATCH (n:Person {name: 'Remy'}) SET n = {name: 'Remy', rank: 1}");
    expectRows("MATCH (n:Person) WHERE n.rank IS NOT NULL RETURN n.name, n.rank", {{"Remy", "1"}});
}

TEST_F(SetFromMapTest, writesAMapBuiltPerRow) {
    applyWrite("MATCH (n:Person) WHERE n.age IS NOT NULL SET n = {name: n.name, years: n.age}");
    expectRows("MATCH (n:Person) WHERE n.years IS NOT NULL RETURN n.name, n.years, n.age, n.isFrench",
               {{"Adam", "32", "null", "null"}, {"Remy", "32", "null", "null"}});
}

TEST_F(SetFromMapTest, writesAMapBoundToAVariable) {
    applyWrite("WITH {name: 'R', age: 1} AS m MATCH (n:Person {name: 'Remy'}) SET n = m");
    expectRows("MATCH (n:Person {name: 'R'}) RETURN n.age, size(keys(n))", {{"1", "2"}});
}

TEST_F(SetFromMapTest, readsThePropertiesTheMapReplaced) {
    expectWriteRows("MATCH (n:Person {name: 'Remy'}) WITH n, n.age AS before SET n = {name: 'Remy'} RETURN before, n.age",
                    {{"32", "null"}});
}

TEST_F(SetFromMapTest, replacesTheEdgeTheQueryCreated) {
    applyWrite("CREATE (:Fresh)-[e:LINKS {a: 1, b: 2}]->(:Fresh) SET e = {a: 3}");
    expectRows("MATCH (:Fresh)-[e:LINKS]->(:Fresh) RETURN e.a, e.b", {{"3", "null"}});
}

TEST_F(SetFromMapTest, writesAMapHeldByAProperty) {
    applyWrite("MATCH (n:Person {name: 'Remy'}) SET n.attrs = {name: 'X', age: 7}");
    applyWrite("MATCH (n:Person {name: 'Remy'}) SET n = n.attrs");
    expectRows("MATCH (n:Person {name: 'X'}) RETURN n.age, n.attrs, size(keys(n))", {{"7", "null", "2"}});
}

TEST_F(SetFromMapTest, writesAMapHeldByACell) {
    applyWrite("UNWIND [{name: 'R', age: 3}] AS m MATCH (n:Person {name: 'Remy'}) SET n = m");
    expectRows("MATCH (n:Person {name: 'R'}) RETURN n.age, size(keys(n))", {{"3", "2"}});
}

TEST_F(SetFromMapTest, removesEveryPropertyForANullMap) {
    applyWrite("MATCH (n:Person {name: 'Adam'}) SET n.attrs = {name: 'X'}");
    applyWrite("MATCH (n:Person {name: 'Remy'}) SET n = n.attrs");
    expectRows("MATCH (n:Person) WHERE n.name IS NULL RETURN size(keys(n))", {{"0"}});
}

TEST_F(SetFromMapTest, replacesTheEdgeProperties) {
    applyWrite("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) SET e.weight = 1");
    applyWrite("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) SET e = {name: 'x'}");
    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) RETURN e.name, e.weight",
               {{"x", "null"}});
    expectRows("MATCH (:Person {name: 'Adam'})-[e:KNOWS_WELL]->(:Person {name: 'Remy'}) RETURN e.name",
               {{"Adam -> Remy"}});
}

TEST_F(SetFromMapTest, replacesTheNodeTheQueryCreated) {
    applyWrite("CREATE (n:Fresh {a: 1, b: 2}) SET n = {a: 3}");
    expectRows("MATCH (n:Fresh) RETURN n.a, n.b", {{"3", "null"}});
}

TEST_F(SetFromMapTest, removesWhatAnEarlierSetOfTheQueryWrote) {
    applyWrite("MATCH (n:Person {name: 'Remy'}) SET n.age = 50 SET n = {name: 'Remy'}");
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.age, size(keys(n))", {{"null", "1"}});
}

TEST_F(SetFromMapTest, keepsTheLastMapWrittenToANode) {
    applyWrite("MATCH (n:Person {name: 'Remy'}) UNWIND [{name: 'A'}, {age: 7}] AS m SET n = m");
    expectRows("MATCH (n:Person) WHERE n.age = 7 RETURN n.name, size(keys(n))", {{"null", "1"}});
}

TEST_F(SetFromMapTest, replacesOnMatchOnly) {
    applyWrite("MERGE (n:Person {name: 'Remy'}) ON MATCH SET n = {name: 'Remy', age: 1}");
    applyWrite("MERGE (n:Person {name: 'Nobody'}) ON MATCH SET n = {name: 'Other'}");
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.age, n.isFrench", {{"1", "null"}});
    expectRows("MATCH (n:Person {name: 'Nobody'}) RETURN count(n)", {{"1"}});
}

TEST_F(SetFromMapTest, rejectsAValueThatIsNoMap) {
    expectWriteError("MATCH (n:Person {name: 'Remy'}) SET n = 5", "SET n = ... takes a map, not 'Integer'");
}

TEST_F(SetFromMapTest, rejectsACellThatHoldsNoMap) {
    expectWriteError("WITH [{name: 'R'}, 5] AS xs MATCH (n:Person {name: 'Remy'}) SET n = xs[1]",
                     "Cannot set the properties of an entity to a value of type 'Integer', which is not a map");
}

TEST_F(SetFromMapTest, rejectsAMapLiteralEntryOfAnotherType) {
    expectWriteError("MATCH (n:Person {name: 'Remy'}) SET n = {age: 'old'}",
                     "types 'Int64' and 'String' are incompatible");
}

TEST_F(SetFromMapTest, rejectsAPropertyCreatedFromAMapReadPerRow) {
    expectWriteError("UNWIND [{fresh: 1}] AS m MATCH (n:Person {name: 'Remy'}) SET n = m",
                     "Cannot create property 'fresh' from a value whose type is only known per row");
}

TEST_F(SetFromMapTest, readsAPropertyAMapLiteralCreatesInTheSameQuery) {
    expectWriteRows("MATCH (n:Person {name: 'Remy'}) SET n = {name: 'Remy', rank: 1} RETURN n.rank", {{"1"}});
}

TEST_F(SetFromMapTest, createsAListPropertyAMapLiteralNames) {
    applyWrite("MATCH (n:Person {name: 'Remy'}) SET n = {name: 'Remy', tags: ['a', 'b']}");
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.tags", {{"[a, b]"}});
}

TEST_F(SetFromMapTest, createsAMapPropertyAMapLiteralNames) {
    applyWrite("MATCH (n:Person {name: 'Remy'}) SET n = {name: 'Remy', inner: {a: 1}}");
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.inner", {{"{a: 1}"}});
}

TEST_F(SetFromMapTest, skipsANullEntryForAPropertyNoEntityHolds) {
    applyWrite("MATCH (n:Person {name: 'Remy'}) SET n = {name: 'Remy', nickname: null}");
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.nickname, size(keys(n))", {{"null", "1"}});
}

TEST_F(SetFromMapTest, skipsANullEntryOfAMapReadPerRowForAPropertyNoEntityHolds) {
    applyWrite("UNWIND [{name: 'R', nickname: null}] AS m MATCH (n:Person {name: 'Remy'}) SET n = m");
    expectRows("MATCH (n:Person {name: 'R'}) RETURN size(keys(n))", {{"1"}});
}

TEST_F(SetFromMapTest, rejectsAPropertyAMapBoundToAVariableCreates) {
    expectWriteError("WITH {name: 'Remy', rank: 2} AS m MATCH (n:Person {name: 'Remy'}) SET n = m RETURN n.rank",
                     "Cannot create property 'rank' from a value whose type is only known per row");
}

TEST_F(SetFromMapTest, rejectsTheListOfAVariableLengthPath) {
    expectWriteError("MATCH (a:Person {name: 'Remy'})-[r:KNOWS_WELL*1..2]->(b) SET r = {weight: 1}",
                     "SET r = ... takes a node or a relationship, not 'List'");
}

TEST_F(SetFromMapTest, ignoresTheValueOfARowWithANullEntity) {
    applyWrite("UNWIND [{name: 'Remy', attrs: {name: 'R'}}, {name: 'Nobody', attrs: 5}] AS x "
               "OPTIONAL MATCH (n:Person {name: x.name}) SET n = x.attrs");
    expectRows("MATCH (n:Person {name: 'R'}) RETURN size(keys(n))", {{"1"}});
}

TEST_F(SetFromMapTest, ignoresTheValueOfARowOnMatchSkips) {
    applyWrite("WITH ['Remy', 'Nobody'] AS names, [{name: 'R'}, 5] AS attrs UNWIND [0, 1] AS i "
               "MERGE (n:Person {name: names[i]}) ON MATCH SET n = attrs[i]");
    expectRows("MATCH (n:Person) WHERE n.name IN ['R', 'Nobody'] RETURN n.name", {{"Nobody"}, {"R"}});
}

TEST_F(SetFromMapTest, mergesIntoANodeAFunctionReturns) {
    applyWrite("MATCH (n:Person {name: 'Remy'}) WITH coalesce(n, n) AS x SET x += {rank: 1}");
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.rank, n.age", {{"1", "32"}});
}

TEST_F(SetFromMapTest, replacesTheEdgeAnAggregateReturns) {
    applyWrite("MATCH (:Person {name: 'Remy'})-[r:KNOWS_WELL]->(:Person {name: 'Adam'}) "
               "WITH head(collect(r)) AS e SET e = {weight: 2}");
    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->(:Person {name: 'Adam'}) RETURN e.name, e.weight",
               {{"null", "2"}});
}

TEST_F(SetFromMapTest, namesThePropertyAMapEntryOfAnotherTypeWrites) {
    expectWriteError("UNWIND [{name: 'x', isFrench: 5}] AS m MATCH (n:Person {name: 'Remy'}) SET n = m",
                     "Cannot set property 'isFrench' of type 'Bool' to a value of type 'Integer'");
}

TEST_F(SetFromMapTest, writesNoRowWhenALaterRowHoldsAnEntryOfAnotherType) {
    commitAfterFailure("UNWIND [{age: 1}, {age: 'a'}] AS m MATCH (n:Person {name: 'Remy'}) SET n += m");
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.age", {{"32"}});
}

TEST_F(SetFromMapTest, writesNoRowWhenALaterRowHoldsNoMap) {
    commitAfterFailure("WITH [{age: 1}, 5] AS xs UNWIND xs AS m MATCH (n:Person {name: 'Remy'}) SET n = m");
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN n.age", {{"32"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
