#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "QueryStatus.h"

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// openCypher's property expression is an atom followed by property lookups, and a function
// call is an atom: startNode(r).name reads a property of the node the call returns, and
// datetime('...').year a component of the instant it returns.
class FunctionCallPropertyTest : public WriteQueryTest {
protected:
    void expectError(std::string_view query, std::string_view expectedError) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);

        ASSERT_FALSE(status.isOk()) << "query: " << query << "\nexpected it to fail";
        EXPECT_NE(status.getError().find(expectedError), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }
};

TEST_F(FunctionCallPropertyTest, readsTheComponentsOfADateTimeCall) {
    expectRows("RETURN datetime('2010-11-25T08:20:45.079Z').year, "
               "datetime('2010-11-25T08:20:45.079Z').month, "
               "datetime('2010-11-25T08:20:45.079Z').millisecond",
               {{"2010", "11", "79"}});
}

TEST_F(FunctionCallPropertyTest, readsAComponentOfADateTimeCallOnEachRow) {
    expectRows("UNWIND ['2010-01-02', '2012-06-30'] AS s RETURN datetime(s).year, datetime(s).month",
               {{"2010", "1"}, {"2012", "6"}});
}

TEST_F(FunctionCallPropertyTest, readsAComponentOfAnAggregate) {
    expectRows("UNWIND ['2010-01-02', '2012-06-30'] AS s RETURN max(datetime(s)).year",
               {{"2012"}});
}

TEST_F(FunctionCallPropertyTest, ordersByAComponentOfADateTimeCall) {
    expectRowsInOrder("UNWIND ['2010-01-02', '2012-06-30', '2011-03-15'] AS s "
                      "RETURN s ORDER BY datetime(s).month DESC",
                      {{"2012-06-30"}, {"2011-03-15"}, {"2010-01-02"}});
}

TEST_F(FunctionCallPropertyTest, readsAPropertyOfTheEndsOfAnEdge) {
    expectRows("MATCH ()-[r:KNOWS_WELL]->() RETURN startNode(r).name, endNode(r).name",
               {{"Remy", "Adam"}, {"Adam", "Remy"}, {"Ghosts", "Remy"}});
}

TEST_F(FunctionCallPropertyTest, filtersOnAPropertyOfAnEndOfAnEdge) {
    expectRows("MATCH ()-[r:KNOWS_WELL]->() WHERE endNode(r).name = 'Remy' RETURN startNode(r).name",
               {{"Adam"}, {"Ghosts"}});
}

TEST_F(FunctionCallPropertyTest, groupsByAPropertyOfAnEndOfAnEdge) {
    expectRows("MATCH ()-[r:KNOWS_WELL]->() RETURN endNode(r).name, count(r)",
               {{"Adam", "1"}, {"Remy", "2"}});
}

TEST_F(FunctionCallPropertyTest, readsAComponentOfAPropertyOfAnEndOfAnEdge) {
    applyWrite("MATCH (p:Person {name: 'Remy'}) SET p.joined = datetime('2019-03-01')");

    expectRows("MATCH ()-[r:KNOWS_WELL]->() RETURN startNode(r).name, startNode(r).joined.year",
               {{"Remy", "2019"}, {"Adam", "null"}, {"Ghosts", "null"}});
}

TEST_F(FunctionCallPropertyTest, readsNullForAPropertyNoEntityCarries) {
    expectRows("MATCH ()-[r:KNOWS_WELL]->() RETURN startNode(r).nosuch",
               {{"null"}, {"null"}, {"null"}});
}

TEST_F(FunctionCallPropertyTest, rejectsANameThatIsNoComponentOfADateTime) {
    expectError("RETURN datetime('2010-11-25').week", "'week' is not a component of a datetime");
}

TEST_F(FunctionCallPropertyTest, rejectsAPropertyOfAnInteger) {
    expectError("RETURN toInteger('1').name", "'Integer' has no property 'name'");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
