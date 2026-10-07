#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A script of statements separated by semicolons runs each statement in order, as if each
// were sent alone, and stops at the first one that fails. The response carries the last
// statement's result.
//
// simpledb holds 8 Person and 10 Interest nodes, and no Fruit node.
class MultiStatementScriptTest : public WriteQueryTest {
protected:
    QueryStatus runScript(std::string_view script, const ChangeID& changeID, StringRowSink& sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              script,
                              _graphName,
                              CommitHash::head(),
                              changeID,
                              &sink);

        return status;
    }

    void expectScriptRows(std::string_view script,
                          const ChangeID& changeID,
                          const std::vector<std::string>& expectedNames,
                          const std::vector<StringRowSink::Row>& expectedRows) {
        StringRowSink sink;
        const QueryStatus status = runScript(script, changeID, sink);
        ASSERT_TRUE(status.isOk()) << "script: " << script << "\nerror: " << status.getError();

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(sink.getNames(), expectedNames) << "script: " << script;
        EXPECT_EQ(rows, expectedRows) << "script: " << script;
    }
};

TEST_F(MultiStatementScriptTest, returnsTheResultOfTheLastStatement) {
    expectScriptRows("MATCH (n:Person) RETURN count(n) AS people; MATCH (n:Interest) RETURN count(n) AS interests",
                     ChangeID::head(),
                     {"interests"},
                     {{"10"}});
}

TEST_F(MultiStatementScriptTest, acceptsATrailingSemicolon) {
    expectScriptRows("MATCH (n:Person) RETURN count(n) AS people; MATCH (n:Interest) RETURN count(n) AS interests;",
                     ChangeID::head(),
                     {"interests"},
                     {{"10"}});
}

TEST_F(MultiStatementScriptTest, runsThreeStatements) {
    expectScriptRows("MATCH (n:Interest) RETURN n.name; MATCH (n) RETURN count(n) AS nodes; MATCH (n:Person) RETURN count(n) AS people",
                     ChangeID::head(),
                     {"people"},
                     {{"8"}});
}

TEST_F(MultiStatementScriptTest, readsWhatAnEarlierStatementCommitted) {
    ChangeID changeID;
    openChange(changeID);

    expectScriptRows("CREATE (:Fruit {name: 'apple'}); COMMIT; MATCH (f:Fruit) RETURN f.name",
                     changeID,
                     {"f.name"},
                     {{"apple"}});
}

TEST_F(MultiStatementScriptTest, runsEveryWriteOfTheScript) {
    ChangeID changeID;
    openChange(changeID);

    StringRowSink sink;
    const QueryStatus status = runScript("CREATE (:Fruit {name: 'apple'}); CREATE (:Fruit {name: 'pear'})", changeID, sink);
    ASSERT_TRUE(status.isOk()) << status.getError();

    submit(changeID);

    expectRows("MATCH (f:Fruit) RETURN f.name", {{"apple"}, {"pear"}});
}

TEST_F(MultiStatementScriptTest, stopsAtTheFirstFailingStatement) {
    ChangeID changeID;
    openChange(changeID);

    StringRowSink sink;
    const QueryStatus status = runScript("CREATE (:Fruit {name: 'apple'}); MATCH (n) RETURN m; CREATE (:Fruit {name: 'pear'})", changeID, sink);
    ASSERT_EQ(status.getStatus(), QueryStatus::Status::ANALYZE_ERROR) << status.getError();
    EXPECT_NE(status.getError().find("Variable 'm' not found"), std::string::npos) << status.getError();
    EXPECT_TRUE(sink.getRows().empty());

    submit(changeID);

    expectRows("MATCH (f:Fruit) RETURN f.name", {{"apple"}});
}

TEST_F(MultiStatementScriptTest, analyzesEachStatementAgainstTheGraphItRunsOn) {
    ChangeID changeID;
    openChange(changeID);

    expectScriptRows("CREATE (:Fruit {name: 'apple', sweetness: 7}); COMMIT; MATCH (f:Fruit) RETURN f.name, f.sweetness + 1",
                     changeID,
                     {"f.name", "f.sweetness + 1"},
                     {{"apple", "8"}});
}

TEST_F(MultiStatementScriptTest, explainsOnlyTheStatementCarryingThePrefix) {
    expectScriptRows("EXPLAIN MATCH (n) RETURN n; MATCH (n:Interest) RETURN count(n) AS interests",
                     ChangeID::head(),
                     {"interests"},
                     {{"10"}});

    StringRowSink sink;
    const QueryStatus status = runScript("MATCH (n:Interest) RETURN count(n); EXPLAIN (nl) MATCH (n) RETURN n", ChangeID::head(), sink);
    ASSERT_TRUE(status.isOk()) << status.getError();

    EXPECT_EQ(sink.getNames(), (std::vector<std::string> {"stage", "dump"}));
    ASSERT_EQ(sink.getRows().size(), 1);
    EXPECT_EQ(sink.getRows().front().front(), "nl");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
