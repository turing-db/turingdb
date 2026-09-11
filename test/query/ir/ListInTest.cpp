#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

class ListInTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    void expectRows(std::string_view query, std::vector<StringRowSink::Row> expected) {
        StringRowSink sink;
        QueryStatus status;

        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        std::sort(expected.begin(), expected.end());
        EXPECT_EQ(rows, expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(ListInTest, literalMembership) {
    expectRows("RETURN 3 IN [1, 2, 3]", {{"true"}});
    expectRows("RETURN 3 IN [1, 2]", {{"false"}});
    expectRows("RETURN 3 IN []", {{"false"}});
}

TEST_F(ListInTest, nullLeftOperand) {
    expectRows("RETURN null IN [1, 2]", {{"null"}});

    // The OR-fold of no elements is false, whatever the left operand is
    expectRows("RETURN null IN []", {{"false"}});
}

TEST_F(ListInTest, nullElements) {
    expectRows("RETURN 3 IN [1, null, 3]", {{"true"}});
    expectRows("RETURN 3 IN [1, null, 2]", {{"null"}});
}

TEST_F(ListInTest, elementTypes) {
    expectRows("RETURN 'two' IN [1, 'two', 3]", {{"true"}});
    expectRows("RETURN 'four' IN [1, 'two', 3]", {{"false"}});
    expectRows("RETURN true IN [false, true]", {{"true"}});
    expectRows("RETURN 2.5 IN [1.5, 2.5]", {{"true"}});
    expectRows("RETURN 2 IN [1, 2.0]", {{"true"}});
}

TEST_F(ListInTest, constantMembershipOverRows) {
    std::vector<StringRowSink::Row> expected(18, {"true"});
    expectRows("MATCH (n) RETURN 3 IN [1, 2, 3]", expected);
}

TEST_F(ListInTest, stringPropertyMembership) {
    expectRows("MATCH (n) WHERE n.name IN ['Gym', 'Travel', 'Nowhere'] RETURN n.name",
               {{"Gym"}, {"Travel"}});

    expectRows("MATCH (n:Person) WHERE n.name IN ['Remy', 'Cyrus'] RETURN n.name",
               {{"Remy"}, {"Cyrus"}});

    expectRows("MATCH (n) WHERE n.name IN [] RETURN n.name", {});
}

TEST_F(ListInTest, nullablePropertyMembership) {
    // Remy and Adam are the only people carrying an age, so every other row is null
    expectRows("MATCH (n:Person) WHERE n.age IN [32] RETURN n.name",
               {{"Remy"}, {"Adam"}});

    expectRows("MATCH (n:Person) RETURN n.name, n.age IN [32]",
               {{"Remy", "true"},
                {"Adam", "true"},
                {"Maxime", "null"},
                {"Luc", "null"},
                {"Martina", "null"},
                {"Suhas", "null"},
                {"Cyrus", "null"},
                {"Doruk", "null"}});
}

TEST_F(ListInTest, unwoundElementMembership) {
    expectRows("UNWIND [1, 2, 3] AS x RETURN x IN [2, 3]",
               {{"false"}, {"true"}, {"true"}});

    // A null element is null on its own account, so it matches neither the 1 nor the
    // list's own null
    expectRows("UNWIND [1, null, 3] AS x RETURN x IN [1, null]",
               {{"true"}, {"null"}, {"null"}});
}

TEST_F(ListInTest, combinedWithBooleanPredicates) {
    expectRows("MATCH (n:Person) WHERE n.name IN ['Remy', 'Martina'] AND n.isFrench = true RETURN n.name",
               {{"Remy"}});

    expectRows("MATCH (n:Person) WHERE n.name IN ['Remy'] OR n.name IN ['Cyrus'] RETURN n.name",
               {{"Remy"}, {"Cyrus"}});

    expectRows("MATCH (n:Person) WHERE NOT n.name IN ['Remy', 'Adam', 'Maxime', 'Luc'] RETURN n.name",
               {{"Martina"}, {"Suhas"}, {"Cyrus"}, {"Doruk"}});
}
