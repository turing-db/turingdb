#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "QueryConfig.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// size() over the kinds of string a query holds: the literal it spells out, the stored
// property it reads back out of a datapart, the string a function builds characters of its
// own for, and the one a heterogeneous list hands on as a type-erased cell. The size is
// the number of Unicode characters the string holds, not the bytes UTF-8 spends on them.
class StringSizeTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void openChange(ChangeID& changeID) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const auto res = system.newChange(_graphName);
        ASSERT_TRUE(res);

        changeID = res.value()->id();
    }

    void submit(const ChangeID& changeID) {
        const QueryState submitState(_graphName,
                                     &_env->getMem(),
                                     &_queryConfig,
                                     nullptr,
                                     CommitHash::head(),
                                     changeID);
        const QueryStatus status = _env->getDB().query("CHANGE SUBMIT", submitState);
        ASSERT_TRUE(status.isOk()) << "CHANGE SUBMIT failed";
    }

    void write(std::string_view query) {
        ChangeID changeID;
        openChange(changeID);

        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              changeID,
                              &_env->getMem(),
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        submit(changeID);
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    void expectRejected(std::string_view query, std::string_view messagePart) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_FALSE(status.isOk()) << "query: " << query << " was accepted";
        EXPECT_NE(status.getError().find(messagePart), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

TEST_F(StringSizeTest, sizesAStringLiteral) {
    expectRows("RETURN size('hello')", {{"5"}});
}

TEST_F(StringSizeTest, sizesAnEmptyStringLiteral) {
    expectRows("RETURN size('')", {{"0"}});
}

TEST_F(StringSizeTest, sizesAMultibyteStringByItsCharacters) {
    expectRows("RETURN size('héllo')", {{"5"}});
}

TEST_F(StringSizeTest, sizesAFourByteCharacterAsOne) {
    expectRows("RETURN size('a😀')", {{"2"}});
}

TEST_F(StringSizeTest, sizesAStoredStringProperty) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN size(n.name)", {{"4"}});
}

TEST_F(StringSizeTest, sizesEveryPersonName) {
    expectRows("MATCH (n:Person) RETURN n.name, size(n.name)",
               {{"Remy", "4"},
                {"Adam", "4"},
                {"Maxime", "6"},
                {"Luc", "3"},
                {"Martina", "7"},
                {"Suhas", "5"},
                {"Cyrus", "5"},
                {"Doruk", "5"}});
}

TEST_F(StringSizeTest, filtersOnTheSizeOfAName) {
    expectRows("MATCH (n:Person) WHERE size(n.name) = 3 RETURN n.name", {{"Luc"}});
}

TEST_F(StringSizeTest, sizesNullWhereTheNodeHasNoString) {
    write("CREATE (a:Tagged {name: 'a', text: 'abcd'})");
    write("CREATE (b:Tagged {name: 'b'})");

    expectRows("MATCH (n:Tagged) RETURN n.name, size(n.text)",
               {{"a", "4"}, {"b", "null"}});
}

TEST_F(StringSizeTest, sizesAnEmptyStoredString) {
    write("CREATE (n:Tagged {name: 'a', text: ''})");

    expectRows("MATCH (n:Tagged) RETURN size(n.text)", {{"0"}});
}

TEST_F(StringSizeTest, sizesAStoredMultibyteStringByItsCharacters) {
    write("CREATE (n:Tagged {name: 'a', text: 'héllo'})");

    expectRows("MATCH (n:Tagged) RETURN size(n.text)", {{"5"}});
}

TEST_F(StringSizeTest, sizesAnEdgeTypeString) {
    expectRows("MATCH ()-[e:KNOWS_WELL]->() RETURN size(type(e))", {{"10"}, {"10"}, {"10"}});
}

TEST_F(StringSizeTest, sizesAStringHeldInATaggedCell) {
    expectRows("RETURN size(head(['abcd', 2]))", {{"4"}});
}

TEST_F(StringSizeTest, sizesAMultibyteStringHeldInATaggedCell) {
    expectRows("RETURN size(head(['héllo', 2]))", {{"5"}});
}

TEST_F(StringSizeTest, sizesAStringHeldInAStoredListCell) {
    write("CREATE (n:Tagged {name: 'a', tags: ['abcd', 2]})");

    expectRows("MATCH (n:Tagged) RETURN size(head(n.tags))", {{"4"}});
}

TEST_F(StringSizeTest, sizesANullCellAsNull) {
    expectRows("RETURN size(head([]))", {{"null"}});
}

TEST_F(StringSizeTest, rejectsTheSizeOfACellHoldingNeitherAListNorAString) {
    expectRejected("RETURN size(head([1, 2]))", "size() and length() read a list or a string");
}

TEST_F(StringSizeTest, rejectsTheSizeOfANumber) {
    expectRejected("RETURN size(1)", "size");
}

TEST_F(StringSizeTest, countsTheNamesLongerThanFiveCharacters) {
    expectRows("MATCH (n:Person) WHERE size(n.name) > 5 RETURN count(*)", {{"2"}});
}
