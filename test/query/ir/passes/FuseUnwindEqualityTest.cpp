#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace turing::test;

namespace {

bool contains(std::string_view text, std::string_view needle) {
    return text.find(needle) != std::string_view::npos;
}

}

class FuseUnwindEqualityTest : public WriteQueryTest {
protected:
    void programOf(std::string_view query, std::string& program) {
        RowSink sink;
        const db::QueryStatus status = runQuery(std::string("EXPLAIN (db) ") + std::string(query), &sink);
        ASSERT_TRUE(status.isOk()) << query << "\n" << status.getError();
        ASSERT_EQ(sink.rows().size(), 1u);

        program = sink.rows().front().back();
    }

    void expectReadsTheElement(std::string_view query) {
        std::string program;
        programOf(query, program);

        EXPECT_FALSE(contains(program, "db.cross_product")) << program;
        EXPECT_FALSE(contains(program, "db.to_integer")) << program;
        EXPECT_TRUE(contains(program, "db.broadcast_constant")) << program;
    }

    void expectReadsTheComparedColumn(std::string_view query, std::string& program) {
        programOf(query, program);

        EXPECT_FALSE(contains(program, "db.cross_product")) << program;
        EXPECT_FALSE(contains(program, "db.to_integer")) << program;
        EXPECT_FALSE(contains(program, "db.broadcast_constant")) << program;
    }
};

TEST_F(FuseUnwindEqualityTest, readsTheFloatElementAnIntegerEquals) {
    const std::string_view query = "UNWIND [32.0] AS x MATCH (n:Person) WHERE n.age = x RETURN n.name, x";

    expectRows(query, {{"Remy", "32.000000"}, {"Adam", "32.000000"}});
    expectReadsTheElement(query);
}

TEST_F(FuseUnwindEqualityTest, readsTheZeroElementWithItsOwnSign) {
    const std::string_view negativeElement = "UNWIND [-0.0] AS x MATCH (n:Person) WHERE n.age - 32 = x RETURN n.name, x";
    const std::string_view negativeValue = "UNWIND [0.0] AS x MATCH (n:Person) WHERE [n.age * -0.0, 1][0] = x RETURN n.name, x";

    expectRows(negativeElement, {{"Remy", "-0.000000"}, {"Adam", "-0.000000"}});
    expectReadsTheElement(negativeElement);

    expectRows(negativeValue, {{"Remy", "0.000000"}, {"Adam", "0.000000"}});
    expectReadsTheElement(negativeValue);
}

TEST_F(FuseUnwindEqualityTest, readsTheIntegerElementAFloatEquals) {
    const std::string_view query = "UNWIND [32] AS x MATCH (n:Person) WHERE n.age * 1.0 = x RETURN n.name, x";

    expectRows(query, {{"Remy", "32"}, {"Adam", "32"}});
    expectReadsTheElement(query);
}

TEST_F(FuseUnwindEqualityTest, readsTheElementATaggedCellEquals) {
    const std::string_view listElement = "UNWIND [32.0] AS x MATCH (n:Person) WHERE [n.age, 'a'][0] = x RETURN n.name, x";
    const std::string_view mapValue = "UNWIND [32.0] AS x MATCH (n:Person) WHERE [{a: n.age}][0].a = x RETURN n.name, x";

    expectRows(listElement, {{"Remy", "32.000000"}, {"Adam", "32.000000"}});
    expectReadsTheElement(listElement);

    expectRows(mapValue, {{"Remy", "32.000000"}, {"Adam", "32.000000"}});
    expectReadsTheElement(mapValue);
}

TEST_F(FuseUnwindEqualityTest, readsTheElementANodeReadOutOfAListEquals) {
    const std::string_view query = "UNWIND [0] AS i MATCH (n) WHERE [n][0] = i RETURN i, n.name";

    expectRows(query, {{"0", "Remy"}});
    expectReadsTheElement(query);
}

TEST_F(FuseUnwindEqualityTest, readsAnIDAsTheIntegerElements) {
    const std::string_view query = "UNWIND [0, 1] AS i MATCH (n) WHERE id(n) = i RETURN i, n.name";

    expectRows(query, {{"0", "Remy"}, {"1", "Adam"}});

    std::string program;
    expectReadsTheComparedColumn(query, program);
    EXPECT_TRUE(contains(program, "db.const_scan_nodes")) << program;
}

TEST_F(FuseUnwindEqualityTest, readsTheIDOfTheNodeAFunctionAnswers) {
    const std::string_view query = "UNWIND [0] AS i MATCH (a)-[e]->(b) WHERE startNode(e) = i RETURN i, b.name";

    expectRows(query, {{"0", "Adam"}, {"0", "Ghosts"}, {"0", "Computers"}, {"0", "Eighties"}});

    std::string program;
    expectReadsTheComparedColumn(query, program);
    EXPECT_TRUE(contains(program, "db.element_id")) << program;
}

TEST_F(FuseUnwindEqualityTest, readsAnIntegerColumnAsTheIntegerElements) {
    const std::string_view query = "UNWIND [32] AS x MATCH (n:Person) WHERE n.age = x RETURN n.name, x";

    expectRows(query, {{"Remy", "32"}, {"Adam", "32"}});

    std::string program;
    expectReadsTheComparedColumn(query, program);
}

TEST_F(FuseUnwindEqualityTest, readsAnIDInTheTypeARegionTakes) {
    const std::string_view query = "UNWIND [0, 1] AS i MATCH (n) WHERE id(n) = i "
                                   "OPTIONAL MATCH (n)-[:KNOWS_WELL]->(m) RETURN i, m.name";

    expectRows(query, {{"0", "Adam"}, {"1", "Remy"}});

    std::string program;
    programOf(query, program);
    EXPECT_FALSE(contains(program, "db.cross_product")) << program;
}

TEST_F(FuseUnwindEqualityTest, keepsEveryElementTheRowEquals) {
    expectRows("UNWIND [0.0, -0.0] AS x MATCH (n) WHERE id(n) = x RETURN n.name", {{"Remy"}, {"Remy"}});
    expectCounts("UNWIND [0.0, -0.0] AS x MATCH (n) WHERE id(n) = x RETURN count(*)", {2});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
