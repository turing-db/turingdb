#include <gtest/gtest.h>

#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A list a SET writes is read back by the query that wrote it: the value sits in the write
// buffer as the encoded list a commit stores, which the read decodes into a list of its own.
class WrittenListPropertyTest : public WriteQueryTest {
protected:
    void initialize() override {
        WriteQueryTest::initialize();

        applyWrite("CREATE (a:Tagged {name: 'a', tags: ['Java', 'Python']})");
        applyWrite("CREATE (b:Tagged {name: 'b'})");
    }
};

TEST_F(WrittenListPropertyTest, readsBackTheListItWrote) {
    expectWriteRows("MATCH (n:Tagged {name: 'b'}) SET n.tags = ['x', 'y'] RETURN n.tags",
                    {{"[x, y]"}});
}

TEST_F(WrittenListPropertyTest, readsBackTheListItGrew) {
    expectWriteRows("MATCH (n:Tagged {name: 'a'}) SET n.tags = 'Cypher' + n.tags RETURN n.tags",
                    {{"[Cypher, Java, Python]"}});
}

TEST_F(WrittenListPropertyTest, readsBackAnElementOfTheListItWrote) {
    expectWriteRows("MATCH (n:Tagged {name: 'b'}) SET n.tags = [1, 2, 3] RETURN n.tags[1]",
                    {{"2"}});
}

TEST_F(WrittenListPropertyTest, keepsTheListItWrote) {
    applyWrite("MATCH (n:Tagged {name: 'b'}) SET n.tags = ['x', 'y']");

    expectRows("MATCH (n:Tagged) RETURN n.name, n.tags",
               {{"a", "[Java, Python]"}, {"b", "[x, y]"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
