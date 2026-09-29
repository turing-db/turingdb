#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

class DurationOfMapFilterTest : public WriteQueryTest {
protected:
    void initialize() override {
        WriteQueryTest::initialize();

        applyWrite("CREATE (m:Message {name: 'early', creationDate: datetime('2024-03-01T00:00:00Z')})");
        applyWrite("CREATE (m:Message {name: 'boundary', creationDate: datetime('2024-07-19T00:00:00Z')})");
        applyWrite("CREATE (m:Message {name: 'late', creationDate: datetime('2024-09-01T00:00:00Z')})");
        applyWrite("CREATE (m:Message {name: 'undated'})");
    }
};

TEST_F(DurationOfMapFilterTest, comparesAgainstAnInstantPlusADuration) {
    expectRows("MATCH (m:Message) WHERE m.creationDate < datetime('2024-01-01T00:00:00Z') + duration({days: 200}) "
               "RETURN m.name",
               {{"early"}});
    expectRows("MATCH (m:Message) WHERE m.creationDate <= datetime('2024-01-01T00:00:00Z') + duration({days: 200}) "
               "RETURN m.name",
               {{"early"}, {"boundary"}});
}

TEST_F(DurationOfMapFilterTest, comparesAgainstABoundInstantPlusADuration) {
    expectRows("WITH datetime('2024-01-01T00:00:00Z') AS date "
               "MATCH (m:Message) WHERE m.creationDate < date + duration({days: 200}) RETURN m.name",
               {{"early"}});
}

TEST_F(DurationOfMapFilterTest, comparesAStoredInstantMinusADuration) {
    expectRows("MATCH (m:Message) WHERE m.creationDate - duration({days: 200}) >= datetime('2024-01-01T00:00:00Z') "
               "RETURN m.name",
               {{"boundary"}, {"late"}});
}
