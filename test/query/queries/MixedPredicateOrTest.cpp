#include <gtest/gtest.h>

#include <string_view>

#include "TuringDB.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemManager.h"
#include "columns/ColumnIDs.h"
#include "dataframe/Dataframe.h"

#include "GraphQueryTest.h"

using namespace turing::test;

class MixedPredicateOrTest : public GraphQueryTest {};

// OR-ing NodeID equalities with a property filter must not crash.
TEST_F(MixedPredicateOrTest, idOrPropertyFilter) {
    auto res = query("MATCH (n) WHERE n = 0 OR n = 1 OR n.name = 'Remy' RETURN n",
                     [](const Dataframe*) {});
    if (!res.isOk()) {
        printf("Status: %d  Error: %s\n", (int)res.getStatus(), res.getError().c_str());
    }
    EXPECT_TRUE(res);
}
