#include <gtest/gtest.h>

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

// A component name is resolved against the type of what it is read off, once that type is
// known, so a wrong name is reported against that type and a wrong base is reported first.
class ComponentNameResolutionTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

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
                                     &_env->getCompilerContext(),
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
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        submit(changeID);
    }

    void expectError(std::string_view query, std::string_view expectedError) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);

        ASSERT_FALSE(status.isOk()) << "query: " << query << "\nexpected it to fail";
        EXPECT_NE(status.getError().find(expectedError), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }

    void writeTasks() {
        write("CREATE (n:Task {name: 'a', took: duration(2000000)})");
        write("CREATE (n:Task {name: 'b', took: duration(90061000000)})");
        write("CREATE (n:Task {name: 'c', took: duration(-1500000)})");
        write("CREATE (n:Task {name: 'd'})");
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

TEST_F(ComponentNameResolutionTest, rejectsAnUnknownUnitOfADurationProperty) {
    writeTasks();

    expectError("MATCH (n:Task) RETURN n.took.fortnight", "'fortnight' is not a component of a duration");
}

TEST_F(ComponentNameResolutionTest, rejectsAnUnknownNameOnAVariableThatIsNoEntity) {
    expectError("WITH 1 AS x RETURN x.a.fortnight", "Variable 'x' is 'Integer' it must be a node or edge");
}

TEST_F(ComponentNameResolutionTest, rejectsAnUnknownNameOnAPropertyTheGraphDoesNotCarry) {
    writeTasks();

    expectError("MATCH (n:Task) RETURN n.unheardOf.fortnight",
                "'fortnight' is not a component of a datetime or a duration");
}

TEST_F(ComponentNameResolutionTest, rejectsAnUnknownNameOnAPropertyThatHasNoComponents) {
    expectError("MATCH (n:Person) RETURN n.name.fortnight",
                "Property 'name' is 'String', only a datetime or a duration has components");
}

TEST_F(ComponentNameResolutionTest, rejectsAnUnknownNameOnAField) {
    expectError("LOAD CSV 'tasks.csv' WITH HEADERS AS row RETURN row.took.fortnight",
                "Field 'took' of 'row' is 'String', only a datetime or a duration has components");
}
