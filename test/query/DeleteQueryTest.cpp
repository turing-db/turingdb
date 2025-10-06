#include <gtest/gtest.h>

#include "Path.h"
#include "QueryInterpreter.h"
#include "QueryTester.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"
#include "SimpleGraph.h"
#include "versioning/Change.h"

using namespace db;
using namespace turing::test;

class DeleteQueryTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        Graph* graph = _env->getSystemManager().createGraph("simpledb");
        SimpleGraph::createSimpleGraph(graph);

        _interp = std::make_unique<QueryInterpreter>(&_env->getSystemManager(),
                                                     &_env->getJobSystem());
    }

protected:
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreter> _interp {nullptr};

    // new change and set the id
    inline static const auto newChange = [](QueryTester& tester) {
        auto res = tester.query("CHANGE NEW")
                       .expectVector<const Change*>({}, false)
                       .execute()
                       .outputColumnVector<const Change*>(0);
        const ChangeID id = res.value()->back()->id();
        tester.setChangeID(id);
        return id;
    };
};

TEST_F(DeleteQueryTest, deleteRemy) {
    QueryTester tester {_env->getMem(), *_interp, "simpledb"};

    tester.query("match (n) return n")
          .expectVector<NodeID>({0,1,2,3,4,5,6,7,8,9,10,11,12})
          .execute();

    newChange(tester);

    tester.query("delete nodes 0")
        .execute();

    tester.query("commit")
          .execute();

    tester.query("match (n) return n")
        .expectVector<NodeID>({0, 1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12})
        .execute();
}
