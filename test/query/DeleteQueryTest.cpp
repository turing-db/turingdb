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

        Graph* graph = _env->getSystemManager().createGraph("simple");
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
    QueryTester tester {_env->getMem(), *_interp};

    tester.query("match (n) return n, n.name")
        .expectVector<NodeID>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
        .expectOptVector<types::String::Primitive>(
            {"Remy", "Adam", "Computers", "Eighties", "Bio", "Cooking", "Ghosts",
             "Paddle", "Maxime", "Luc", "Animals", "Martina", "Suhas"});

    tester.query("match (n)-[e]-(m) return e, e.name")
        .expectVector<EdgeID>({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
        .expectOptVector<types::String::Primitive>({
            "Remy -> Adam",
            "Remy -> Ghosts",
            "Remy -> Computers",
            "Remy -> Eighties",
            "Adam -> Remy",
            "Adam -> Bio",
            "Adam -> Cooking",
            "Ghosts -> Remy",
            "Maxime -> Bio",
            "Maxime -> Paddle",
            "Luc -> Animals",
            "Luc -> Computers",
            "Martina -> Cooking",
        })
        .execute();

    newChange(tester);

    tester.query("delete nodes 0")
        .execute();

    tester.query("commit")
          .execute();

    tester.query("match (n)-[e]-(m) return n, n.name")
        // nodes in 1st DP << 1, node 6 missing
        .expectVector<NodeID>({0, 1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12})
        // Remy gone
        .expectOptVector<types::String::Primitive>(
            {"Adam", "Computers", "Eighties", "Bio", "Cooking", "Ghosts", "Paddle",
             "Maxime", "Luc", "Animals", "Martina", "Suhas"});
        // edges incident to remy are gone
    tester.query("match (n)-[e]-(m) return e, e.name")
        .expectVector<EdgeID>({0, 1, 8, 9, 10, 11, 12})
        .expectOptVector<types::String::Primitive>({
            "Adam -> Bio",
            "Adam -> Cooking",
            "Maxime -> Bio",
            "Maxime -> Paddle",
            "Luc -> Animals",
            "Luc -> Computers",
            "Martina -> Cooking",
        })

        .execute();
}
