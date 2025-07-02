#include <gtest/gtest.h>

#include "ID.h"
#include "LocalMemory.h"
#include "QueryInterpreter.h"
#include "QueryTester.h"
#include "TuringDB.h"
#include "TypingGraph.h"
#include "TuringTest.h"

using namespace db;

class QueryAnalyzerTest : public turing::test::TuringTest {
public:
    void initialize() override {
        SystemManager& sysMan = _db.getSystemManager();
        Graph* graph = sysMan.createGraph("simple");
        TypingGraph::createTypingGraph(graph);
        _interp = std::make_unique<QueryInterpreter>(&_db.getSystemManager(),
                                                     &_db.getJobSystem());
    }
    
protected:
    TuringDB _db;
    LocalMemory _mem;
    std::unique_ptr<QueryInterpreter> _interp {nullptr};
};

TEST_F(QueryAnalyzerTest, typeCheckInt64) {
    QueryTester tester {_mem, *_interp};

    // Query Int64 with Int64
    tester.query("MATCH n:Typer{pos_int = 256} return n")
        .expectVector<NodeID>({0})
        .execute();

    // Query Int64 with UInt64
    // NOTE: 26/6/25 @Remy @Cyrus decided not to support this coercion
    tester.query("MATCH n:Typer{pos_int = 256u} return n")
        .expectError()
        .execute();

    // Query Int64 with Double
    tester.query("MATCH n:Typer{pos_int = 256.} return n")
        .expectError()
        .execute();

    // Query Int64 with String
    tester.query("MATCH n:Typer{pos_int = \"256\"} return n")
        .expectError()
        .execute();

    // Query Int64 with Bool
    tester.query("MATCH n:Typer{pos_int = true} return n")
        .expectError()
        .execute();

}

TEST_F(QueryAnalyzerTest, typeCheckUInt64) {
    QueryTester tester {_mem, *_interp};

    // Query UInt64 with Int64 NOTE: Coercion implemented
    tester.query("MATCH n:Typer{uint = 333} return n")
        .expectVector<NodeID>({0})
        .execute();

    // Query UInt64 with UInt64
    tester.query("MATCH n:Typer{uint = 333u} return n")
        .expectVector<NodeID>({0})
        .execute();

    // Query UInt64 with Double
    tester.query("MATCH n:Typer{uint = 333.} return n")
        .expectError()
        .execute();

    // Query UInt64 with String
    tester.query("MATCH n:Typer{uint = \"333\"} return n")
        .expectError()
        .execute();

    // Query UInt64 with Bool
    tester.query("MATCH n:Typer{uint = true} return n")
        .expectError()
        .execute();
}

TEST_F(QueryAnalyzerTest, typeCheckString) {
    QueryTester tester {_mem, *_interp};

    // Query String with String
    tester.query("MATCH n:Typer{str = \"string property\"} return n")
        .expectVector<NodeID>({0})
        .execute();

    // Query String with Int64
    tester.query("MATCH n:Typer{str:12} return n")
        .expectError()
        .execute();

    // Query String with UInt64
    tester.query("MATCH n:Typer{str:12u} return n")
        .expectError()
        .execute();

    // Query String with Bool
    tester.query("MATCH n:Typer{str:true} return n")
        .expectError()
        .execute();
    
    // Query String with Double
    tester.query("MATCH n:Typer{str:20.} return n")
        .expectError()
        .execute();
}

TEST_F(QueryAnalyzerTest, typeCheckDouble) {
    QueryTester tester {_mem, *_interp};

    // Query Double with Double
    tester.query("MATCH n:Typer{dbl:1.618} return n")
        .expectVector<NodeID>({0})
        .execute();

    // Query Double with Int64
    tester.query("MATCH n:Typer{dbl:1618} return n")
        .expectError()
        .execute();

    // Query Double with UInt64
    tester.query("MATCH n:Typer{dbl:1618u} return n")
        .expectError()
        .execute();

    // Query Double with String
    tester.query("MATCH n:Typer{dbl:\"1.618\"} return n")
        .expectError()
        .execute();
    
    // Query Double with Bool
    tester.query("MATCH n:Typer{dbl:true} return n")
        .expectError()
        .execute();
}

TEST_F(QueryAnalyzerTest, typeCheckBool) {
    QueryTester tester {_mem, *_interp};

    // Query Bool with Bool
    tester.query("MATCH n:Typer{bool_t:true} return n")
        .expectVector<NodeID>({0})
        .execute();

    // Query Bool with Int64
    tester.query("MATCH n:Typer{bool_t:12} return n")
        .expectError()
        .execute();

    // Query Bool with UInt64
    tester.query("MATCH n:Typer{bool_t:12u} return n")
        .expectError()
        .execute();

    // Query Bool with String
    tester.query("MATCH n:Typer{bool_t:\"true\"} return n")
        .expectError()
        .execute();
    
    // Query Bool with Double
    tester.query("MATCH n:Typer{bool_t:12.} return n")
        .expectError()
        .execute();
}

TEST_F(QueryAnalyzerTest, checkMatchVariableUniqueness) {
    QueryTester tester {_mem, *_interp};

    tester.query("MATCH n--m return n")
        .expectVector<NodeID>({0})
        .execute();

    tester.query("MATCH n--m--n return n")
        .expectError()
        .execute();

    tester.query("MATCH n:Typer--m:Friend return n")
        .expectVector<NodeID>({0})
        .execute();

    tester.query("MATCH n:Typer--m:Friend--n:Typer return n")
        .expectError()
        .execute();

    tester.query("MATCH n-[e]-m-[r]-p return n")
        .expectVector<NodeID>({})
        .execute();

    tester.query("MATCH n-[e:FRIENDS_WITH]-m-[r:FRIENDS_WITH]-p return n")
        .expectVector<NodeID>({})
        .execute();

    tester.query("MATCH n-[e]-m-[e]-p return n")
        .expectError()
        .execute();

    tester.query("MATCH n-[e:FRIENDS_WITH]-m-[e:FRIENDS_WITH]-p return n")
        .expectError()
        .execute();
    
}
