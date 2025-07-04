#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "ID.h"
#include "LocalMemory.h"
#include "QueryInterpreter.h"
#include "QueryTester.h"
#include "TuringDB.h"
#include "TypingGraph.h"
#include "TuringTest.h"
#include "views/GraphView.h"

using namespace db;

class QueryAnalyzerTest : public turing::test::TuringTest {
public:
    void initialize() override {
        SystemManager& sysMan = _db.getSystemManager();
        // XXX: Bug (I think), does not work unless set to "simple"
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
        .expectErrorMessage(
            "Variable 'pos_int' of type Int64 cannot be compared to value of type UInt64"
        )
        .execute();

    // Query Int64 with Double
    tester.query("MATCH n:Typer{pos_int = 256.} return n")
        .expectError()
        .expectErrorMessage(
            "Variable 'pos_int' of type Int64 cannot be compared to value of type Double"
        )
        .execute();

    // Query Int64 with String
    tester.query("MATCH n:Typer{pos_int = \"256\"} return n")
        .expectError()
        .expectErrorMessage(
            "Variable 'pos_int' of type Int64 cannot be compared to value of type String"
        )
        .execute();

    // Query Int64 with Bool
    tester.query("MATCH n:Typer{pos_int = true} return n")
        .expectError()
        .expectErrorMessage(
            "Variable 'pos_int' of type Int64 cannot be compared to value of type Bool"
        )
        .execute();

}

TEST_F(QueryAnalyzerTest, typeCheckUInt64) {
    QueryTester tester {_mem, *_interp};

    // Query UInt64 with Int64
    // NOTE: Coercion implemented
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
        .expectErrorMessage(
            "Variable 'uint' of type UInt64 cannot be compared to value of type Double"
        )
        .execute();

    // Query UInt64 with String
    tester.query("MATCH n:Typer{uint = \"333\"} return n")
        .expectError()
        .expectErrorMessage(
            "Variable 'uint' of type UInt64 cannot be compared to value of type String"
        )
        .execute();

    // Query UInt64 with Bool
    tester.query("MATCH n:Typer{uint = true} return n")
        .expectError()
        .expectErrorMessage(
            "Variable 'uint' of type UInt64 cannot be compared to value of type Bool"
        )
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
        .expectErrorMessage(
            "Variable 'str' of type String cannot be compared to value of type Int64"
        )
        .execute();

    // Query String with UInt64
    tester.query("MATCH n:Typer{str:12u} return n")
        .expectError()
        .expectErrorMessage(
            "Variable 'str' of type String cannot be compared to value of type UInt64"
        )
        .execute();

    // Query String with Bool
    tester.query("MATCH n:Typer{str:true} return n")
        .expectError()
        .expectErrorMessage(
            "Variable 'str' of type String cannot be compared to value of type Bool"
        )
        .execute();
    
    // Query String with Double
    tester.query("MATCH n:Typer{str:20.} return n")
        .expectError()
        .expectErrorMessage(
            "Variable 'str' of type String cannot be compared to value of type Double"
        )
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
        .expectErrorMessage(
            "Variable 'dbl' of type Double cannot be compared to value of type Int64"
        )
        .execute();

    // Query Double with UInt64
    tester.query("MATCH n:Typer{dbl:1618u} return n")
        .expectError()
        .expectErrorMessage(
            "Variable 'dbl' of type Double cannot be compared to value of type UInt64"
        )
        .execute();

    // Query Double with String
    tester.query("MATCH n:Typer{dbl:\"1.618\"} return n")
        .expectError()
        .expectErrorMessage(
            "Variable 'dbl' of type Double cannot be compared to value of type String"
        )
        .execute();
    
    // Query Double with Bool
    tester.query("MATCH n:Typer{dbl:true} return n")
        .expectError()
        .expectErrorMessage(
            "Variable 'dbl' of type Double cannot be compared to value of type Bool"
        )
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
        .expectErrorMessage(
            "Variable 'bool_t' of type Bool cannot be compared to value of type Int64"
        )
        .execute();

    // Query Bool with UInt64
    tester.query("MATCH n:Typer{bool_t:12u} return n")
        .expectError()
        .expectErrorMessage(
            "Variable 'bool_t' of type Bool cannot be compared to value of type UInt64"
        )
        .execute();

    // Query Bool with String
    tester.query("MATCH n:Typer{bool_t:\"true\"} return n")
        .expectError()
        .expectErrorMessage(
            "Variable 'bool_t' of type Bool cannot be compared to value of type String"
        )
        .execute();
    
    // Query Bool with Double
    tester.query("MATCH n:Typer{bool_t:12.} return n")
        .expectError()
        .expectErrorMessage(
            "Variable 'bool_t' of type Bool cannot be compared to value of type Double"
        )
        .execute();
}


TEST_F(QueryAnalyzerTest, checkMatchVariableUniqueness) {
    QueryTester tester {_mem, *_interp};

    tester.query("MATCH n--m return n")
        .expectVector<NodeID>({0})
        .execute();

    tester.query("MATCH n--m--n return n")
        .expectError()
        .expectErrorMessage("Variable n occurs multiple times in MATCH query")
        .execute();

    tester.query("MATCH n:Typer--m:Friend return n")
        .expectVector<NodeID>({0})
        .execute();

    tester.query("MATCH n:Typer--m:Friend--n:Typer return n")
        .expectError()
        .expectErrorMessage("Variable n occurs multiple times in MATCH query")
        .execute();

    tester.query("MATCH n-[e]-m-[r]-p return n")
        .expectVector<NodeID>({})
        .execute();

    tester.query("MATCH n-[e:FRIENDS_WITH]-m-[r:FRIENDS_WITH]-p return n")
        .expectVector<NodeID>({})
        .execute();

    tester.query("MATCH n-[e]-m-[e]-p return n")
        .expectError()
        .expectErrorMessage("Variable e occurs multiple times in MATCH query")
        .execute();

    tester.query("MATCH n-[e:FRIENDS_WITH]-m-[e:FRIENDS_WITH]-p return n")
        .expectError()
        .expectErrorMessage("Variable e occurs multiple times in MATCH query")
        .execute();
    
}

TEST_F(QueryAnalyzerTest, testApproxStringOperator) {
    QueryTester tester {_mem, *_interp};

    // Correct usage of the string approximate operator
    const std::string query1 = "MATCH (n:Typer{str~=\"string property\"}) return n";
    // Incorrect usage of the string approximate operator: comparing string to int
    const std::string query2 = "MATCH (n:Typer{str~=2}) return n";

    tester.query(query1)
        .expectError()
        .expectErrorMessage("OPERATOR '~=' NOT SUPPORTED")
        .execute();

    tester.query(query2)
        .expectError()
        .expectErrorMessage("Operator '~=' must be used with values of type 'String'.")
        .execute();
}
