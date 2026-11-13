#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/Parser/Parser.h"

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "reader/GraphReader.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "IRTestRows.h"
#include "LocalMemory.h"
#include "NLDialect.h"
#include "NLInterpreter.h"
#include "StorageDialect.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

// head and last over a nullable list: laid out over rows, made a constant by nl.row_constant,
// and both of those holding a null, which a split by a null delimiter answers
const char* const nullableListHeadProgram = R"mlir(
func.func @main() {
  %word = nl.constant("a,b" : !storage.string)
  %comma = nl.constant("," : !storage.string)
  %bad = nl.constant("x" : !storage.string)

  %words = nl.broadcast_constant %word : (!nl.chunk<!storage.string>) -> !nl.chunk<!storage.nullable<!storage.string>>
  %commas = nl.broadcast_constant %comma : (!nl.chunk<!storage.string>) -> !nl.chunk<!storage.nullable<!storage.string>>
  %noNumber = nl.to_integer %bad : (!nl.chunk<!storage.string>) -> !nl.chunk<!storage.nullable<i64>>
  %noDelimiter = nl.to_string %noNumber : (!nl.chunk<!storage.nullable<i64>>) -> !nl.chunk<!storage.nullable<!storage.string>>

  %parts = nl.split %words, %commas : (!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>) -> !nl.chunk<!storage.nullable<!storage.list<!storage.string>>>
  %noParts = nl.split %words, %noDelimiter : (!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>) -> !nl.chunk<!storage.nullable<!storage.list<!storage.string>>>
  %partList = nl.row_constant %parts : <!storage.nullable<!storage.list<!storage.string>>>
  %noPartList = nl.row_constant %noParts : <!storage.nullable<!storage.list<!storage.string>>>

  %constHead = nl.head %partList : (!nl.chunk<!storage.nullable<!storage.list<!storage.string>>>) -> !nl.chunk<!storage.any>
  %constLast = nl.last %partList : (!nl.chunk<!storage.nullable<!storage.list<!storage.string>>>) -> !nl.chunk<!storage.any>
  %nullConstHead = nl.head %noPartList : (!nl.chunk<!storage.nullable<!storage.list<!storage.string>>>) -> !nl.chunk<!storage.any>
  %nullConstLast = nl.last %noPartList : (!nl.chunk<!storage.nullable<!storage.list<!storage.string>>>) -> !nl.chunk<!storage.any>
  %rowHead = nl.head %parts : (!nl.chunk<!storage.nullable<!storage.list<!storage.string>>>) -> !nl.chunk<!storage.any>
  %rowLast = nl.last %parts : (!nl.chunk<!storage.nullable<!storage.list<!storage.string>>>) -> !nl.chunk<!storage.any>
  %nullRowHead = nl.head %noParts : (!nl.chunk<!storage.nullable<!storage.list<!storage.string>>>) -> !nl.chunk<!storage.any>
  %nullRowLast = nl.last %noParts : (!nl.chunk<!storage.nullable<!storage.list<!storage.string>>>) -> !nl.chunk<!storage.any>

  nl.output(%constHead, %constLast, %nullConstHead, %nullConstLast, %rowHead, %rowLast, %nullRowHead, %nullRowLast) names ["constHead", "constLast", "nullConstHead", "nullConstLast", "rowHead", "rowLast", "nullRowHead", "nullRowLast"] : !nl.chunk<!storage.any>, !nl.chunk<!storage.any>, !nl.chunk<!storage.any>, !nl.chunk<!storage.any>, !nl.chunk<!storage.any>, !nl.chunk<!storage.any>, !nl.chunk<!storage.any>, !nl.chunk<!storage.any>
  func.return
}
)mlir";

}

class NullableConstantListHeadTest : public TuringTest {
protected:
    void initialize() override {
        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());

        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void runProgram(const char* programText, RowSink& sink) {
        mlir::MLIRContext context;
        context.getOrLoadDialect<mlir::func::FuncDialect>();
        context.getOrLoadDialect<mlir::storage::Storage>();
        context.getOrLoadDialect<mlir::nl::NL>();

        const mlir::ParserConfig parserConfig(&context);
        mlir::OwningOpRef<mlir::ModuleOp> module = mlir::parseSourceString<mlir::ModuleOp>(programText, parserConfig);
        ASSERT_TRUE(module);

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();

        LocalMemory memory;
        NLInterpreter interpreter(*module, &reader.getView(), &sink, &memory);
        interpreter.run();
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, expected) << "query: " << query << "\ngot:\n" << actualText;
    }

    std::unique_ptr<Graph> _graph;
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    const std::string _graphName {"simpledb"};
};

TEST_F(NullableConstantListHeadTest, headAndLastReadNullableLists) {
    RowSink sink;
    runProgram(nullableListHeadProgram, sink);

    const Rows& rows = sink.rows();
    std::string text;
    describeRows(rows, text);

    const Rows expected {{"a", "b", "null", "null", "a", "b", "null", "null"}};
    EXPECT_EQ(rows, expected) << text;
}

TEST_F(NullableConstantListHeadTest, headAndLastOfASplitInCypher) {
    expectRows("UNWIND [1, 2] AS x RETURN head(split('a,b', ',')), last(split('a,b', ','))", {{"a", "b"}, {"a", "b"}});
    expectRows("UNWIND [1, 2] AS x RETURN head(split('a,b', null)), last(split('a,b', null))", {{"null", "null"}, {"null", "null"}});
    expectRows("RETURN head(null), last(null)", {{"null", "null"}});
}
