#include "V3QueryTestRunner.h"

#include <memory>
#include <optional>
#include <span>
#include <string>
#include <variant>
#include <vector>

#include <spdlog/fmt/bundled/format.h>

#include "llvm/Support/raw_ostream.h"
#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"

#include "DBDialect.h"
#include "DBProgramGenerator.h"
#include "NLDialect.h"
#include "NLOutputSink.h"
#include "StorageDialect.h"

#include "BioAssert.h"
#include "CompilerException.h"
#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"
#include "Projection.h"
#include "QueryCommand.h"
#include "SinglePartQuery.h"
#include "TuringException.h"
#include "stmt/ReturnStmt.h"

#include "Graph.h"
#include "ID.h"
#include "ProcedureManager.h"
#include "QueryCallbacks.h"
#include "QueryConfig.h"
#include "QueryInterpreterV3.h"
#include "QueryResultFormatter.h"
#include "QueryState.h"
#include "QueryStatus.h"
#include "QueryTestRunner.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "TuringTestEnv.h"
#include "TuringTime.h"
#include "columns/ColumnVector.h"
#include "dataframe/Dataframe.h"
#include "dataframe/NamedColumn.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

using namespace db;

namespace turing::test {

namespace {

class CollectingNLSink : public NLOutputSink {
public:
    explicit CollectingNLSink(std::vector<std::vector<std::string>>& rows)
        : _rows(rows)
    {
    }

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        QueryResultFormatter::appendChunkRows(_rows, _values, chunks, offset, rowCount);
    }

private:
    std::vector<std::vector<std::string>>& _rows;
    std::vector<std::string> _values;
};

// The v3 result must line up with the shared expect.result, so its header uses the
// same column names the v2 pipeline assigns - the analyzed RETURN projection, read in
// the same order as PipelineGenerator::translateProduceResultsNode.
void collectReturnColumnNames(const CypherAST& ast, std::vector<std::string>& columnNames) {
    columnNames.clear();

    const Projection* projection = nullptr;
    for (const QueryCommand* command : ast.queries()) {
        if (command->getKind() != QueryCommand::Kind::SINGLE_PART_QUERY) {
            continue;
        }

        const SinglePartQuery* query = static_cast<const SinglePartQuery*>(command);
        const ReturnStmt* returnStmt = query->getReturnStmt();
        if (returnStmt) {
            projection = returnStmt->getProjection();
        }
    }

    if (!projection) {
        return;
    }

    for (const Projection::ReturnItem& item : projection->items()) {
        std::optional<std::string_view> name;
        if (Expr* const* exprPtr = std::get_if<Expr*>(&item)) {
            name = projection->getName(*exprPtr);
        } else if (VarDecl* const* declPtr = std::get_if<VarDecl*>(&item)) {
            name = projection->getName(*declPtr);
        }

        if (name) {
            columnNames.emplace_back(*name);
        }
    }
}

void generateMLIRProgram(std::string& out,
                         std::vector<std::string>& columnNames,
                         std::string_view query,
                         GraphView view) {
    out.clear();
    columnNames.clear();

    auto procedures = std::make_unique<ProcedureManager>();
    procedures->init();

    CypherAST ast(procedures.get(), query);
    CypherParser parser(&ast);
    try {
        parser.parse(query);
    } catch (const CompilerException& e) {
        out = fmt::format("PARSE ERROR\n{}", e.what());
        return;
    }

    CypherAnalyzer analyzer(&ast, view);
    analyzer.setV3();
    try {
        analyzer.analyze();
    } catch (const CompilerException& e) {
        out = fmt::format("ANALYZE ERROR\n{}", e.what());
        return;
    }

    collectReturnColumnNames(ast, columnNames);

    mlir::MLIRContext context;
    context.getOrLoadDialect<mlir::func::FuncDialect>();
    context.getOrLoadDialect<mlir::storage::Storage>();
    context.getOrLoadDialect<mlir::db::DB>();
    context.getOrLoadDialect<mlir::nl::NL>();

    mlir::OpBuilder builder(&context);
    mlir::OwningOpRef<mlir::ModuleOp> owningModule = mlir::ModuleOp::create(builder.getUnknownLoc());
    mlir::ModuleOp module = owningModule.get();

    DBProgramGenerator generator(&module);
    try {
        generator.generate(&ast);
    } catch (const CompilerException& e) {
        out = fmt::format("PLAN ERROR\n{}", e.what());
        return;
    } catch (const TuringException& e) {
        out = fmt::format("PLAN ERROR\n{}", e.what());
        return;
    }

    llvm::raw_string_ostream stream(out);
    module.print(stream);
}

}

V3QueryTestResult V3QueryTestRunner::runTest(const QueryTestSpec& spec, const fs::Path& outDir) {
    V3QueryTestResult result;
    result._name = spec._name;

    auto env = turing::test::TuringTestEnv::create(outDir);
    Graph* graph = nullptr;
    {
        SystemAccessor system = env->getSystemManager().accessUnique();
        graph = system.createGraph(spec._graphName);
    }
    SimpleGraph::createSimpleGraph(graph);
    TuringDB* db = &env->getDB();

    std::string mlirOutput;
    std::vector<std::string> columnNames;
    {
        const Transaction tx = graph->openTransaction();
        const GraphView view = tx.viewGraph();
        generateMLIRProgram(mlirOutput, columnNames, spec._query, view);
    }

    QueryConfig queryConfig;

    ChangeID changeID = ChangeID::head();
    if (spec._writeRequired) {
        QueryCallbacks changeNewCallbacks;
        changeNewCallbacks.setOnOutputData([&](const Dataframe* df) {
            NamedColumn* col = df->getColumn(ColumnTag {0});
            bioassert(col, "Column not found");

            ColumnVector<ChangeID>& changeIDs = *static_cast<ColumnVector<ChangeID>*>(col->getColumn());
            bioassert(changeIDs.size() == 1, "Expected 1 change");

            changeID = changeIDs[0];
        });

        const QueryState changeNewState(spec._graphName, &env->getMem(), &queryConfig, &changeNewCallbacks);
        db->query("CHANGE NEW", changeNewState);
    }

    std::vector<std::vector<std::string>> rows;
    CollectingNLSink sink(rows);

    QueryStatus status;
    QueryInterpreterV3 interpreter(&env->getSystemManager());

    const auto queryStart = Clock::now();
    interpreter.execute(status, spec._query, spec._graphName, CommitHash::head(), changeID, &env->getMem(), &sink);
    const auto queryEnd = Clock::now();

    result._timeUs = static_cast<uint64_t>(duration<Microseconds>(queryStart, queryEnd));

    if (spec._writeRequired) {
        QueryCallbacks submitCallbacks;
        const QueryState submitState(spec._graphName, &env->getMem(), &queryConfig, &submitCallbacks,
                                     CommitHash::head(), changeID);
        db->query("CHANGE SUBMIT", submitState);
    }

    QueryTestRunner::normalizeOutput(
        result._resultOutput,
        QueryResultFormatter::formatResultOutput(status, columnNames, rows));
    QueryTestRunner::normalizeOutput(result._mlirOutput, mlirOutput);

    std::string expected;

    QueryTestRunner::normalizeOutput(expected, spec._expectResult);
    result._resultMatched = expected == result._resultOutput;

    QueryTestRunner::normalizeOutput(expected, spec._expectMlir);
    result._mlirMatched = expected == result._mlirOutput;

    return result;
}

}
