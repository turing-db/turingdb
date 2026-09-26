#include "QueryInterpreterV3.h"

#include <optional>
#include <sstream>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/OwningOpRef.h"

#include "DBDialect.h"
#include "DBDialectInterpreter.h"
#include "DBProgramGenerator.h"
#include "ExplainReport.h"
#include "NLSystemContext.h"
#include "NLDialect.h"
#include "StorageDialect.h"
#include "iterators/ChunkConfig.h"

#include "CypherAST.h"
#include "CypherASTDumper.h"
#include "QueryCommand.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"

#include "LocalMemory.h"
#include "ProcedureContext.h"
#include "SystemManager.h"
#include "SystemAccessor.h"
#include "versioning/CommitBuilder.h"
#include "versioning/CommitWriteBuffer.h"
#include "writers/MetadataBuilder.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "CompilerException.h"
#include "FatalException.h"
#include "TuringException.h"
#include "TuringTime.h"

using namespace db;

namespace {

// Takes back what a statement staged and interned unless it runs to the end, so a query
// that fails leaves nothing of itself for the commit
class StatementWrites {
public:
    StatementWrites(CommitWriteBuffer* writeBuffer, MetadataBuilder* metadataBuilder)
        : _writeBuffer(writeBuffer),
        _metadataBuilder(metadataBuilder)
    {
        if (_writeBuffer) {
            _writeBuffer->beginStatement();
        }

        if (_metadataBuilder) {
            _metadataBuilder->beginStatement();
        }
    }

    ~StatementWrites() {
        if (_writeBuffer) {
            _writeBuffer->rollbackStatement();
        }

        if (_metadataBuilder) {
            _metadataBuilder->rollbackStatement();
        }
    }

    StatementWrites(const StatementWrites&) = delete;
    StatementWrites& operator=(const StatementWrites&) = delete;

    void keep() {
        if (_writeBuffer) {
            _writeBuffer->endStatement();
        }

        _writeBuffer = nullptr;
        _metadataBuilder = nullptr;
    }

private:
    CommitWriteBuffer* _writeBuffer {nullptr};
    MetadataBuilder* _metadataBuilder {nullptr};
};

// A command - COMMIT, CHANGE SUBMIT, a load - can replace the change's buffer as it runs,
// and is no statement whose writes a failure takes back
bool writesOnlyThroughQueries(const CypherAST& ast) {
    for (const QueryCommand* query : ast.queries()) {
        const QueryCommand::Kind kind = query->getKind();
        const bool isQuery = kind == QueryCommand::Kind::SINGLE_PART_QUERY
                          || kind == QueryCommand::Kind::UNION_QUERY;

        if (!isQuery) {
            return false;
        }
    }

    return true;
}

}

QueryInterpreterV3::QueryInterpreterV3(SystemManager* sysMan)
    : _sysMan(sysMan)
{
}

QueryInterpreterV3::~QueryInterpreterV3() {
}

void QueryInterpreterV3::execute(QueryStatus& status,
                                 std::string_view query,
                                 std::string_view graphName,
                                 CommitHash hash,
                                 ChangeID changeID,
                                 LocalMemory* mem,
                                 NLOutputSink* sink) {
    const TimePoint start = Clock::now();
    executeImpl(status, query, graphName, hash, changeID, mem, sink);
    const TimePoint end = Clock::now();

    status.setTotalTime(end - start);
}

void QueryInterpreterV3::executeImpl(QueryStatus& status,
                                     std::string_view query,
                                     std::string_view graphName,
                                     CommitHash hash,
                                     ChangeID changeID,
                                     LocalMemory* mem,
                                     NLOutputSink* sink) {
    SystemAccessor system = _sysMan->accessShared();

    auto txRes = system.openTransaction(graphName, hash, changeID);
    if (!txRes) {
        switch (txRes.error().getType()) {
            case ChangeErrorType::GRAPH_NOT_FOUND: {
                status.setStatus(QueryStatus::Status::GRAPH_NOT_FOUND);
                return;
            }
            break;
            case ChangeErrorType::CHANGE_NOT_FOUND: {
                status.setStatus(QueryStatus::Status::CHANGE_NOT_FOUND);
                return;
            }
            break;
            case ChangeErrorType::COMMIT_NOT_LOADED: {
                status.setStatus(QueryStatus::Status::COMMIT_NOT_LOADED);
                status.setMessage(txRes.error().fmtMessage());
                return;
            }
            break;
            default: {
                status.setStatus(QueryStatus::Status::COMMIT_NOT_FOUND);
                return;
            }
            break;
        }
    }

    GraphView view = txRes->viewGraph();

    CommitWriteBuffer* writeBuffer = nullptr;
    MetadataBuilder* metadataBuilder = nullptr;
    CommitBuilder* commitBuilder = nullptr;
    if (txRes->writingPendingCommit()) {
        commitBuilder = txRes->get<PendingCommitWriteTx>().commitBuilder();
        writeBuffer = &commitBuilder->writeBuffer();
        metadataBuilder = &commitBuilder->metadata();

        view.setChangeDeletions(&writeBuffer->deletedNodes(), &writeBuffer->deletedEdges());
    }

    // Filled for every query, since which statement this one is only becomes known
    // once it is parsed. An ordinary query never reads it.
    NLSystemContext systemContext;
    systemContext.setSystemManager(_sysMan);
    systemContext.setAccessor(&system);
    systemContext.setTransaction(&txRes.value());
    systemContext.setCommitBuilder(commitBuilder);
    systemContext.setGraphName(graphName);

    CypherAST ast(system.getProcedures(), query);
    CypherParser parser(&ast);
    try {
        parser.parse(query);
    } catch (const CompilerException& e) {
        status.setStatus(QueryStatus::Status::PARSE_ERROR);
        status.setMessage(e.what());
        return;
    } catch (const std::exception& e) {
        status.setStatus(QueryStatus::Status::PARSE_ERROR);
        status.setMessage(std::string("Unexpected exception: ") + e.what());
        return;
    } catch (...) {
        status.setStatus(QueryStatus::Status::PARSE_ERROR);
        status.setMessage("Unknown exception occurred");
        return;
    }

    CypherAnalyzer analyzer(&ast, view);
    try {
        analyzer.analyze();
    } catch (const CompilerException& e) {
        status.setStatus(QueryStatus::Status::ANALYZE_ERROR);
        status.setMessage(e.what());
        return;
    } catch (const std::exception& e) {
        status.setStatus(QueryStatus::Status::ANALYZE_ERROR);
        status.setMessage(std::string("Unexpected exception: ") + e.what());
        return;
    } catch (...) {
        status.setStatus(QueryStatus::Status::ANALYZE_ERROR);
        status.setMessage("Unknown exception occurred");
        return;
    }

    std::optional<ExplainReport> explainReport;
    const ExplainRequest* const explainRequest = ast.getExplainRequest();
    if (explainRequest) {
        explainReport.emplace(explainRequest);

        if (explainRequest->isRequested(ExplainStage::AST)) {
            std::ostringstream tree;
            CypherASTDumper dumper(&ast);
            dumper.dump(tree);

            explainReport->addText(ExplainRequest::getStageName(ExplainStage::AST), tree.str());
        }
    }

    ExplainReport* const explain = explainRequest ? &explainReport.value() : nullptr;

    mlir::MLIRContext context;
    context.getOrLoadDialect<mlir::func::FuncDialect>();
    context.getOrLoadDialect<mlir::storage::Storage>();
    context.getOrLoadDialect<mlir::db::DB>();
    context.getOrLoadDialect<mlir::nl::NL>();

    mlir::OpBuilder builder(&context);
    mlir::OwningOpRef<mlir::ModuleOp> owningModule = mlir::ModuleOp::create(builder.getUnknownLoc());
    mlir::ModuleOp module = owningModule.get();

    const bool hasPendingWrites = writeBuffer
                                  && (writeBuffer->containsCreates()
                                      || writeBuffer->containsDeletes()
                                      || writeBuffer->containsUpdates());

    const mlir::db::DBPassContext passContext {._view = &view, ._hasPendingWrites = hasPendingWrites};

    DBProgramGenerator generator(&module, explain, passContext);
    try {
        generator.generate(&ast);
    } catch (const CompilerException& e) {
        status.setStatus(QueryStatus::Status::PLAN_ERROR);
        status.setMessage(e.what());
        return;
    } catch (const FatalException& e) {
        status.setStatus(QueryStatus::Status::PLAN_ERROR);
        status.setMessage(std::string("Unexpected exception: ") + e.what());
        return;
    } catch (const TuringException& e) {
        // The generator rejects unsupported constructs with a plain TuringException:
        // those are deliberate user-input rejections, not internal errors
        status.setStatus(QueryStatus::Status::PLAN_ERROR);
        status.setMessage(e.what());
        return;
    } catch (const std::exception& e) {
        status.setStatus(QueryStatus::Status::PLAN_ERROR);
        status.setMessage(std::string("Unexpected exception: ") + e.what());
        return;
    } catch (...) {
        status.setStatus(QueryStatus::Status::PLAN_ERROR);
        status.setMessage("Unknown exception occurred");
        return;
    }

    ProcedureContext procedureContext;
    procedureContext.setGraph(system.getGraph(graphName));
    procedureContext.setGraphView(&view);
    procedureContext.setTransaction(&txRes.value());
    procedureContext.setProcedures(system.getProcedures());
    procedureContext.setChunkSize(_chunkSize);
    procedureContext.setListBuffer(&mem->listBuffer());

    DBDialectInterpreter interpreter(module,
                                     &view,
                                     sink,
                                     mem,
                                     _chunkSize,
                                     writeBuffer,
                                     metadataBuilder,
                                     &procedureContext,
                                     &systemContext);
    const bool guardsTheStatement = writesOnlyThroughQueries(ast);
    StatementWrites statementWrites(guardsTheStatement ? writeBuffer : nullptr,
                                    guardsTheStatement ? metadataBuilder : nullptr);

    try {
        if (explain) {
            interpreter.explain(*explain);
            reportExplain(*explain, &context, &view, mem, sink);
        } else {
            interpreter.run();
        }

        statementWrites.keep();
    } catch (const CompilerException& e) {
        status.setStatus(QueryStatus::Status::EXEC_ERROR);
        status.setMessage(e.what());
        return;
    } catch (const FatalException& e) {
        status.setStatus(QueryStatus::Status::EXEC_ERROR);
        status.setMessage(std::string("Unexpected exception: ") + e.what());
        return;
    } catch (const TuringException& e) {
        // What a loop reads outside the graph rejects malformed input with a plain
        // TuringException - a CSV record whose fields do not match the file's, say:
        // those are deliberate user-input rejections, not internal errors
        status.setStatus(QueryStatus::Status::EXEC_ERROR);
        status.setMessage(e.what());
        return;
    } catch (const std::exception& e) {
        status.setStatus(QueryStatus::Status::EXEC_ERROR);
        status.setMessage(std::string("Unexpected exception: ") + e.what());
        return;
    } catch (...) {
        status.setStatus(QueryStatus::Status::EXEC_ERROR);
        status.setMessage("Unknown exception occurred");
        return;
    }
}

void QueryInterpreterV3::reportExplain(const ExplainReport& report,
                                       mlir::MLIRContext* context,
                                       const GraphView* view,
                                       LocalMemory* memory,
                                       NLOutputSink* sink) {
    mlir::OpBuilder builder(context);
    mlir::OwningOpRef<mlir::ModuleOp> owningModule = mlir::ModuleOp::create(builder.getUnknownLoc());
    mlir::ModuleOp module = owningModule.get();

    DBProgramGenerator generator(&module);
    generator.generateExplainResult(report);

    DBDialectInterpreter interpreter(module, view, sink, memory);
    interpreter.run();
}
