#include "CompilerContext.h"

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/MLIRContext.h"

#include "DBDialect.h"
#include "DBPassPipeline.h"
#include "NLDialect.h"
#include "StorageDialect.h"

using namespace db;

namespace {

// Uniqued attributes are never freed, so the context grows with every query it compiles
constexpr size_t MAX_QUERIES_PER_CONTEXT = 16 * 1024;

}

CompilerContext::CompilerContext()
{
}

CompilerContext::~CompilerContext() {
}

void CompilerContext::prepareForQuery() {
    const bool exhausted = _preparedQueries == MAX_QUERIES_PER_CONTEXT;
    if (!_context || _needsRebuild || exhausted) {
        _passPipeline.reset();

        _context = std::make_unique<mlir::MLIRContext>(mlir::MLIRContext::Threading::DISABLED);
        _context->getOrLoadDialect<mlir::func::FuncDialect>();
        _context->getOrLoadDialect<mlir::storage::Storage>();
        _context->getOrLoadDialect<mlir::db::DB>();
        _context->getOrLoadDialect<mlir::nl::NL>();

        _passPipeline = std::make_unique<DBPassPipeline>(_context.get());

        _preparedQueries = 0;
        _needsRebuild = false;
    }

    _preparedQueries++;
}

void CompilerContext::handleCompileError() {
    _needsRebuild = true;
}
