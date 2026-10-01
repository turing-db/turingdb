#include "CompilerContext.h"

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/MLIRContext.h"

#include "DBDialect.h"
#include "DBPassPipeline.h"
#include "NLDialect.h"
#include "StorageDialect.h"

using namespace db;

CompilerContext::CompilerContext()
{
}

CompilerContext::~CompilerContext() {
}

void CompilerContext::prepareForQuery() {
    if (!_context || _needsRebuild) {
        _passPipeline.reset();

        _context = std::make_unique<mlir::MLIRContext>(mlir::MLIRContext::Threading::DISABLED);
        _context->getOrLoadDialect<mlir::func::FuncDialect>();
        _context->getOrLoadDialect<mlir::storage::Storage>();
        _context->getOrLoadDialect<mlir::db::DB>();
        _context->getOrLoadDialect<mlir::nl::NL>();

        _passPipeline = std::make_unique<DBPassPipeline>(_context.get());

        _needsRebuild = false;
    }
}

void CompilerContext::handleCompileError() {
    _needsRebuild = true;
}
