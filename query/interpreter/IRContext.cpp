#include "IRContext.h"

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/MLIRContext.h"

#include "DBDialect.h"
#include "NLDialect.h"
#include "StorageDialect.h"

using namespace db;

namespace {

// Uniqued attributes are never freed, so every literal a query emits stays in the
// context until it is rebuilt. The literals come from the query text, which bounds them.
constexpr size_t defaultQueryBytesBudget = 64 * 1024 * 1024;

}

IRContext::IRContext()
    : _queryBytesBudget(defaultQueryBytesBudget)
{
}

IRContext::IRContext(size_t queryBytesBudget)
    : _queryBytesBudget(queryBytesBudget)
{
}

IRContext::~IRContext() {
}

void IRContext::prepareForQuery(std::string_view query) {
    _retainedQueryBytes += query.size();

    const bool overBudget = _retainedQueryBytes > _queryBytesBudget;
    if (!_context || overBudget) {
        _context = std::make_unique<mlir::MLIRContext>(mlir::MLIRContext::Threading::DISABLED);
        _context->getOrLoadDialect<mlir::func::FuncDialect>();
        _context->getOrLoadDialect<mlir::storage::Storage>();
        _context->getOrLoadDialect<mlir::db::DB>();
        _context->getOrLoadDialect<mlir::nl::NL>();

        _retainedQueryBytes = query.size();
    }
}
