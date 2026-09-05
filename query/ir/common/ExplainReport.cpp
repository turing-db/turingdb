#include "ExplainReport.h"

#include "llvm/Support/raw_ostream.h"
#include "mlir/IR/Operation.h"
#include "mlir/IR/OperationSupport.h"

using namespace db;

ExplainReport::ExplainReport(const ExplainRequest* request)
    : _request(request)
{
}

ExplainReport::~ExplainReport() {
}

void ExplainReport::addModule(ExplainStage stage, mlir::Operation* module) {
    if (!isRequested(stage)) {
        return;
    }

    addModule(ExplainRequest::getStageName(stage), module);
}

void ExplainReport::addModule(std::string_view label, mlir::Operation* module) {
    std::string text;
    renderModule(module, text);

    addText(label, text);
}

void ExplainReport::addText(std::string_view label, std::string_view text) {
    _stages.emplace_back(label);
    _dumps.emplace_back(text);
}

void ExplainReport::renderModule(mlir::Operation* module, std::string& text) {
    llvm::raw_string_ostream out(text);

    mlir::OpPrintingFlags flags;
    flags.assumeVerified();

    module->print(out, flags);
}
