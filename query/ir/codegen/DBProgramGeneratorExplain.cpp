#include "DBProgramGenerator.h"

#include <algorithm>
#include <memory>
#include <span>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/ValueRange.h"
#include "mlir/Pass/Pass.h"
#include "mlir/Pass/PassManager.h"
#include "llvm/ADT/SmallVector.h"
#include "llvm/ADT/StringRef.h"

#include "DBOps.h"
#include "DBPassPipeline.h"
#include "DBTypes.h"
#include "StorageTypes.h"
#include "ExplainReport.h"

#include "VariableDependencyGraphDumper.h"

#include "FatalException.h"
#include "TuringException.h"

using namespace db;

namespace {

// The stage a dump of one pass is reported under: "after fuse_scan_edges", or "after
// trim_unread_columns 2" for a run of a pass the pipeline runs more than once
void makePassLabel(std::string_view selector, std::string_view passName, size_t run, std::string& label) {
    label = selector;
    label += passName;

    if (run > 0) {
        label += ' ';
        label += std::to_string(run);
    }
}

// Which run of its pass the pipeline position is, counted from 1, or 0 for a pass the
// pipeline runs once
size_t passRunNumber(std::span<const std::string_view> pipelinePasses, size_t passIndex) {
    const std::string_view passName = pipelinePasses[passIndex];
    if (std::ranges::count(pipelinePasses, passName) == 1) {
        return 0;
    }

    return static_cast<size_t>(std::ranges::count(pipelinePasses.first(passIndex + 1), passName));
}

}

void DBProgramGenerator::generateExplainResult(const ExplainReport& report) {
    createMain({});

    const mlir::Location uloc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType stringColumn = allocColumnType(mlir::storage::StringType::get(_mlirCtxt));

    const std::vector<std::string>& stages = report.getStages();
    const std::vector<std::string>& dumps = report.getDumps();

    const llvm::SmallVector<llvm::StringRef> stageNames(stages.begin(), stages.end());
    const llvm::SmallVector<llvm::StringRef> stageDumps(dumps.begin(), dumps.end());

    mlir::db::Explain explain = _opBuilder.create<mlir::db::Explain>(uloc,
                                                                     stringColumn,
                                                                     stringColumn,
                                                                     _opBuilder.getStrArrayAttr(stageNames),
                                                                     _opBuilder.getStrArrayAttr(stageDumps));

    _opBuilder.create<mlir::db::Output>(uloc,
                                        mlir::ValueRange {explain.getStages(), explain.getDumps()},
                                        _opBuilder.getStrArrayAttr({"stage", "dump"}));

    _opBuilder.create<mlir::func::ReturnOp>(uloc);
}

void DBProgramGenerator::explainModule(ExplainStage stage) {
    if (!_explain) {
        return;
    }

    _explain->addModule(stage, *_module);
}

void DBProgramGenerator::explainDependencyGraph() {
    if (!_explain || !_explain->isRequested(ExplainStage::VDG)) {
        return;
    }

    _explainedParts++;

    std::ostringstream graph;
    VariableDependencyGraphDumper::dumpMermaid(_vdg, graph);

    const std::string label = "vdg " + std::to_string(_explainedParts);
    _explain->addText(label, graph.str());
}

bool DBProgramGenerator::explainsPasses() const {
    if (!_explain) {
        return false;
    }

    return _explain->isRequested(ExplainStage::PASSES) || _explain->hasNamedPasses();
}

void DBProgramGenerator::runExplainedPasses() {
    throwOnUnknownExplainPass();

    const bool printsEveryPass = _explain->isRequested(ExplainStage::PASSES);

    std::vector<std::string_view> pipelinePasses;
    DBPassPipeline::fillPassNames(pipelinePasses);

    std::string module;
    ExplainReport::renderModule(*_module, module);

    for (size_t passIndex = 0; passIndex < pipelinePasses.size(); passIndex++) {
        std::unique_ptr<mlir::Pass> pass = DBPassPipeline::createPass(passIndex, &_passContext);
        const std::string_view passName = pipelinePasses[passIndex];
        const size_t run = passRunNumber(pipelinePasses, passIndex);

        if (_explain->isPassPrintedBefore(passName)) {
            std::string label;
            makePassLabel("before ", passName, run, label);

            _explain->addText(label, module);
        }

        mlir::PassManager passManager(_mlirCtxt);
        passManager.enableVerifier(false);
        passManager.addPass(std::move(pass));

        if (mlir::failed(passManager.run(*_module))) {
            throw FatalException("DB pass pipeline failed");
        }

        std::string rewritten;
        ExplainReport::renderModule(*_module, rewritten);

        const bool changedTheModule = rewritten != module;
        if (_explain->isPassPrintedAfter(passName) || (printsEveryPass && changedTheModule)) {
            std::string label;
            makePassLabel("after ", passName, run, label);

            _explain->addText(label, rewritten);
        }

        module = rewritten;
    }
}

void DBProgramGenerator::throwOnUnknownExplainPass() const {
    std::vector<std::string_view> pipelinePasses;
    DBPassPipeline::fillPassNames(pipelinePasses);

    const std::string_view unknownPass = _explain->findUnknownPass(pipelinePasses);
    if (unknownPass.empty()) {
        return;
    }

    std::string message = "Unknown EXPLAIN pass '";
    message += unknownPass;
    message += "'. The pipeline runs: ";

    for (size_t index = 0; index < pipelinePasses.size(); index++) {
        if (index > 0) {
            message += ", ";
        }

        message += pipelinePasses[index];
    }

    throw TuringException(std::move(message));
}
