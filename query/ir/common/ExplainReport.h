#pragma once

#include <stddef.h>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "ExplainRequest.h"

namespace mlir {
class Operation;
}

namespace db {

// The dumps an EXPLAIN prefix collects while its query is compiled, in the order the
// compiler produced them. Every stage is offered to the report and the ones the
// request did not ask for are dropped here, so a caller dumps unconditionally.
class ExplainReport {
public:
    explicit ExplainReport(const ExplainRequest* request);
    ~ExplainReport();

    bool isRequested(ExplainStage stage) const { return _request->isRequested(stage); }
    bool isPassPrintedBefore(std::string_view passName) const { return _request->isPassPrintedBefore(passName); }
    bool isPassPrintedAfter(std::string_view passName) const { return _request->isPassPrintedAfter(passName); }
    bool hasNamedPasses() const { return _request->hasNamedPasses(); }

    std::string_view findUnknownPass(std::span<const std::string_view> passes) const { return _request->findUnknownPass(passes); }

    // Renders the operation under the stage's own name, dropping the stages the
    // request did not ask for
    void addModule(ExplainStage stage, mlir::Operation* module);

    // Renders under a label the caller spells out: which query part a dependency
    // graph belongs to, which pass a dump sits before or after
    void addModule(std::string_view label, mlir::Operation* module);
    void addText(std::string_view label, std::string_view text);

    size_t getRowCount() const { return _stages.size(); }
    const std::vector<std::string>& getStages() const { return _stages; }
    const std::vector<std::string>& getDumps() const { return _dumps; }

    static void renderModule(mlir::Operation* module, std::string& text);

private:
    const ExplainRequest* _request {nullptr};
    std::vector<std::string> _stages;
    std::vector<std::string> _dumps;
};

}
