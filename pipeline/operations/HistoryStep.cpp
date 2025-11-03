#include "HistoryStep.h"

#include "DataPart.h"
#include "Profiler.h"
#include "versioning/CommitView.h"
#include "versioning/Transaction.h"
#include "versioning/Commit.h"

using namespace db;

HistoryStep::HistoryStep(ColumnVector<std::string>* log)
    : _log(log)
{
}

HistoryStep::~HistoryStep() {
}

void HistoryStep::execute() {
    Profile profile {"HistoryStep::execute"};

    static constexpr auto formatCommitLog = [](std::string& str,
                                               const CommitView& commit) {
        str = fmt::format("Commit: {:x}", commit.hash().get());
        // TODO: re-add
        // if (commit.isHead()) {
        //     str += " (HEAD)";
        // }
        str += "\\n";

        size_t i = 0;
        for (const auto& part : commit.dataparts()) {
            str += fmt::format(" - Part {}: {} nodes, {} edges\\n",
                               i + 1, part->getNodeCount(), part->getEdgeCount());
            i++;
        }
    };

    _log->clear();
    for (const auto& commit : _view->commits()) {
        auto& str = _log->emplace_back();
        formatCommitLog(str, commit);
    }
}

void HistoryStep::describe(std::string& descr) const {
    descr.assign("HistoryStep");
}
