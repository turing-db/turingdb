#include "HistoryStep.h"

#include "DataPart.h"
#include "versioning/CommitView.h"

using namespace db;

HistoryStep::HistoryStep(ColumnVector<std::string>* log)
    : _log(log)
{
}

HistoryStep::~HistoryStep() {
}

void HistoryStep::execute() {
    _log->clear();
    for (const auto& commit : _view->commits()) {
        auto& str = _log->emplace_back();
        str += fmt::format("Commit: {:x}", commit.hash().get());
        if (commit.isHead()) {
            str += " (HEAD)";
        }
        str += "\n";

        size_t i = 0;
        for (const auto& part : commit.dataparts()) {
            str += fmt::format(" - Part {}: {} nodes, {} edges\n",
                               i + 1, part->getNodeCount(), part->getEdgeCount());
            i++;
        }
    }
}

void HistoryStep::describe(std::string& descr) const {
    descr.assign("HistoryStep");
}
