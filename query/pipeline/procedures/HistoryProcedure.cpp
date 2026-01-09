#include "HistoryProcedure.h"

#include <span>

#include "ExecutionContext.h"
#include "procedures/Procedure.h"
#include "columns/ColumnVector.h"
#include "DataPart.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "PipelineException.h"

using namespace db;

namespace {

struct Data : public ProcedureData {
    std::span<const db::CommitView>::iterator _it;
};

}

std::unique_ptr<ProcedureData> HistoryProcedure::allocData() {
    return std::make_unique<Data>();
}

void HistoryProcedure::execute(Procedure& proc) {
    Data& data = proc.data<Data>();
    const ExecutionContext* ctxt = proc.ctxt();
    const GraphView& _view = ctxt->getGraphView();

    Column* rawCommitCol = data.getReturnColumn(0);
    Column* rawNodeCountCol = data.getReturnColumn(1);
    Column* rawEdgeCountCol = data.getReturnColumn(2);
    Column* rawPartCountCol = data.getReturnColumn(3);

    auto* commitCol = static_cast<ColumnVector<std::string>*>(rawCommitCol);
    auto* nodeCountCol = static_cast<ColumnVector<types::UInt64::Primitive>*>(rawNodeCountCol);
    auto* edgeCountCol = static_cast<ColumnVector<types::UInt64::Primitive>*>(rawEdgeCountCol);
    auto* partCountCol = static_cast<ColumnVector<types::UInt64::Primitive>*>(rawPartCountCol);

    switch (proc.step()) {
        case Procedure::Step::PREPARE: {
            data._it = _view.commits().begin();
            return;
        }

        case Procedure::Step::RESET: {
            return;
        }

        case Procedure::Step::EXECUTE: {
            size_t remaining = std::distance(data._it, _view.commits().end());
            remaining = std::min(remaining, ctxt->getChunkSize());

            if (commitCol) {
                commitCol->clear();
            }

            if (nodeCountCol) {
                nodeCountCol->clear();
            }

            if (edgeCountCol) {
                edgeCountCol->clear();
            }

            if (partCountCol) {
                partCountCol->clear();
            }

            size_t prevTotalNodeCount {0};
            size_t prevTotalEdgeCount {0};

            for (size_t i = 0; i < remaining; ++i) {
                const CommitView& commit = *data._it;
                const GraphReader commitReader = commit.openTransaction().readGraph();
                const CommitHash& hash = commit.hash();
                const std::span parts = commit.dataparts();

                if (commitCol) {
                    commitCol->push_back(fmt::format("{:x}", hash.get()));
                }

                if (nodeCountCol) {
                    const size_t totalNodeCount = commitReader.getNodeCount();
                    const size_t thisPartNodeCount = totalNodeCount - prevTotalNodeCount;

                    nodeCountCol->push_back(thisPartNodeCount);

                    prevTotalNodeCount = totalNodeCount;
                }

                if (edgeCountCol) {
                    const size_t totalEdgeCount = commitReader.getEdgeCount();
                    const size_t thisPartedgeCount = totalEdgeCount - prevTotalEdgeCount;

                    edgeCountCol->push_back(thisPartedgeCount);

                    prevTotalEdgeCount = totalEdgeCount;
                }

                if (partCountCol) {
                    partCountCol->push_back(parts.size());
                }

                ++data._it;
            }

            if (data._it == _view.commits().end()) {
                proc.finish();
            }

            return;
        }
    }

    throw PipelineException("Unknown procedure step");
}

