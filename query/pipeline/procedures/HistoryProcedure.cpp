#include "HistoryProcedure.h"

#include <span>

#include "ExecutionContext.h"
#include "procedures/Procedure.h"
#include "columns/ColumnVector.h"
#include "DataPart.h"
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

            for (size_t i = 0; i < remaining; ++i) {
                const CommitView& commit = *data._it;
                const CommitHash& hash = commit.hash();
                const std::span parts = commit.dataparts();

                if (commitCol) {
                    commitCol->push_back(fmt::format("{:x}", hash.get()));
                }

                if (nodeCountCol) {
                    size_t nodeCount = 0;
                    for (const auto& part : parts) {
                        nodeCount += part->getNodeContainerSize();
                    }
                    nodeCountCol->push_back(nodeCount);
                }

                if (edgeCountCol) {
                    size_t edgeCount = 0;
                    for (const auto& part : parts) {
                        edgeCount += part->getEdgeContainerSize();
                    }
                    edgeCountCol->push_back(edgeCount);
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

