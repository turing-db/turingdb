#include "HistoryProcedure.h"

#include <span>

#include "ExecutionContext.h"
#include "ProcedureState.h"
#include "Procedure.h"
#include "ProcedureNamespace.h"
#include "columns/ColumnVector.h"
#include "DataPart.h"
#include "views/GraphView.h"

#include "PipelineException.h"

using namespace db;

namespace {

using UInt64Col = ColumnVector<types::UInt64::Primitive>;

struct Data : public ProcedureData {
    std::span<const db::CommitView>::iterator _it {};
};

void writeChunk(Data* data,
                ProcedureState* proc,
                const GraphView* view,
                size_t chunkSize) {
    auto* commitCol = static_cast<ColumnVector<std::string>*>(data->getReturnColumn(0));
    auto* nodeCountCol = static_cast<UInt64Col*>(data->getReturnColumn(1));
    auto* edgeCountCol = static_cast<UInt64Col*>(data->getReturnColumn(2));
    auto* partCountCol = static_cast<UInt64Col*>(data->getReturnColumn(3));

    const size_t dist = std::distance(data->_it, view->commits().end());
    const size_t remaining = std::min(dist, chunkSize);

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
        const CommitView& commit = *data->_it;
        const std::span parts = commit.dataparts();

        if (commitCol) {
            commitCol->push_back(fmt::format("{:x}", commit.hash().get()));
        }

        size_t nodeCount = 0;
        size_t edgeCount = 0;
        for (const auto& part : parts) {
            nodeCount += part->getNodeContainerSize();
            edgeCount += part->getEdgeContainerSize();
        }

        if (nodeCountCol) {
            nodeCountCol->push_back(nodeCount);
        }
        if (edgeCountCol) {
            edgeCountCol->push_back(edgeCount);
        }
        if (partCountCol) {
            partCountCol->push_back(parts.size());
        }

        ++data->_it;
    }

    if (data->_it == view->commits().end()) {
        proc->finish();
    }
}

}

ProcedureData* HistoryProcedure::allocData() {
    return new Data();
}

void HistoryProcedure::deallocData(ProcedureData* data) {
    delete data;
}

void HistoryProcedure::registerProcedure(ProcedureNamespace* ns) {
    Procedure* proc = new Procedure("history");
    proc->setExecuteCallback(&execute);
    proc->setAllocCallback(&allocData);
    proc->setDeallocCallback(&deallocData);
    proc->addReturnValue("commit", ProcedureType::STRING);
    proc->addReturnValue("nodeCount", ProcedureType::UINT_64);
    proc->addReturnValue("edgeCount", ProcedureType::UINT_64);
    proc->addReturnValue("partCount", ProcedureType::UINT_64);
    ns->addProcedure(proc);
}

void HistoryProcedure::execute(ProcedureState* proc) {
    Data& data = proc->data<Data>();
    const ExecutionContext* ctxt = proc->getContext();
    const GraphView& view = ctxt->getGraphView();

    switch (proc->getStep()) {
        case ProcedureState::Step::PREPARE: {
            data._it = view.commits().begin();
            return;
        }
        break;

        case ProcedureState::Step::RESET: {
            return;
        }
        break;

        case ProcedureState::Step::EXECUTE: {
            writeChunk(&data, proc, &view, ctxt->getChunkSize());
            return;
        }
        break;
    }

    throw PipelineException("Unknown procedure step");
}
