#include "HistoryProcedure.h"

#include "ExecutionContext.h"
#include "ProcedureState.h"
#include "Procedure.h"
#include "ProcedureNamespace.h"
#include "Graph.h"
#include "SystemManager.h"
#include "versioning/VersionController.h"
#include "columns/ColumnVector.h"

using namespace db;

namespace {

using UInt64Col = ColumnVector<types::UInt64::Primitive>;

struct Data : public ProcedureData {
    const Commit* _commit {nullptr};
};

void writeChunk(Data* data,
                ProcedureState* proc,
                size_t chunkSize) {
    size_t count = 0;

    auto* commitCol = static_cast<ColumnVector<std::string>*>(data->getReturnColumn(0));
    auto* nodeCountCol = static_cast<UInt64Col*>(data->getReturnColumn(1));
    auto* edgeCountCol = static_cast<UInt64Col*>(data->getReturnColumn(2));
    auto* partCountCol = static_cast<UInt64Col*>(data->getReturnColumn(3));

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

    // Traverse through the commit history chain until we reach the root commit or
    // we have outputed a chunksize worth of commits.
    while (data->_commit && count < chunkSize) {
        const Commit* commit = data->_commit;
        const bool isHead = (count == 0);

        if (commitCol) {
            commitCol->push_back(isHead
                ? fmt::format("{:x}(HEAD)", commit->hash().get())
                : fmt::format("{:x}", commit->hash().get()));
        }
        if (nodeCountCol) {
            nodeCountCol->push_back(commit->getNumNodes());
        }
        if (edgeCountCol) {
            edgeCountCol->push_back(commit->getNumEdges());
        }
        if (partCountCol) {
            partCountCol->push_back(commit->getNumDataParts());
        }

        ++count;
        data->_commit = data->_commit->getPreviousCommit();
    }

    if (!data->_commit) {
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
    const std::string& graphName = ctxt->getGraphName().data();
    const VersionController& controller = ctxt->getSystemManager()->getGraph(graphName)->getVersionController();

    const size_t chunkSize = ctxt->getChunkSize();

    switch (proc->getStep()) {
        case ProcedureState::Step::PREPARE:
            data._commit = controller.getCommitSafe(ctxt->getGraphView().headCommitHash());
            bioassert(data._commit, "headCommitHash not found");
        break;

        case ProcedureState::Step::RESET:
        break;

        case ProcedureState::Step::EXECUTE:
            writeChunk(&data, proc, chunkSize);
        break;
    }
}
