#include "ShowIndexesProcedure.h"

#include "ExecutionContext.h"
#include "FatalException.h"
#include "Graph.h"
#include "ID.h"
#include "ProcedureData.h"
#include "ProcedureNamespace.h"
#include "SystemManager.h"
#include "columns/ColumnVector.h"
#include "metadata/PropertyType.h"
#include "procedures/Procedure.h"
#include "procedures/ProcedureState.h"
#include "procedures/ProcedureTypeVector.h"

#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "versioning/VersionController.h"
#include <string_view>

using namespace db;

namespace {

struct Data : public ProcedureData {
    const Commit* _commit {nullptr};
};

void writeChunk(Data* data, ProcedureState* procedure, size_t chunkSize) {
    auto* names = dynamic_cast<ColumnVector<std::string_view>*>(data->getReturnColumn(0));
    // auto* propIDs = dynamic_cast<ColumnVector<PropertyTypeID>*>(data->getReturnColumn(1));
    // auto* propNames = dynamic_cast<ColumnVector<std::string>*>(data->getReturnColumn(2));
    auto* sizes = dynamic_cast<ColumnVector<types::UInt64::Primitive>*>(data->getReturnColumn(1));

    // Not all columns may be YIELDed
    if (names) {
        names->clear();
    }
    // if (propIDs) {
    //     propIDs->clear();
    // }
    // if (propNames) {
    //     propNames->clear();
    // }
    if (sizes) {
        sizes->clear();
    }

    const Commit* commit = data->_commit;
    bioassert(commit, "Invalid commit.");

    const CommitHistory& history = commit->history();
    const CommitHistory::IndexSpan indexes = history.validIndexes();

    // TODO: Add chunking approach
    if (indexes.size() > chunkSize) {
        throw FatalException("Too many indexes.");
    }

    for (const WeakArc<Index>& index : indexes) {
        if (names) {
            const std::string_view name = index->name();
            names->push_back(name);
        }

        if (sizes) {
            sizes->push_back(index->size());
        }
    }
}

void prepare(ProcedureState* proc) {
    Data& data = proc->data<Data>();
    ExecutionContext* ctxt = proc->getContext();

    const std::string graphName = std::string(ctxt->getGraphName());

    const SystemManager* sysMan = ctxt->getSystemManager();
    const Graph* graph = sysMan->getGraph(graphName);
    const VersionController& vc = graph->getVersionController();

    Transaction * tx = ctxt->getTransaction();
    if (tx->readingFrozenCommit()) {
        const auto& frzTx = tx->get<FrozenCommitTx>();
        const CommitHash headHash = frzTx.getCommitHash();
        const Commit* headCommit = vc.getCommitSafe(headHash);
        bioassert(headCommit, "Failed to get head commit.");

        data._commit = headCommit;
    } else if (tx->readingPendingCommit()) {
        const auto& pcrTx = tx->get<PendingCommitReadTx>();
        const CommitBuilder* commitBuilder = pcrTx.commitBuilder();
        bioassert(commitBuilder, "Failed to get commit builder.");
        const Commit* pendingCommit = commitBuilder->commit();
        bioassert(pendingCommit, "Failed to get pending commit.");

        data._commit = pendingCommit;
    } else if (tx->writingPendingCommit()) {
        auto& pcwTx = tx->get<PendingCommitWriteTx>();
        const CommitBuilder* commitBuilder = pcwTx.commitBuilder();
        bioassert(commitBuilder, "Failed to get commit builder.");
        const Commit* pendingCommit = commitBuilder->commit();
        bioassert(pendingCommit, "Failed to get pending commit.");

        data._commit = pendingCommit;
    }
    bioassert(data._commit, "Failed to get current commit data.");
}

}

ProcedureData* ShowIndexesProcedure::allocData() {
    return new Data();
}

void ShowIndexesProcedure::deallocData(ProcedureData* data) {
    delete data;
}

void ShowIndexesProcedure::registerProcedure(ProcedureNamespace* ns) {
    Procedure* proc = new Procedure("showIndexes");

    proc->setExecuteCallback(&execute);
    proc->setAllocCallback(&allocData);
    proc->setDeallocCallback(&deallocData);

    proc->addReturnValue("name", ProcedureType::STRING_VIEW);
    // proc->addReturnValue("propertyTypeID", ProcedureType::PROPERTY_TYPE_ID);
    // proc->addReturnValue("propertyName", ProcedureType::STRING);
    proc->addReturnValue("size", ProcedureType::UINT_64);

    ns->addProcedure(proc);
}

void ShowIndexesProcedure::execute(ProcedureState* proc) {
    switch (proc->getStep()) {
        case ProcedureState::Step::PREPARE: {
            prepare(proc);
        }
        break;
        case ProcedureState::Step::RESET:
        break;
        case ProcedureState::Step::EXECUTE:
            const ExecutionContext* ctxt = proc->getContext();
            Data& data = proc->data<Data>();
            const size_t chunkSize = ctxt->getChunkSize();

            writeChunk(&data, proc, chunkSize);
            proc->finish();
        break;
    }
}
