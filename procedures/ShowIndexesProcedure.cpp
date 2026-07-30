#include "ShowIndexesProcedure.h"

#include "ProcedureContext.h"
#include "FatalException.h"
#include "Graph.h"
#include "ID.h"
#include "ProcedureData.h"
#include "ProcedureNamespace.h"
#include "columns/ColumnVector.h"
#include "metadata/PropertyType.h"
#include "Procedure.h"
#include "ProcedureState.h"
#include "ProcedureTypeVector.h"

#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "versioning/VersionController.h"
#include <string_view>

using namespace db;

namespace {

struct Data : public ProcedureData {
    const Commit* _commit {nullptr};
    size_t _written {0};
};

void writeChunk(Data* data, ProcedureState* procedure, size_t chunkSize) {
    auto* names = dynamic_cast<ColumnVector<std::string_view>*>(data->getReturnColumn(0));
    auto* sizes = dynamic_cast<ColumnVector<types::UInt64::Primitive>*>(data->getReturnColumn(1));

    // Not all columns may be YIELDed, only fill those which are
    if (names) {
        names->clear();
    }
    if (sizes) {
        sizes->clear();
    }

    const Commit* commit = data->_commit;
    bioassert(commit, "Invalid commit.");

    const CommitHistory& history = commit->history();
    const CommitHistory::IndexSpan indexes = history.validIndexes();

    if (indexes.empty()) {
        return;
    }

    bioassert(indexes.size() > data->_written, "Invalid state.");

    const size_t remaining = indexes.size() - data->_written;
    const size_t canWrite = std::min(remaining, chunkSize);
    size_t& from = data->_written;
    const size_t to = from + canWrite;

    for (; from < to; from++) {
        const size_t i = from;

        const WeakArc<Index>& index = indexes[i];
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
    const ProcedureContext* ctxt = proc->getContext();

    const Graph* graph = ctxt->getGraph();
    const VersionController& vc = graph->getVersionController();

    Transaction* tx = ctxt->getTransaction();
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
    proc->addReturnValue("size", ProcedureType::UINT_64);

    ns->addProcedure(proc);
}

void ShowIndexesProcedure::execute(ProcedureState* proc) {
    switch (proc->getStep()) {
        case ProcedureState::Step::PREPARE: {
            prepare(proc);
        }
        break;
        case ProcedureState::Step::RESET: {
            // Writing a chunk advances the written count, so a rewind puts it back to
            // the first index; the commit it reads them from does not change.
            Data& data = proc->data<Data>();
            data._written = 0;
        }
        break;
        case ProcedureState::Step::EXECUTE:
            const ProcedureContext* ctxt = proc->getContext();
            Data& data = proc->data<Data>();
            const size_t chunkSize = ctxt->getChunkSize();

            writeChunk(&data, proc, chunkSize);
            proc->finish();
        break;
    }
}
