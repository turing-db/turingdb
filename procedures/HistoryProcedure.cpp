#include "HistoryProcedure.h"

#include "ProcedureContext.h"
#include "ProcedureState.h"
#include "Procedure.h"
#include "ProcedureNamespace.h"
#include "TypedProcedure.h"
#include "Graph.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "versioning/VersionController.h"
#include "columns/ColumnVector.h"
#include "buffers/StringBuffer.h"

using namespace db;

namespace {

struct Data : public ProcedureData {
    // The commit the walk starts from - the one the query reads - resolved once when the
    // call is prepared, so a rewind restarts from it without asking the transaction again.
    const Commit* _headCommit {nullptr};

    // The walk's cursor, stepped back one commit per emitted row until it runs past the root.
    const Commit* _commit {nullptr};
};

// Resolve the commit the history is walked back from: the one the query reads.
void resolveHeadCommit(Data* data, const ProcedureContext* ctxt, const VersionController& controller) {
    Transaction* tx = ctxt->getTransaction();
    if (tx->readingFrozenCommit()) {
        data->_headCommit = controller.getCommitSafe(tx->get<FrozenCommitTx>().getCommitHash());
    } else if (tx->readingPendingCommit()) {
        data->_headCommit = tx->get<PendingCommitReadTx>().commitBuilder()->commit();
    } else if (tx->writingPendingCommit()) {
        data->_headCommit = tx->get<PendingCommitWriteTx>().commitBuilder()->commit();
    }

    bioassert(data->_headCommit, "headCommitHash not found");
}

void writeChunk(Data* data,
                ProcedureState* proc,
                size_t chunkSize,
                StringBuffer* stringBuffer) {
    size_t count = 0;

    auto* commitCol = getReturnColumn<HistoryProcedure, 0>(data);
    auto* nodeCountCol = getReturnColumn<HistoryProcedure, 1>(data);
    auto* edgeCountCol = getReturnColumn<HistoryProcedure, 2>(data);
    auto* partCountCol = getReturnColumn<HistoryProcedure, 3>(data);

    data->clearReturnColumns();

    // Traverse through the commit history chain until we reach the root commit or
    // we have outputed a chunksize worth of commits.
    while (data->_commit && count < chunkSize) {
        const Commit* commit = data->_commit;
        const bool isHead = (count == 0);

        if (commitCol) {
            const std::string hash = isHead
                ? fmt::format("{:x}(HEAD)", commit->hash().get())
                : fmt::format("{:x}", commit->hash().get());

            commitCol->push_back(stringBuffer->insert(hash));
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

void HistoryProcedure::registerProcedure(ProcedureNamespace* ns) {
    ns->addProcedure(createTypedProcedure<HistoryProcedure, Data>("history"));
}

void HistoryProcedure::execute(ProcedureState* proc) {
    Data& data = proc->data<Data>();
    const ProcedureContext* ctxt = proc->getContext();
    const VersionController& controller = ctxt->getGraph()->getVersionController();

    const size_t chunkSize = ctxt->getChunkSize();
    StringBuffer* stringBuffer = ctxt->getStringBuffer();

    switch (proc->getStep()) {
        case ProcedureState::Step::PREPARE:
            resolveHeadCommit(&data, ctxt, controller);
            data._commit = data._headCommit;
        break;

        case ProcedureState::Step::RESET:
            // Walking the history consumes the cursor, so a rewind puts it back to the
            // commit resolved when the call was prepared.
            data._commit = data._headCommit;
        break;

        case ProcedureState::Step::EXECUTE:
            writeChunk(&data, proc, chunkSize, stringBuffer);
        break;
    }
}
