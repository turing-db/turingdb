#include "DescribeCommitProcedure.h"

#include "ProcedureContext.h"
#include "ProcedureData.h"
#include "ProcedureState.h"
#include "Procedure.h"
#include "ProcedureNamespace.h"
#include "ProcUtils.h"
#include "TypedProcedure.h"
#include "Graph.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnConst.h"
#include "versioning/Commit.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "versioning/VersionController.h"

using namespace db;

namespace {

struct Data : public TypedProcedureData<DescribeCommitProcedure, IndexedProcedureData> {
    size_t _i {0};
    const Commit* _headCommit {nullptr};
};

struct CommitStats {
    size_t _nodeCount {0};
    size_t _edgeCount {0};
    size_t _partCount {0};
};

void getCommitStats(CommitStats* stats,
                    std::string_view inputHash,
                    const Commit* headCommit) {
    stats->_nodeCount = 0;
    stats->_edgeCount = 0;
    stats->_partCount = 0;

    //Strip away the '(HEAD)' substring from the head hash value
    const std::string_view hashText = inputHash.substr(0, inputHash.find('('));

    const auto hash = CommitHash::fromString(hashText);
    if (!hash) {
        return;
    }

    const Commit* commit = headCommit;

    // Traverse the commit history chain to find the relevant commit
    while (commit) {
        if (commit->hash() == hash.value()) {
            stats->_nodeCount = commit->getNumNodes();
            stats->_edgeCount = commit->getNumEdges();
            stats->_partCount = commit->getNumDataParts();
            return;
        }

        commit = commit->getPreviousCommit();
    }
}

}

void DescribeCommitProcedure::registerProcedure(ProcedureNamespace* ns) {
    Procedure* proc = createTypedProcedure<Data>("describeCommit");
    proc->addArgument("commit", ProcedureType::STRING_VIEW);
    ns->addProcedure(proc);
}

void DescribeCommitProcedure::execute(ProcedureState* proc) {
    Data& data = proc->data<Data>();
    const ProcedureContext* ctxt = proc->getContext();

    const Column* rawCommitCol = data.getInputColumn(0);
    auto* nodeCountCol = data.getReturnColumn<0>();
    auto* edgeCountCol = data.getReturnColumn<1>();
    auto* partCountCol = data.getReturnColumn<2>();

    switch (proc->getStep()) {
        case ProcedureState::Step::PREPARE: {
            bioassert(rawCommitCol, "db.describeCommit: must be provided a commit hash");

            const Graph* graph = ctxt->getGraph();
            const VersionController* controller = &graph->getVersionController();

            Transaction* tx = ctxt->getTransaction();
            if (tx->readingFrozenCommit()) {
                data._headCommit = controller->getCommitSafe(tx->get<FrozenCommitTx>().getCommitHash());
            } else if (tx->readingPendingCommit()) {
                data._headCommit = tx->get<PendingCommitReadTx>().commitBuilder()->commit();
            } else if (tx->writingPendingCommit()) {
                data._headCommit = tx->get<PendingCommitWriteTx>().commitBuilder()->commit();
            }
            bioassert(data._headCommit, "headCommitHash not found");
        }
        break;

        case ProcedureState::Step::RESET:
            data._i = 0;
        break;

        case ProcedureState::Step::EXECUTE: {
            if (nodeCountCol) {
                nodeCountCol->clear();
            }

            if (edgeCountCol) {
                edgeCountCol->clear();
            }

            if (partCountCol) {
                partCountCol->clear();
            }

            ColumnIndices* indices = data.indices();
            if (indices) {
                indices->clear();
            }

            const auto pushStats = [&](const CommitStats* stats, size_t inputRow) {
                if (nodeCountCol) {
                    nodeCountCol->push_back(stats->_nodeCount);
                }

                if (edgeCountCol) {
                    edgeCountCol->push_back(stats->_edgeCount);
                }

                if (partCountCol) {
                    partCountCol->push_back(stats->_partCount);
                }

                if (indices) {
                    indices->push_back(inputRow);
                }
            };

            const bool constantHash = ColumnKind::extractContainerKind(rawCommitCol->getKind())
                                   == ContainerKind::code<ColumnConst<void>>();
            const size_t rowCount = constantHash ? 1 : rawCommitCol->size();
            const size_t stepEnd = std::min(rowCount, data._i + ctxt->getChunkSize());

            CommitStats stats;
            for (size_t row = data._i; row < stepEnd; row++) {
                const std::optional<std::string_view> hash =
                    ProcUtils::rowArgument<std::string_view>(rawCommitCol, row, "db.describeCommit: commit hash must be a string");
                getCommitStats(&stats, hash.value_or(""), data._headCommit);
                pushStats(&stats, row);
            }

            data._i = stepEnd;

            if (data._i >= rowCount) {
                proc->finish();
            }
        }
        break;
    }
}
