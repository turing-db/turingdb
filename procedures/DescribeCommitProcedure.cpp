#include "DescribeCommitProcedure.h"

#include "ProcedureContext.h"
#include "FatalException.h"
#include "ProcedureData.h"
#include "ProcedureState.h"
#include "Procedure.h"
#include "ProcedureNamespace.h"
#include "Graph.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnConst.h"
#include "versioning/Commit.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "versioning/VersionController.h"

using namespace db;

namespace {

using UInt64Col = ColumnVector<types::UInt64::Primitive>;

struct Data : public IndexedProcedureData {
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

    std::string lower(inputHash);
    std::transform(lower.begin(), lower.end(), lower.begin(),
                   [](char c) { return std::tolower(c); });

    //Strip away the '(HEAD)' substring from the head hash value
    const size_t parenPos = lower.find('(');
    if (parenPos != std::string::npos) {
        lower.resize(parenPos);
    }

    const Commit* commit = headCommit;

    // Traverse the commit history chain to find the relevant commit
    while (commit) {
        const std::string hashStr = fmt::format("{:x}", commit->hash().get());

        if (lower == hashStr) {
            stats->_nodeCount = commit->getNumNodes();
            stats->_edgeCount = commit->getNumEdges();
            stats->_partCount = commit->getNumDataParts();
            return;
        }

        commit = commit->getPreviousCommit();
    }
}

template <typename T>
void extractString(std::string& result, const T& val) {
    if constexpr (requires { val.has_value(); }) {
        result = val.value_or("");
    } else {
        result = val;
    }
}

template <template <typename> class ColType, typename F>
void dispatchStringInternal(const Column* col, const F& fn) {
    switch (col->getKind()) {
        case ColType<std::string>::staticKind():
            fn(static_cast<const ColType<std::string>*>(col));
        break;

        case ColType<std::string_view>::staticKind():
            fn(static_cast<const ColType<std::string_view>*>(col));
        break;

        case ColType<std::optional<std::string>>::staticKind():
            fn(static_cast<const ColType<std::optional<std::string>>*>(col));
        break;

        case ColType<std::optional<std::string_view>>::staticKind():
            fn(static_cast<const ColType<std::optional<std::string_view>>*>(col));
        break;

        default:
            throw FatalException(fmt::format(
                "Unexpected column kind: {}", col->getKind()));
        break;
    }
}

}

ProcedureData* DescribeCommitProcedure::allocData() {
    return new Data();
}

void DescribeCommitProcedure::deallocData(ProcedureData* data) {
    delete data;
}

void DescribeCommitProcedure::registerProcedure(ProcedureNamespace* ns) {
    Procedure* proc = new Procedure("describeCommit");
    proc->setExecuteCallback(&execute);
    proc->setAllocCallback(&allocData);
    proc->setDeallocCallback(&deallocData);
    proc->addArgument("commit", ProcedureType::STRING);
    proc->setHasIndices(true);
    proc->addReturnValue("nodeCount", ProcedureType::UINT_64);
    proc->addReturnValue("edgeCount", ProcedureType::UINT_64);
    proc->addReturnValue("partCount", ProcedureType::UINT_64);
    ns->addProcedure(proc);
}

void DescribeCommitProcedure::execute(ProcedureState* proc) {
    Data& data = proc->data<Data>();
    const ProcedureContext* ctxt = proc->getContext();

    const Column* rawCommitCol = data.getInputColumn(0);
    auto* nodeCountCol = static_cast<UInt64Col*>(data.getReturnColumn(0));
    auto* edgeCountCol = static_cast<UInt64Col*>(data.getReturnColumn(1));
    auto* partCountCol = static_cast<UInt64Col*>(data.getReturnColumn(2));

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

            const auto containerKind =
                ColumnKind::extractContainerKind(rawCommitCol->getKind());
            const auto internalKind =
                ColumnKind::extractInternalKind(rawCommitCol->getKind());

            bioassert(containerKind == ContainerKind::code<ColumnVector<void>>()
                       || containerKind == ContainerKind::code<ColumnConst<void>>(),
                      "db.describeCommit: commit hash must be a vector or const column");
            bioassert(internalKind == InternalKind::code<std::string_view>()
                       || internalKind == InternalKind::code<std::string>()
                       || internalKind == InternalKind::code<std::optional<std::string_view>>()
                       || internalKind == InternalKind::code<std::optional<std::string>>(),
                      "db.describeCommit: commit hash must be a string or string_view");
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

            const auto treatVector = [&]<typename T>(const ColumnVector<T>* col) {
                const auto& colVec = *col;
                const size_t remaining =
                    std::min(colVec.size() - data._i, ctxt->getChunkSize());

                std::string input;
                CommitStats stats;
                for (size_t i = data._i; i < remaining + data._i; ++i) {
                    extractString(input, colVec[i]);
                    getCommitStats(&stats, input, data._headCommit);
                    pushStats(&stats, i);
                }

                data._i += remaining;

                if (data._i >= colVec.size()) {
                    proc->finish();
                }
            };

            const auto treatConst = [&]<typename T>(const ColumnConst<T>* col) {
                std::string input;
                CommitStats stats;
                extractString(input, col->getRaw());
                getCommitStats(&stats, input, data._headCommit);
                pushStats(&stats, 0);
                proc->finish();
            };

            const auto containerKind =
                ColumnKind::extractContainerKind(rawCommitCol->getKind());

            if (containerKind == ContainerKind::code<ColumnVector<void>>()) {
                dispatchStringInternal<ColumnVector>(rawCommitCol, treatVector);
            } else {
                dispatchStringInternal<ColumnConst>(rawCommitCol, treatConst);
            }
        }
        break;
    }
}
