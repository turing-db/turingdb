#include "DescribeCommitProcedure.h"

#include <span>

#include "ExecutionContext.h"
#include "FatalException.h"
#include "ProcedureState.h"
#include "Procedure.h"
#include "ProcedureNamespace.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnConst.h"
#include "DataPart.h"
#include "views/GraphView.h"

#include "PipelineException.h"

using namespace db;

namespace {

using UInt64Col = ColumnVector<types::UInt64::Primitive>;

struct Data : public ProcedureData {
    size_t _i {0};
};

struct CommitStats {
    size_t nodeCount {0};
    size_t edgeCount {0};
    size_t partCount {0};
};

void getCommitStats(CommitStats& stats,
                    std::string_view inputHash,
                    const GraphView& view) {

    stats.nodeCount = 0;
    stats.edgeCount = 0;
    stats.partCount = 0;

    std::string lower(inputHash);
    std::transform(lower.begin(), lower.end(), lower.begin(),
                   [](char c) { return std::tolower(c); });

    for (const auto& commit : view.commits()) {
        const std::string hashStr = fmt::format("{:x}", commit.hash().get());

        if (lower != hashStr) {
            continue;
        }

        const std::span parts = commit.dataparts();
        stats.partCount = parts.size();

        for (const auto& part : parts) {
            stats.nodeCount += part->getNodeContainerSize();
            stats.edgeCount += part->getEdgeContainerSize();
        }

        return;
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
void dispatchStringInternal(const Column* col, F&& fn) {
    switch (col->getKind()) {
        case ColType<std::string>::staticKind():
            fn(*static_cast<const ColType<std::string>*>(col));
        break;

        case ColType<std::string_view>::staticKind():
            fn(*static_cast<const ColType<std::string_view>*>(col));
        break;

        case ColType<std::optional<std::string>>::staticKind():
            fn(*static_cast<const ColType<std::optional<std::string>>*>(col));
        break;

        case ColType<std::optional<std::string_view>>::staticKind():
            fn(*static_cast<const ColType<std::optional<std::string_view>>*>(col));
        break;

        default:
            throw FatalException(fmt::format(
                "Unexpected column kind: {}", col->getKind()));
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
    proc->addReturnValue("nodeCount", ProcedureType::UINT_64);
    proc->addReturnValue("edgeCount", ProcedureType::UINT_64);
    proc->addReturnValue("partCount", ProcedureType::UINT_64);
    ns->addProcedure(proc);
}

void DescribeCommitProcedure::execute(ProcedureState& proc) {
    Data& data = proc.data<Data>();
    const ExecutionContext* ctxt = proc.getContext();
    const GraphView& view = ctxt->getGraphView();

    const Column* rawCommitCol = data.getInputColumn(0);
    auto* nodeCountCol = static_cast<UInt64Col*>(data.getReturnColumn(0));
    auto* edgeCountCol = static_cast<UInt64Col*>(data.getReturnColumn(1));
    auto* partCountCol = static_cast<UInt64Col*>(data.getReturnColumn(2));

    switch (proc.getStep()) {
        case ProcedureState::Step::PREPARE: {
            bioassert(rawCommitCol, "db.describeCommit: must be provided a commit hash");

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
            return;
        }

        case ProcedureState::Step::RESET:
            return;

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

            const auto pushStats = [&](const CommitStats& s) {
                if (nodeCountCol) {
                    nodeCountCol->push_back(s.nodeCount);
                }

                if (edgeCountCol) {
                    edgeCountCol->push_back(s.edgeCount);
                }

                if (partCountCol) {
                    partCountCol->push_back(s.partCount);
                }
            };

            const auto treatVector = [&]<typename T>(const ColumnVector<T>& col) {
                const size_t remaining =
                    std::min(rawCommitCol->size(), ctxt->getChunkSize());

                std::string input;
                CommitStats stats;
                for (size_t i = data._i; i < remaining + data._i; ++i) {
                    extractString(input, col[i]);
                    getCommitStats(stats, input, view);
                    pushStats(stats);
                }

                data._i += remaining;

                if (data._i == col.size()) {
                    proc.finish();
                }
            };

            const auto treatConst = [&]<typename T>(const ColumnConst<T>& col) {
                std::string input;
                CommitStats stats;
                extractString(input, col.getRaw());
                getCommitStats(stats, input, view);
                pushStats(stats);
                proc.finish();
            };

            const auto containerKind =
                ColumnKind::extractContainerKind(rawCommitCol->getKind());

            if (containerKind == ContainerKind::code<ColumnVector<void>>()) {
                dispatchStringInternal<ColumnVector>(rawCommitCol, treatVector);
            } else {
                dispatchStringInternal<ColumnConst>(rawCommitCol, treatConst);
            }

            return;
        }
    }

    throw PipelineException("Unknown procedure step");
}
