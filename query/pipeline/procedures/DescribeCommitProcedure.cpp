#include "DescribeCommitProcedure.h"

#include <span>

#include "ExecutionContext.h"
#include "FatalException.h"
#include "procedures/Procedure.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnConst.h"
#include "DataPart.h"
#include "views/GraphView.h"

#include "PipelineException.h"

using namespace db;

namespace {

struct Data : public ProcedureData {
    size_t _i {0};
    ContainerKind::Code _containerKind {ContainerKind::Invalid};
    InternalKind::Code _internalKind {InternalKind::Invalid};
};

}

std::unique_ptr<ProcedureData> DescribeCommitProcedure::allocData() {
    return std::make_unique<Data>();
}

void DescribeCommitProcedure::execute(Procedure& proc) {
    Data& data = proc.data<Data>();
    const ExecutionContext* ctxt = proc.ctxt();
    const GraphView& _view = ctxt->getGraphView();

    const Column* rawCommitCol = data.getInputColumn(0);
    Column* rawNodeCountCol = data.getReturnColumn(0);
    Column* rawEdgeCountCol = data.getReturnColumn(1);
    Column* rawPartCountCol = data.getReturnColumn(2);

    auto* nodeCountCol = static_cast<ColumnVector<types::UInt64::Primitive>*>(rawNodeCountCol);
    auto* edgeCountCol = static_cast<ColumnVector<types::UInt64::Primitive>*>(rawEdgeCountCol);
    auto* partCountCol = static_cast<ColumnVector<types::UInt64::Primitive>*>(rawPartCountCol);

    switch (proc.step()) {
        case Procedure::Step::PREPARE: {
            bioassert(rawCommitCol, "db.describeCommit: must be provided a commit hash");

            data._containerKind = ColumnKind::extractContainerKind(rawCommitCol->getKind());
            data._internalKind = ColumnKind::extractInternalKind(rawCommitCol->getKind());

            bioassert(data._containerKind == ContainerKind::code<ColumnVector<void>>()
                          || data._containerKind == ContainerKind::code<ColumnConst<void>>(),
                      "db.describeCommit: must be provided a commit hash as a vector or const");
            bioassert(data._internalKind == InternalKind::code<std::string_view>()
                          || data._internalKind == InternalKind::code<std::string>(),
                      "db.describeCommit: must be provided a commit hash as a string or string_view");
            return;
        }

        case Procedure::Step::RESET: {
            return;
        }

        case Procedure::Step::EXECUTE: {
            if (nodeCountCol) {
                nodeCountCol->clear();
            }

            if (edgeCountCol) {
                edgeCountCol->clear();
            }

            if (partCountCol) {
                partCountCol->clear();
            }

            const auto writeCommit = [&](const CommitView& commit) {
                const std::span parts = commit.dataparts();

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
            };

            size_t remaining = rawCommitCol->size();
            remaining = std::min(remaining, ctxt->getChunkSize());

            const auto treatVector = [&]<typename T>(const ColumnVector<T>& vecCol) {
                for (size_t i = data._i; i < remaining + data._i; ++i) {
                    std::string currentCommitStr;
                    std::string inputCommitStr {vecCol[i]};
                    std::transform(inputCommitStr.begin(),
                                   inputCommitStr.end(),
                                   inputCommitStr.begin(),
                                   [](char c) { return std::tolower(c); });

                    // Iterate over all commits and find the one that matches the current input
                    for (const auto& commit : _view.commits()) {
                        currentCommitStr = fmt::format("{:x}", commit.hash().get());

                        if (inputCommitStr == currentCommitStr) {
                            writeCommit(commit);
                            break;
                        }
                    }
                }

                data._i += remaining;

                if (data._i == vecCol.size()) {
                    proc.finish();
                }
            };

            const auto treatConst = [&]<typename T>(const ColumnConst<T>& constCol) {
                std::string currentCommitStr;
                std::string inputCommitStr {constCol.getRaw()};
                std::transform(inputCommitStr.begin(),
                               inputCommitStr.end(),
                               inputCommitStr.begin(),
                               [](char c) { return std::tolower(c); });

                // Iterate over all commits and find the one that matches the current input
                for (const auto& commit : _view.commits()) {
                    currentCommitStr = fmt::format("{:x}", commit.hash().get());

                    if (inputCommitStr == currentCommitStr) {
                        writeCommit(commit);
                        break;
                    }
                }

                proc.finish();
            };

            // If received mutiple commit hashes as input
            switch (rawCommitCol->getKind()) {
                case ColumnVector<std::string>::staticKind(): {
                    treatVector(*static_cast<const ColumnVector<std::string>*>(rawCommitCol));
                } break;
                case ColumnVector<std::string_view>::staticKind(): {
                    treatVector(*static_cast<const ColumnVector<std::string_view>*>(rawCommitCol));
                } break;
                case ColumnConst<std::string>::staticKind(): {
                    treatConst(*static_cast<const ColumnConst<std::string>*>(rawCommitCol));
                } break;
                case ColumnConst<std::string_view>::staticKind(): {
                    treatConst(*static_cast<const ColumnConst<std::string_view>*>(rawCommitCol));
                } break;

                default: {
                    throw FatalException(fmt::format("Unexpected column kind: {}", rawCommitCol->getKind()));
                }
            }

            return;
        }
    }

    throw PipelineException("Unknown procedure step");
}

