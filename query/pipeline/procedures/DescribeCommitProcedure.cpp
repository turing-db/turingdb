#include "DescribeCommitProcedure.h"

#include <span>

#include "ExecutionContext.h"
#include "procedures/Procedure.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnConst.h"
#include "DataPart.h"
#include "views/GraphView.h"

#include "PipelineException.h"

using namespace db;

namespace {

struct Data : public ProcedureData {
    std::vector<std::string_view>::const_iterator _it;
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

    const auto* commitVecCol = dynamic_cast<const ColumnVector<types::String::Primitive>*>(rawCommitCol);
    const auto* commitConstCol = dynamic_cast<const ColumnConst<types::String::Primitive>*>(rawCommitCol);

    bioassert(commitVecCol || commitConstCol,
              "db.describeCommit: must be provided a commit hash");

    switch (proc.step()) {
        case Procedure::Step::PREPARE: {
            if (commitVecCol) {
                // if received multiple commit hashes as input
                data._it = commitVecCol->begin();
            }
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

            size_t remaining = commitVecCol ? std::distance(data._it, commitVecCol->end()) : 1;
            remaining = std::min(remaining, ctxt->getChunkSize());

            // If received mutiple commit hashes as input
            if (commitVecCol) {
                for (size_t i = 0; i < remaining; ++i) {
                    std::string currentCommitStr;
                    std::string inputCommitStr {*data._it};
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

                    ++data._it;

                    if (data._it == commitVecCol->end()) {
                        proc.finish();
                    }
                }
            } else {
                // Const case
                std::string currentCommitStr;
                std::string inputCommitStr {commitConstCol->getRaw()};
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
            }

            return;
        }
    }

    throw PipelineException("Unknown procedure step");
}

