#include "Change.h"

#include "reader/GraphReader.h"
#include <range/v3/view/enumerate.hpp>

#include "versioning/CommitBuilder.h"
#include "versioning/DataPartRebaser.h"
#include "versioning/MetadataRebaser.h"
#include "versioning/VersionController.h"
#include "versioning/Transaction.h"
#include "writers/DataPartBuilder.h"

using namespace db;

namespace rg = ranges;
namespace rv = rg::views;

Change::~Change() = default;

Change::Change(VersionController* versionController, ChangeID id, CommitHash base)
    : _id(id),
    _versionController(versionController),
    _base(versionController->openTransaction(base).commitData())
{
    auto tip = CommitBuilder::prepare(*_versionController,
                                      this,
                                      GraphView {*_base});
    _tip = tip.get();
    _commitOffsets.emplace(_tip->hash(), _commits.size());
    _commits.emplace_back(std::move(tip));
}

std::unique_ptr<Change> Change::create(VersionController* versionController,
                                       ChangeID id,
                                       CommitHash base) {
    auto* ptr = new Change(versionController, id, base);

    return std::unique_ptr<Change> {ptr};
}

PendingCommitWriteTx Change::openWriteTransaction() {
    return PendingCommitWriteTx {this->access(), this->_tip};
}

PendingCommitReadTx Change::openReadTransaction(CommitHash commitHash) {
    if (commitHash == CommitHash::head()) {
        return PendingCommitReadTx {this->access(), this->_tip};
    }

    auto it = _commitOffsets.find(commitHash);
    if (it != _commitOffsets.end()) {
        return PendingCommitReadTx {this->access(), _commits[it->second].get()};
    }

    return {};
}

CommitResult<void> Change::commit(JobSystem& jobsystem) {
    Profile profile {"Change::commit"};

    if (auto res = _tip->buildAllPending(jobsystem); !res) {
        return res;
    }

    auto newTip = CommitBuilder::prepare(*_versionController,
                                         this,
                                         _commits.back()->viewGraph());
    _tip = newTip.get();
    _commitOffsets.emplace(_tip->hash(), _commits.size());
    _commits.emplace_back(std::move(newTip));

    return {};
}

CommitResult<void> Change::rebase(JobSystem& jobsystem) {
    Profile profile {"Change::rebase"};

    // Get the next Edge and Node IDs at the time this change branched
    NodeID oldNextNodeID =
        _base->commits().back().openTransaction().readGraph().getNodeCount();
    EdgeID oldNextEdgeID =
        _base->commits().back().openTransaction().readGraph().getEdgeCount();

    // Get current state of main
    _base = _versionController->openTransaction().commitData();

    // Get the current next Edge and Node IDs on main
    NodeID newNextNodeID =
        _base->commits().back().openTransaction().readGraph().getNodeCount();
    EdgeID newNextEdgeID =
        _base->commits().back().openTransaction().readGraph().getEdgeCount();

    MetadataRebaser metadataRebaser;
    DataPartRebaser dataPartRebaser(oldNextNodeID, oldNextEdgeID, newNextNodeID,
                                    newNextEdgeID);

    const auto* prevCommitData = _base.get();
    const auto* prevHistory = &_base->history();

    for (auto& commitBuilder : _commits) {
        // Rebasing means:
        // 1. Rebase the metadata
        // 2. Get all commits/dataparts from the previous commit history
        // 3. Add back dataparts of current commit and rebase them

        metadataRebaser.clear();
        metadataRebaser.rebase(prevCommitData->metadata(), commitBuilder->metadata());

        CommitData& data = commitBuilder->commitData();
        CommitHistory& history = data.history();

        CommitHistoryRebaser historyRebaser {history};
        historyRebaser.rebase(metadataRebaser, dataPartRebaser, *prevHistory);

        prevCommitData = &commitBuilder->commitData();
        prevHistory = &prevCommitData->history();
    }

    return {};
}

CommitHash Change::baseHash() const {
    return _base->hash();
}

CommitResult<void> Change::submit(JobSystem& jobsystem) {
    Profile profile {"Change::submit"};

    // TODO: Rebase, build new DPs, update this history
    if (auto res = _tip->buildAllPending(jobsystem); !res) {
        return res;
    }

    // TODO: Submit this change by requesting
    if (auto res = _versionController->submitChange(this, jobsystem); !res) {
        return res;
    }

    return {};
}

GraphView Change::viewGraph(CommitHash commitHash) const {
    if (commitHash == CommitHash::head()) {
        return _tip->viewGraph();
    }

    auto it = _commitOffsets.find(commitHash);
    if (it != _commitOffsets.end()) {
        return _commits[it->second]->viewGraph();
    }

    return {};
}


CommitResult<void> Change::applyModifications(JobSystem& jobSystem) {
    // assumptions on entry:
    // 1. @param change contains the latest state of main
    // 2. No new/modified dataparts which will be created due to the modifications made by
    // @param change are applied yet

    auto& thisCommit = _commits.back();
    // No changes applied yet
    msgbioassert(thisCommit->isEmpty(), "Latest commit must be empty");

    // XXX: Need to ensure a new builder is used each time
    DataPartBuilder& dpBuilder = thisCommit->getCurrentBuilder();

    // Modification application process:
    // 1. Build all nodes: any new nodes cannot have been deleted within the same change
    // 2. Build edges whose incident nodes have not been deleted
    // 3. Apply modifications to dataparts with deleted nodes/edges

    CommitWriteBuffer& wb = thisCommit->writeBuffer();

    using CWB = CommitWriteBuffer;
    std::unordered_map<CWB::PendingNodeOffset, NodeID> tempIDMap;

    for (const auto& [offset, node]: wb.pendingNodes() | rv::enumerate) {
        NodeID nodeID = dpBuilder.addPendingNode(node);
        tempIDMap[offset] = nodeID;
    }

    for (const auto& edge : wb.pendingEdges()) {
        dpBuilder.addPendingEdge(wb, edge);
    }

    return {};
}
