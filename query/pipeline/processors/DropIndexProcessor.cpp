#include "DropIndexProcessor.h"

#include <string_view>

#include <spdlog/fmt/bundled/format.h>

#include "ExecutionContext.h"
#include "PipelineException.h"
#include "PipelineExecutor.h"
#include "indexes/Index.h"
#include "versioning/CommitBuilder.h"
#include "versioning/CommitWriteBuffer.h"
#include "versioning/Transaction.h"

#include "PipelinePort.h"

#include "FatalException.h"

using namespace db;

DropIndexProcessor::DropIndexProcessor(std::string_view indexName)
    :_indexName(indexName)
{
}

std::string DropIndexProcessor::describe() const {
    return fmt::format("DropIndexProcessor @={}", fmt::ptr(this));
}

DropIndexProcessor* DropIndexProcessor::create(PipelineV2* pipeline,
                                               std::string_view indexName) {
    DropIndexProcessor* proc = new DropIndexProcessor(indexName);

    {
        PipelineOutputPort* out = PipelineOutputPort::create(pipeline, proc);

        proc->_output.setPort(out);
        proc->addOutput(out);
    }

    proc->postCreate(pipeline);

    return proc;
}

void DropIndexProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;

    Transaction* rawTx = ctxt->getTransaction();
    if (!rawTx) {
        throw FatalException("Attempted to drop index without transaction.");
    }

    if (!rawTx->writingPendingCommit()) {
        throw PipelineException(
            "DROP INDEX: Cannot perform writes outside of a write transaction");
    }

    auto& tx = rawTx->get<PendingCommitWriteTx>();
    _commitBuilder = tx.commitBuilder();

    bioassert(_commitBuilder, "Could not get commit builder to drop index.");

    markAsPrepared();
}

void DropIndexProcessor::reset() {
    markAsReset();
}

void DropIndexProcessor::execute() {
    const GraphView view = _ctxt->getGraphView();
    const std::span indexes = view.indexes();

    const auto name = [&](const WeakArc<Index>& index) -> std::string_view {
        return index->name();
    };

    const auto foundIt = std::ranges::find(indexes, _indexName, name);

    if (foundIt == end(indexes)) {
        std::string err = fmt::format("Index {} does not exist.", _indexName);
        throw PipelineException(std::move(err));
    }

    const WeakArc<Index>& droppedIndex = *foundIt;

    CommitWriteBuffer& wb = _commitBuilder->writeBuffer();
    wb.addDroppedIndex(droppedIndex);

    finish();
}
