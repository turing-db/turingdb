#include "NLExecutor.h"

#include <span>

#include "iterators/GetEdgesIterator.h"
#include "ID.h"
#include "iterators/GetInEdgesIterator.h"
#include "iterators/GetInEdgesByTypeIterator.h"
#include "iterators/GetInEdgesByLabelIterator.h"
#include "iterators/GetInEdgesByTypeAndLabelIterator.h"
#include "iterators/GetOutEdgesIterator.h"
#include "iterators/GetOutEdgesByTypeIterator.h"
#include "iterators/GetOutEdgesByLabelIterator.h"
#include "iterators/GetOutEdgesByTypeAndLabelIterator.h"
#include "columns/ColumnVector.h"

#include "NLProgram.h"
#include "NLPendingEdges.h"
#include "NLLoopSteps.h"

using namespace db;

void NLExecutor::runGetOutEdgesLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLEdgeLoopData* loopData = static_cast<NLEdgeLoopData*>(data);
    const ColumnNodeIDs* inputNodeIDs = loopData->getInput();

    if (inputNodeIDs->empty()) {
        return;
    }

    GetOutEdgesChunkWriter chunkWriter(*context->getView(), inputNodeIDs);
    chunkWriter.setIndices(loopData->getIndices());
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setTgtIDs(loopData->getTargets());

    NLPendingEdgeHop pendingEdges(context, loopData, NLPendingEdgeHop::Direction::Out, loopData->getTargets());

    runEdgeLoopSteps(context, loopData, &chunkWriter, &pendingEdges, loopData->getSources());
}

void NLExecutor::runGetInEdgesLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLEdgeLoopData* loopData = static_cast<NLEdgeLoopData*>(data);
    const ColumnNodeIDs* inputNodeIDs = loopData->getInput();

    if (inputNodeIDs->empty()) {
        return;
    }

    GetInEdgesChunkWriter chunkWriter(*context->getView(), inputNodeIDs);
    chunkWriter.setIndices(loopData->getIndices());
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setSrcIDs(loopData->getSources());

    NLPendingEdgeHop pendingEdges(context, loopData, NLPendingEdgeHop::Direction::In, loopData->getSources());

    runEdgeLoopSteps(context, loopData, &chunkWriter, &pendingEdges, loopData->getTargets());
}

void NLExecutor::runGetEdgesLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLEdgeLoopData* loopData = static_cast<NLEdgeLoopData*>(data);
    const ColumnNodeIDs* inputNodeIDs = loopData->getInput();

    if (inputNodeIDs->empty()) {
        return;
    }

    GetEdgesChunkWriter chunkWriter(*context->getView(), inputNodeIDs);
    chunkWriter.setIndices(loopData->getIndices());
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setOtherIDs(loopData->getTargets());

    NLPendingEdgeHop pendingEdges(context, loopData, NLPendingEdgeHop::Direction::Either, loopData->getTargets());

    runEdgeLoopSteps(context, loopData, &chunkWriter, &pendingEdges, loopData->getSources());
}

void NLExecutor::runGetOutEdgesByTypeLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLEdgeByTypeLoopData* loopData = static_cast<NLEdgeByTypeLoopData*>(data);
    const ColumnNodeIDs* inputNodeIDs = loopData->getInput();

    // No type the pattern named is in the schema, so no edge can match; and an empty
    // input has no edges to walk. Either way the loop body never runs.
    if (!loopData->isMatchable() || inputNodeIDs->empty()) {
        return;
    }

    GetOutEdgesByTypeChunkWriter chunkWriter(*context->getView(), inputNodeIDs, loopData->getRequestedTypes());
    chunkWriter.setIndices(loopData->getIndices());
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setTgtIDs(loopData->getTargets());

    NLPendingEdgeHop pendingEdges(context, loopData, NLPendingEdgeHop::Direction::Out, loopData->getTargets());
    pendingEdges.setEdgeTypes(loopData->getRequestedTypes());

    runEdgeLoopSteps(context, loopData, &chunkWriter, &pendingEdges, loopData->getSources());
}

void NLExecutor::runGetInEdgesByTypeLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLEdgeByTypeLoopData* loopData = static_cast<NLEdgeByTypeLoopData*>(data);
    const ColumnNodeIDs* inputNodeIDs = loopData->getInput();

    if (!loopData->isMatchable() || inputNodeIDs->empty()) {
        return;
    }

    GetInEdgesByTypeChunkWriter chunkWriter(*context->getView(), inputNodeIDs, loopData->getRequestedTypes());
    chunkWriter.setIndices(loopData->getIndices());
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setSrcIDs(loopData->getSources());

    NLPendingEdgeHop pendingEdges(context, loopData, NLPendingEdgeHop::Direction::In, loopData->getSources());
    pendingEdges.setEdgeTypes(loopData->getRequestedTypes());

    runEdgeLoopSteps(context, loopData, &chunkWriter, &pendingEdges, loopData->getTargets());
}

void NLExecutor::runGetOutEdgesByLabelLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLEdgeByLabelLoopData* loopData = static_cast<NLEdgeByLabelLoopData*>(data);
    const ColumnNodeIDs* inputNodeIDs = loopData->getInput();

    // A requested label absent from the schema is carried by no node, and an empty input
    // has no edges to walk: either way the loop body never runs.
    if (!loopData->isMatchable() || inputNodeIDs->empty()) {
        return;
    }

    // The LabelSetHandle borrows the loop data's owned LabelSet, which lives for the whole
    // program, so the handle stays valid for every fill below.
    const LabelSetHandle labelset(loopData->getLabelSet());

    GetOutEdgesByLabelChunkWriter chunkWriter(*context->getView(), inputNodeIDs, labelset);
    chunkWriter.setIndices(loopData->getIndices());
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setTgtIDs(loopData->getTargets());

    NLPendingEdgeHop pendingEdges(context, loopData, NLPendingEdgeHop::Direction::Out, loopData->getTargets());
    pendingEdges.setEndpointLabelSet(loopData->getLabelSet());

    runEdgeLoopSteps(context, loopData, &chunkWriter, &pendingEdges, loopData->getSources());
}

void NLExecutor::runGetInEdgesByLabelLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLEdgeByLabelLoopData* loopData = static_cast<NLEdgeByLabelLoopData*>(data);
    const ColumnNodeIDs* inputNodeIDs = loopData->getInput();

    if (!loopData->isMatchable() || inputNodeIDs->empty()) {
        return;
    }

    const LabelSetHandle labelset(loopData->getLabelSet());

    GetInEdgesByLabelChunkWriter chunkWriter(*context->getView(), inputNodeIDs, labelset);
    chunkWriter.setIndices(loopData->getIndices());
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setSrcIDs(loopData->getSources());

    NLPendingEdgeHop pendingEdges(context, loopData, NLPendingEdgeHop::Direction::In, loopData->getSources());
    pendingEdges.setEndpointLabelSet(loopData->getLabelSet());

    runEdgeLoopSteps(context, loopData, &chunkWriter, &pendingEdges, loopData->getTargets());
}

void NLExecutor::runGetOutEdgesByTypeAndLabelLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLEdgeByTypeAndLabelLoopData* loopData = static_cast<NLEdgeByTypeAndLabelLoopData*>(data);
    const ColumnNodeIDs* inputNodeIDs = loopData->getInput();

    if (!loopData->isMatchable() || inputNodeIDs->empty()) {
        return;
    }

    const std::span<const EdgeTypeID> requestedTypes = loopData->getRequestedTypes();
    const LabelSetHandle labelset(loopData->getLabelSet());

    GetOutEdgesByTypeAndLabelChunkWriter chunkWriter(*context->getView(), inputNodeIDs, requestedTypes, labelset);
    chunkWriter.setIndices(loopData->getIndices());
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setTgtIDs(loopData->getTargets());

    NLPendingEdgeHop pendingEdges(context, loopData, NLPendingEdgeHop::Direction::Out, loopData->getTargets());
    pendingEdges.setEdgeTypes(requestedTypes);
    pendingEdges.setEndpointLabelSet(loopData->getLabelSet());

    runEdgeLoopSteps(context, loopData, &chunkWriter, &pendingEdges, loopData->getSources());
}

void NLExecutor::runGetInEdgesByTypeAndLabelLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLEdgeByTypeAndLabelLoopData* loopData = static_cast<NLEdgeByTypeAndLabelLoopData*>(data);
    const ColumnNodeIDs* inputNodeIDs = loopData->getInput();

    if (!loopData->isMatchable() || inputNodeIDs->empty()) {
        return;
    }

    const std::span<const EdgeTypeID> requestedTypes = loopData->getRequestedTypes();
    const LabelSetHandle labelset(loopData->getLabelSet());

    GetInEdgesByTypeAndLabelChunkWriter chunkWriter(*context->getView(), inputNodeIDs, requestedTypes, labelset);
    chunkWriter.setIndices(loopData->getIndices());
    chunkWriter.setEdgeIDs(loopData->getEdgeIDs());
    chunkWriter.setEdgeTypes(loopData->getEdgeTypes());
    chunkWriter.setSrcIDs(loopData->getSources());

    NLPendingEdgeHop pendingEdges(context, loopData, NLPendingEdgeHop::Direction::In, loopData->getSources());
    pendingEdges.setEdgeTypes(requestedTypes);
    pendingEdges.setEndpointLabelSet(loopData->getLabelSet());

    runEdgeLoopSteps(context, loopData, &chunkWriter, &pendingEdges, loopData->getTargets());
}
