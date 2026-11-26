#pragma once

#include <algorithm>

#include "ID.h"
#include "columns/ColumnVector.h"

#include "NLExecutionContext.h"

#include "NLProgram.h"
#include "NLRuntimeProperty.h"
#include "NLPendingEdges.h"
#include "NLColumnKernels.h"

namespace db {

// Execute a body of statements
inline void runBody(NLExecutionContext* context, const NLStmtContainer* body) {
    for (const NLFunctionDescriptor& descriptor : body->stmts()) {
        const auto func = descriptor.getFunction();
        NLFunctionData* funcData = descriptor.getData();
        func(context, funcData);
    }
}

// Execute a get_out_edges/get_in_edges loop
template <typename ChunkWriterType>
void runEdgeLoopSteps(NLExecutionContext* context,
                      NLExpansionLoopData* loopData,
                      ChunkWriterType* chunkWriter,
                      NLPendingEdgeHop* pendingEdges,
                      ColumnNodeIDs* gatheredNodeIDs) {
    const NLStmtContainer* loopBody = loopData->getStmts();

    // Same early-exit as the scan loop: a null limit is unbounded, otherwise the
    // loop stops once the budget is spent, and the break unwinds any enclosing
    // loop carrying the same handle. The limit is fixed for the whole loop, so
    // the null check is hoisted out of the per-iteration condition.
    const NLLimitState* limit = loopData->getLimit();
    const size_t chunkSize = context->getChunkSize();

    const auto hasStep = [&]() {
        return chunkWriter->isValid() || (pendingEdges && pendingEdges->isValid());
    };

    const auto runIteration = [&](size_t rowBudget) {
        if (chunkWriter->isValid()) {
            chunkWriter->fill(rowBudget);
        } else {
            pendingEdges->fill(rowBudget);
        }

        const ColumnVector<size_t>* indices = loopData->getIndices();
        if (indices->empty()) {
            return;
        }

        // Column many not have been allocated (=nullptr) if its var wasn't used
        if (gatheredNodeIDs) {
            gatherColumn<NodeID>(loopData->getInput(), indices, gatheredNodeIDs);
        }

        // Transform all the columns in the carried set according to indices
        for (const NLCarriedColumn& carriedColumn : loopData->carriedColumns()) {
            const auto gatherFunc = carriedColumn.getGatherFunc();
            gatherFunc(carriedColumn.getInput(), indices, carriedColumn.getOutput());
        }

        runBody(context, loopBody);
    };

    if (limit) {
        // A body that filters keeps the budget unspent, so asking for exactly what the
        // limit still needs would pay a chunk's fixed cost once per surviving row: the
        // request doubles until it is back at a whole chunk.
        size_t rowBudget = 0;

        while (hasStep() && limit->getRemaining() > 0) {
            rowBudget = std::min(chunkSize, std::max(rowBudget * 2, limit->getRemaining()));

            runIteration(rowBudget);
        }
    } else {
        while (hasStep()) {
            runIteration(chunkSize);
        }
    }
}

}
