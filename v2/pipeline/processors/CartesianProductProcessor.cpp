#include "CartesianProductProcessor.h"

#include "ExecutionContext.h"
#include "PipelineV2.h"
#include "PipelinePort.h"
#include "columns/ColumnSwitch.h"
#include "dataframe/Dataframe.h"

#include "BioAssert.h"

using namespace db::v2;

CartesianProductProcessor::CartesianProductProcessor()
{
}

CartesianProductProcessor::~CartesianProductProcessor()
{
}

CartesianProductProcessor* CartesianProductProcessor::create(PipelineV2* pipeline) {
    CartesianProductProcessor* processor = new CartesianProductProcessor();

    {
        PipelineInputPort* lhsInput = PipelineInputPort::create(pipeline, processor);
        processor->_lhs.setPort(lhsInput);
        processor->addInput(lhsInput);
    }

    {
        PipelineInputPort* rhsInput = PipelineInputPort::create(pipeline, processor);
        processor->_rhs.setPort(rhsInput);
        processor->addInput(rhsInput);
    }

    {
        PipelineOutputPort* output = PipelineOutputPort::create(pipeline, processor);
        processor->_out.setPort(output);
        processor->addOutput(output);
    }

    processor->postCreate(pipeline);
    return processor;
}

void CartesianProductProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;

    markAsPrepared();
}

void CartesianProductProcessor::reset() {
    markAsReset();
}

void CartesianProductProcessor::execute() {
    PipelineInputPort* lPort = _lhs.getPort();
    PipelineInputPort* rPort = _rhs.getPort();
    PipelineOutputPort* oPort = _out.getPort();
    
    Dataframe* lDF = _lhs.getDataframe();
    lPort->consume();
    Dataframe* rDF = _rhs.getDataframe();
    rPort->consume();
    Dataframe* oDF = _out.getDataframe();

    // Left DF is n x p dimensional
    const size_t n = lDF->getRowCount();
    const size_t p = lDF->size();

    // Right DF is m x q dimensional
    const size_t m = rDF->getRowCount();
    const size_t q = rDF->size();

    msgbioassert(n * m <= _ctxt->getChunkSize(),
                 "Cartesian Product is only supported in the strongly bounded case "
                 "(output size <= CHUNK_SIZE).");

    // Resize all columns in the output to be the correct size, then we can just copy in
    for (NamedColumn* col : oDF->cols()) {
        dispatchColumnVector(col->getColumn(),
                             [&](auto* columnVector) { columnVector->resize(n * m); });
    }

    // Copy over LHS columns to output, column-wise
    for (size_t colPtr = 0; colPtr < p; ++colPtr) {
        dispatchColumnVector(lDF->cols()[colPtr]->getColumn(), [&](auto* lhsColumn) {
            auto* outCol =
                static_cast<decltype(lhsColumn)>(oDF->cols()[colPtr]->getColumn());

            auto& outRaw = outCol->getRaw();

            // Each row from LHS needs to appear m times consecutively, so we can use a
            // memset-esque approach to materialise each row from LHS m times
            // consecutively in output
            for (size_t lhsRow = 0; lhsRow < n; lhsRow++) {
                const auto& currentLSHElement = lhsColumn->at(lhsRow);
                auto startIt = outRaw.begin() + (lhsRow * m);
                std::fill(startIt, startIt + m, currentLSHElement);
            }
        });
    }

    // Copy over RHS columns to output, column-wise
    for (size_t colPtr = 0; colPtr < q; colPtr++) {
        dispatchColumnVector(rDF->cols()[colPtr]->getColumn(), [&](auto* rhsColumn) {
            auto* outCol =
                static_cast<decltype(rhsColumn)>(oDF->cols()[p + colPtr]->getColumn());

            const auto& rhsRaw = rhsColumn->getRaw();
            auto& outRaw = outCol->getRaw();

            // All rows from RHS needed for each row in LHS; for each unique row on LHS
            // copy the entire RHS column over starting from that unique LHS row's first
            // index
            for (size_t lhsRow = 0; lhsRow < n; lhsRow++) {
                auto startIt = outRaw.begin() + lhsRow * m;
                std::copy(rhsRaw.begin(), rhsRaw.end(), startIt);
            }
        });
    }

    finish();
    oPort->writeData();
}

