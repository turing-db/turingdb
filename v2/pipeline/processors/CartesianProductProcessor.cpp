#include "CartesianProductProcessor.h"

#include "ExecutionContext.h"
#include "PipelineV2.h"
#include "PipelinePort.h"
#include "columns/ColumnSwitch.h"
#include "dataframe/Dataframe.h"

#include "BioAssert.h"
#include "iterators/ChunkConfig.h"

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
    [[maybe_unused]] PipelineOutputPort* oPort = _out.getPort();
    
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
    [[maybe_unused]] const size_t q = rDF->size();

    msgbioassert(n * m <= _ctxt->getChunkSize(),
                 "Cartesian Product is only supported in the strongly bounded case "
                 "(output size <= CHUNK_SIZE).");

    // Resize all columns in the output to be the correct size
    for (NamedColumn* col : oDF->cols()) {
        dispatchColumnVector(col->getColumn(),
                             [&](auto* columnVector) { columnVector->resize(n * m); });
    }

    // Iterate down each column, per column, for better cache performance
    for (size_t colPtr{0}; colPtr < p; colPtr++) {
        for (size_t rowPtr{0}; rowPtr < n; rowPtr++) {
            // Fill output with m copies of this row from the LHS
            dispatchColumnVector(lDF->cols()[colPtr]->getColumn(),
                [&](auto* columnVector) -> void {
                    auto& currentLHSElement = columnVector->at(rowPtr);

                    // Respective column in output DF should be same type: cast it
                    Column* respectiveOutputCol = oDF->cols()[colPtr]->getColumn();
                    auto* casted =
                        static_cast<decltype(columnVector)>(respectiveOutputCol);

                    auto& rawOut = casted->getRaw();

                    // Set m rows to be the current element
                    auto startIt = rawOut.begin() + rowPtr * m;
                    auto endIt = startIt + m;
                    std::fill(startIt, endIt, currentLHSElement);
                }
            );
            // Fill output with m copies of this column from the RHS
            dispatchColumnVector(rDF->cols()[colPtr]->getColumn(),
                 [&](auto* columnVector) -> void {
                     Column* respectiveOutputCol = oDF->cols()[colPtr + p]->getColumn();
                     auto* casted =
                         static_cast<decltype(columnVector)>(respectiveOutputCol);

                     auto& rawOut = casted->getRaw();
                     auto& rawRHS = columnVector->getRaw();

                     auto startIt = rawOut.begin() + rowPtr * m;
                     std::copy(rawRHS.begin(), rawRHS.end(), startIt);
                 }
            );
        }
    }

    finish();
    oPort->writeData();
}

