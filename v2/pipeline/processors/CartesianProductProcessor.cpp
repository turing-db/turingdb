#include "CartesianProductProcessor.h"

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

void CartesianProductProcessor::prepare([[maybe_unused]] ExecutionContext* ctxt) {
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
    const size_t p = rDF->size();

    // Right DF is m x q dimensional
    const size_t m = lDF->getRowCount();
    [[maybe_unused]] const size_t q = rDF->size();

    msgbioassert(n * m <= ChunkConfig::CHUNK_SIZE,
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
            dispatchColumnVector(
                lDF->cols()[colPtr]->getColumn(), [&](auto* columnVector) -> void {
                    auto x = columnVector->at(rowPtr);

                    // Respective column in output DF should be same type: cast it
                    auto* respectiveOutputCol = oDF->cols()[colPtr]->getColumn();
                    auto* casted = static_cast<decltype(columnVector)>(respectiveOutputCol);

                    auto& raw = casted->getRaw();
                    auto startIt = raw.begin() + rowPtr;
                    auto endIt = startIt + m;
                    std::fill(startIt, endIt, x);
                });
        }
    }

    finish();
    oPort->writeData();
}

