#include "CartesianProductProcessor.h"

#include "PipelineV2.h"
#include "PipelinePort.h"
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
    PipelineOutputPort* oPort = _out.getPort();
    
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
    const size_t q = rDF->size();

    msgbioassert(m * n <= ChunkConfig::CHUNK_SIZE,
                 "Cartesian Product is only supported in the strongly bounded case "
                 "(output size <= CHUNK_SIZE).");

}

