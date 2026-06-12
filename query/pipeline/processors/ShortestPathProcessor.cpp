#include "ShortestPathProcessor.h"

#include "LocalMemory.h"
#include "PipelineV2.h"
#include "PipelinePort.h"
#include "ExecutionContext.h"
#include "iterators/GetOutEdgesIterator.h"
#include "iterators/GetPropertiesIterator.h"
#include "dataframe/Dataframe.h"

using namespace db;

template <SupportedType T>
ShortestPathProcessor<T>::ShortestPathProcessor(LocalMemory* mem,
                                                ColumnTag sourceTag,
                                                ColumnTag targetTag,
                                                const PropertyType& edgeType)
    : _mem(mem),
    _sourceColumn(sourceTag),
    _targetColumn(targetTag),
    _edgeType(edgeType)
{
}

template <SupportedType T>
ShortestPathProcessor<T>::~ShortestPathProcessor() = default;

template <SupportedType T>
ShortestPathProcessor<T>* ShortestPathProcessor<T>::create(PipelineV2* pipeline,
                                                           LocalMemory* mem,
                                                           ColumnTag sourceTag,
                                                           ColumnTag targetTag,
                                                           const PropertyType& edgeType) {
    auto* processor = new ShortestPathProcessor(mem,
                                                sourceTag,
                                                targetTag,
                                                edgeType);

    {
        PipelineInputPort* sourceInputPort = PipelineInputPort::create(pipeline, processor);
        processor->_source.setPort(sourceInputPort);
        processor->addInput(sourceInputPort);
        // For the inputs - they can fill up independently so neither port needs data
        // for the processor to run
        sourceInputPort->setNeedsData(false);
    }

    {
        PipelineInputPort* targetInputPort = PipelineInputPort::create(pipeline, processor);
        processor->_target.setPort(targetInputPort);
        processor->addInput(targetInputPort);
        targetInputPort->setNeedsData(false);
    }

    {
        PipelineOutputPort* output = PipelineOutputPort::create(pipeline, processor);
        processor->_out.setPort(output);
        processor->addOutput(output);
    }

    processor->postCreate(pipeline);
    return processor;
}

template <SupportedType T>
void ShortestPathProcessor<T>::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;
    const GraphView& view = _ctxt->getGraphView();

    _input = _mem->alloc<ColumnNodeIDs>();
    _outputEdges = _mem->alloc<ColumnEdgeIDs>();
    _outputNodes = _mem->alloc<ColumnNodeIDs>();
    _outputIndices = _mem->alloc<ColumnIndices>();

    _getOutEdgesWriter = std::make_unique<GetOutEdgesChunkWriter>(view, _input);
    _getOutEdgesWriter->setIndices(_outputIndices);
    _getOutEdgesWriter->setEdgeIDs(_outputEdges);
    _getOutEdgesWriter->setTgtIDs(_outputNodes);

    _propertyIndices = _mem->alloc<ColumnIndices>();
    _properties = _mem->alloc<ColumnVector<EdgePropType>>();
    _getPropertiesWriter = std::make_unique<GetPropertiesChunkWriter<EdgeID, T>>(view,
                                                                                 _edgeType._id,
                                                                                 _outputEdges);
    _getPropertiesWriter->setOutput(_properties);
    _getPropertiesWriter->setIndices(_propertyIndices);

    _runner.initialize(_input, _outputEdges, _outputNodes, _outputIndices,
                       _getOutEdgesWriter.get(),
                       _propertyIndices, _properties, _getPropertiesWriter.get());

    markAsPrepared();
}

template <SupportedType T>
void ShortestPathProcessor<T>::reset() {
    markAsReset();
}

template <SupportedType T>
void ShortestPathProcessor<T>::execute() {
    if (_target.getPort()->hasData()) {
        const Dataframe* targetDf = _target.getDataframe();
        auto* col = targetDf->getColumn<ColumnNodeIDs>(_targetColumn);
        if (!col) {
            throw TuringException("Could not find target column");
        }

        for (const NodeID val : *col) {
            _targetNodes.insert(val);
        }

        _target.getPort()->consume();
    }

    if (_source.getPort()->hasData()) {
        const Dataframe* sourceDf = _source.getDataframe();
        auto* col = sourceDf->getColumn<ColumnNodeIDs>(_sourceColumn);

        if (!col) {
            throw TuringException("Could not find source column");
        }

        for (const auto val : *col) {
            _heap.push({val, NodeID(), EdgeID(), 0});
            _heapValueMap.insert({
                val, {NodeID(), EdgeID(), 0}
            });
        }

        _source.getPort()->consume();
    }

    // execute the algorithm only once we have all the data
    if (!(_source.getPort()->isClosed() &&
        _target.getPort()->isClosed())) {
        finish();
        return;
    }
    _runner.run(_heap, _heapValueMap, _targetNodes, true);

    const Dataframe* outDf = _out.getDataframe();

    ColumnVector<EdgePropType>* distCol = outDf->getColumn<ColumnVector<EdgePropType>>(_distTag);
    if (!distCol) {
        throw TuringException("Could not find distance column");
    }

    ColumnVector<Path>* pathCol = outDf->getColumn<ColumnVector<Path>>(_pathTag);
    if (!pathCol) {
        throw TuringException("Could not find path column");
    }

    if (!_runner.results().empty()) {
        const DijkstraResult<EdgePropType>& result = _runner.results().front();
        distCol->push_back(result.distance);
        auto& pathVec = pathCol->emplace_back();
        pathVec = result.path;
    }

    _out.getPort()->writeData();
    finish();
}

namespace db {
template class ShortestPathProcessor<types::UInt64>;
template class ShortestPathProcessor<types::Int64>;
template class ShortestPathProcessor<types::Double>;
}
