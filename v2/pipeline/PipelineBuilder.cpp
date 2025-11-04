#include "PipelineBuilder.h"

#include "processors/ScanNodesProcessor.h"
#include "processors/GetOutEdgesProcessor.h"
#include "processors/MaterializeProcessor.h"
#include "processors/LambdaProcessor.h"
#include "processors/GetPropertiesProcessor.h"
#include "processors/SkipProcessor.h"
#include "processors/LimitProcessor.h"
#include "processors/CountProcessor.h"

#include "columns/ColumnEdgeTypes.h"

#include "PipelineException.h"

using namespace db::v2;
using namespace db;

namespace {

void duplicateDataframeShape(LocalMemory* mem, Dataframe* src, Dataframe* dest) {
    for (const NamedColumn* col : src->cols()) {
        Column* newCol = mem->allocSame(col->getColumn());
        NamedColumn::create(dest, newCol, col->getHeader());
    }
}

}

PipelineBuilder& PipelineBuilder::addScanNodes() {
    openMaterialize();

    ScanNodesProcessor* proc = ScanNodesProcessor::create(_pipeline);
    PipelineNodeOutputInterface& outNodeIDs = proc->outNodeIDs();

    NamedColumn* nodeIDs = allocColumn<ColumnNodeIDs>(outNodeIDs.getDataframe());
    outNodeIDs.setNodeIDs(nodeIDs);

    _matProc->getMaterializeData().addToStep<ColumnNodeIDs>(nodeIDs);

    _pendingOutput = &outNodeIDs;

    return *this;
}

PipelineBuilder& PipelineBuilder::addGetOutEdges() {
    GetOutEdgesProcessor* getOutEdges = GetOutEdgesProcessor::create(_pipeline);

    switch (_pendingOutput->getKind()) {
        case PipelineInterfaceKind::NODE:
        {
            auto* nodeInput = static_cast<PipelineNodeOutputInterface*>(_pendingOutput);
            nodeInput->connectTo(getOutEdges->inNodeIDs());
        }
        break;
        case PipelineInterfaceKind::EDGE:
        {
            auto* edgeInput = static_cast<PipelineEdgeOutputInterface*>(_pendingOutput);
            edgeInput->connectTo(getOutEdges->inNodeIDs());
        }
        break;

        default:
            throw PipelineException("PipelineBuilder: cannot add get out edges to non-node or edge output");
        break;
    }

    PipelineEdgeOutputInterface& outEdges = getOutEdges->outEdges();
    Dataframe* outDf = outEdges.getDataframe();

    NamedColumn* indices = allocColumn<ColumnIndices>(outDf);
    outEdges.setIndices(indices);
    
    MaterializeData& matData = _matProc->getMaterializeData();
    matData.createStep(indices);

    NamedColumn* edgeIDs = allocColumn<ColumnEdgeIDs>(outDf);
    NamedColumn* edgeTypes = allocColumn<ColumnEdgeTypes>(outDf);
    NamedColumn* targetNodes = allocColumn<ColumnNodeIDs>(outDf);
    outEdges.setEdges(edgeIDs, edgeTypes, targetNodes);
    matData.addToStep<ColumnEdgeIDs>(edgeIDs);
    matData.addToStep<ColumnEdgeTypes>(edgeTypes);
    matData.addToStep<ColumnNodeIDs>(targetNodes);

    _pendingOutput = &outEdges;

    return *this;
}

PipelineBuilder& PipelineBuilder::openMaterialize() {
    _matProc = MaterializeProcessor::create(_pipeline, _mem);
    return *this;
}

PipelineBuilder& PipelineBuilder::closeMaterialize() {
    _matProc = nullptr;
    return *this;
}

PipelineBuilder& PipelineBuilder::connectToBlockInput(PipelineBlockInputInterface& input) {
    switch (_pendingOutput->getKind()) {
        case PipelineInterfaceKind::NODE:
            static_cast<PipelineNodeOutputInterface*>(_pendingOutput)->connectTo(input);
        break;
        case PipelineInterfaceKind::EDGE:
            static_cast<PipelineEdgeOutputInterface*>(_pendingOutput)->connectTo(input);
        break;
        case PipelineInterfaceKind::BLOCK:
            static_cast<PipelineBlockOutputInterface*>(_pendingOutput)->connectTo(input);
        break;
        case PipelineInterfaceKind::VALUES:
            static_cast<PipelineValuesOutputInterface*>(_pendingOutput)->connectTo(input);
        break;
        case PipelineInterfaceKind::VALUE:
            static_cast<PipelineValueOutputInterface*>(_pendingOutput)->connectTo(input);
        break;
    }

    return *this;
}

PipelineBuilder& PipelineBuilder::addMaterialize() {
    connectToBlockInput(_matProc->input());
    _pendingOutput = &_matProc->output();
    closeMaterialize();

    return *this;
}

PipelineBuilder& PipelineBuilder::addLambda(const LambdaProcessor::Callback& callback) {
    if (_matProc) {
        throw PipelineException("PipelineBuilder: cannot add lambda on pipeline with active materialize");
    }

    LambdaProcessor* lambda = LambdaProcessor::create(_pipeline, callback);
    connectToBlockInput(lambda->input());
    _pendingOutput = nullptr;

    return *this;
}

PipelineBuilder& PipelineBuilder::addSkip(size_t count) {
    SkipProcessor* skip = SkipProcessor::create(_pipeline, count);
    connectToBlockInput(skip->input());

    duplicateDataframeShape(_mem, skip->input().getDataframe(), skip->output().getDataframe());
    
    _pendingOutput = &skip->output();

    return *this;
}

PipelineBuilder& PipelineBuilder::addLimit(size_t count) {
    LimitProcessor* limit = LimitProcessor::create(_pipeline, count);
    connectToBlockInput(limit->input());

    duplicateDataframeShape(_mem, limit->input().getDataframe(), limit->output().getDataframe());

    _pendingOutput = &limit->output();

    return *this;
}

PipelineBuilder& PipelineBuilder::addCount() {
    CountProcessor* count = CountProcessor::create(_pipeline);
    connectToBlockInput(count->input());

    NamedColumn* countColumn = allocColumn<ColumnConst<size_t>>(count->output().getDataframe());
    count->output().setValue(countColumn);

    _pendingOutput = &count->output();
    return *this;
}

PipelineBuilder& PipelineBuilder::addLambdaSource(const LambdaSourceProcessor::Callback& callback) {
    LambdaSourceProcessor* source = LambdaSourceProcessor::create(_pipeline, callback);
    _pendingOutput = &source->output();
    return *this;
}

template <db::SupportedType T>
PipelineBuilder& PipelineBuilder::addGetNodeProperties(PropertyType propertyType) {
    auto* getProps = GetNodePropertiesProcessor<T>::create(_pipeline, propertyType);

    switch (_pendingOutput->getKind()) {
        case PipelineInterfaceKind::NODE:
        {
            auto* nodeInput = static_cast<PipelineNodeOutputInterface*>(_pendingOutput);
            nodeInput->connectTo(getProps->inIDs());
        }
        break;
        case PipelineInterfaceKind::EDGE:
        {
            auto* edgeInput = static_cast<PipelineEdgeOutputInterface*>(_pendingOutput);
            edgeInput->connectTo(getProps->inIDs());
        }
        break;
        default:
            throw PipelineException("PipelineBuilder: cannot add GetNodeProperties to non-node output");
        break;
    }

    using ColumnValues = ColumnVector<typename T::Primitive>;

    PipelineValuesOutputInterface& outValues = getProps->outValues();
    Dataframe* outDf = outValues.getDataframe();

    NamedColumn* indices = allocColumn<ColumnIndices>(outDf);
    outValues.setIndices(indices);

    MaterializeData& matData = _matProc->getMaterializeData();
    matData.createStep(indices);

    NamedColumn* values = allocColumn<ColumnValues>(outDf);
    outValues.setValues(values);
    matData.addToStep<ColumnValues>(values);

    _pendingOutput = &getProps->outValues();
    
    return *this;
}

template PipelineBuilder& PipelineBuilder::addGetNodeProperties<db::types::Int64>(PropertyType propertyType);
template PipelineBuilder& PipelineBuilder::addGetNodeProperties<db::types::UInt64>(PropertyType propertyType);
template PipelineBuilder& PipelineBuilder::addGetNodeProperties<db::types::Double>(PropertyType propertyType);
template PipelineBuilder& PipelineBuilder::addGetNodeProperties<db::types::String>(PropertyType propertyType);
template PipelineBuilder& PipelineBuilder::addGetNodeProperties<db::types::Bool>(PropertyType propertyType);
