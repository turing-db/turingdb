#include "ScanLabelsProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "PipelineV2.h"
#include "dataframe/Dataframe.h"
#include "iterators/ScanLabelsIterator.h"
#include "dataframe/NamedColumn.h"
#include "PipelinePort.h"
#include "ExecutionContext.h"

#include "PipelineException.h"
#include "views/GraphView.h"

using namespace db::v2;

ScanLabelsProcessor::ScanLabelsProcessor()
{
}

ScanLabelsProcessor::~ScanLabelsProcessor() {
}

std::string ScanLabelsProcessor::describe() const {
    return fmt::format("ScanLabelsProcessor @={}", fmt::ptr(this));
}

ScanLabelsProcessor* ScanLabelsProcessor::create(PipelineV2* pipeline) {
    ScanLabelsProcessor* scanLabels = new ScanLabelsProcessor;

    PipelineOutputPort* output = PipelineOutputPort::create(pipeline, scanLabels);
    scanLabels->_output.setPort(output);
    scanLabels->addOutput(output);

    scanLabels->postCreate(pipeline);
    return scanLabels;
}

void ScanLabelsProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;

    Dataframe* outDf = _output.getDataframe();

    ColumnVector<LabelID>* idsCol = nullptr;
    ColumnVector<std::string_view>* namesCol = nullptr;

    if (!_idsTag.isValid() && !_namesTag.isValid()) {
        throw PipelineException("ScanLabelsProcessor must output at least one column");
    }

    if (_idsTag.isValid()) {
        NamedColumn* labelIDs = outDf->getColumn(_idsTag);
        if (!labelIDs) [[unlikely]] {
            throw PipelineException(fmt::format("LabelIDs column is null"));
        }

        idsCol = dynamic_cast<ColumnVector<LabelID>*>(labelIDs->getColumn());
        if (!idsCol) [[unlikely]] {
            throw PipelineException(fmt::format("LabelIDs column is not a vector<LabelID>"));
        }
    }

    if (_namesTag.isValid()) {
        NamedColumn* labelNames = outDf->getColumn(_namesTag);
        if (!labelNames) [[unlikely]] {
            throw PipelineException(fmt::format("LabelNames column is null"));
        }

        namesCol = dynamic_cast<ColumnVector<std::string_view>*>(labelNames->getColumn());
        if (!namesCol) [[unlikely]] {
            throw PipelineException(fmt::format("LabelNames column is not a vector<string_view>"));
        }
    }

    const GraphView& graphView = ctxt->getGraphView();
    const GraphMetadata& metadata = graphView.metadata();
    const LabelMap& labelMap = metadata.labels();

    _it = std::make_unique<ScanLabelsChunkWriter>(labelMap);
    _it->setLabels(idsCol);
    _it->setNames(namesCol);

    markAsPrepared();
}

void ScanLabelsProcessor::reset() {
    _it->reset();
}

void ScanLabelsProcessor::execute() {
    _it->fill(_ctxt->getChunkSize());

    if (!_it->isValid()) {
        finish();
    }

    _output.getPort()->writeData();
}
