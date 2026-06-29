#include "ListAvailableGraphsProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "ExecutionContext.h"
#include "SystemManager.h"
#include "columns/ColumnVector.h"
#include "dataframe/NamedColumn.h"
#include "metadata/PropertyType.h"

using namespace db;

ListAvailableGraphsProcessor::ListAvailableGraphsProcessor()
{
}

ListAvailableGraphsProcessor::~ListAvailableGraphsProcessor() {
}

std::string ListAvailableGraphsProcessor::describe() const {
    return fmt::format("ListAvailableGraphsProcessor @={}", fmt::ptr(this));
}

ListAvailableGraphsProcessor* ListAvailableGraphsProcessor::create(PipelineV2* pipeline) {
    ListAvailableGraphsProcessor* proc = new ListAvailableGraphsProcessor();

    PipelineOutputPort* output = PipelineOutputPort::create(pipeline, proc);
    proc->_output.setPort(output);
    proc->addOutput(output);

    proc->postCreate(pipeline);
    return proc;
}

void ListAvailableGraphsProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;
    markAsPrepared();
}

void ListAvailableGraphsProcessor::reset() {
    markAsReset();
}

void ListAvailableGraphsProcessor::execute() {
    SystemAccessor* system = _ctxt->getSystemAccessor();

    auto* colName = _nameCol->as<ColumnVector<std::string>>();
    auto* colLoaded = _isLoadedCol->as<ColumnVector<types::Bool::Primitive>>();
    auto* colLoading = _isLoadingCol->as<ColumnVector<types::Bool::Primitive>>();

    // Every on-disk graph, each tagged with its current load state. isLoaded /
    // isLoading come from the SystemAccessor (the same source the removed
    // /get_graph_status endpoint used).
    std::vector<std::string> names;
    system->listAvailableGraphs(names);

    for (std::string& name : names) {
        colLoaded->push_back(system->getGraph(name) != nullptr);
        colLoading->push_back(system->isGraphLoading(name));
        colName->push_back(std::move(name));
    }

    _output.getPort()->writeData();
    finish();
}
