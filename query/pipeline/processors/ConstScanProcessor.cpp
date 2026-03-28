#include "ConstScanProcessor.h"

#include <algorithm>

#include <spdlog/fmt/fmt.h>
#include <type_traits>

#include "BioAssert.h"
#include "ID.h"
#include "PipelineV2.h"
#include "PipelinePort.h"
#include "ExecutionContext.h"
#include "columns/AllowedKinds.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "dataframe/NamedColumn.h"
#include "views/GraphView.h"
#include "reader/GraphReader.h"

using namespace db;

namespace {

struct SortAndFilterID {
    // Node and Edge IDs need to be valid: filter invalids and then sort
    template <TypedInternalID IDT>
    void operator()(ColumnVector<IDT>* col) {
        const GraphReader reader = _ctxt->getGraphView().read();

        const auto invalid = [&reader](IDT id) -> bool {
            if constexpr (std::is_same_v<NodeID, IDT>) {
                return !reader.graphHasNode(id);
            }
            if constexpr (std::is_same_v<EdgeID, IDT>) {
                return !reader.graphHasEdge(id);
            }
            return false;
        };

        std::vector<IDT>& raw = col->getRaw();
        
        // Remove invalid IDs
        std::erase_if(begin(raw), end(raw), invalid);

        // Sort for better access patterns in subsequent processors
        std::sort(begin(raw), end(raw));
    }

    // Otherwise: no need to do anything
    template <typename T>
    void operator()(ColumnVector<T>*) {
    }

    ExecutionContext* _ctxt {nullptr};
};

}

ConstScanProcessor::ConstScanProcessor(Column* values)
    : _values(values)
{
}

ConstScanProcessor::~ConstScanProcessor() {
}

std::string ConstScanProcessor::describe() const {
    return fmt::format("ConstScanProcessor n={} @={}", _values->size(), fmt::ptr(this));
}

ConstScanProcessor* ConstScanProcessor::create(PipelineV2* pipeline, Column* values) {
    ConstScanProcessor* proc = new ConstScanProcessor(values);

    PipelineOutputPort* output = PipelineOutputPort::create(pipeline, proc);
    proc->_output.setPort(output);
    proc->addOutput(output);

    proc->postCreate(pipeline);
    return proc;
}

void ConstScanProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;

    bioassert(_values, "Null values column");

    using Types = ConstScanTypes;
    using ValidateSortIDs =
        ColumnSingleDispatcher<Types::Allowed, SortAndFilterID, Types::Excluded>;

    SortAndFilterID preprocessor {._ctxt = _ctxt};
    ValidateSortIDs::dispatch(_values, preprocessor);

    _offset = 0;

    markAsPrepared();
}

void ConstScanProcessor::reset() {
    _offset = 0;
    markAsReset();
}

struct ExecuteCycle {
    
};

void ConstScanProcessor::execute() {
    const NamedColumn* namedValues = _output.getValues();
    Column* outCol = namedValues->getColumn();

    const size_t remaining = _values->size() - _offset;
    const size_t chunkSize = std::min(remaining, _ctxt->getChunkSize());

    _outCol->resize(chunkSize);
    std::copy_n(_sortedValues.data() + _offset, chunkSize, _outCol->data());
    _offset += chunkSize;

    _output.getPort()->writeData();

    if (_offset >= _sortedValues.size()) {
        finish();
    }
}

namespace db {
template class ConstScanProcessor<NodeID>;
}
