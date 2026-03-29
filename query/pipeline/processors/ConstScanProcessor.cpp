#include "ConstScanProcessor.h"

#include <algorithm>

#include <spdlog/fmt/fmt.h>
#include <type_traits>

#include "ID.h"
#include "PipelineV2.h"
#include "PipelinePort.h"
#include "ExecutionContext.h"
#include "columns/AllowedKinds.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "dataframe/NamedColumn.h"
#include "views/GraphView.h"
#include "reader/GraphReader.h"

#include "BioAssert.h"

using namespace db;

namespace {

struct SortAndFilterID {
    // Node and Edge IDs need to be valid: filter invalids and then sort
    template <TypedInternalID IDT>
    void operator()(const ColumnVector<IDT>*) {
        auto* col = dynamic_cast<ColumnVector<IDT>*>(_values);
        bioassert(_values, "Null values column");

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
        std::erase_if(raw, invalid);

        // Sort for better access patterns in subsequent processors
        std::ranges::sort(raw);
    }

    // Otherwise: no need to do anything
    template <typename T>
    void operator()(ColumnVector<T>*) {}

    ExecutionContext* _ctxt {nullptr};
    Column* _values {nullptr};
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

    SortAndFilterID preprocessor {._ctxt = _ctxt, ._values = _values};
    ValidateSortIDs::dispatch(_values, preprocessor);

    _offset = 0;

    markAsPrepared();
}

void ConstScanProcessor::reset() {
    _offset = 0;
    markAsReset();
}

struct ExecuteCycle {
    template <typename T>
    void operator()(const ColumnVector<T>* src) {
        const size_t remaining = src->size() - _offset;
        const size_t canWrite = std::min(remaining, _chunkSize);

        auto* casted = dynamic_cast<ColumnVector<T>*>(_out);

        casted->resize(canWrite);

        std::copy_n(src->begin() + _offset, canWrite, casted->data());
        _offset += canWrite;
    }

    Column* _out {nullptr};
    size_t& _offset;
    size_t _chunkSize {0};
};

void ConstScanProcessor::execute() {
    const NamedColumn* namedValues = _output.getValues();
    Column* outCol = namedValues->getColumn();

    using Types = ConstScanTypes;
    using Dispatcher =
        ColumnSingleDispatcher<Types::Allowed, ExecuteCycle, Types::Excluded>;

    ExecuteCycle exctor {
        ._out = outCol, ._offset = _offset, ._chunkSize = _ctxt->getChunkSize()};

    Dispatcher::dispatch(_values, exctor);

    _output.getPort()->writeData();

    if (_offset >= _values->size()) {
        finish();
    }
}

