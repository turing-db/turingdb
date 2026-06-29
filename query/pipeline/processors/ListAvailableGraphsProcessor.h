#pragma once

#include "Processor.h"

#include "interfaces/PipelineBlockOutputInterface.h"

namespace db {

class NamedColumn;

class ListAvailableGraphsProcessor final : public Processor {
public:
    static ListAvailableGraphsProcessor* create(PipelineV2* pipeline);

    std::string describe() const final;

    PipelineBlockOutputInterface& output() { return _output; }

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

    void setNameColumn(NamedColumn* col) { _nameCol = col; }
    void setIsLoadedColumn(NamedColumn* col) { _isLoadedCol = col; }
    void setIsLoadingColumn(NamedColumn* col) { _isLoadingCol = col; }

private:
    ExecutionContext* _ctxt {nullptr};
    PipelineBlockOutputInterface _output;

    // One row per on-disk graph, tagged with its current load state. The name
    // column owns its strings (ColumnVector<std::string>), so no separate
    // backing buffer is needed.
    NamedColumn* _nameCol {nullptr};
    NamedColumn* _isLoadedCol {nullptr};
    NamedColumn* _isLoadingCol {nullptr};

    ListAvailableGraphsProcessor();
    ~ListAvailableGraphsProcessor();
};

}
