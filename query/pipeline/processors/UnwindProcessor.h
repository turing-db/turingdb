#pragma once

#include "Processor.h"

#include "ListView.h"
#include "interfaces/PipelineValuesOutputInterface.h"
#include "metadata/PropertyType.h"

namespace db {

class Column;

class UnwindProcessor final : public Processor {
public:
    static UnwindProcessor* create(PipelineV2* pipeline, ListView list);
    static UnwindProcessor* create(PipelineV2* pipeline,
                                   ListView list,
                                   ValueType homogeneity);

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;
    std::string describe() const final { return "UnwindProcessor"; }

    ListView list() const { return _list; }

    PipelineValuesOutputInterface& output() { return _output; }

    ValueType homogeneity() const { return _homogeneity.value(); }

private:
    explicit UnwindProcessor(ListView list);
    UnwindProcessor(ListView list, ValueType homogeneity);
    ~UnwindProcessor() final;
    
    PipelineValuesOutputInterface _output;

    ListView _list;

    /// If @ref _list::size > @ref ChunkConfig::CHUNK_SIZE, chunk outputs
    size_t _index {0};

    std::optional<ValueType> _homogeneity;
    bool _isHomogeneous {false};

    void fillHeterogeneous(Column* outCol);
    void fillHomogeneous(Column* outCol);
};
}
