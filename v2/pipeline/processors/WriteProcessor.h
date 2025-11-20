#pragma once

#include "Processor.h"

#include "interfaces/PipelineBlockInputInterface.h"
#include "interfaces/PipelineBlockOutputInterface.h"
#include "metadata/PropertyType.h"

namespace db {

class Dataframe;

}

namespace db::v2 {

class PipelineV2;

class WriteProcessor final : public Processor {
public:
    static WriteProcessor* create(PipelineV2* pipeline);

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

    void performDeletes();
    void performCreates();
    void performUpdates();

    std::string describe() const final {
        return "WriteProcessor";
    }

private:
    PipelineBlockInputInterface _input;
    PipelineBlockOutputInterface _output;

    struct PropertyConstraint {
        std::string_view _propName;
        ValueType _type;
        ColumnTag _tag;
    };

    struct PropertyConstraints {
        std::vector<PropertyConstraint> _properties;
    };

    struct PendingNode {
        std::vector<std::string_view> _labels;
        PropertyConstraints* _properties;
    };

    struct PendingEdge {
        ColumnTag _srcTag;
        ColumnTag _tgtTag;
        std::string_view _edgeType;
        PropertyConstraints* _properties;
    };

    WriteProcessor() = default;
    ~WriteProcessor() final = default;
};
    
}
