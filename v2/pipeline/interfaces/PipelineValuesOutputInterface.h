#pragma once

#include <string_view>

#include "PipelineInputInterface.h"
#include "PipelineOutputInterface.h"

namespace db {
class NamedColumn;
}

namespace db::v2 {

class PipelineValuesOutputInterface : public PipelineOutputInterface {
public:
    PipelineValuesOutputInterface() = default;

    constexpr PipelineInterfaceKind getKind() const override { return PipelineInterfaceKind::VALUES; }

    void setIndices(NamedColumn* indices) { _indices = indices; }
    void setValues(NamedColumn* values) { _values = values; }

    NamedColumn* getIndices() const { return _indices; }
    NamedColumn* getValues() const { return _values; }

    void rename(const std::string_view& name) override;
    void connectTo(PipelineBlockInputInterface& input) override;
    void connectTo(PipelineNodeInputInterface& input) override;
    void connectTo(PipelineEdgeInputInterface& input) override;
    void connectTo(PipelineValuesInputInterface& input) override;

private:
    NamedColumn* _indices {nullptr};
    NamedColumn* _values {nullptr};
};

}
