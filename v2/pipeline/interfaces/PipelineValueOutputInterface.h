#pragma once

#include <string_view>

#include "PipelineInputInterface.h"
#include "PipelineOutputInterface.h"

namespace db {
class NamedColumn;
}

namespace db::v2 {

class PipelineValueOutputInterface : public PipelineOutputInterface {
public:
    constexpr PipelineInterfaceKind getKind() const override { return PipelineInterfaceKind::VALUE; }

    void setIndices(NamedColumn* indices) { _indices = indices; }
    void setValue(NamedColumn* value) { _value = value; }

    NamedColumn* getIndices() const { return _indices; }
    NamedColumn* getValue() const { return _value; }

    void rename(const std::string_view& name) override;
    void connectTo(PipelineBlockInputInterface& input) override;

private:
    NamedColumn* _indices {nullptr};
    NamedColumn* _value {nullptr};
};

}
