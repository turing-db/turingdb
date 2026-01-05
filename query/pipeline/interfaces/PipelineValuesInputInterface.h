#pragma once

#include "PipelineInputInterface.h"

namespace db {
class NamedColumn;
}

namespace db::v2 {

class PipelineValuesInputInterface : public PipelineInputInterface {
public:
    constexpr PipelineInterfaceKind getKind() const override { return PipelineInterfaceKind::VALUES; }

    void setValues(NamedColumn* values) { _values = values; }

    NamedColumn* getValues() const { return _values; }

private:
    NamedColumn* _values {nullptr};
};

}
