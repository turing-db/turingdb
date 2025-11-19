#pragma once

#include "EntityOutputStream.h"
#include "PipelineInputInterface.h"

namespace db {
class NamedColumn;
}

namespace db::v2 {

class PipelineValuesInputInterface : public PipelineInputInterface {
public:
    constexpr PipelineInterfaceKind getKind() const override { return PipelineInterfaceKind::VALUES; }

    void setStream(const EntityOutputStream& stream) { _stream = stream; }
    void setValues(NamedColumn* values) { _values = values; }

    const EntityOutputStream& getStream() const { return _stream; }
    NamedColumn* getValues() const { return _values; }

private:
    EntityOutputStream _stream;

    NamedColumn* _values {nullptr};
};

}
