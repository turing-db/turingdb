#pragma once

#include <string_view>

#include "ColumnVector.h"

namespace db {

class ExecutionContext;
class SystemManager;

class ListGraphStep {
public:
    struct Tag {};

    explicit ListGraphStep(ColumnVector<std::string_view>* graphNames);

    void prepare(ExecutionContext* ctxt);

    void reset() {}

    inline bool isFinished() const { return true; }

    void execute();

private:
    ColumnVector<std::string_view>* _graphNames {nullptr};
    SystemManager* _sysMan {nullptr};
};

}
