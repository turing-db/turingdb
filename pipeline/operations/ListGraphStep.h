#pragma once

#include <string_view>
#include <string>

#include "columns/ColumnVector.h"

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

    void describe(std::string& descr) const;

private:
    ColumnVector<std::string_view>* _graphNames {nullptr};
    SystemManager* _sysMan {nullptr};
};

}
