#pragma once

#include <string>

#include "columns/ColumnConst.h"
#include "types/PropertyType.h"

namespace db {

class ExecutionContext;

class CountStep {
public:
    struct Tag {};
    using ColumnCount = ColumnConst<types::UInt64::Primitive>;

    CountStep(const Column* input, ColumnCount* count)
        : _input(input),
        _count(count)
    {
    }

    void prepare(ExecutionContext* ctxt) {}

    inline void reset() {}

    inline bool isFinished() const { return true; }

    inline void execute() {
        auto& countValue = _count->getRaw();
        countValue += _input->size();
    }

    void describe(std::string& descr) const;

private:
    const Column* _input {nullptr};
    ColumnCount* _count {nullptr};
};

}
