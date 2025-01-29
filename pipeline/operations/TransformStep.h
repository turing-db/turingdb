#pragma once

#include <vector>

#include "columns/ColumnIndices.h"
#include "columns/Block.h"

namespace db {

class TransformData;
class ExecutionContext;
class LocalMemory;
class VarDecl;

class TransformStep {
public:
    struct Tag {};

    TransformStep(TransformData* transformData);
    ~TransformStep();

    void prepare(ExecutionContext* ctxt) {}

    inline void reset() {}

    inline bool isFinished() { return true; }

    void execute();

private:
    TransformData* _transformData;
    ColumnVector<size_t> _transform;
};

class TransformData {
public:
    friend TransformStep;
    
    TransformData(LocalMemory* mem);
    ~TransformData();

    const Block& getOutput() const { return _output; }

    void createStep(const ColumnIndices* indices);

    template <typename T>
    void addColumn(const T* col, VarDecl* varDecl);

private:
    using Columns = std::vector<const Column*>;

    LocalMemory* _mem {nullptr};
    Block _output;
    size_t _step {0};
    std::vector<const ColumnIndices*> _indices;
    std::vector<Columns> _colInfo;
    size_t _colCount {0};
};

}
