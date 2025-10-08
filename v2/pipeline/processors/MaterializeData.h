#pragma once

#include <vector>

#include "columns/ColumnIndices.h"

namespace db {
class LocalMemory;
class Block;
}

namespace db::v2 {

class PipelineV2;

class MaterializeData {
public:
    friend PipelineV2;

    using Columns = std::vector<const Column*>;
    using ColumnsPerStep = std::vector<Columns>;
    using Indices = std::vector<const ColumnIndices*>;

    static MaterializeData* create(PipelineV2* pipeline, LocalMemory* mem);

    MaterializeData(const MaterializeData& other) = delete;
    MaterializeData& operator=(const MaterializeData& other) = delete;
    MaterializeData(MaterializeData&& other) = delete;
    MaterializeData& operator=(MaterializeData&& other) = delete;

    void setOutput(Block* output) { _output = output; }

    const Indices& getIndices() const { return _indices; }
    const ColumnsPerStep& getColumnsPerStep() const { return _columnsPerStep; }
    size_t getColumnCount() const { return _colCount; }

    // Call this before adding columns to the step
    void createStep(const ColumnIndices* indices);

    // Add a column to the same step
    template <typename T>
    void addToStep(const T* col);

private:
    LocalMemory* _mem {nullptr};
    Block* _output {nullptr};
    size_t _step {0};
    Indices _indices;
    ColumnsPerStep _columnsPerStep;
    size_t _colCount {0};

    MaterializeData(LocalMemory* mem);
    ~MaterializeData();
};

}
