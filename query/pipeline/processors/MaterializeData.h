#pragma once

#include <vector>

#include "columns/ColumnIndices.h"

namespace db {
class LocalMemory;
class DataframeManager;
class Dataframe;
class NamedColumn;
}

namespace db {

class MaterializeData {
public:
    using Columns = std::vector<const Column*>;
    using ColumnsPerStep = std::vector<Columns>;
    using Indices = std::vector<const ColumnIndices*>;

    explicit MaterializeData(LocalMemory* mem,
                             DataframeManager* dfMan);

    void initFromPrev(LocalMemory* mem,
                      DataframeManager* dfMan,
                      const MaterializeData& prevData);

    /*
    Needed when a processor takes a materialised input and produces the new set
    of columns to be propogated in a separate dataframe.
    */
    void initFromDf(LocalMemory* mem, DataframeManager* dfMan, const Dataframe* df);

    /*
    Needed in the fork where we want to copy the mat proc across multiple instances
    */
    void cloneFromPrev(LocalMemory* mem, DataframeManager* dfMan,
                       const MaterializeData& prevData);

    ~MaterializeData();

    MaterializeData(const MaterializeData& other) = delete;
    MaterializeData& operator=(const MaterializeData& other) = delete;
    MaterializeData(MaterializeData&& other) = delete;
    MaterializeData& operator=(MaterializeData&& other) = delete;

    void setOutput(Dataframe* output) { _output = output; }

    bool isSingleStep() const { return _step == 0; }

    const Indices& getIndices() const { return _indices; }
    const ColumnsPerStep& getColumnsPerStep() const { return _columnsPerStep; }
    ColumnsPerStep& getColumnsPerStep() { return _columnsPerStep; }
    size_t getColumnCount() const { return _colCount; }

    // Call this before adding columns to the step
    void createStep(const NamedColumn* indices);

    // Add a column to the same step
    template <typename ColumnType>
    void addToStep(const NamedColumn* col);

private:
    LocalMemory* _mem {nullptr};
    DataframeManager* _dfMan {nullptr};
    Dataframe* _output {nullptr};
    size_t _step {0};
    Indices _indices;
    ColumnsPerStep _columnsPerStep;
    size_t _colCount {0};
};

}
