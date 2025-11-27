#include "MaterializeData.h"

#include "dataframe/DataframeManager.h"
#include "dataframe/Dataframe.h"
#include "dataframe/NamedColumn.h"
#include "LocalMemory.h"

#include "PipelineException.h"

using namespace db::v2;
using namespace db;

MaterializeData::MaterializeData(LocalMemory* mem, DataframeManager* dfMan)
    : _mem(mem),
    _dfMan(dfMan),
    _columnsPerStep(1)
{
}

void MaterializeData::initFromPrev(LocalMemory* mem,
                                   DataframeManager* dfMan,
                                   const MaterializeData& prevData) {
    _colCount = prevData._colCount;

    for (const auto& col : prevData._output->cols()) {
        Column* newCol = mem->allocSame(col->getColumn());
        auto* newNamedCol = NamedColumn::create(dfMan, newCol, col->getTag());

        _columnsPerStep[0].push_back(col->getColumn());
        _output->addColumn(newNamedCol);
    }
}

void MaterializeData::initFromDf(LocalMemory* mem,
                                 DataframeManager* dfMan,
                                 const Dataframe* df) {
    _colCount = df->size();
    for (const auto* col : df->cols()) {
        _columnsPerStep[0].push_back(col->getColumn());

        Column* outCol = _mem->allocSame(col->getColumn());
        NamedColumn* outNamedCol = NamedColumn::create(_dfMan, outCol, col->getTag());

        _output->addColumn(outNamedCol);
    }
}

void MaterializeData::cloneFromPrev(LocalMemory* mem,
                                    DataframeManager* dfMan,
                                    const MaterializeData& prevData) {
    _colCount = prevData._colCount;

    for (const auto& col : prevData._output->cols()) {
        Column* newCol = mem->allocSame(col->getColumn());
        auto* newNamedCol = NamedColumn::create(dfMan, newCol, col->getTag());

        _output->addColumn(newNamedCol);
    }
    _step = prevData._step;
    _indices = prevData._indices;
    _columnsPerStep = prevData._columnsPerStep;
    _colCount = prevData._colCount;
}

MaterializeData::~MaterializeData() {
}

void MaterializeData::createStep(const NamedColumn* indices) {
    const ColumnIndices* indicesCol = dynamic_cast<const ColumnIndices*>(indices->getColumn());
    if (!indicesCol) {
        throw PipelineException("MaterializeData: indices column is not set correctly");
    }

    _indices.push_back(indicesCol);
    _columnsPerStep.emplace_back();
    ++_step;
}

template <typename ColumnType>
void MaterializeData::addToStep(const NamedColumn* col) {
    ++_colCount;
    _columnsPerStep[_step].push_back(col->getColumn());

    ColumnType* outCol = _mem->alloc<ColumnType>();
    NamedColumn* outNamedCol = NamedColumn::create(_dfMan, outCol, col->getTag());
    _output->addColumn(outNamedCol);
}

#define INSTANTIATE(Type) \
    template void MaterializeData::addToStep<Type>(const NamedColumn* col);

INSTANTIATE(ColumnVector<EntityID>);
INSTANTIATE(ColumnVector<NodeID>);
INSTANTIATE(ColumnVector<EdgeID>);
INSTANTIATE(ColumnVector<LabelID>);
INSTANTIATE(ColumnVector<PropertyTypeID>);
INSTANTIATE(ColumnVector<EdgeTypeID>);
INSTANTIATE(ColumnVector<types::UInt64::Primitive>);
INSTANTIATE(ColumnVector<types::Int64::Primitive>);
INSTANTIATE(ColumnVector<types::Double::Primitive>);
INSTANTIATE(ColumnVector<types::String::Primitive>);
INSTANTIATE(ColumnVector<types::Bool::Primitive>);
INSTANTIATE(ColumnOptVector<types::UInt64::Primitive>);
INSTANTIATE(ColumnOptVector<types::Int64::Primitive>);
INSTANTIATE(ColumnOptVector<types::Double::Primitive>);
INSTANTIATE(ColumnOptVector<types::String::Primitive>);
INSTANTIATE(ColumnOptVector<types::Bool::Primitive>);
