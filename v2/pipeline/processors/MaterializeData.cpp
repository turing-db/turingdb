#include "MaterializeData.h"

#include "dataframe/Dataframe.h"
#include "dataframe/NamedColumn.h"
#include "columns/ColumnVector.h"

#include "LocalMemory.h"

#include "PipelineException.h"

using namespace db::v2;
using namespace db;

MaterializeData::MaterializeData(LocalMemory* mem)
    : _mem(mem),
    _columnsPerStep(1)
{
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
    NamedColumn::create(_output, outCol, col->getHeader());
}

#define INSTANTIATE(Type) \
    template void MaterializeData::addToStep<Type>(const NamedColumn* col);

INSTANTIATE(ColumnVector<EntityID>);
INSTANTIATE(ColumnVector<NodeID>);
INSTANTIATE(ColumnVector<EdgeID>);
INSTANTIATE(ColumnVector<types::UInt64::Primitive>);
INSTANTIATE(ColumnVector<types::Int64::Primitive>);
INSTANTIATE(ColumnVector<types::Double::Primitive>);
INSTANTIATE(ColumnVector<types::String::Primitive>);
INSTANTIATE(ColumnVector<types::Bool::Primitive>);
