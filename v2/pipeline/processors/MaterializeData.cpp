#include "MaterializeData.h"

#include "columns/Block.h"
#include "columns/ColumnVector.h"

#include "LocalMemory.h"

using namespace db::v2;
using namespace db;

MaterializeData::MaterializeData(LocalMemory* mem)
    : _mem(mem),
    _columnsPerStep(1)
{
}

MaterializeData::~MaterializeData() {
}

void MaterializeData::createStep(const ColumnIndices* indices) {
    _indices.push_back(indices);
    _columnsPerStep.emplace_back();
    ++_step;
}

template <typename T>
void MaterializeData::addToStep(const T* col) {
    ++_colCount;
    _columnsPerStep[_step].push_back(col);

    Column* outCol = _mem->alloc<T>();
    _output->addColumn(outCol);
}

#define INSTANTIATE(Type) \
    template void MaterializeData::addToStep<Type>(const Type* col);

INSTANTIATE(ColumnVector<EntityID>);
INSTANTIATE(ColumnVector<NodeID>);
INSTANTIATE(ColumnVector<EdgeID>);
INSTANTIATE(ColumnVector<types::UInt64::Primitive>);
INSTANTIATE(ColumnVector<types::Int64::Primitive>);
INSTANTIATE(ColumnVector<types::Double::Primitive>);
INSTANTIATE(ColumnVector<types::String::Primitive>);
INSTANTIATE(ColumnVector<types::Bool::Primitive>);
