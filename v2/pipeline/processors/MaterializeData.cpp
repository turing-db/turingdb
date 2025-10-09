#include "MaterializeData.h"

#include "PipelineV2.h"
#include "PipelineBuffer.h"

#include "LocalMemory.h"

using namespace db::v2;
using namespace db;

MaterializeData::MaterializeData(LocalMemory* mem, PipelineBuffer* buffer)
    : _mem(mem),
    _output(buffer),
    _columnsPerStep(1)
{
}

MaterializeData::~MaterializeData() {
}

MaterializeData* MaterializeData::create(PipelineV2* pipeline, LocalMemory* mem) {
    PipelineBuffer* buffer = PipelineBuffer::create(pipeline);
    MaterializeData* data = new MaterializeData(mem, buffer);
    pipeline->addMaterializeData(data);
    return data;
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
    _output->getBlock().addColumn(outCol);
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
