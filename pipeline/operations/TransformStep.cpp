#include "TransformStep.h"

#include <sstream>
#include <range/v3/view/enumerate.hpp>
#include <range/v3/view/reverse.hpp>

#include "Profiler.h"
#include "columns/ColumnOperators.h"
#include "columns/Block.h"
#include "LocalMemory.h"
#include "VarDecl.h"

#include "BioAssert.h"

using namespace db;

namespace {

#define COPY_CHUNK_CASE(TType)                                                              \
    case TType::staticKind(): {                                                             \
        const auto& src = *static_cast<const TType*>(srcPtr);                               \
        auto& dst = *static_cast<TType*>(dstPtr);                                           \
        ColumnOperators::copyChunk<typename TType::ValueType>(src.begin(), src.end(), dst); \
        return;                                                                             \
    }

inline void copyChunk(const Column* srcPtr,
                      Column* dstPtr) {
    switch (srcPtr->getKind()) {
        COPY_CHUNK_CASE(ColumnVector<EntityID>)
        COPY_CHUNK_CASE(ColumnVector<NodeID>)
        COPY_CHUNK_CASE(ColumnVector<EdgeID>)
        COPY_CHUNK_CASE(ColumnVector<types::UInt64::Primitive>)
        COPY_CHUNK_CASE(ColumnVector<types::Int64::Primitive>)
        COPY_CHUNK_CASE(ColumnVector<types::Double::Primitive>)
        COPY_CHUNK_CASE(ColumnVector<types::String::Primitive>)
        COPY_CHUNK_CASE(ColumnVector<types::Bool::Primitive>)

        default: {
            bioassert(false, "copyChunk operator not handled between columns of kind {} and {}",
                      srcPtr->getKind(), dstPtr->getKind());
        }
    }
}

#define COPY_TRANSFORMED_CHUNK_CASE(TType)                                \
    case TType::staticKind(): {                                           \
        ColumnOperators::copyTransformedChunk<typename TType::ValueType>( \
            transform,                                                    \
            *static_cast<const TType*>(srcPtr),                           \
            *static_cast<TType*>(dstPtr));                                \
        return;                                                           \
    }

inline void copyTransformedChunk(const ColumnVector<size_t>& transform,
                                 const Column* srcPtr,
                                 Column* dstPtr) {
    switch (srcPtr->getKind()) {
        COPY_TRANSFORMED_CHUNK_CASE(ColumnVector<EntityID>)
        COPY_TRANSFORMED_CHUNK_CASE(ColumnVector<NodeID>)
        COPY_TRANSFORMED_CHUNK_CASE(ColumnVector<EdgeID>)
        COPY_TRANSFORMED_CHUNK_CASE(ColumnVector<types::UInt64::Primitive>)
        COPY_TRANSFORMED_CHUNK_CASE(ColumnVector<types::Int64::Primitive>)
        COPY_TRANSFORMED_CHUNK_CASE(ColumnVector<types::Double::Primitive>)
        COPY_TRANSFORMED_CHUNK_CASE(ColumnVector<types::String::Primitive>)
        COPY_TRANSFORMED_CHUNK_CASE(ColumnVector<types::Bool::Primitive>)
        default: {
            bioassert(false, "copyTransformedChunk operator not handled between columns of kind {} and {}",
                       srcPtr->getKind(), dstPtr->getKind());
        }
    }
}

}

TransformData::TransformData(LocalMemory* mem)
    : _mem(mem),
    _colInfo(1)
{
}

TransformData::~TransformData() {
}

template <typename T>
void TransformData::addColumn(const T* col, VarDecl* varDecl) {
    bioassert(_step < _colInfo.size(),
              "Step was not registered with addIndices step {}", _step);
    ++_colCount;
    _colInfo[_step].push_back(col);

    auto* outCol = _mem->alloc<T>();
    _output.addColumn(outCol);

    if (varDecl) {
        varDecl->setColumn(outCol);
    }
}

void TransformData::createStep(const ColumnIndices* indices) {
    _indices.push_back(indices);
    _colInfo.emplace_back();
    ++_step;
}

// TransformStep

TransformStep::TransformStep(TransformData* transformData)
    : _transformData(transformData)
{
}

TransformStep::~TransformStep() {
}

void TransformStep::execute() {
    Profile profile {"TransformStep::execute"};
    auto& output = _transformData->_output;
    const auto& indices = _transformData->_indices;
    const auto& colInfo = _transformData->_colInfo;
    const auto colCount = _transformData->_colCount;

    // Handle the simplest case in which no indices were provided
    if (indices.empty()) {
        size_t currentColIndex = 0;
        for (const auto& cols : colInfo) {
            for (const auto* col : cols) {
                copyChunk(col, output[currentColIndex]);
                ++currentColIndex;
            }
        }
        return;
    }

    const auto& lastIndices = *indices.back();
    const size_t lineCount = lastIndices.size();

    _transform = lastIndices;
    size_t currentColIndex = colCount - 1;
    const size_t lastStep = colInfo.size() - 1;

    for (const auto& [currentStep, cols] : colInfo
                                               | ranges::views::enumerate
                                               | ranges::views::reverse) {
        // If last step, don't use the transform, just copy columns
        if (currentStep == lastStep) {
            for (const auto* colPtr : cols | ranges::views::reverse) {
                copyChunk(colPtr, output[currentColIndex]);
                --currentColIndex;
            }
            continue;
        }

        // Else use transform on all columns of the current step
        for (const auto* colPtr : cols | ranges::views::reverse) {
            copyTransformedChunk(_transform, colPtr, output[currentColIndex]);
            --currentColIndex;
        }

        if (currentStep == 0) {
            break;
        }

        // Update transform for the next step
        const auto& a = *indices[currentStep - 1];

        for (size_t i = 0; i < lineCount; i++) {
#ifdef NDEBUG
            _transform[i] = a[_transform[i]];
#else
            _transform[i] = a.at(_transform[i]);
#endif
        }
    }
}

void TransformStep::describe(std::string& descr) const {
    std::stringstream ss;
    ss << "TransformStep";
    ss << " output={";
    for (const auto& col : _transformData->_output.columns()) {
        ss << std::hex << col << " ";
    }
    ss << "}";
    ss << " indices={";
    for (const auto& indices : _transformData->_indices) {
        ss << std::hex << indices << " ";
    }
    ss << "}";
    ss << " colInfo={";
    for (const auto& cols : _transformData->_colInfo) {
        ss << "{";
        for (const auto& col : cols) {
            ss << std::hex << col << " ";
        }
        ss << "}";
    }
    ss << "}";
    ss << " transform=" << std::hex << &_transform;
    descr.assign(ss.str());
}

#define INSTANTIATE(Type) \
    template void TransformData::addColumn<Type>(const Type* col, VarDecl* decl);

INSTANTIATE(ColumnVector<EntityID>);
INSTANTIATE(ColumnVector<NodeID>);
INSTANTIATE(ColumnVector<EdgeID>);
INSTANTIATE(ColumnVector<types::UInt64::Primitive>);
INSTANTIATE(ColumnVector<types::Int64::Primitive>);
INSTANTIATE(ColumnVector<types::Double::Primitive>);
INSTANTIATE(ColumnVector<types::String::Primitive>);
INSTANTIATE(ColumnVector<types::Bool::Primitive>);
