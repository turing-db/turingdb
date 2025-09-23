#include "MaterializeProcessor.h"

#include "MaterializeData.h"
#include "columns/ColumnOperators.h"
#include "columns/ColumnIndices.h"

#include "PipelineV2.h"
#include "PipelineBuffer.h"

#include "PipelineException.h"
#include "Panic.h"

using namespace db::v2;
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
            panic("copyChunk operator not handled between columns of kind {} and {}",
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
    msgbioassert(srcPtr, "col is invalid");
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
            panic("copyTransformedChunk operator not handled between columns of kind {} and {}",
                  srcPtr->getKind(), dstPtr->getKind());
        }
    }
}

}

MaterializeProcessor::MaterializeProcessor(MaterializeData* matData)
    : _matData(matData)
{
}

MaterializeProcessor::~MaterializeProcessor() {
}

MaterializeProcessor* MaterializeProcessor::create(PipelineV2* pipeline, MaterializeData* matData) {
    MaterializeProcessor* processor = new MaterializeProcessor(matData);

    PipelineBuffer* input = PipelineBuffer::create(pipeline);
    processor->_input = input;
    processor->addInput(input);

    PipelineBuffer* output = PipelineBuffer::create(pipeline);
    processor->_output = output;
    processor->addOutput(output);

    processor->postCreate(pipeline);
    return processor;
}

void MaterializeProcessor::prepare(ExecutionContext* ctxt) {
}

void MaterializeProcessor::reset() {
}

void MaterializeProcessor::execute() {
    Block& output = _matData->getOutput();
    const MaterializeData::Indices& indices = _matData->getIndices();
    const MaterializeData::ColumnsPerStep& columnsPerStep = _matData->getColumnsPerStep();
    const size_t colCount = _matData->getColumnCount();

    // Handle the simplest case in which no indices were provided
    if (indices.empty()) {
        size_t currentColIndex = 0;
        for (const MaterializeData::Columns& cols : columnsPerStep) {
            for (const Column* col : cols) {
                copyChunk(col, output[currentColIndex]);
                ++currentColIndex;
            }
        }
        return;
    }

    const ColumnIndices& lastIndices = *indices.back();
    const size_t lineCount = lastIndices.size();

    ColumnVector<size_t>& transform = _transform;
    transform = lastIndices;
    size_t currentColIndex = colCount - 1;
    const size_t lastStep = columnsPerStep.size() - 1;

    for (size_t currentStep = lastStep; currentStep > 0; --currentStep) {
        const MaterializeData::Columns& cols = columnsPerStep[currentStep];

        // If last step, don't use the transform, just copy columns
        if (currentStep == lastStep) {
            // Copy columns in reverse order
            for (auto colIt = cols.rbegin(); colIt != cols.rend(); ++colIt) {
                const Column* colPtr = *colIt;
                copyChunk(colPtr, output[currentColIndex]);
                --currentColIndex;
            }

            continue;
        }

        // Else use transform on all columns of the current step
        for (auto colIt = cols.rbegin(); colIt != cols.rend(); ++colIt) {
            const Column* colPtr = *colIt;
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
            transform[i] = a[transform[i]];
#else
            transform[i] = a.at(transform[i]);
#endif
        }
    }
}
