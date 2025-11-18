#include "MaterializeProcessor.h"

#include "MaterializeData.h"

#include "dataframe/Dataframe.h"
#include "dataframe/NamedColumn.h"
#include "columns/ColumnOperators.h"
#include "columns/ColumnIndices.h"

#include "PipelineV2.h"
#include "PipelineBuffer.h"
#include "PipelinePort.h"

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

MaterializeProcessor::MaterializeProcessor(LocalMemory* mem, DataframeManager* dfMan)
    : _matData(mem, dfMan)
{
}

MaterializeProcessor::~MaterializeProcessor() {
}

std::string MaterializeProcessor::describe() const {
    return fmt::format("MaterializeProcessor @={}", fmt::ptr(this));
}

MaterializeProcessor* MaterializeProcessor::create(PipelineV2* pipeline, LocalMemory* mem) {
    MaterializeProcessor* materialize = new MaterializeProcessor(mem, pipeline->getDataframeManager());

    PipelineInputPort* input = PipelineInputPort::create(pipeline, materialize);
    materialize->_input.setPort(input);
    materialize->addInput(input);

    PipelineOutputPort* output = PipelineOutputPort::create(pipeline, materialize);
    materialize->_output.setPort(output);
    materialize->addOutput(output);

    materialize->_matData.setOutput(output->getBuffer()->getDataframe());

    materialize->postCreate(pipeline);
    return materialize;
}

void MaterializeProcessor::prepare(ExecutionContext* ctxt) {
    markAsPrepared();
}

void MaterializeProcessor::reset() {
    markAsReset();
}

void MaterializeProcessor::execute() {
    _input.getPort()->consume();
    _output.getPort()->writeData();
    finish();

    const Dataframe::NamedColumns& output = _output.getDataframe()->cols();
    const MaterializeData::Indices& indices = _matData.getIndices();
    const MaterializeData::ColumnsPerStep& columnsPerStep = _matData.getColumnsPerStep();
    const size_t colCount = _matData.getColumnCount();

    // Handle the simplest case in which no indices were provided
    if (indices.empty()) {
        size_t currentColIndex = 0;
        for (const MaterializeData::Columns& cols : columnsPerStep) {
            for (const Column* col : cols) {
                NamedColumn* destCol = output[currentColIndex];
                copyChunk(col, destCol->getColumn());
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

    for (size_t currentStep = lastStep; currentStep >= 0; --currentStep) {
        const MaterializeData::Columns& cols = columnsPerStep[currentStep];

        // If last step, don't use the transform, just copy columns
        if (currentStep == lastStep) {
            // Copy columns in reverse order
            for (auto colIt = cols.rbegin(); colIt != cols.rend(); ++colIt) {
                const Column* colPtr = *colIt;
                NamedColumn* destCol = output[currentColIndex];
                copyChunk(colPtr, destCol->getColumn());
                --currentColIndex;
            }

            continue;
        }

        // Else use transform on all columns of the current step
        for (auto colIt = cols.rbegin(); colIt != cols.rend(); ++colIt) {
            const Column* colPtr = *colIt;
            NamedColumn* destCol = output[currentColIndex];
            copyTransformedChunk(_transform, colPtr, destCol->getColumn());
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
