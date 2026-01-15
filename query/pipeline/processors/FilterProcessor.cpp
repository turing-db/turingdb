#include "FilterProcessor.h"

#include <spdlog/fmt/fmt.h>
#include <range/v3/view/drop.hpp>

#include "ID.h"
#include "columns/ColumnKind.h"
#include "columns/ColumnMask.h"
#include "columns/ColumnOperators.h"
#include "columns/ColumnOptVector.h"
#include "dataframe/NamedColumn.h"
#include "dataframe/Dataframe.h"

#include "PipelinePort.h"

#include "PipelineException.h"
#include "metadata/PropertyType.h"
#include "processors/PredicateProgram.h"

#include "FatalException.h"

using namespace db;

namespace rg = ranges;
namespace rv = rg::views;

namespace {

#define APPLY_MASK_CASE(Type)                \
    case Type::staticKind(): {               \
        ColumnOperators::applyMask(          \
            static_cast<const Type*>(src),   \
            mask,                            \
            static_cast<Type*>(dest));       \
    }                                        \
    break;


void applyMask(const Column* src,
               const ColumnMask* mask,
               Column* dest) {
    switch (src->getKind()) {
        APPLY_MASK_CASE(ColumnVector<types::Bool::Primitive>)
        APPLY_MASK_CASE(ColumnVector<types::Int64::Primitive>)
        APPLY_MASK_CASE(ColumnVector<types::String::Primitive>) // Also covers string_view
        APPLY_MASK_CASE(ColumnVector<types::UInt64::Primitive>) // Also covers size_t
        APPLY_MASK_CASE(ColumnVector<types::Double::Primitive>)

        APPLY_MASK_CASE(ColumnOptVector<types::Bool::Primitive>)
        APPLY_MASK_CASE(ColumnOptVector<types::Int64::Primitive>)
        APPLY_MASK_CASE(ColumnOptVector<types::String::Primitive>) // Also covers string_view
        APPLY_MASK_CASE(ColumnOptVector<types::UInt64::Primitive>) // Also covers size_t
        APPLY_MASK_CASE(ColumnOptVector<types::Double::Primitive>)

        APPLY_MASK_CASE(ColumnVector<EntityID>)
        APPLY_MASK_CASE(ColumnVector<NodeID>)
        APPLY_MASK_CASE(ColumnVector<EdgeID>)
        APPLY_MASK_CASE(ColumnVector<LabelSetID>)
        APPLY_MASK_CASE(ColumnVector<EdgeTypeID>)
        APPLY_MASK_CASE(ColumnVector<PropertyTypeID>)
        APPLY_MASK_CASE(ColumnVector<std::string>)

        default: {
            throw PipelineException(fmt::format("Unsupported mask application for kind {}",
                                                src->getKind()));
        }
    }
}

}

std::string FilterProcessor::describe() const {
    return fmt::format("FilterProcessor @={}", fmt::ptr(this));
}

FilterProcessor::FilterProcessor(PredicateProgram* predProg)
    : _predProg(predProg)
{
}

FilterProcessor* FilterProcessor::create(PipelineV2* pipeline, PredicateProgram* predProg) {
    auto* proc = new FilterProcessor(predProg);

    {
        PipelineInputPort* filterInput = PipelineInputPort::create(pipeline, proc);
        proc->_input.setPort(filterInput);
        proc->addInput(filterInput);
    }

    {
        PipelineOutputPort* out = PipelineOutputPort::create(pipeline, proc);
        proc->_output.setPort(out);
        proc->addOutput(out);
    }

    proc->postCreate(pipeline);

    return proc;
}

void FilterProcessor::prepare(ExecutionContext*) {
    // Check dataframes have same number of columns
    const Dataframe* srcDF = _input.getDataframe();
    const Dataframe* destDF = _output.getDataframe();
    if (!srcDF->hasSameShape(destDF)) {
        throw PipelineException("FilterProcessor input and output dataframes must have same size and columns of same type.");
    }

    markAsPrepared();
}

void FilterProcessor::reset() {
    markAsReset();
}

void FilterProcessor::execute() {
    const Dataframe* srcDF = _input.getDataframe();
    Dataframe* destDF = _output.getDataframe();

    _predProg->evaluateInstructions();
    const std::vector<Column*>& predResults = _predProg->getTopLevelPredicates();

    // XXX: Should this be an error, or should we just not apply any filters?
    if (predResults.empty()) {
        throw FatalException("Pipeline encountered empty FilterProcessor.");
    }

    // Ensure all instructions outputs are the same size, so they may be combined
    const size_t maskSize = predResults.front()->size();
    if (!std::ranges::all_of(predResults, [maskSize](const Column* res) {
            return res->size() == maskSize;
        })) {
        throw FatalException("FilterProcessor PredicateProgram contained instructions with "
                             "output columns of differing sizes.");
    }
    // Ensure all instruction outputs are Column(Opt)Vector bools, so they can be converted to masks
    if (!std::ranges::all_of(predResults, [](const Column* res) {
            const ColumnKind::ColumnKindCode thisKind = res->getKind();

            const ColumnKind::ColumnKindCode optBoolKind = ColumnOptMask::staticKind();

            return thisKind == optBoolKind;
        })) {
        throw FatalException("FilterProcessor PredicateProgram contained an instruction which "
                             "was not a predicate.");
    }

    // Fold over all instructions, combining their output masks with AND, but maintaining
    // null values
    ColumnOptMask finalOptMask(maskSize, true);
    {
        for (Column* predicateResult : _predProg->getTopLevelPredicates()) {
            const auto* predResOptMask = dynamic_cast<ColumnOptMask*>(predicateResult);
            if (!predResOptMask) {
                throw FatalException(
                    "FilterProcessor PredicateProgram encountered non-predicate instruction.");
            }
            ColumnOperators::andOp(&finalOptMask, &finalOptMask, predResOptMask);
        }
    }

    ColumnMask finalMask;
    finalMask.fromColumnOptVector(finalOptMask);

    const size_t colCount = srcDF->size();

    bioassert(colCount == destDF->size(), "FilterProcessor input and output dataframes "
                                          "have a differing number of columns.");

    const auto& srcCols = srcDF->cols();
    const auto& destCols = destDF->cols();
    for (size_t i = 0; i < colCount; i++) {
        const Column* srcCol = srcCols[i]->getColumn();
        Column* destCol = destCols[i]->getColumn(); 
        applyMask(srcCol, &finalMask, destCol);
    }

    _input.getPort()->consume();

    // Only ever emit an empty chunk if our input is closed, to allow the next processor
    // to proceed.
    const bool haveOutput = destDF->getRowCount() != 0;
    const bool inputClosed = _input.getPort()->isClosed();
    if (haveOutput || inputClosed) {
        _output.getPort()->writeData();
    }

    finish();
}
