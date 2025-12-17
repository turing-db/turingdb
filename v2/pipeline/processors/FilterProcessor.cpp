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
#include "processors/ExprProgram.h"

#include "FatalException.h"

using namespace db;
using namespace db::v2;

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

FilterProcessor::FilterProcessor(ExprProgram* exprProg)
    : _exprProg(exprProg)
{
}

FilterProcessor* FilterProcessor::create(PipelineV2* pipeline, ExprProgram* exprProg) {
    auto* proc = new FilterProcessor(exprProg);

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

    _exprProg->evaluateInstructions();
    const ExprProgram::Instructions instrs = _exprProg->instrs();

    // XXX: Should this be an error, or should we just not apply any filters?
    if (instrs.empty()) {
        throw FatalException("Pipeline encountered empty FilterProcessor.");
    }

    // Ensure all instructions outputs are the same size, so they may be combined
    const size_t maskSize = instrs.front()._res->size();
    if (!std::ranges::all_of(instrs, [maskSize](const ExprProgram::Instruction& instr) {
            return instr._res->size() == maskSize;
        })) {
        throw FatalException("FilterProcessor ExprProgram contained instructions with "
                             "output columns of differing sizes.");
    }
    // Ensure all instruction outputs are Column(Opt)Vector bools, so they can be converted to masks
    if (!std::ranges::all_of(instrs, [](const ExprProgram::Instruction& instr) {
            const ColumnKind::ColumnKindCode thisKind = instr._res->getKind();
            const ColumnKind::ColumnKindCode optBoolKind =
                ColumnOptVector<types::Bool::Primitive>::staticKind();
            const ColumnKind::ColumnKindCode boolKind =
                ColumnVector<types::Bool::Primitive>::staticKind();

            return (thisKind == optBoolKind) || (thisKind == boolKind);
        })) {
        throw FatalException("FilterProcessor ExprProgram contained an instruction which "
                             "was not a predicate.");
    }

    // Fold over all instructions, combining their output masks with column-wise AND
    ColumnMask finalMask(maskSize, true);
    {
        ColumnMask instrMask;
        for (Column* res : _exprProg->getTopLevelResults()) {
            if (const auto* boolRes = dynamic_cast<ColumnVector<types::Bool::Primitive>*>(res);
                boolRes) {
                instrMask.ofColumnVector(*boolRes);
            } else if (const auto* optBoolRes = dynamic_cast<ColumnOptVector<types::Bool::Primitive>*>(res);
               optBoolRes) {
                instrMask.ofColumnVector(*optBoolRes);
            } else {
                throw FatalException(
                    "FilterProcessor ExprProgram encountered non-predicate instruction.");
            }

            ColumnOperators::andOp(&finalMask, &finalMask, &instrMask);
        }
    }

    const size_t colCount = srcDF->size();
    const auto& srcCols = srcDF->cols();
    const auto& destCols = destDF->cols();
    for (size_t i = 0; i < colCount; i++) {
        const Column* srcCol = srcCols[i]->getColumn();
        Column* destCol = destCols[i]->getColumn(); 
        applyMask(srcCol, &finalMask, destCol);
    }

    _input.getPort()->consume();
    _output.getPort()->writeData();
    finish();
}
