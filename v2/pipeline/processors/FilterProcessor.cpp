#include "FilterProcessor.h"

#include <spdlog/fmt/fmt.h>
#include <range/v3/view/drop.hpp>

#include "columns/ColumnMask.h"
#include "columns/ColumnOperators.h"
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


[[maybe_unused]] void applyMask(const Column* src,
               const ColumnMask* mask,
               Column* dest) {
    switch (src->getKind()) {
        APPLY_MASK_CASE(ColumnVector<size_t>)
        APPLY_MASK_CASE(ColumnVector<EntityID>)
        APPLY_MASK_CASE(ColumnVector<NodeID>)
        APPLY_MASK_CASE(ColumnVector<EdgeID>)
        APPLY_MASK_CASE(ColumnVector<LabelSetID>)
        APPLY_MASK_CASE(ColumnVector<std::string>)
        APPLY_MASK_CASE(ColumnVector<std::string_view>)

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

void FilterProcessor::prepare(ExecutionContext* ctxt) {
    // Check dataframes have same number of columns
    const Dataframe* srcDF = _input.getDataframe();
    const Dataframe* destDF = _output.getDataframe();
    if (!srcDF->hasSameShape(destDF)) {
        throw PipelineException("FilterProcessor input and output dataframes must have same size and columns of same type");
    }

    markAsPrepared();
}

void FilterProcessor::reset() {
    markAsReset();
}

void FilterProcessor::execute() {
    const Dataframe* srcDF = _input.getDataframe();
    Dataframe* destDF = _output.getDataframe();

    // 1. Evaluate all instructions
    _exprProg->evaluateInstructions();
    // 2. Combine into single mask
    const ExprProgram::Instructions instrs = _exprProg->instrs();

    ColumnMask mask;
    if (instrs.front()._res->getKind() != ColumnVector<types::Bool::Primitive>::staticKind()) {
        throw FatalException("Attempted to evaluate a non-Boolean ColumnVector result column in FilterProcessor.");
    }
    mask.ofColumnVector(*instrs.front()._res->cast<ColumnVector<types::Bool::Primitive>>());

    for (const ExprProgram::Instruction& instr : instrs | rv::drop(1)) {
        const Column* res = instr._res;

        if (res->getKind() != ColumnMask::staticKind()) {
            throw FatalException(
                "Attempted to evaluate a non-mask result column in FilterProcessor.");
        }
        const auto* instrMask = static_cast<const ColumnMask*>(res);

        ColumnOperators::andOp(&mask, &mask, instrMask);
    }

    const size_t colCount = srcDF->size();
    const auto& srcCols = srcDF->cols();
    const auto& destCols = destDF->cols();
    for (size_t i = 0; i < colCount; i++) {
        const Column* srcCol = srcCols[i]->getColumn();
        Column* destCol = destCols[i]->getColumn(); 
        destCol->assign(srcCol);
    }

    _input.getPort()->consume();
    _output.getPort()->writeData();
    finish();
}
