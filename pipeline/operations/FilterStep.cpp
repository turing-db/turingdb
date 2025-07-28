#include "FilterStep.h"

#include <sstream>
#include <string>
#include <string_view>

#include "ID.h"
#include "Profiler.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnKind.h"
#include "columns/ColumnOperators.h"
#include "metadata/LabelSet.h"
#include "Panic.h"

using namespace db;

namespace {

constexpr ColumnKind::ColumnKindCode getBaseFromKind(ColumnKind::ColumnKindCode kind) {
    return kind / ColumnKind::getInternalTypeKindCount();
}

constexpr ColumnKind::ColumnKindCode getInternalFromKind(ColumnKind::ColumnKindCode kind) {
    return kind % ColumnKind::getInternalTypeKindCount();
}


constexpr ColumnKind::ColumnKindCode getPairKind(ColumnKind::ColumnKindCode lhs, ColumnKind::ColumnKindCode rhs) {
    // Comparing each pair of the same internal type (ex: Vector<size_t> and Const<size_t>
    const auto baseLhs = getBaseFromKind(lhs);
    const auto baseRhs = getBaseFromKind(rhs);
    const auto baseCount = ColumnKind::getBaseColumnKindCount();
    const auto typeCount = ColumnKind::getInternalTypeKindCount();
    const auto basePairCount = baseCount * baseCount;
    const auto typePairCount = typeCount * typeCount;
    const auto counts = basePairCount + typePairCount;
    const auto internalLhs = getInternalFromKind(lhs);
    const auto internalRhs = getInternalFromKind(rhs);
    return baseLhs * counts + baseRhs * typePairCount + internalLhs * typeCount + internalRhs;
}

constexpr ColumnKind::ColumnKindCode getOpCase(ColumnOperator op, ColumnKind::ColumnKindCode lhs, ColumnKind::ColumnKindCode rhs) {
    const auto internalCount = ColumnKind::getInternalTypeKindCount();
    const auto basePairCount = ColumnKind::getBasePairColumnCount();
    return op * basePairCount * internalCount + getPairKind(lhs, rhs);
}

}

FilterStep::FilterStep(ColumnVector<size_t>* indices)
    : _indices(indices)
{
}

FilterStep::FilterStep() {
}

FilterStep::~FilterStep() {
}

template <ColumnOperator Op, typename Lhs, typename Rhs>
static constexpr ColumnKind::ColumnKindCode OpCase = getOpCase(Op, Lhs::staticKind(), Rhs::staticKind());

#define EQUAL_CASE(Lhs, Rhs)                      \
    case OpCase<OP_EQUAL, Lhs, Rhs>: {            \
        ColumnOperators::equal(                   \
            *expr._mask,                          \
            *static_cast<const Lhs*>(expr._lhs),  \
            *static_cast<const Rhs*>(expr._rhs)); \
        break;                                    \
    }

#define AND_CASE(Lhs, Rhs)                        \
    case OpCase<OP_AND, Lhs, Rhs>: {              \
        ColumnOperators::andOp(                   \
            *expr._mask,                          \
            *static_cast<const Lhs*>(expr._lhs),  \
            *static_cast<const Rhs*>(expr._rhs)); \
        break;                                    \
    }

#define OR_CASE(Lhs, Rhs)                         \
    case OpCase<OP_OR, Lhs, Rhs>: {               \
        ColumnOperators::orOp(                    \
            *expr._mask,                          \
            *static_cast<const Lhs*>(expr._lhs),  \
            *static_cast<const Rhs*>(expr._rhs)); \
        break;                                    \
    }

#define PROJECT_CASE(Lhs, Rhs)                    \
    case OpCase<OP_PROJECT, Lhs, Rhs>: {          \
        ColumnOperators::projectOp(               \
            *expr._mask,                          \
            *static_cast<const Lhs*>(expr._lhs),  \
            *static_cast<const Rhs*>(expr._rhs)); \
        break;                                    \
    }

#define IN_CASE(Lhs, Rhs)                         \
    case OpCase<OP_IN, Lhs, Rhs>: {               \
        ColumnOperators::inOp(                    \
            *expr._mask,                          \
            *static_cast<const Lhs*>(expr._lhs),  \
            *static_cast<const Rhs*>(expr._rhs)); \
        break;                                    \
    }


void FilterStep::compute() {
    for (const Expression& expr : _expressions) {
        switch (getOpCase(expr._op, expr._lhs->getKind(), expr._rhs->getKind())) {
            EQUAL_CASE(ColumnVector<size_t>, ColumnVector<size_t>)
            EQUAL_CASE(ColumnVector<size_t>, ColumnConst<size_t>)
            EQUAL_CASE(ColumnConst<size_t>, ColumnVector<size_t>)
            EQUAL_CASE(ColumnConst<size_t>, ColumnConst<size_t>)
            EQUAL_CASE(ColumnVector<EntityID>, ColumnVector<EntityID>)
            EQUAL_CASE(ColumnVector<EntityID>, ColumnConst<EntityID>)
            EQUAL_CASE(ColumnConst<EntityID>, ColumnVector<EntityID>)
            EQUAL_CASE(ColumnConst<EntityID>, ColumnConst<EntityID>)
            EQUAL_CASE(ColumnVector<NodeID>, ColumnVector<NodeID>)
            EQUAL_CASE(ColumnVector<NodeID>, ColumnConst<NodeID>)
            EQUAL_CASE(ColumnConst<NodeID>, ColumnVector<NodeID>)
            EQUAL_CASE(ColumnConst<NodeID>, ColumnConst<NodeID>)
            EQUAL_CASE(ColumnVector<EdgeID>, ColumnVector<EdgeID>)
            EQUAL_CASE(ColumnVector<EdgeID>, ColumnConst<EdgeID>)
            EQUAL_CASE(ColumnConst<EdgeID>, ColumnVector<EdgeID>)
            EQUAL_CASE(ColumnConst<EdgeID>, ColumnConst<EdgeID>)
            EQUAL_CASE(ColumnVector<LabelSetID>, ColumnVector<LabelSetID>)
            EQUAL_CASE(ColumnVector<LabelSetID>, ColumnConst<LabelSetID>)
            EQUAL_CASE(ColumnConst<LabelSetID>, ColumnVector<LabelSetID>)
            EQUAL_CASE(ColumnConst<LabelSetID>, ColumnConst<LabelSetID>)
            EQUAL_CASE(ColumnVector<LabelSet>, ColumnVector<LabelSet>)
            EQUAL_CASE(ColumnVector<LabelSet>, ColumnConst<LabelSet>)
            EQUAL_CASE(ColumnConst<LabelSet>, ColumnVector<LabelSet>)
            EQUAL_CASE(ColumnConst<LabelSet>, ColumnConst<LabelSet>)
            EQUAL_CASE(ColumnVector<std::string>, ColumnVector<std::string>)
            EQUAL_CASE(ColumnVector<std::string>, ColumnConst<std::string>)
            EQUAL_CASE(ColumnConst<std::string>, ColumnVector<std::string>)
            EQUAL_CASE(ColumnConst<std::string>, ColumnConst<std::string>)
            EQUAL_CASE(ColumnVector<std::string_view>, ColumnVector<std::string_view>)
            EQUAL_CASE(ColumnVector<std::string_view>, ColumnConst<std::string_view>)
            EQUAL_CASE(ColumnConst<std::string_view>, ColumnVector<std::string_view>)
            EQUAL_CASE(ColumnConst<std::string_view>, ColumnConst<std::string_view>)
            EQUAL_CASE(ColumnVector<std::string_view>, ColumnVector<std::string>)
            EQUAL_CASE(ColumnVector<std::string_view>, ColumnConst<std::string>)
            EQUAL_CASE(ColumnConst<std::string_view>, ColumnVector<std::string>)
            EQUAL_CASE(ColumnConst<std::string_view>, ColumnConst<std::string>)
            EQUAL_CASE(ColumnVector<std::string>, ColumnVector<std::string_view>)
            EQUAL_CASE(ColumnVector<std::string>, ColumnConst<std::string_view>)
            EQUAL_CASE(ColumnConst<std::string>, ColumnVector<std::string_view>)
            EQUAL_CASE(ColumnConst<std::string>, ColumnConst<std::string_view>)
            EQUAL_CASE(ColumnVector<CustomBool>, ColumnConst<CustomBool>)
            EQUAL_CASE(ColumnConst<CustomBool>, ColumnVector<CustomBool>)
            EQUAL_CASE(ColumnVector<double>, ColumnConst<double>)
            EQUAL_CASE(ColumnConst<double>, ColumnVector<double>)
            EQUAL_CASE(ColumnVector<int64_t>, ColumnConst<int64_t>)
            EQUAL_CASE(ColumnConst<int64_t>, ColumnVector<int64_t>)

            AND_CASE(ColumnMask, ColumnMask)
            OR_CASE(ColumnMask, ColumnMask)

            PROJECT_CASE(ColumnVector<size_t>, ColumnMask)
            PROJECT_CASE(ColumnMask, ColumnVector<size_t>)

            IN_CASE(ColumnVector<size_t>, ColumnSet<size_t>)
            IN_CASE(ColumnVector<EntityID>, ColumnSet<EntityID>)
            IN_CASE(ColumnVector<NodeID>, ColumnSet<NodeID>)
            IN_CASE(ColumnVector<EdgeID>, ColumnSet<EdgeID>)
            IN_CASE(ColumnVector<LabelSetID>, ColumnSet<LabelSetID>)
            IN_CASE(ColumnVector<LabelSet>, ColumnSet<LabelSet>)
            IN_CASE(ColumnVector<std::string>, ColumnSet<std::string>)
            // XXX: Do we need these?
            //IN_CASE(ColumnVector<std::string_view>, ColumnSet<std::string_view>)
            //IN_CASE(ColumnVector<std::string_view>, ColumnSet<std::string>)
            //IN_CASE(ColumnVector<std::string>, ColumnSet<std::string_view>)
            IN_CASE(ColumnVector<double>, ColumnSet<double>)
            IN_CASE(ColumnVector<int64_t>, ColumnSet<int64_t>)

            default: {
                panic("Operator not implemented (kinds: {} and {})",
                      expr._lhs->getKind(),
                      expr._rhs->getKind());
            }
        }
    }
}

static constexpr auto s0 = ColumnConst<std::string_view>::staticKind();
static constexpr auto s1 = ColumnVector<std::string>::staticKind();

static constexpr auto s2 = ColumnVector<std::string_view>::staticKind();
static constexpr auto s3 = ColumnConst<std::string_view>::staticKind();

static constexpr auto t1 = OpCase<OP_EQUAL, ColumnConst<std::string_view>, ColumnVector<std::string>>;
static constexpr auto t2 = OpCase<OP_EQUAL, ColumnVector<std::string_view>, ColumnConst<std::string_view>>;

void FilterStep::generateIndices() {
    auto maskd = _expressions.back()._mask->data();
    auto size = _expressions.back()._mask->size();

    _indices->clear();
    _indices->reserve(size);

    for (size_t i = 0; i < size; i++) {
        // if the mask's index value is true record the index for the transform step:
        if (maskd[i]) {
            _indices->push_back(i);
        }
    }
}

#define APPLY_MASK_CASE(Type)               \
    case Type::staticKind(): {              \
        applyMask<Type>(                    \
            *mask,                          \
            *static_cast<const Type*>(src), \
            *static_cast<Type*>(dst));      \
        return;                             \
    }

template <typename T>
void applyMask(const ColumnMask& mask, const T& src, T& dst) {
    msgbioassert(mask.size() == src.size(),
                 "Mask and source must have matching dimensions");
    dst.clear();
    for (size_t i = 0; i < mask.size(); i++) {
        if (mask[i]) {
            dst.push_back(src[i]);
        }
    }
}

void apply(const ColumnMask* mask, const Column* src, Column* dst) {
    const ColumnKind::ColumnKindCode srcKind = src->getKind();
    [[maybe_unused]] const ColumnKind::ColumnKindCode dstKind = dst->getKind();

    msgbioassert(src, "Src cannot be nullptr");
    msgbioassert(dst, "Src cannot be nullptr");
    msgbioassert(srcKind == dstKind,
                 "Can not apply a mask to convert a column into a different type "
                 "(src and tgt must have the same type)");

    switch (srcKind) {
        APPLY_MASK_CASE(ColumnVector<size_t>)
        APPLY_MASK_CASE(ColumnVector<EntityID>)
        APPLY_MASK_CASE(ColumnVector<NodeID>)
        APPLY_MASK_CASE(ColumnVector<EdgeID>)
        APPLY_MASK_CASE(ColumnVector<LabelSetID>)
        APPLY_MASK_CASE(ColumnVector<std::string>)
        APPLY_MASK_CASE(ColumnVector<std::string_view>)

        default: {
            panic("Mask application not implemented");
        }
    }
}

void FilterStep::addExpression(Expression&& expr) {
    _expressions.emplace_back(expr);
}
void FilterStep::addOperand(Operand&& operand) {
    _operands.emplace_back(operand);
}

void FilterStep::reset() {
}

void FilterStep::execute() {
    Profile profile {"FilterStep::execute"};

    compute();

    if (_indices) {
        generateIndices();
    }

    for (const auto& operand : _operands) {
        apply(operand._mask, operand._src, operand._dest);
    }
}

void FilterStep::describe(std::string& descr) const {
    std::stringstream ss;
    ss << "FilterStep";
    ss << " indices=" << std::hex << _indices;
    ss << " expressions={\n";

    for (const auto& expr : _expressions) {
        const std::string_view op = ColumnOperatorDescription::value(expr._op);
        ss << fmt::format("     ({} mask={} [lhs={}] [rhs={}])\n",
                          op,
                          fmt::ptr(expr._mask),
                          fmt::ptr(expr._lhs),
                          fmt::ptr(expr._rhs));
    }

    ss << "}\n";

    ss << " operands={\n";
    for (const auto& operand : _operands) {
        ss << fmt::format("     (mask={} [src={}] [dest={}])\n",
                          fmt::ptr(operand._mask),
                          fmt::ptr(operand._src),
                          fmt::ptr(operand._dest));
    }
    ss << "}\n";

    descr.assign(ss.str());
}
