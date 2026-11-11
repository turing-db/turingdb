#include "DBColumnElement.h"

#include <string_view>
#include <unordered_map>

#include "mlir/IR/BuiltinTypes.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/Operation.h"
#include "mlir/IR/Value.h"
#include "mlir/IR/ValueRange.h"
#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SmallVector.h"

#include "DBTypes.h"
#include "NullLiteralType.h"
#include "StorageTypes.h"

using namespace db;

namespace storage = mlir::storage;

namespace {

using ElementRule = mlir::Type (*)(mlir::MLIRContext* context, mlir::Type inputElement);

mlir::Type stringElement(mlir::MLIRContext* context, mlir::Type inputElement) {
    return storage::StringType::get(context);
}

mlir::Type nodeIDElement(mlir::MLIRContext* context, mlir::Type inputElement) {
    return storage::NodeIDType::get(context);
}

mlir::Type labelListElement(mlir::MLIRContext* context, mlir::Type inputElement) {
    return storage::ListType::get(context, storage::StringType::get(context));
}

mlir::Type mapElement(mlir::MLIRContext* context, mlir::Type inputElement) {
    return storage::MapType::get(context);
}

mlir::Type integerElement(mlir::MLIRContext* context, mlir::Type inputElement) {
    return mlir::IntegerType::get(context, 64);
}

mlir::Type floatElement(mlir::MLIRContext* context, mlir::Type inputElement) {
    return mlir::Float64Type::get(context);
}

mlir::Type sameElement(mlir::MLIRContext* context, mlir::Type inputElement) {
    return inputElement;
}

mlir::Type negatedElement(mlir::MLIRContext* context, mlir::Type inputElement) {
    const auto integerType = mlir::dyn_cast<mlir::IntegerType>(inputElement);
    if (integerType && integerType.isUnsigned()) {
        return mlir::IntegerType::get(context, 64);
    }

    return inputElement;
}

mlir::Type booleanElement(mlir::MLIRContext* context, mlir::Type inputElement) {
    return mlir::IntegerType::get(context, 1);
}

mlir::Type dateTimeElement(mlir::MLIRContext* context, mlir::Type inputElement) {
    return storage::DateTimeType::get(context);
}

mlir::Type durationElement(mlir::MLIRContext* context, mlir::Type inputElement) {
    return storage::DurationType::get(context);
}

mlir::Type pointElement(mlir::MLIRContext* context, mlir::Type inputElement) {
    return storage::PointType::get(context);
}

// A list function reads a list cell, or the type-erased cell an unwind of a list of lists
// hands its nested lists on as
bool readsAList(mlir::Type inputElement) {
    return mlir::isa<storage::ListType, storage::AnyType>(inputElement);
}

mlir::Type sizeElement(mlir::MLIRContext* context, mlir::Type inputElement) {
    if (!readsAList(inputElement) && !mlir::isa<storage::StringType>(inputElement)) {
        return mlir::Type {};
    }

    return mlir::IntegerType::get(context, 64);
}

mlir::Type isEmptyElement(mlir::MLIRContext* context, mlir::Type inputElement) {
    const bool readsAValue = readsAList(inputElement)
                          || mlir::isa<storage::StringType, storage::MapType>(inputElement);

    if (!readsAValue) {
        return mlir::Type {};
    }

    return booleanElement(context, inputElement);
}

// A stored list may mix the types of its elements, so one read out of a cell is the
// type-erased tagged scalar a heterogeneous list's elements ride, whatever element type
// the cell's own column resolved to.
mlir::Type listItemElement(mlir::MLIRContext* context, mlir::Type inputElement) {
    if (!readsAList(inputElement)) {
        return mlir::Type {};
    }

    return storage::AnyType::get(context);
}

// The list a tail leaves is over the elements the list it came from held, so it keeps that
// list's own element type. A list read out of a tagged cell names no element type of its
// own, so what is left of it is a list of tagged cells.
mlir::Type listTailElement(mlir::MLIRContext* context, mlir::Type inputElement) {
    if (!readsAList(inputElement)) {
        return mlir::Type {};
    } else if (mlir::isa<storage::AnyType>(inputElement)) {
        return storage::ListType::get(context, inputElement);
    }

    return inputElement;
}

mlir::Type reverseElement(mlir::MLIRContext* context, mlir::Type inputElement) {
    if (readsAList(inputElement)) {
        return inputElement;
    }

    return storage::StringType::get(context);
}

template <ElementRule convertedElement>
mlir::Type convertedListElement(mlir::MLIRContext* context, mlir::Type inputElement) {
    if (!readsAList(inputElement)) {
        return mlir::Type {};
    }

    return storage::ListType::get(context, convertedElement(context, inputElement));
}

const std::unordered_map<std::string_view, ElementRule> elementRules = {
    {"db.labels",     &labelListElement},
    {"db.edge_type",  &stringElement},
    {"db.keys",       &labelListElement},
    {"db.properties", &mapElement},
    {"db.start_node", &nodeIDElement},
    {"db.end_node",   &nodeIDElement},

    {"db.to_integer",  &integerElement},
    {"db.to_float",    &floatElement},
    {"db.to_boolean",  &booleanElement},
    {"db.to_datetime", &dateTimeElement},
    {"db.to_duration", &durationElement},
    {"db.to_point",    &pointElement},

    {"db.datetime_year",        &integerElement},
    {"db.datetime_month",       &integerElement},
    {"db.datetime_day",         &integerElement},
    {"db.datetime_hour",        &integerElement},
    {"db.datetime_minute",      &integerElement},
    {"db.datetime_second",      &integerElement},
    {"db.datetime_millisecond", &integerElement},
    {"db.datetime_microsecond", &integerElement},

    {"db.duration_years",                  &integerElement},
    {"db.duration_quarters",               &integerElement},
    {"db.duration_months",                 &integerElement},
    {"db.duration_weeks",                  &integerElement},
    {"db.duration_days",                   &integerElement},
    {"db.duration_hours",                  &integerElement},
    {"db.duration_minutes",                &integerElement},
    {"db.duration_seconds",                &integerElement},
    {"db.duration_milliseconds",           &integerElement},
    {"db.duration_microseconds",           &integerElement},
    {"db.duration_quarters_of_year",       &integerElement},
    {"db.duration_months_of_year",         &integerElement},
    {"db.duration_months_of_quarter",      &integerElement},
    {"db.duration_days_of_week",           &integerElement},
    {"db.duration_minutes_of_hour",        &integerElement},
    {"db.duration_seconds_of_minute",      &integerElement},
    {"db.duration_milliseconds_of_second", &integerElement},
    {"db.duration_microseconds_of_second", &integerElement},

    {"db.point_x",         &floatElement},
    {"db.point_y",         &floatElement},
    {"db.point_z",         &floatElement},
    {"db.point_longitude", &floatElement},
    {"db.point_latitude",  &floatElement},
    {"db.point_height",    &floatElement},
    {"db.point_crs",       &stringElement},
    {"db.point_srid",      &integerElement},

    {"db.to_string",          &stringElement},
    {"db.to_integer_or_null", &integerElement},
    {"db.to_float_or_null",   &floatElement},
    {"db.to_boolean_or_null", &booleanElement},
    {"db.to_string_or_null",  &stringElement},
    {"db.to_integer_list",    &convertedListElement<&integerElement>},
    {"db.to_float_list",      &convertedListElement<&floatElement>},
    {"db.to_boolean_list",    &convertedListElement<&booleanElement>},
    {"db.to_string_list",     &convertedListElement<&stringElement>},
    {"db.element_id",         &integerElement},
    {"db.node_id_of",         &nodeIDElement},
    {"db.size",               &sizeElement},
    {"db.is_empty",           &isEmptyElement},
    {"db.char_length",        &integerElement},
    {"db.head",               &listItemElement},
    {"db.last",               &listItemElement},
    {"db.tail",               &listTailElement},

    {"db.abs",        &sameElement},
    {"db.negate",     &negatedElement},
    {"db.unary_plus", &sameElement},
    {"db.sign",       &integerElement},
    {"db.ceil",       &floatElement},
    {"db.floor",      &floatElement},
    {"db.round",      &floatElement},
    {"db.sqrt",       &floatElement},
    {"db.exp",        &floatElement},
    {"db.log",        &floatElement},
    {"db.log10",      &floatElement},
    {"db.sin",        &floatElement},
    {"db.cos",        &floatElement},
    {"db.tan",        &floatElement},
    {"db.cot",        &floatElement},
    {"db.asin",       &floatElement},
    {"db.acos",       &floatElement},
    {"db.atan",       &floatElement},
    {"db.degrees",    &floatElement},
    {"db.radians",    &floatElement},
    {"db.haversin",   &floatElement},

    {"db.to_upper",      &stringElement},
    {"db.to_lower",      &stringElement},
    {"db.trim",          &stringElement},
    {"db.ltrim",         &stringElement},
    {"db.rtrim",         &stringElement},
    {"db.reverse",       &reverseElement},
    {"db.normalize",     &stringElement},
    {"db.is_normalized", &booleanElement},
};

ElementRule lookupElementRule(mlir::Operation* function) {
    const mlir::OperationName operationName = function->getName();
    const llvm::StringRef name = operationName.getStringRef();
    const auto it = elementRules.find(std::string_view(name.data(), name.size()));

    return it == elementRules.end() ? nullptr : it->second;
}

using OperatorElementRule = mlir::Type (*)(mlir::MLIRContext* context, mlir::Type lhsElement, mlir::Type rhsElement);

bool isNumberElement(mlir::Type element) {
    const auto integerType = mlir::dyn_cast<mlir::IntegerType>(element);

    return mlir::isa<mlir::Float64Type>(element) || (integerType && integerType.getWidth() == 64);
}

// Arithmetic over anything but two numbers computes a temporal or reads a tagged cell, which
// lowering resolves
mlir::Type arithmeticElement(mlir::MLIRContext* context, mlir::Type lhsElement, mlir::Type rhsElement) {
    if (!isNumberElement(lhsElement) || !isNumberElement(rhsElement)) {
        return mlir::Type {};
    }

    return promotedNumberElement(context, lhsElement, rhsElement);
}

mlir::Type powerElement(mlir::MLIRContext* context, mlir::Type lhsElement, mlir::Type rhsElement) {
    if (!isNumberElement(lhsElement) || !isNumberElement(rhsElement)) {
        return mlir::Type {};
    }

    return mlir::Float64Type::get(context);
}

mlir::Type concatenationElement(mlir::MLIRContext* context, mlir::Type lhsElement, mlir::Type rhsElement) {
    if (!mlir::isa<storage::StringType>(lhsElement) || !mlir::isa<storage::StringType>(rhsElement)) {
        return mlir::Type {};
    }

    return storage::StringType::get(context);
}

const std::unordered_map<std::string_view, OperatorElementRule> operatorElementRules = {
    {"db.add",    &arithmeticElement},
    {"db.sub",    &arithmeticElement},
    {"db.mul",    &arithmeticElement},
    {"db.div",    &arithmeticElement},
    {"db.mod",    &arithmeticElement},
    {"db.pow",    &powerElement},
    {"db.concat", &concatenationElement},
};

OperatorElementRule lookupOperatorElementRule(mlir::Operation* op) {
    const mlir::OperationName operationName = op->getName();
    const llvm::StringRef name = operationName.getStringRef();
    const auto it = operatorElementRules.find(std::string_view(name.data(), name.size()));

    return it == operatorElementRules.end() ? nullptr : it->second;
}

// The leading operands whose elements a rule answers @param op's element over, none without a rule
size_t ruledOperandCount(mlir::Operation* op) {
    if (lookupElementRule(op)) {
        return 1;
    } else if (lookupOperatorElementRule(op)) {
        return 2;
    }

    return 0;
}

}

mlir::Type db::declaredColumnElement(mlir::Value column) {
    const mlir::db::ColumnType columnType = mlir::cast<mlir::db::ColumnType>(column.getType());
    const mlir::Type element = columnType.getType();
    const auto nullable = mlir::dyn_cast<storage::NullableType>(element);

    return nullable ? nullable.getValueType() : element;
}

mlir::Type db::selectedColumnElement(mlir::MLIRContext* context, mlir::ValueRange values) {
    mlir::Type shared;

    for (const mlir::Value value : values) {
        const mlir::Type declared = mlir::cast<mlir::db::ColumnType>(value.getType()).getType();
        if (mlir::isNullLiteralType(declared)) {
            continue;
        }

        // A mask and a boolean value hold the same truth value, which lowering reads as an i1
        mlir::Type element = declaredColumnElement(value);
        if (mlir::isa<storage::BoolType>(element)) {
            element = mlir::IntegerType::get(context, 1);
        }

        const bool unresolved = mlir::isa<mlir::NoneType>(element);
        const bool disagrees = shared && shared != element;
        if (unresolved || disagrees) {
            return mlir::NoneType::get(context);
        }

        shared = element;
    }

    if (!shared) {
        return mlir::NoneType::get(context);
    } else if (mlir::holdsItsOwnNull(shared)) {
        return shared;
    }

    return storage::NullableType::get(context, shared);
}

mlir::Type db::unaryFunctionElement(mlir::Operation* function, mlir::Type inputElement) {
    const ElementRule rule = lookupElementRule(function);
    if (!rule) {
        return mlir::Type {};
    }

    return rule(function->getContext(), inputElement);
}

mlir::Type db::inferredColumnElement(mlir::Value column) {
    mlir::MLIRContext* const context = column.getContext();
    const mlir::Type unknown = mlir::NoneType::get(context);

    llvm::SmallDenseMap<mlir::Value, mlir::Type, 8> inferred;
    llvm::SmallVector<mlir::Value> worklist {column};
    llvm::SmallVector<mlir::Type, 2> operandElements;

    while (!worklist.empty()) {
        const mlir::Value value = worklist.back();
        if (inferred.contains(value)) {
            worklist.pop_back();
            continue;
        }

        const mlir::Type declared = declaredColumnElement(value);
        mlir::Operation* const definingOp = value.getDefiningOp();
        const size_t ruledOperands = definingOp ? ruledOperandCount(definingOp) : 0;

        if (!mlir::isa<mlir::NoneType>(declared) || ruledOperands == 0) {
            inferred[value] = declared;
            worklist.pop_back();
            continue;
        }

        operandElements.clear();
        bool operandsInferred = true;
        for (const mlir::Value operand : definingOp->getOperands().take_front(ruledOperands)) {
            const auto inferredIt = inferred.find(operand);
            if (inferredIt == inferred.end()) {
                worklist.push_back(operand);
                operandsInferred = false;
            } else {
                operandElements.push_back(inferredIt->second);
            }
        }

        if (!operandsInferred) {
            continue;
        }

        const bool readsAnUnknownElement = llvm::any_of(operandElements, [](mlir::Type element) {
            return mlir::isa<mlir::NoneType>(element);
        });

        mlir::Type element;
        if (!readsAnUnknownElement && ruledOperands == 1) {
            element = unaryFunctionElement(definingOp, operandElements.front());
        } else if (!readsAnUnknownElement) {
            element = lookupOperatorElementRule(definingOp)(context, operandElements[0], operandElements[1]);
        }

        inferred[value] = element ? element : unknown;
        worklist.pop_back();
    }

    return inferred.lookup(column);
}

mlir::Type db::promotedNumberElement(mlir::MLIRContext* context, mlir::Type lhs, mlir::Type rhs) {
    const bool anyFloat = mlir::isa<mlir::Float64Type>(lhs) || mlir::isa<mlir::Float64Type>(rhs);
    if (anyFloat) {
        return mlir::Float64Type::get(context);
    }

    // Cypher has one integer type and it is signed. A tally is carried unsigned - it can
    // never be negative - but arithmetic over it can be, so a count entering an expression
    // is promoted like any other integer instead of wrapping around zero
    return mlir::IntegerType::get(context, 64);
}
