#include "DBColumnElement.h"

#include <string_view>
#include <unordered_map>

#include "mlir/IR/BuiltinTypes.h"
#include "mlir/IR/MLIRContext.h"
#include "mlir/IR/Operation.h"
#include "mlir/IR/Value.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SmallVector.h"

#include "DBTypes.h"
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

    {"db.to_string",          &stringElement},
    {"db.to_integer_or_null", &integerElement},
    {"db.to_float_or_null",   &floatElement},
    {"db.to_boolean_or_null", &booleanElement},
    {"db.to_string_or_null",  &stringElement},
    {"db.element_id",         &integerElement},
    {"db.size",               &sizeElement},
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
    const llvm::StringRef name = function->getName().getStringRef();
    const auto it = elementRules.find(std::string_view(name.data(), name.size()));

    return it == elementRules.end() ? nullptr : it->second;
}

mlir::Type declaredElement(mlir::Value column) {
    const mlir::Type element = mlir::cast<mlir::db::ColumnType>(column.getType()).getType();
    const auto nullable = mlir::dyn_cast<storage::NullableType>(element);

    return nullable ? nullable.getValueType() : element;
}

}

mlir::Type db::unaryFunctionElement(mlir::Operation* function, mlir::Type inputElement) {
    const ElementRule rule = lookupElementRule(function);
    if (!rule) {
        return mlir::Type {};
    }

    return rule(function->getContext(), inputElement);
}

mlir::Type db::inferredColumnElement(mlir::Value column) {
    llvm::SmallVector<mlir::Operation*> functions;

    mlir::Value current = column;
    mlir::Type element = declaredElement(current);

    while (mlir::isa<mlir::NoneType>(element)) {
        mlir::Operation* const definingOp = current.getDefiningOp();
        const bool isAUnaryFunction = definingOp && lookupElementRule(definingOp);
        if (!isAUnaryFunction) {
            return element;
        }

        functions.push_back(definingOp);
        current = definingOp->getOperand(0);
        element = declaredElement(current);
    }

    for (mlir::Operation* const function : llvm::reverse(functions)) {
        element = unaryFunctionElement(function, element);
        if (!element) {
            return mlir::NoneType::get(column.getContext());
        }
    }

    return element;
}
