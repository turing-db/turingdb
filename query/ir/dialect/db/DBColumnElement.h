#pragma once

namespace mlir {
class MLIRContext;
class Operation;
class Type;
class Value;
class ValueRange;
}

namespace db {

// The element a unary function answers over @param inputElement, its input's element with
// the nullability stripped. A null type when @param function is no unary function, or when
// it reads no value of that element.
mlir::Type unaryFunctionElement(mlir::Operation* function, mlir::Type inputElement);

// The element @param column declares, with the nullability stripped
mlir::Type declaredColumnElement(mlir::Value column);

// The element @param column holds, with the nullability stripped. A function names none on
// its result, so where the column declares none the element is the one the chain of unary
// functions producing it answers over the first column of the chain that declares one.
mlir::Type inferredColumnElement(mlir::Value column);

// The element a selection between @param values holds: the one value element they all
// hold, nullable unless it holds its own null. The null literal holds whatever the others
// do. None when a value declares none, or when two hold different elements, which lowering
// then promotes into one.
mlir::Type selectedColumnElement(mlir::MLIRContext* context, mlir::ValueRange values);

}
