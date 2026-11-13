#pragma once

namespace llvm {
class StringRef;
}

namespace mlir {
class MLIRContext;
class Operation;
class Type;
class TypeRange;
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

// The column the function or the operator named @param opName computes over @param operands:
// the element its rule answers over the elements they declare, nullable unless it holds its
// own null. A column of none where an operand declares none or the rule reads no value of
// the elements they declare.
mlir::Type inferColumnType(mlir::MLIRContext* context, llvm::StringRef opName, mlir::ValueRange operands);

// Whether each of @param declared is the column of @param inferred beside it, or that column
// without its null
bool areCompatibleColumnTypes(mlir::TypeRange inferred, mlir::TypeRange declared);

// The number two numbers of elements @param lhs and @param rhs compute in: a double where
// either is one, else the one integer type Cypher has
mlir::Type promotedNumberElement(mlir::MLIRContext* context, mlir::Type lhs, mlir::Type rhs);

// The element a selection between @param values holds: the one value element they all
// hold, nullable unless it holds its own null. The null literal holds whatever the others
// do. None when a value declares none, or when two hold different elements, which lowering
// then promotes into one.
mlir::Type selectedColumnElement(mlir::MLIRContext* context, mlir::ValueRange values);

}
