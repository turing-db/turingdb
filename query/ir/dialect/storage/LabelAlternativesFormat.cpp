#include "LabelAlternativesFormat.h"

#include "mlir/IR/Builders.h"

#include "llvm/ADT/STLExtras.h"

#include "StorageAttributes.h"

using namespace mlir;

ParseResult mlir::parseLabelAlternatives(OpAsmParser& parser, ArrayAttr& alternatives) {
    ArrayAttr written;
    if (parser.parseAttribute(written)) {
        return failure();
    }

    const bool isConjunction = llvm::all_of(written, [](Attribute label) {
        return isa<StringAttr, storage::ParameterAttr>(label);
    });

    if (isConjunction) {
        alternatives = parser.getBuilder().getArrayAttr({written});
    } else {
        alternatives = written;
    }

    return success();
}

void mlir::printLabelAlternatives(OpAsmPrinter& printer, Operation* op, ArrayAttr alternatives) {
    if (alternatives.size() == 1) {
        printer.printAttribute(alternatives[0]);
    } else {
        printer.printAttribute(alternatives);
    }
}
