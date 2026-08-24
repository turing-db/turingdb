#pragma once

#include "llvm/ADT/DenseMap.h"
#include "mlir/IR/Operation.h"
#include "mlir/IR/Value.h"

namespace db {

class Column;
class LocalMemory;
class NLProgram;
class NLStmtContainer;

// Translates the nl system commands - nl.load_graph, nl.change, nl.commit and
// their siblings - into the statements NLSystemExecutor runs.
//
// It sits beside NLTranslator rather than inside it because a system command has
// none of the machinery the dataflow ops need: no loop to open, no chunk type to
// resolve against the schema, no accumulator to allocate. All it does is allocate
// the few columns the command reports its result table in, publish them under the
// op's results so the nl.output that follows finds them, and record the one
// statement that runs the command.
class NLSystemTranslator {
public:
    using ValueSlots = llvm::DenseMap<mlir::Value, Column*>;

    NLSystemTranslator(NLProgram* program, LocalMemory* memory, ValueSlots* valueSlots);
    ~NLSystemTranslator();

    // Records the statement of one nl system command. False when the operation is
    // not one, leaving the program untouched.
    bool translate(mlir::Operation& operation, NLStmtContainer* body);

private:
    NLProgram* _program {nullptr};
    LocalMemory* _memory {nullptr};
    ValueSlots* _valueSlots {nullptr};

    // Allocate the column one result of a command is reported in and publish it
    // under that result, so the nl.output reading the command finds it
    template <typename ColumnType>
    ColumnType* allocResult(mlir::Value result);
};

}
