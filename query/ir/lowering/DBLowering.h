#pragma once

#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/SmallVector.h"

#include "DBOps.h"

namespace db {

// Lowers a function in the set-at-a-time `db` dialect into the equivalent
// nested-loop function in the `nl` dialect.
//
// The db dialect describes a query as whole-column dataflow: db.scan_nodes
// produces a column, each db.get_out_edges consumes columns and produces more,
// and db.output names the result projection. The nl dialect makes execution
// explicit: every column becomes a chunk bound by an nl.for loop, and every db
// op that produces columns becomes an nl iterator wrapped in such a loop.
// Lowering therefore turns the flat db dataflow into a loop nest:
//
//   db.scan_nodes / db.get_out_edges  ->  nl source op + nl.for over its chunks
//   db.output                          ->  nl.output inside the binding loop
//
// A db op is placed in the loop that binds its input chunk, so a linear chain
// becomes a straight nest, while two ops reading the same column become sibling
// loops. No nl type is ever spelled out: the source ops infer their iterator
// type and the nl.for builder reads the loop-variable types off that iterator.
class DBLowering {
public:
    explicit DBLowering(mlir::MLIRContext* context);
    ~DBLowering();

    // Builds the nl-dialect counterpart of dbFunction, appends it to module
    // under the same symbol name, and returns it.
    mlir::func::FuncOp lower(mlir::func::FuncOp dbFunction, mlir::ModuleOp module);

private:
    mlir::OpBuilder _builder;

    // db column SSA value -> the nl chunk (an nl.for loop variable) it lowers to
    llvm::DenseMap<mlir::Value, mlir::Value> _valueMap;

    // Entry block of the nl function being built: home of the node scan and of
    // the function terminator
    mlir::Block* _entryBlock {nullptr};

    void lowerOperation(mlir::Operation& operation);
    void lowerScanNodes(mlir::db::ScanNodes scanNodes);
    void lowerGetOutEdges(mlir::db::GetOutEdges getOutEdges);
    void lowerOutput(mlir::db::Output output);

    // Wrap an nl source op's iterator in an nl.for and bind each db result
    // column to the matching loop variable
    void buildLoopForSource(mlir::Value iterator, mlir::Operation* dbOp);

    // Point the builder just before the terminator of block, where the next
    // lowered op belongs
    void setInsertionInto(mlir::Block* block);

    // The nl chunk a db value lowered to, and the loop body that binds a chunk
    mlir::Value mapValue(mlir::Value dbValue) const;
    static mlir::Block* ownerBlock(mlir::Value chunkValue);
};

}
