# MLIR CALL procedure support

We want to implement the CALL procedure statement in the new MLIR-based query execution engine.

Procedures for the current pipeline are implemented through several facilities:
* ProcedureData: stores input columns and result columns pointers
* Procedure abstract base class: each specific procedure derives from the Procedure abstract base class.

Each procedure must implement an alloc, execute and dealloc callback.

In the new MLIR engine, we want to be able to call the execute function of a procedure for each chunk
produced in nested loop iterations. Thus the execute callback will be called multiple times.
The input and result column pointers must correspond to what is indicated by the MLIR variables passed as argument and yield.

The execute callback is responsible for clearing the result columns each time it is called to preserve the chunking
semantics and not accumulate.

A finalize callback needs to be added for supporting aggregating procedures that may need to see every chunk
and then produce their result when they are finished.
