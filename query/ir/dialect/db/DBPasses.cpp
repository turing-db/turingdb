#include "DBPasses.h"

#include "mlir/IR/Operation.h"

#include "DBDialect.h"
#include "DBOps.h"

namespace mlir::db {

#define GEN_PASS_DEF_GETOUTEDGESDCE
#include "DBPasses.h.inc"

namespace {

struct GetOutEdgesDCE : public impl::GetOutEdgesDCEBase<GetOutEdgesDCE> {
    void runOnOperation() override {
        Operation* const root = getOperation();

        root->walk([&](GetOutEdges op) {
            // TODO: replace unused GetOutEdges results with null
        });
    }
};

}

}
