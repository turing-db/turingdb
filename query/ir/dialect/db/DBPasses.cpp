#include "DBPasses.h"

#include "mlir/IR/Operation.h"
#include "mlir/IR/ValueRange.h"

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
            mlir::ValueRange results = op.getResults();
            for (mlir::Value result : results) {
                const size_t uses = result.getNumUses();
                if (uses == 0) {
                    // TODO replace with null
                }
            }
        });
    }
};

}

}
