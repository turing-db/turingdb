#include "DBPasses.h"

#include "mlir/IR/Operation.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "DBTypes.h"

namespace mlir::db {

#define GEN_PASS_DEF_GETOUTEDGESDCE
#include "DBPasses.h.inc"

namespace {

constexpr unsigned fixedResultCount = 4;

struct GetOutEdgesDCE : public impl::GetOutEdgesDCEBase<GetOutEdgesDCE> {
    void runOnOperation() override {
        Operation* const root = getOperation();
        MLIRContext* const context = root->getContext();
        const NullptrType nullType = NullptrType::get(context);

        root->walk([&](GetOutEdges op) {
            for (unsigned index = 0; index < fixedResultCount; index++) {
                mlir::OpResult result = op->getResult(index);
                if (!result.use_empty()) {
                    continue;
                }

                result.setType(nullType);
            }
        });
    }
};

}

}
