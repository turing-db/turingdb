#pragma once

#include "mlir/Pass/Pass.h"

#include "DBDialect.h"

namespace db {
class GraphView;
}

namespace mlir::db {

#define GEN_PASS_DECL
#include "DBPasses.h.inc"

#define GEN_PASS_REGISTRATION
#include "DBPasses.h.inc"

// What a pass reads from outside the IR. fuse_hash_join reads the graph: a cross product cut
// by an equality is not always dearer than the join that replaces it, and which one wins
// depends on the graph rather than on the program, so the cost model needs the graph the
// query reads. _forcesHashJoin fuses every cut the pass matches whatever it estimates, and
// clearing _usesHashJoin fuses none - the two overrides the v2 planner carries as well.
// With no graph there is nothing to estimate, so every match fuses. count_from_metadata
// reads _hasPendingWrites: the query then sees writes the graph's counts leave out.
struct DBPassContext {
    const ::db::GraphView* _view {nullptr};
    bool _forcesHashJoin {false};
    bool _usesHashJoin {true};
    bool _hasPendingWrites {false};
};

std::unique_ptr<Pass> createFuseHashJoin(const DBPassContext& context);
std::unique_ptr<Pass> createCountFromMetadata(const DBPassContext& context);

}
