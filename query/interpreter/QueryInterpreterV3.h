#pragma once

#include <string_view>

#include "NLOutputSink.h"
#include "QueryStatus.h"
#include "versioning/CommitHash.h"
#include "versioning/ChangeID.h"

namespace mlir {
class MLIRContext;
}

namespace db {

class SystemManager;
class LocalMemory;
class ExplainReport;
class GraphView;

class QueryInterpreterV3 {
public:
    explicit QueryInterpreterV3(SystemManager* sysMan);
    ~QueryInterpreterV3();

    void execute(QueryStatus& status,
                 std::string_view query,
                 std::string_view graphName,
                 CommitHash hash,
                 ChangeID changeID,
                 LocalMemory* mem,
                 NLOutputSink* sink);

    // The overrides of the join cost model, which otherwise leaves a cross product cut by
    // an equality as it stands whenever it estimates the product the cheaper of the two.
    // The v3 siblings of PlanGenConfig's flags, and they mean what those mean: forcing
    // fuses every cut the pass matches, and clearing use fuses none.
    void setForceValueHashJoin(bool force) { _forcesValueHashJoin = force; }
    void setUseValueHashJoin(bool use) { _usesValueHashJoin = use; }

private:
    SystemManager* _sysMan {nullptr};
    bool _forcesValueHashJoin {false};
    bool _usesValueHashJoin {true};

    void executeImpl(QueryStatus& status,
                     std::string_view query,
                     std::string_view graphName,
                     CommitHash hash,
                     ChangeID changeID,
                     LocalMemory* mem,
                     NLOutputSink* sink);

    // Emits the dumps an explained query collected, by compiling and running the
    // small program that reports them - so an EXPLAIN returns its rows through the
    // same path as any other statement
    void reportExplain(const ExplainReport& report,
                       mlir::MLIRContext* context,
                       const GraphView* view,
                       LocalMemory* memory,
                       NLOutputSink* sink);
};

}
