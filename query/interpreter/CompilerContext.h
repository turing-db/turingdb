#pragma once

#include <stddef.h>
#include <memory>

namespace mlir {
class MLIRContext;
}

namespace db {

class DBPassPipeline;

class CompilerContext {
public:
    CompilerContext();
    ~CompilerContext();

    void prepareForQuery();
    void handleCompileError();

    mlir::MLIRContext* getContext() { return _context.get(); }
    DBPassPipeline* getPassPipeline() { return _passPipeline.get(); }

private:
    std::unique_ptr<mlir::MLIRContext> _context;
    std::unique_ptr<DBPassPipeline> _passPipeline;
    size_t _preparedQueries {0};
    bool _needsRebuild {false};
};

}
