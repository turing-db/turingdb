#pragma once

#include <stddef.h>
#include <memory>
#include <string_view>

namespace mlir {
class MLIRContext;
}

namespace db {

class DBPassPipeline;

class IRContext {
public:
    IRContext();
    explicit IRContext(size_t queryBytesBudget);
    ~IRContext();

    void prepareForQuery(std::string_view query);

    mlir::MLIRContext* getContext() { return _context.get(); }
    DBPassPipeline* getPassPipeline() { return _passPipeline.get(); }

private:
    std::unique_ptr<mlir::MLIRContext> _context;
    std::unique_ptr<DBPassPipeline> _passPipeline;
    size_t _queryBytesBudget {0};
    size_t _retainedQueryBytes {0};
};

}
