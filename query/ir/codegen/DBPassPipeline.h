#pragma once

#include <stddef.h>
#include <memory>
#include <string_view>
#include <vector>

#include "mlir/IR/BuiltinOps.h"
#include "mlir/Pass/PassManager.h"

#include "DBPasses.h"

namespace db {

class DBPassPipeline {
public:
    explicit DBPassPipeline(mlir::MLIRContext* context);
    ~DBPassPipeline();

    void run(mlir::ModuleOp module, const mlir::db::DBPassContext& passContext);

    static void fillPassNames(std::vector<std::string_view>& passNames);
    static std::unique_ptr<mlir::Pass> createPass(size_t passIndex, const mlir::db::DBPassContext* passContext);

private:
    mlir::db::DBPassContext _passContext;
    mlir::PassManager _passManager;
};

}
