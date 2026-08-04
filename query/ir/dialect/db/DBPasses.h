#pragma once

#include "mlir/Pass/Pass.h"

#include "DBDialect.h"

namespace mlir::db {

#define GEN_PASS_DECL
#include "DBPasses.h.inc"

#define GEN_PASS_REGISTRATION
#include "DBPasses.h.inc"

}
