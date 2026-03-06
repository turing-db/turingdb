#pragma once

namespace db {
class ProcedureNamespace;
}

// Init callback type - receives ProcedureNamespace* to register procedures
using TuringExtensionInitCallback = void (*)(db::ProcedureNamespace*);

// Each extension declares one of these
struct TuringExtensionDef {
    const char* _namespaceName {nullptr};
    TuringExtensionInitCallback _initCallback {nullptr};
};

// Every extension shared library (.so on Linux, .dylib on macOS) must export:
//   extern "C" const TuringExtensionDef* turing_extension();
