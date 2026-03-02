#include "ExtensionInterface.h"

#include "procedures/ProcedureState.h"
#include "procedures/Procedure.h"
#include "procedures/ProcedureNamespace.h"
#include "columns/ColumnVector.h"

using namespace db;

namespace {

struct HelloData : public ProcedureData {
};

ProcedureData* helloAlloc() {
    return new HelloData();
}

void helloDealloc(ProcedureData* data) {
    delete data;
}

void helloExecute(ProcedureState* proc) {
    HelloData& data = proc->data<HelloData>();
    Column* col = data.getReturnColumn(0);
    auto* msgCol = static_cast<ColumnVector<std::string>*>(col);

    switch (proc->getStep()) {
        case ProcedureState::Step::PREPARE:
        break;

        case ProcedureState::Step::RESET:
        break;

        case ProcedureState::Step::EXECUTE: {
            msgCol->clear();
            msgCol->push_back("Hello, World!");
            proc->finish();
        }
        break;
    }
}

void initGreeter(ProcedureNamespace* ns) {
    Procedure* proc = new Procedure("hello");
    proc->setExecuteCallback(&helloExecute);
    proc->setAllocCallback(&helloAlloc);
    proc->setDeallocCallback(&helloDealloc);
    proc->addReturnValue("message", ProcedureType::STRING);
    ns->addProcedure(proc);
}

}

extern "C" const TuringExtensionDef* turing_extension() {
    static const TuringExtensionDef def {
        ._namespaceName = "greeter",
        ._initCallback = &initGreeter,
    };
    return &def;
}
