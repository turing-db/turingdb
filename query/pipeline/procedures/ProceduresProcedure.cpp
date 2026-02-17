#include "ProceduresProcedure.h"

#include "ExecutionContext.h"
#include "ProcedureState.h"
#include "Procedure.h"
#include "ProcedureNamespace.h"
#include "ProcedureManager.h"
#include "ProcedureTypeVector.h"
#include "columns/ColumnVector.h"

#include "PipelineException.h"

using namespace db;

namespace {

struct Data : public ProcedureData {
    size_t _nsIndex {0};
    size_t _procIndex {0};
};

void buildSignature(std::string& result, const Procedure* proc) {
    result.clear();
    result += proc->getFullName();
    result += "(";

    bool first = true;
    for (const auto& arg : proc->argumentTypes()) {
        if (!first) {
            result += ", ";
        }
        result += arg._name;
        result += " :: ";
        result += ProcedureTypeName::value(arg._type);
        first = false;
    }

    result += ") :: (";

    first = true;
    for (const auto& rv : proc->returnValues()) {
        if (!first) {
            result += ", ";
        }
        result += rv._name;
        result += " :: ";
        result += ProcedureTypeName::value(rv._type);
        first = false;
    }
    result += ")";
}

void writeProcedures(Data* data,
                     ProcedureState* proc,
                     const ProcedureManager* manager,
                     ColumnVector<std::string>* nameCol,
                     ColumnVector<std::string>* signatureCol,
                     size_t chunkSize) {
    const auto& namespaces = manager->namespaces();

    if (nameCol) {
        nameCol->clear();
    }

    if (signatureCol) {
        signatureCol->clear();
    }

    size_t remaining = chunkSize;
    std::string signature;

    while (remaining > 0
           && data->_nsIndex < namespaces.size()) {

        const ProcedureNamespace* ns = namespaces[data->_nsIndex];
        const auto& procs = ns->procedures();

        while (remaining > 0
               && data->_procIndex < procs.size()) {

            const Procedure* proc = procs[data->_procIndex];

            if (nameCol) {
                nameCol->push_back(proc->getFullName());
            }

            if (signatureCol) {
                buildSignature(signature, proc);
                signatureCol->push_back(signature);
            }

            ++data->_procIndex;
            --remaining;
        }

        if (data->_procIndex >= procs.size()) {
            ++data->_nsIndex;
            data->_procIndex = 0;
        }
    }

    if (data->_nsIndex >= namespaces.size()) {
        proc->finish();
    }
}

}

ProcedureData* ProceduresProcedure::allocData() {
    return new Data();
}

void ProceduresProcedure::deallocData(ProcedureData* data) {
    delete data;
}

void ProceduresProcedure::registerProcedure(ProcedureNamespace* ns) {
    Procedure* proc = new Procedure("procedures");
    proc->setExecuteCallback(&execute);
    proc->setAllocCallback(&allocData);
    proc->setDeallocCallback(&deallocData);
    proc->addReturnValue("name", ProcedureType::STRING);
    proc->addReturnValue("signature", ProcedureType::STRING);
    ns->addProcedure(proc);
}

void ProceduresProcedure::execute(ProcedureState* proc) {
    Data& data = proc->data<Data>();
    const ExecutionContext* ctxt = proc->getContext();
    const ProcedureManager* manager = ctxt->getProcedures();

    Column* rawNameCol = data.getReturnColumn(0);
    Column* rawSignatureCol = data.getReturnColumn(1);

    auto* nameCol = static_cast<ColumnVector<std::string>*>(rawNameCol);
    auto* signatureCol = static_cast<ColumnVector<std::string>*>(rawSignatureCol);

    switch (proc->getStep()) {
        case ProcedureState::Step::PREPARE: {
            data._nsIndex = 0;
            data._procIndex = 0;
        }
        break;

        case ProcedureState::Step::RESET: {
            data._nsIndex = 0;
            data._procIndex = 0;
        }
        break;

        case ProcedureState::Step::EXECUTE: {
            writeProcedures(&data,
                            proc,
                            manager,
                            nameCol,
                            signatureCol,
                            ctxt->getChunkSize());
        }
        break;
    }

    throw PipelineException("Unknown procedure step");
}
