#include "ProceduresProcedure.h"

#include "ProcedureContext.h"
#include "ProcedureState.h"
#include "Procedure.h"
#include "ProcedureNamespace.h"
#include "TypedProcedure.h"
#include "ProcedureManager.h"
#include "ProcedureTypeVector.h"
#include "columns/ColumnVector.h"
#include "buffers/StringBuffer.h"

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
                     ColumnVector<std::string_view>* nameCol,
                     ColumnVector<std::string_view>* signatureCol,
                     size_t chunkSize,
                     StringBuffer* stringBuffer) {
    ProcedureManager::Namespaces namespaces;
    manager->getNamespaces(namespaces);

    data->clearReturnColumns();

    size_t remaining = chunkSize;
    std::string signature;
    ProcedureNamespace::Procedures procs;

    while (remaining > 0
           && data->_nsIndex < namespaces.size()) {
        const ProcedureNamespace* ns = namespaces[data->_nsIndex];
        ns->getProcedures(procs);

        while (remaining > 0
               && data->_procIndex < procs.size()) {

            const Procedure* procedure = procs[data->_procIndex];

            if (nameCol) {
                nameCol->push_back(procedure->getFullName());
            }

            if (signatureCol) {
                buildSignature(signature, procedure);
                signatureCol->push_back(stringBuffer->insert(signature));
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

void ProceduresProcedure::registerProcedure(ProcedureNamespace* ns) {
    ns->addProcedure(createTypedProcedure<ProceduresProcedure, Data>("procedures"));
}

void ProceduresProcedure::execute(ProcedureState* proc) {
    Data& data = proc->data<Data>();
    const ProcedureContext* ctxt = proc->getContext();
    const ProcedureManager* manager = ctxt->getProcedures();

    auto* nameCol = getReturnColumn<ProceduresProcedure, 0>(&data);
    auto* signatureCol = getReturnColumn<ProceduresProcedure, 1>(&data);

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
                            ctxt->getChunkSize(),
                            ctxt->getStringBuffer());
        }
        break;
    }
}
