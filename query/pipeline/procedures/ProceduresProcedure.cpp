#include "ProceduresProcedure.h"

#include "ExecutionContext.h"
#include "procedures/Procedure.h"
#include "procedures/ProcedureBlueprintMap.h"
#include "procedures/ProcedureBlueprint.h"
#include "procedures/ProcedureReturnValues.h"
#include "columns/ColumnVector.h"

#include "PipelineException.h"

using namespace db;

namespace {

struct Data : public ProcedureData {
    std::vector<ProcedureBlueprint>::const_iterator _it;
};

void buildSignature(std::string& result, const ProcedureBlueprint& blueprint) {
    result.clear();
    result += blueprint._name;
    result += "() :: (";

    bool first = true;
    for (const auto& rv : blueprint._returnValues) {
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

}

std::unique_ptr<ProcedureData> ProceduresProcedure::allocData() {
    return std::make_unique<Data>();
}

void ProceduresProcedure::execute(Procedure& proc) {
    Data& data = proc.data<Data>();
    const ExecutionContext* ctxt = proc.ctxt();
    const ProcedureBlueprintMap* blueprints = ctxt->getProcedures();

    Column* rawNameCol = data.getReturnColumn(0);
    Column* rawSignatureCol = data.getReturnColumn(1);

    auto* nameCol = static_cast<ColumnVector<types::String::Primitive>*>(rawNameCol);
    auto* signatureCol = static_cast<ColumnVector<std::string>*>(rawSignatureCol);

    switch (proc.step()) {
        case Procedure::Step::PREPARE: {
            data._it = blueprints->getAll().begin();
            return;
        }

        case Procedure::Step::RESET: {
            data._it = blueprints->getAll().begin();
            return;
        }

        case Procedure::Step::EXECUTE: {
            const auto& allBlueprints = blueprints->getAll();
            size_t remaining = std::distance(data._it, allBlueprints.end());
            remaining = std::min(remaining, ctxt->getChunkSize());

            if (nameCol) {
                nameCol->clear();
            }

            if (signatureCol) {
                signatureCol->clear();
            }

            std::string signature;
            for (size_t i = 0; i < remaining; ++i) {
                const ProcedureBlueprint& blueprint = *data._it;

                if (nameCol) {
                    nameCol->push_back(blueprint._name);
                }

                if (signatureCol) {
                    buildSignature(signature, blueprint);
                    signatureCol->push_back(signature);
                }

                ++data._it;
            }

            if (data._it == allBlueprints.end()) {
                proc.finish();
            }

            return;
        }
    }

    throw PipelineException("Unknown procedure step");
}
