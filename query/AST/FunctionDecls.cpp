#include "FunctionDecls.h"

#include "procedures/ProcedureBlueprint.h"
#include "procedures/ProcedureBlueprintMap.h"

#include "FatalException.h"

using namespace db;

FunctionDecls::FunctionDecls()
{
}

FunctionDecls::~FunctionDecls() {
}

std::unique_ptr<FunctionDecls> FunctionDecls::createDefault(const ProcedureBlueprintMap& procedures) {
    auto decls = std::make_unique<FunctionDecls>();

    // Metadata
    for (const auto& blueprint : procedures.getAll()) {
        auto declBuilder = decls->create(blueprint._name);
        declBuilder.setIsDatabaseProcedure(true);

        for (const auto& returnItem : blueprint._returnValues) {
            switch (returnItem._type) {
                case ProcedureReturnType::INVALID:
                    throw FatalException("Invalid procedure return type");
                    break;

                case ProcedureReturnType::NODE:
                    declBuilder.addReturnType(EvaluatedType::NodePattern, returnItem._name);
                    break;
                case ProcedureReturnType::EDGE:
                    declBuilder.addReturnType(EvaluatedType::EdgePattern, returnItem._name);
                    break;
                case ProcedureReturnType::VALUE_TYPE:
                    declBuilder.addReturnType(EvaluatedType::ValueType, returnItem._name);
                    break;
                case ProcedureReturnType::LABEL_ID:
                case ProcedureReturnType::EDGE_TYPE_ID:
                case ProcedureReturnType::PROPERTY_TYPE_ID:
                case ProcedureReturnType::UINT_64:
                case ProcedureReturnType::INT64:
                    declBuilder.addReturnType(EvaluatedType::Integer, returnItem._name);
                    break;
                case ProcedureReturnType::DOUBLE:
                    declBuilder.addReturnType(EvaluatedType::Double, returnItem._name);
                    break;
                case ProcedureReturnType::BOOL:
                    declBuilder.addReturnType(EvaluatedType::Bool, returnItem._name);
                    break;
                case ProcedureReturnType::STRING_VIEW:
                case ProcedureReturnType::STRING:
                    declBuilder.addReturnType(EvaluatedType::String, returnItem._name);
                    break;

                case ProcedureReturnType::_SIZE:
                    throw FatalException("Invalid procedure return type: _SIZE");
                    break;
            }
        }
    }

    // Entity patterns
    decls->create("edgeTypes")
        .setArguments({EvaluatedType::EdgePattern})
        .setReturnTypes({{EvaluatedType::String}});
    decls->create("labels")
        .setArguments({EvaluatedType::NodePattern})
        .setReturnTypes({{EvaluatedType::String}});
    decls->create("keys")
        .setArguments({EvaluatedType::NodePattern})
        .setReturnTypes({{EvaluatedType::String}});
    decls->create("keys")
        .setArguments({EvaluatedType::EdgePattern})
        .setReturnTypes({{EvaluatedType::String}});

    // Aggregate functions
    decls->create("count")
        .setArguments({EvaluatedType::NodePattern})
        .setReturnTypes({{EvaluatedType::Integer}})
        .setIsAggregate(true);
    decls->create("count")
        .setArguments({EvaluatedType::EdgePattern})
        .setReturnTypes({{EvaluatedType::Integer}})
        .setIsAggregate(true);
    decls->create("count")
        .setArguments({EvaluatedType::Integer})
        .setReturnTypes({{EvaluatedType::Integer}})
        .setIsAggregate(true);
    decls->create("count")
        .setArguments({EvaluatedType::Double})
        .setReturnTypes({{EvaluatedType::Integer}})
        .setIsAggregate(true);
    decls->create("count")
        .setArguments({EvaluatedType::String})
        .setReturnTypes({{EvaluatedType::Integer}})
        .setIsAggregate(true);
    decls->create("count")
        .setArguments({EvaluatedType::Char})
        .setReturnTypes({{EvaluatedType::Integer}})
        .setIsAggregate(true);
    decls->create("count")
        .setArguments({EvaluatedType::Bool})
        .setReturnTypes({{EvaluatedType::Integer}})
        .setIsAggregate(true);
    decls->create("count")
        .setArguments({EvaluatedType::Wildcard})
        .setReturnTypes({{EvaluatedType::Integer}})
        .setIsAggregate(true);

    decls->create("min")
        .setArguments({EvaluatedType::Integer})
        .setReturnTypes({{EvaluatedType::Integer}})
        .setIsAggregate(true);
    decls->create("min")
        .setArguments({EvaluatedType::Double})
        .setReturnTypes({{EvaluatedType::Double}})
        .setIsAggregate(true);

    decls->create("max")
        .setArguments({EvaluatedType::Integer})
        .setReturnTypes({{EvaluatedType::Integer}})
        .setIsAggregate(true);
    decls->create("max")
        .setArguments({EvaluatedType::Double})
        .setReturnTypes({{EvaluatedType::Double}})
        .setIsAggregate(true);

    decls->create("avg")
        .setArguments({EvaluatedType::Integer})
        .setReturnTypes({{EvaluatedType::Double}})
        .setIsAggregate(true);
    decls->create("avg")
        .setArguments({EvaluatedType::Double})
        .setReturnTypes({{EvaluatedType::Double}})
        .setIsAggregate(true);

    return decls;
}

FunctionDecls::FunctionSignatureBuilder FunctionDecls::create(std::string_view fullName) {
    auto func = std::make_unique<FunctionSignature>(fullName);
    FunctionSignature* ptr = func.get();
    _decls.emplace(fullName, std::move(func));

    return FunctionSignatureBuilder(ptr);
}
