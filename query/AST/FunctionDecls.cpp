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
                case ProcedureType::INVALID:
                    throw FatalException("Invalid procedure return type");
                    break;

                case ProcedureType::NODE:
                    declBuilder.addReturnType(EvaluatedType::NodePattern, returnItem._name);
                    break;
                case ProcedureType::EDGE:
                    declBuilder.addReturnType(EvaluatedType::EdgePattern, returnItem._name);
                    break;
                case ProcedureType::VALUE_TYPE:
                    declBuilder.addReturnType(EvaluatedType::ValueType, returnItem._name);
                    break;
                case ProcedureType::LABEL_ID:
                case ProcedureType::EDGE_TYPE_ID:
                case ProcedureType::PROPERTY_TYPE_ID:
                case ProcedureType::UINT_64:
                case ProcedureType::INT64:
                    declBuilder.addReturnType(EvaluatedType::Integer, returnItem._name);
                    break;
                case ProcedureType::DOUBLE:
                    declBuilder.addReturnType(EvaluatedType::Double, returnItem._name);
                    break;
                case ProcedureType::BOOL:
                    declBuilder.addReturnType(EvaluatedType::Bool, returnItem._name);
                    break;
                case ProcedureType::STRING_VIEW:
                case ProcedureType::STRING:
                    declBuilder.addReturnType(EvaluatedType::String, returnItem._name);
                    break;

                case ProcedureType::_SIZE:
                    throw FatalException("Invalid procedure return type: _SIZE");
                break;
            }
        }

        for (const auto& arg : blueprint._argumentTypes) {
            switch (arg._type) {
                case ProcedureType::INVALID:
                    throw FatalException("Invalid procedure return type");
                    break;

                case ProcedureType::NODE:
                    declBuilder.addArgument(EvaluatedType::NodePattern);
                    break;
                case ProcedureType::EDGE:
                    declBuilder.addArgument(EvaluatedType::EdgePattern);
                    break;
                case ProcedureType::VALUE_TYPE:
                    declBuilder.addArgument(EvaluatedType::ValueType);
                    break;
                case ProcedureType::LABEL_ID:
                case ProcedureType::EDGE_TYPE_ID:
                case ProcedureType::PROPERTY_TYPE_ID:
                case ProcedureType::UINT_64:
                case ProcedureType::INT64:
                    declBuilder.addArgument(EvaluatedType::Integer);
                    break;
                case ProcedureType::DOUBLE:
                    declBuilder.addArgument(EvaluatedType::Double);
                    break;
                case ProcedureType::BOOL:
                    declBuilder.addArgument(EvaluatedType::Bool);
                    break;
                case ProcedureType::STRING_VIEW:
                case ProcedureType::STRING:
                    declBuilder.addArgument(EvaluatedType::String);
                    break;

                case ProcedureType::_SIZE:
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
