#include "FunctionDecls.h"

using namespace db::v2;

FunctionDecls::FunctionDecls()
{
}

FunctionDecls::~FunctionDecls() {
}

std::unique_ptr<FunctionDecls> FunctionDecls::createDefault() {
    auto decls = std::make_unique<FunctionDecls>();

    // Metadata
    decls->create("db.labels")
        .setReturnType(EvaluatedType::String);
    decls->create("db.propertyTypes")
        .setReturnType(EvaluatedType::String);
    decls->create("db.edgeTypes")
        .setReturnType(EvaluatedType::String);

    // Entity patterns
    decls->create("edgeTypes")
        .setArguments({EvaluatedType::EdgePattern})
        .setReturnType(EvaluatedType::String);
    decls->create("labels")
        .setArguments({EvaluatedType::NodePattern})
        .setReturnType(EvaluatedType::String);
    decls->create("keys")
        .setArguments({EvaluatedType::NodePattern})
        .setReturnType(EvaluatedType::String);
    decls->create("keys")
        .setArguments({EvaluatedType::EdgePattern})
        .setReturnType(EvaluatedType::String);

    // Aggregate functions
    decls->create("count")
        .setArguments({EvaluatedType::NodePattern})
        .setReturnType(EvaluatedType::Integer)
        .setIsAggregate(true);
    decls->create("count")
        .setArguments({EvaluatedType::EdgePattern})
        .setReturnType(EvaluatedType::Integer)
        .setIsAggregate(true);
    decls->create("count")
        .setArguments({EvaluatedType::Integer})
        .setReturnType(EvaluatedType::Integer)
        .setIsAggregate(true);
    decls->create("count")
        .setArguments({EvaluatedType::Double})
        .setReturnType(EvaluatedType::Integer)
        .setIsAggregate(true);
    decls->create("count")
        .setArguments({EvaluatedType::String})
        .setReturnType(EvaluatedType::Integer)
        .setIsAggregate(true);
    decls->create("count")
        .setArguments({EvaluatedType::Char})
        .setReturnType(EvaluatedType::Integer)
        .setIsAggregate(true);
    decls->create("count")
        .setArguments({EvaluatedType::Bool})
        .setReturnType(EvaluatedType::Integer)
        .setIsAggregate(true);
    decls->create("count")
        .setArguments({EvaluatedType::Wildcard})
        .setReturnType(EvaluatedType::Integer)
        .setIsAggregate(true);

    decls->create("min")
        .setArguments({EvaluatedType::Integer})
        .setReturnType(EvaluatedType::Integer)
        .setIsAggregate(true);
    decls->create("min")
        .setArguments({EvaluatedType::Double})
        .setReturnType(EvaluatedType::Double)
        .setIsAggregate(true);

    decls->create("max")
        .setArguments({EvaluatedType::Integer})
        .setReturnType(EvaluatedType::Integer)
        .setIsAggregate(true);
    decls->create("max")
        .setArguments({EvaluatedType::Double})
        .setReturnType(EvaluatedType::Double)
        .setIsAggregate(true);

    decls->create("avg")
        .setArguments({EvaluatedType::Integer})
        .setReturnType(EvaluatedType::Double)
        .setIsAggregate(true);
    decls->create("avg")
        .setArguments({EvaluatedType::Double})
        .setReturnType(EvaluatedType::Double)
        .setIsAggregate(true);

    return decls;
}

FunctionDecls::FunctionSignatureBuilder FunctionDecls::create(std::string_view fullName) {
    auto func = std::make_unique<FunctionSignature>(fullName);
    FunctionSignature* ptr = func.get();
    _decls.emplace(fullName, std::move(func));

    return FunctionSignatureBuilder(ptr);
}
