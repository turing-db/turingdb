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
        .setIsDatabaseProcedure(true)
        .setReturnTypes({
            FunctionReturnType {EvaluatedType::String,  "label"},
            FunctionReturnType {EvaluatedType::Integer, "id"  },
    });

    decls->create("db.propertyTypes")
        .setIsDatabaseProcedure(true)
        .setReturnTypes({
            FunctionReturnType {EvaluatedType::String,  "propertyType"},
            FunctionReturnType {EvaluatedType::Integer, "id"  },
            FunctionReturnType {EvaluatedType::ValueType, "valueType"  },
    });

    decls->create("db.edgeTypes")
        .setIsDatabaseProcedure(true)
        .setReturnTypes({
            FunctionReturnType {EvaluatedType::String,  "edgeType"},
            FunctionReturnType {EvaluatedType::Integer, "id"  },
    });

    decls->create("db.history")
        .setIsDatabaseProcedure(true)
        .setReturnTypes({
            FunctionReturnType {EvaluatedType::String,  "commit_hash"},
            FunctionReturnType {EvaluatedType::Integer, "part_count" },
            FunctionReturnType {EvaluatedType::Integer, "node_count" },
            FunctionReturnType {EvaluatedType::Integer, "edge_count" },
    });

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
