#include "FunctionDecls.h"

using namespace db;

FunctionDecls::FunctionDecls()
{
}

FunctionDecls::~FunctionDecls() {
}

void FunctionDecls::initDefault() {
    // Entity patterns
    FunctionSignature* edgeType = createFunction("edgeType");
    edgeType->setArguments({EvaluatedType::EdgePattern});
    edgeType->setReturnTypes({{EvaluatedType::String}});

    FunctionSignature* type = createFunction("type");
    type->setArguments({EvaluatedType::EdgePattern});
    type->setReturnTypes({{EvaluatedType::String}});

    FunctionSignature* labels = createFunction("labels");
    labels->setArguments({EvaluatedType::NodePattern});
    labels->setReturnTypes({{EvaluatedType::String}});

    FunctionSignature* keysNodes = createFunction("keys");
    keysNodes->setArguments({EvaluatedType::NodePattern});
    keysNodes->setReturnTypes({{EvaluatedType::String}});

    FunctionSignature* keysEdges = createFunction("keys");
    keysEdges->setArguments({EvaluatedType::EdgePattern});
    keysEdges->setReturnTypes({{EvaluatedType::String}});

    // Aggregate functions
    FunctionSignature* countNodes = createFunction("count");
    countNodes->setArguments({EvaluatedType::NodePattern});
    countNodes->setReturnTypes({{EvaluatedType::Integer}});
    countNodes->setIsAggregate(true);

    FunctionSignature* countEdges = createFunction("count");
    countEdges->setArguments({EvaluatedType::EdgePattern});
    countEdges->setReturnTypes({{EvaluatedType::Integer}});
    countEdges->setIsAggregate(true);

    FunctionSignature* countIntegers = createFunction("count");
    countIntegers->setArguments({EvaluatedType::Integer});
    countIntegers->setReturnTypes({{EvaluatedType::Integer}});
    countIntegers->setIsAggregate(true);

    FunctionSignature* countDoubles = createFunction("count");
    countDoubles->setArguments({EvaluatedType::Double});
    countDoubles->setReturnTypes({{EvaluatedType::Integer}});
    countDoubles->setIsAggregate(true);

    FunctionSignature* countStrings = createFunction("count");
    countStrings->setArguments({EvaluatedType::String});
    countStrings->setReturnTypes({{EvaluatedType::Integer}});
    countStrings->setIsAggregate(true);

    FunctionSignature* countChars = createFunction("count");
    countChars->setArguments({EvaluatedType::Char});
    countChars->setReturnTypes({{EvaluatedType::Integer}});
    countChars->setIsAggregate(true);

    FunctionSignature* countBools = createFunction("count");
    countBools->setArguments({EvaluatedType::Bool});
    countBools->setReturnTypes({{EvaluatedType::Integer}});
    countBools->setIsAggregate(true);

    FunctionSignature* countWildcard = createFunction("count");
    countWildcard->setArguments({EvaluatedType::Wildcard});
    countWildcard->setReturnTypes({{EvaluatedType::Integer}});
    countWildcard->setIsAggregate(true);

    FunctionSignature* minInt = createFunction("min");
    minInt->setArguments({EvaluatedType::Integer});
    minInt->setReturnTypes({{EvaluatedType::Integer}});
    minInt->setIsAggregate(true);

    FunctionSignature* minDouble = createFunction("min");
    minDouble->setArguments({EvaluatedType::Double});
    minDouble->setReturnTypes({{EvaluatedType::Double}});
    minDouble->setIsAggregate(true);

    FunctionSignature* maxInt = createFunction("max");
    maxInt->setArguments({EvaluatedType::Integer});
    maxInt->setReturnTypes({{EvaluatedType::Integer}});
    maxInt->setIsAggregate(true);

    FunctionSignature* maxDouble = createFunction("max");
    maxDouble->setArguments({EvaluatedType::Double});
    maxDouble->setReturnTypes({{EvaluatedType::Double}});
    maxDouble->setIsAggregate(true);

    FunctionSignature* avgInt = createFunction("avg");
    avgInt->setArguments({EvaluatedType::Integer});
    avgInt->setReturnTypes({{EvaluatedType::Double}});
    avgInt->setIsAggregate(true);

    FunctionSignature* avgDouble = createFunction("avg");
    avgDouble->setArguments({EvaluatedType::Double});
    avgDouble->setReturnTypes({{EvaluatedType::Double}});
    avgDouble->setIsAggregate(true);

    // sum() enabled for v3 analyzer 
    FunctionSignature* sumInt = createFunction("sum");
    sumInt->setArguments({EvaluatedType::Integer});
    sumInt->setReturnTypes({{EvaluatedType::Integer}});
    sumInt->setIsAggregate(true);

    FunctionSignature* sumDouble = createFunction("sum");
    sumDouble->setArguments({EvaluatedType::Double});
    sumDouble->setReturnTypes({{EvaluatedType::Double}});
    sumDouble->setIsAggregate(true);

    // Conversion functions
    FunctionSignature* toInteger = createFunction("toInteger");
    toInteger->setArguments({EvaluatedType::String});
    toInteger->setReturnTypes({{EvaluatedType::Integer}});

    FunctionSignature* toFloat = createFunction("toFloat");
    toFloat->setArguments({EvaluatedType::String});
    toFloat->setReturnTypes({{EvaluatedType::Double}});

    FunctionSignature* toBoolean = createFunction("toBoolean");
    toBoolean->setArguments({EvaluatedType::String});
    toBoolean->setReturnTypes({{EvaluatedType::Bool}});

    // Embedding distance functions
    FunctionSignature* cosineSim = createFunction("cosine_similarity");
    cosineSim->setArguments({EvaluatedType::Embedding, EvaluatedType::Embedding});
    cosineSim->setReturnTypes({{EvaluatedType::Double}});

    FunctionSignature* euclidDist = createFunction("euclidean_distance");
    euclidDist->setArguments({EvaluatedType::Embedding, EvaluatedType::Embedding});
    euclidDist->setReturnTypes({{EvaluatedType::Double}});
}

FunctionSignature* FunctionDecls::createFunction(std::string_view fullName) {
    auto func = std::make_unique<FunctionSignature>(fullName);
    FunctionSignature* ptr = func.get();
    _owned.push_back(std::move(func));
    _nameMap[fullName].push_back(ptr);

    return ptr;
}

FunctionResolver::FunctionSignatureRange FunctionDecls::lookup(std::string_view fullName) {
    const auto it = _nameMap.find(fullName);
    if (it == _nameMap.end()) {
        return FunctionSignatureRange();
    }

    FunctionSignatures& sigs = it->second;
    return FunctionSignatureRange(sigs.data(), sigs.data() + sigs.size());
}
