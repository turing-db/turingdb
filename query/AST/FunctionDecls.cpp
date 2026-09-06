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

    FunctionSignature* countListItems = createFunction("count");
    countListItems->setArguments({EvaluatedType::ListItem});
    countListItems->setReturnTypes({{EvaluatedType::Integer}});
    countListItems->setIsAggregate(true);

    // collect gathers the values of its group into a list, so it reduces like the others
    // but returns a list rather than a scalar
    FunctionSignature* collectIntegers = createFunction("collect");
    collectIntegers->setArguments({EvaluatedType::Integer});
    collectIntegers->setReturnTypes({{EvaluatedType::List}});
    collectIntegers->setIsAggregate(true);
    collectIntegers->setCollectsItsArgument(true);

    FunctionSignature* collectDoubles = createFunction("collect");
    collectDoubles->setArguments({EvaluatedType::Double});
    collectDoubles->setReturnTypes({{EvaluatedType::List}});
    collectDoubles->setIsAggregate(true);
    collectDoubles->setCollectsItsArgument(true);

    FunctionSignature* collectStrings = createFunction("collect");
    collectStrings->setArguments({EvaluatedType::String});
    collectStrings->setReturnTypes({{EvaluatedType::List}});
    collectStrings->setIsAggregate(true);
    collectStrings->setCollectsItsArgument(true);

    FunctionSignature* collectBools = createFunction("collect");
    collectBools->setArguments({EvaluatedType::Bool});
    collectBools->setReturnTypes({{EvaluatedType::List}});
    collectBools->setIsAggregate(true);
    collectBools->setCollectsItsArgument(true);

    FunctionSignature* collectNodes = createFunction("collect");
    collectNodes->setArguments({EvaluatedType::NodePattern});
    collectNodes->setReturnTypes({{EvaluatedType::List}});
    collectNodes->setIsAggregate(true);
    collectNodes->setCollectsItsArgument(true);

    FunctionSignature* collectEdges = createFunction("collect");
    collectEdges->setArguments({EvaluatedType::EdgePattern});
    collectEdges->setReturnTypes({{EvaluatedType::List}});
    collectEdges->setIsAggregate(true);
    collectEdges->setCollectsItsArgument(true);

    // A list collects into a list of lists: one level deeper over the same innermost
    // elements, so unwinding it twice hands those elements back with their own type.
    FunctionSignature* collectLists = createFunction("collect");
    collectLists->setArguments({EvaluatedType::List});
    collectLists->setReturnTypes({{EvaluatedType::List}});
    collectLists->setIsAggregate(true);
    collectLists->setCollectsItsArgument(true);

    // A type-erased cell collects too: the list gathers the values the cells hold, each
    // keeping its own type, so the list is homogeneous in nothing and hands tagged
    // scalars back out.
    FunctionSignature* collectListItems = createFunction("collect");
    collectListItems->setArguments({EvaluatedType::ListItem});
    collectListItems->setReturnTypes({{EvaluatedType::List}});
    collectListItems->setIsAggregate(true);
    collectListItems->setCollectsItsArgument(true);

    // collect(null) is a query that can be asked: every row's null is dropped, so the list
    // comes out empty, and the overload is what keeps the answer from being an argument error
    FunctionSignature* collectNulls = createFunction("collect");
    collectNulls->setArguments({EvaluatedType::Null});
    collectNulls->setReturnTypes({{EvaluatedType::List}});
    collectNulls->setIsAggregate(true);
    collectNulls->setCollectsItsArgument(true);

    // count(null) is a query that can be asked: a null is never charged, so the tally is
    // zero, and the overload is what keeps the answer from being an argument error
    FunctionSignature* countNulls = createFunction("count");
    countNulls->setArguments({EvaluatedType::Null});
    countNulls->setReturnTypes({{EvaluatedType::Integer}});
    countNulls->setIsAggregate(true);

    // count over a whole list: a list is never null, so the tally is every row. Only the
    // MLIR engine lays a list cell out over the driving relation - the legacy planner
    // reduces the single row the cell is, and count([1, 2]) answers 1 where the relation
    // holds more - so this overload and the embedding one below are v3-only.
    FunctionSignature* countLists = createFunction("count");
    countLists->setArguments({EvaluatedType::List});
    countLists->setReturnTypes({{EvaluatedType::Integer}});
    countLists->setIsAggregate(true);
    countLists->setIsV3Only(true);

    FunctionSignature* countEmbeddings = createFunction("count");
    countEmbeddings->setArguments({EvaluatedType::Embedding});
    countEmbeddings->setReturnTypes({{EvaluatedType::Integer}});
    countEmbeddings->setIsAggregate(true);
    countEmbeddings->setIsV3Only(true);

    // The schema types a CALL can yield are columns like any other, so count tallies their
    // rows too. Without these overloads a YIELD of a label, a property type or a value type
    // could be returned but never aggregated.
    FunctionSignature* countLabels = createFunction("count");
    countLabels->setArguments({EvaluatedType::Label});
    countLabels->setReturnTypes({{EvaluatedType::Integer}});
    countLabels->setIsAggregate(true);

    FunctionSignature* countEdgeTypes = createFunction("count");
    countEdgeTypes->setArguments({EvaluatedType::EdgeType});
    countEdgeTypes->setReturnTypes({{EvaluatedType::Integer}});
    countEdgeTypes->setIsAggregate(true);

    FunctionSignature* countPropertyTypes = createFunction("count");
    countPropertyTypes->setArguments({EvaluatedType::PropertyType});
    countPropertyTypes->setReturnTypes({{EvaluatedType::Integer}});
    countPropertyTypes->setIsAggregate(true);

    FunctionSignature* countValueTypes = createFunction("count");
    countValueTypes->setArguments({EvaluatedType::ValueType});
    countValueTypes->setReturnTypes({{EvaluatedType::Integer}});
    countValueTypes->setIsAggregate(true);

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

    // An extremum asks only that the group's values be ordered against each other, which
    // Cypher orders strings and booleans by as much as numbers: min(n.name) is the least
    // name of the group, and false orders below true.
    FunctionSignature* minString = createFunction("min");
    minString->setArguments({EvaluatedType::String});
    minString->setReturnTypes({{EvaluatedType::String}});
    minString->setIsAggregate(true);

    FunctionSignature* maxString = createFunction("max");
    maxString->setArguments({EvaluatedType::String});
    maxString->setReturnTypes({{EvaluatedType::String}});
    maxString->setIsAggregate(true);

    FunctionSignature* minBool = createFunction("min");
    minBool->setArguments({EvaluatedType::Bool});
    minBool->setReturnTypes({{EvaluatedType::Bool}});
    minBool->setIsAggregate(true);

    FunctionSignature* maxBool = createFunction("max");
    maxBool->setArguments({EvaluatedType::Bool});
    maxBool->setReturnTypes({{EvaluatedType::Bool}});
    maxBool->setIsAggregate(true);

    FunctionSignature* avgInt = createFunction("avg");
    avgInt->setArguments({EvaluatedType::Integer});
    avgInt->setReturnTypes({{EvaluatedType::Double}});
    avgInt->setIsAggregate(true);

    FunctionSignature* avgDouble = createFunction("avg");
    avgDouble->setArguments({EvaluatedType::Double});
    avgDouble->setReturnTypes({{EvaluatedType::Double}});
    avgDouble->setIsAggregate(true);

    // avg over a type-erased column of tagged cells - what a list mixing numeric types,
    // holding a null or holding nothing unwinds into. Each cell is read through its tag,
    // and an average is a double whatever those tags were. Only the MLIR engine folds such
    // a column, so the overload is v3-only like the whole-cell counts above.
    FunctionSignature* avgListItems = createFunction("avg");
    avgListItems->setArguments({EvaluatedType::ListItem});
    avgListItems->setReturnTypes({{EvaluatedType::Double}});
    avgListItems->setIsAggregate(true);
    avgListItems->setIsV3Only(true);

    // sum() enabled for v3 analyzer 
    FunctionSignature* sumInt = createFunction("sum");
    sumInt->setArguments({EvaluatedType::Integer});
    sumInt->setReturnTypes({{EvaluatedType::Integer}});
    sumInt->setIsAggregate(true);

    FunctionSignature* sumDouble = createFunction("sum");
    sumDouble->setArguments({EvaluatedType::Double});
    sumDouble->setReturnTypes({{EvaluatedType::Double}});
    sumDouble->setIsAggregate(true);

    // A type-erased cell carries its number's type per row. Mixed numeric tags are what
    // leaves a list type-erased, and Cypher sums those to a float, so this reduces to one
    // whichever tags turn up - and errors on a cell that is no number at all. Only the
    // MLIR engine folds such a column, so the overload is v3-only as avg's is.
    FunctionSignature* sumListItems = createFunction("sum");
    sumListItems->setArguments({EvaluatedType::ListItem});
    sumListItems->setReturnTypes({{EvaluatedType::Double}});
    sumListItems->setIsAggregate(true);
    sumListItems->setIsV3Only(true);

    // Conversion functions
    FunctionSignature* toInteger = createFunction("toInteger");
    toInteger->setArguments({EvaluatedType::String});
    toInteger->setReturnTypes({{EvaluatedType::Integer}});

    FunctionSignature* toIntegerOfInteger = createFunction("toInteger");
    toIntegerOfInteger->setArguments({EvaluatedType::Integer});
    toIntegerOfInteger->setReturnTypes({{EvaluatedType::Integer}});

    FunctionSignature* toIntegerOfDouble = createFunction("toInteger");
    toIntegerOfDouble->setArguments({EvaluatedType::Double});
    toIntegerOfDouble->setReturnTypes({{EvaluatedType::Integer}});

    FunctionSignature* toFloat = createFunction("toFloat");
    toFloat->setArguments({EvaluatedType::String});
    toFloat->setReturnTypes({{EvaluatedType::Double}});

    FunctionSignature* toFloatOfInteger = createFunction("toFloat");
    toFloatOfInteger->setArguments({EvaluatedType::Integer});
    toFloatOfInteger->setReturnTypes({{EvaluatedType::Double}});

    FunctionSignature* toFloatOfDouble = createFunction("toFloat");
    toFloatOfDouble->setArguments({EvaluatedType::Double});
    toFloatOfDouble->setReturnTypes({{EvaluatedType::Double}});

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
