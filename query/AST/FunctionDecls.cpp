#include "FunctionDecls.h"

using namespace db;

FunctionDecls::FunctionDecls()
{
    initDefault();
}

FunctionDecls::~FunctionDecls() {
}

const FunctionDecls& FunctionDecls::getBuiltins() {
    static const FunctionDecls builtins;
    return builtins;
}

void FunctionDecls::initDefault() {
    // Entity patterns
    FunctionSignature* type = createFunction("type");
    type->setArguments({EvaluatedType::EdgePattern});
    type->setReturnTypes({{EvaluatedType::String}});

    FunctionSignature* labels = createFunction("labels");
    labels->setArguments({EvaluatedType::NodePattern});
    labels->setReturnTypes({{EvaluatedType::List}});

    // The ends of an edge, in the order the graph stores it: a hop walked backwards or
    // undirected binds the same edge, so its start is the node the edge leaves whichever
    // way the pattern reached it.
    FunctionSignature* startNode = createFunction("startNode");
    startNode->setArguments({EvaluatedType::EdgePattern});
    startNode->setReturnTypes({{EvaluatedType::NodePattern}});

    FunctionSignature* endNode = createFunction("endNode");
    endNode->setArguments({EvaluatedType::EdgePattern});
    endNode->setReturnTypes({{EvaluatedType::NodePattern}});

    FunctionSignature* idOfNode = createFunction("id");
    idOfNode->setArguments({EvaluatedType::NodePattern});
    idOfNode->setReturnTypes({{EvaluatedType::Integer}});

    FunctionSignature* idOfEdge = createFunction("id");
    idOfEdge->setArguments({EvaluatedType::EdgePattern});
    idOfEdge->setReturnTypes({{EvaluatedType::Integer}});

    // The three over a type-erased cell, which is what an UNWIND or an index of a stored
    // list binds: the list names no element type, so its entities are known per row rather
    // than in the plan. A cell holding neither a node nor an edge is the row's type error.
    FunctionSignature* startNodeOfCell = createFunction("startNode");
    startNodeOfCell->setArguments({EvaluatedType::ListItem});
    startNodeOfCell->setReturnTypes({{EvaluatedType::NodePattern}});

    FunctionSignature* endNodeOfCell = createFunction("endNode");
    endNodeOfCell->setArguments({EvaluatedType::ListItem});
    endNodeOfCell->setReturnTypes({{EvaluatedType::NodePattern}});

    FunctionSignature* idOfCell = createFunction("id");
    idOfCell->setArguments({EvaluatedType::ListItem});
    idOfCell->setReturnTypes({{EvaluatedType::Integer}});

    FunctionSignature* keysNodes = createFunction("keys");
    keysNodes->setArguments({EvaluatedType::NodePattern});
    keysNodes->setReturnTypes({{EvaluatedType::String}});

    FunctionSignature* keysEdges = createFunction("keys");
    keysEdges->setArguments({EvaluatedType::EdgePattern});
    keysEdges->setReturnTypes({{EvaluatedType::String}});

    FunctionArgumentType nodeOrGroup(EvaluatedType::NodePattern);
    nodeOrGroup.setTakesAVariableLengthPath(true);

    FunctionArgumentType edgeOrPath(EvaluatedType::EdgePattern);
    edgeOrPath.setTakesAVariableLengthPath(true);

    // Aggregate functions
    FunctionSignature* countNodes = createFunction("count");
    countNodes->setArguments({nodeOrGroup});
    countNodes->setReturnTypes({{EvaluatedType::Integer}});
    countNodes->setIsAggregate(true);

    FunctionSignature* countEdges = createFunction("count");
    countEdges->setArguments({edgeOrPath});
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

    FunctionSignature* countDateTimes = createFunction("count");
    countDateTimes->setArguments({EvaluatedType::DateTime});
    countDateTimes->setReturnTypes({{EvaluatedType::Integer}});
    countDateTimes->setIsAggregate(true);

    FunctionSignature* countDurations = createFunction("count");
    countDurations->setArguments({EvaluatedType::Duration});
    countDurations->setReturnTypes({{EvaluatedType::Integer}});
    countDurations->setIsAggregate(true);

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

    FunctionSignature* collectDateTimes = createFunction("collect");
    collectDateTimes->setArguments({EvaluatedType::DateTime});
    collectDateTimes->setReturnTypes({{EvaluatedType::List}});
    collectDateTimes->setIsAggregate(true);
    collectDateTimes->setCollectsItsArgument(true);

    FunctionSignature* collectDurations = createFunction("collect");
    collectDurations->setArguments({EvaluatedType::Duration});
    collectDurations->setReturnTypes({{EvaluatedType::List}});
    collectDurations->setIsAggregate(true);
    collectDurations->setCollectsItsArgument(true);

    FunctionSignature* collectNodes = createFunction("collect");
    collectNodes->setArguments({nodeOrGroup});
    collectNodes->setReturnTypes({{EvaluatedType::List}});
    collectNodes->setIsAggregate(true);
    collectNodes->setCollectsItsArgument(true);

    FunctionSignature* collectEdges = createFunction("collect");
    collectEdges->setArguments({edgeOrPath});
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

    // count over a whole list: a list is never null, so the tally is every row.
    FunctionSignature* countLists = createFunction("count");
    countLists->setArguments({EvaluatedType::List});
    countLists->setReturnTypes({{EvaluatedType::Integer}});
    countLists->setIsAggregate(true);

    FunctionSignature* countMaps = createFunction("count");
    countMaps->setArguments({EvaluatedType::Map});
    countMaps->setReturnTypes({{EvaluatedType::Integer}});
    countMaps->setIsAggregate(true);

    FunctionSignature* countPaths = createFunction("count");
    countPaths->setArguments({EvaluatedType::GraphPath});
    countPaths->setReturnTypes({{EvaluatedType::Integer}});
    countPaths->setIsAggregate(true);

    FunctionSignature* countEmbeddings = createFunction("count");
    countEmbeddings->setArguments({EvaluatedType::Embedding});
    countEmbeddings->setReturnTypes({{EvaluatedType::Integer}});
    countEmbeddings->setIsAggregate(true);

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

    // An extremum of a datetime column is the earliest or the latest instant in it
    FunctionSignature* minDateTime = createFunction("min");
    minDateTime->setArguments({EvaluatedType::DateTime});
    minDateTime->setReturnTypes({{EvaluatedType::DateTime}});
    minDateTime->setIsAggregate(true);

    FunctionSignature* maxDateTime = createFunction("max");
    maxDateTime->setArguments({EvaluatedType::DateTime});
    maxDateTime->setReturnTypes({{EvaluatedType::DateTime}});
    maxDateTime->setIsAggregate(true);

    FunctionSignature* minDuration = createFunction("min");
    minDuration->setArguments({EvaluatedType::Duration});
    minDuration->setReturnTypes({{EvaluatedType::Duration}});
    minDuration->setIsAggregate(true);

    FunctionSignature* maxDuration = createFunction("max");
    maxDuration->setArguments({EvaluatedType::Duration});
    maxDuration->setReturnTypes({{EvaluatedType::Duration}});
    maxDuration->setIsAggregate(true);

    // Cypher orders lists, maps and values of different types against each other, so
    // their extremum is defined too: lists compare element by element, and across types
    // MAP < LIST < STRING < BOOLEAN < NUMBER.
    FunctionSignature* minList = createFunction("min");
    minList->setArguments({EvaluatedType::List});
    minList->setReturnTypes({{EvaluatedType::List}});
    minList->setReturnsItsArgumentShape(true);
    minList->setIsAggregate(true);

    FunctionSignature* maxList = createFunction("max");
    maxList->setArguments({EvaluatedType::List});
    maxList->setReturnTypes({{EvaluatedType::List}});
    maxList->setReturnsItsArgumentShape(true);
    maxList->setIsAggregate(true);

    FunctionSignature* minMap = createFunction("min");
    minMap->setArguments({EvaluatedType::Map});
    minMap->setReturnTypes({{EvaluatedType::Map}});
    minMap->setIsAggregate(true);

    FunctionSignature* maxMap = createFunction("max");
    maxMap->setArguments({EvaluatedType::Map});
    maxMap->setReturnTypes({{EvaluatedType::Map}});
    maxMap->setIsAggregate(true);

    FunctionSignature* minListItems = createFunction("min");
    minListItems->setArguments({EvaluatedType::ListItem});
    minListItems->setReturnTypes({{EvaluatedType::ListItem}});
    minListItems->setIsAggregate(true);

    FunctionSignature* maxListItems = createFunction("max");
    maxListItems->setArguments({EvaluatedType::ListItem});
    maxListItems->setReturnTypes({{EvaluatedType::ListItem}});
    maxListItems->setIsAggregate(true);

    // An extremum of nothing is null, a sum of nothing is 0 and an average of nothing is
    // null, so a column that is null on every row - a name no property in the graph carries,
    // or the null literal - reduces to an answer rather than to an argument error, exactly
    // as count(null) and collect(null) do.
    FunctionSignature* minNulls = createFunction("min");
    minNulls->setArguments({EvaluatedType::Null});
    minNulls->setReturnTypes({{EvaluatedType::Null}});
    minNulls->setIsAggregate(true);

    FunctionSignature* maxNulls = createFunction("max");
    maxNulls->setArguments({EvaluatedType::Null});
    maxNulls->setReturnTypes({{EvaluatedType::Null}});
    maxNulls->setIsAggregate(true);

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
    // and an average is a double whatever those tags were.
    FunctionSignature* avgListItems = createFunction("avg");
    avgListItems->setArguments({EvaluatedType::ListItem});
    avgListItems->setReturnTypes({{EvaluatedType::Double}});
    avgListItems->setIsAggregate(true);

    FunctionSignature* avgNulls = createFunction("avg");
    avgNulls->setArguments({EvaluatedType::Null});
    avgNulls->setReturnTypes({{EvaluatedType::Null}});
    avgNulls->setIsAggregate(true);

    FunctionSignature* sumInt = createFunction("sum");
    sumInt->setArguments({EvaluatedType::Integer});
    sumInt->setReturnTypes({{EvaluatedType::Integer}});
    sumInt->setIsAggregate(true);

    FunctionSignature* sumDouble = createFunction("sum");
    sumDouble->setArguments({EvaluatedType::Double});
    sumDouble->setReturnTypes({{EvaluatedType::Double}});
    sumDouble->setIsAggregate(true);

    // A sum of no value is 0 rather than null, so this one answers an integer where the
    // extremums above answer null
    FunctionSignature* sumNulls = createFunction("sum");
    sumNulls->setArguments({EvaluatedType::Null});
    sumNulls->setReturnTypes({{EvaluatedType::Integer}});
    sumNulls->setIsAggregate(true);

    // A type-erased cell carries its number's type per row. Mixed numeric tags are what
    // leaves a list type-erased, and Cypher sums those to a float, so this reduces to one
    // whichever tags turn up - and errors on a cell that is no number at all.
    FunctionSignature* sumListItems = createFunction("sum");
    sumListItems->setArguments({EvaluatedType::ListItem});
    sumListItems->setReturnTypes({{EvaluatedType::Double}});
    sumListItems->setIsAggregate(true);

    // stDev spreads the values as a sample of a population, stDevP as the whole of it. Too
    // few values to spread - none, or the one a sample has no second for - spread by 0
    // rather than by null.
    const std::vector<EvaluatedType> numbers = {
        EvaluatedType::Integer,
        EvaluatedType::Double,
        EvaluatedType::ListItem,
        EvaluatedType::Null,
    };

    for (const std::string_view name : {"stDev", "stDevP"}) {
        for (const EvaluatedType number : numbers) {
            FunctionSignature* deviation = createFunction(name);
            deviation->setArguments({number});
            deviation->setReturnTypes({{EvaluatedType::Double}});
            deviation->setIsAggregate(true);
        }
    }

    // The percentile has to be the same for every value of a group, so it may not read a
    // row. percentileCont interpolates between two values, which makes a double of them;
    // percentileDisc answers one of the values as it is.
    for (const EvaluatedType percentileType : {EvaluatedType::Double, EvaluatedType::Integer}) {
        FunctionArgumentType percentile(percentileType);
        percentile.setName("percentile");
        percentile.setConstant(true);

        for (const EvaluatedType number : numbers) {
            const bool valuesAreNull = number == EvaluatedType::Null;

            FunctionSignature* continuous = createFunction("percentileCont");
            continuous->setArguments({number, percentile});
            continuous->setReturnTypes({{valuesAreNull ? EvaluatedType::Null : EvaluatedType::Double}});
            continuous->setIsAggregate(true);

            FunctionSignature* discrete = createFunction("percentileDisc");
            discrete->setArguments({number, percentile});
            discrete->setReturnTypes({{number}});
            discrete->setIsAggregate(true);
        }
    }

    // List functions.
    FunctionSignature* size = createFunction("size");
    size->setArguments({EvaluatedType::List});
    size->setReturnTypes({{EvaluatedType::Integer}});

    FunctionSignature* sizeString = createFunction("size");
    sizeString->setArguments({EvaluatedType::String});
    sizeString->setReturnTypes({{EvaluatedType::Integer}});

    // The first element of a list has the type the list names for its elements, and a
    // stored list names none, since it may mix them: there head answers with the tagged
    // scalar an UNWIND of the list binds. An empty list - and an absent one - heads into null.
    FunctionSignature* head = createFunction("head");
    head->setArguments({EvaluatedType::List});
    head->setReturnTypes({{EvaluatedType::ListItem}});
    head->setReturnsAnElementOfItsArgument(true);

    // The last element, typed as head types the first; an empty list - and an absent one -
    // has no final element, so it reads as null.
    FunctionSignature* last = createFunction("last");
    last->setArguments({EvaluatedType::List});
    last->setReturnTypes({{EvaluatedType::ListItem}});
    last->setReturnsAnElementOfItsArgument(true);

    // Dropping the first element leaves a list over the same elements, so it nests as
    // deeply as the one it came from; the tail of an empty list is empty, not null.
    FunctionSignature* tail = createFunction("tail");
    tail->setArguments({EvaluatedType::List});
    tail->setReturnTypes({{EvaluatedType::List}});
    tail->setReturnsItsArgumentShape(true);

    FunctionSignature* reverseList = createFunction("reverse");
    reverseList->setArguments({EvaluatedType::List});
    reverseList->setReturnTypes({{EvaluatedType::List}});
    reverseList->setReturnsItsArgumentShape(true);

    // The same four over a type-erased cell, which is the only thing an UNWIND of a
    // stored list of lists can bind: a stored list names no element type, so what its
    // elements are is known per row rather than in the plan. A cell holding a null
    // answers null; one holding neither a list nor, under size(), a string is the type
    // error the row raises.
    FunctionSignature* sizeCell = createFunction("size");
    sizeCell->setArguments({EvaluatedType::ListItem});
    sizeCell->setReturnTypes({{EvaluatedType::Integer}});

    FunctionSignature* headCell = createFunction("head");
    headCell->setArguments({EvaluatedType::ListItem});
    headCell->setReturnTypes({{EvaluatedType::ListItem}});

    FunctionSignature* lastCell = createFunction("last");
    lastCell->setArguments({EvaluatedType::ListItem});
    lastCell->setReturnTypes({{EvaluatedType::ListItem}});

    // The cell names no element type, so the list left of it names none either: an UNWIND
    // of this tail binds tagged cells again rather than a type it could promise
    FunctionSignature* tailCell = createFunction("tail");
    tailCell->setArguments({EvaluatedType::ListItem});
    tailCell->setReturnTypes({{EvaluatedType::List}});

    // length() is the other Cypher name for size(): the same three arguments answered the
    // same way, down to the db.size the call emits.
    FunctionSignature* lengthList = createFunction("length");
    lengthList->setArguments({EvaluatedType::List});
    lengthList->setReturnTypes({{EvaluatedType::Integer}});

    FunctionSignature* lengthString = createFunction("length");
    lengthString->setArguments({EvaluatedType::String});
    lengthString->setReturnTypes({{EvaluatedType::Integer}});

    FunctionSignature* lengthCell = createFunction("length");
    lengthCell->setArguments({EvaluatedType::ListItem});
    lengthCell->setReturnTypes({{EvaluatedType::Integer}});

    // range counts from its first bound to its second, both included, by the stride the
    // third gives - 1 where it is left out, and a negative one counting down. The list
    // holds integers whatever the bounds were, so the shape is the signature's to name.
    FunctionSignature* range = createFunction("range");
    range->setArguments({EvaluatedType::Integer, EvaluatedType::Integer, EvaluatedType::Integer});
    range->setRequiredArgCount(2);
    range->setReturnTypes({{EvaluatedType::List}});
    range->setReturnedListShape(ListShape(EvaluatedType::Integer, 1));

    // The hop count of a variable-length path
    FunctionSignature* sizePath = createFunction("size");
    sizePath->setArguments({edgeOrPath});
    sizePath->setReturnTypes({{EvaluatedType::Integer}});

    FunctionSignature* sizeGroup = createFunction("size");
    sizeGroup->setArguments({nodeOrGroup});
    sizeGroup->setReturnTypes({{EvaluatedType::Integer}});

    // The three reads of a named path: its hop count, the nodes it runs through and its
    // relationships, the two lists typed so that a comprehension over them binds an entity
    FunctionSignature* lengthPath = createFunction("length");
    lengthPath->setArguments({EvaluatedType::GraphPath});
    lengthPath->setReturnTypes({{EvaluatedType::Integer}});

    FunctionSignature* nodesPath = createFunction("nodes");
    nodesPath->setArguments({EvaluatedType::GraphPath});
    nodesPath->setReturnTypes({{EvaluatedType::List}});
    nodesPath->setReturnedListShape(ListShape(EvaluatedType::NodePattern, 1));

    FunctionSignature* relationshipsPath = createFunction("relationships");
    relationshipsPath->setArguments({EvaluatedType::GraphPath});
    relationshipsPath->setReturnTypes({{EvaluatedType::List}});
    relationshipsPath->setReturnedListShape(ListShape(EvaluatedType::EdgePattern, 1));

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

    FunctionSignature* toStringOfString = createFunction("toString");
    toStringOfString->setArguments({EvaluatedType::String});
    toStringOfString->setReturnTypes({{EvaluatedType::String}});

    FunctionSignature* toStringOfInteger = createFunction("toString");
    toStringOfInteger->setArguments({EvaluatedType::Integer});
    toStringOfInteger->setReturnTypes({{EvaluatedType::String}});

    FunctionSignature* toStringOfDouble = createFunction("toString");
    toStringOfDouble->setArguments({EvaluatedType::Double});
    toStringOfDouble->setReturnTypes({{EvaluatedType::String}});

    FunctionSignature* toStringOfBool = createFunction("toString");
    toStringOfBool->setArguments({EvaluatedType::Bool});
    toStringOfBool->setReturnTypes({{EvaluatedType::String}});

    FunctionSignature* toBoolean = createFunction("toBoolean");
    toBoolean->setArguments({EvaluatedType::String});
    toBoolean->setReturnTypes({{EvaluatedType::Bool}});

    // An argument is required on the two overloads that take one, so that datetime() picks
    // the nullary overload rather than matching these with nothing to convert.
    FunctionSignature* dateTimeOfString = createFunction("datetime");
    dateTimeOfString->setArguments({EvaluatedType::String});
    dateTimeOfString->setReturnTypes({{EvaluatedType::DateTime}});
    dateTimeOfString->setRequiredArgCount(1);

    FunctionSignature* dateTimeOfEpochSeconds = createFunction("datetime");
    dateTimeOfEpochSeconds->setArguments({EvaluatedType::Integer});
    dateTimeOfEpochSeconds->setReturnTypes({{EvaluatedType::DateTime}});
    dateTimeOfEpochSeconds->setRequiredArgCount(1);

    FunctionSignature* currentDateTime = createFunction("datetime");
    currentDateTime->setArguments({});
    currentDateTime->setReturnTypes({{EvaluatedType::DateTime}});

    FunctionSignature* durationOfMicroseconds = createFunction("duration");
    durationOfMicroseconds->setArguments({EvaluatedType::Integer});
    durationOfMicroseconds->setReturnTypes({{EvaluatedType::Duration}});

    FunctionSignature* durationOfMap = createFunction("duration");
    durationOfMap->setArguments({EvaluatedType::Map});
    durationOfMap->setReturnTypes({{EvaluatedType::Duration}});

    FunctionSignature* durationOfCell = createFunction("duration");
    durationOfCell->setArguments({EvaluatedType::ListItem});
    durationOfCell->setReturnTypes({{EvaluatedType::Duration}});

    // Numeric functions
    for (const EvaluatedType numberType : {EvaluatedType::Integer, EvaluatedType::Double, EvaluatedType::ListItem}) {
        FunctionSignature* abs = createFunction("abs");
        abs->setArguments({numberType});
        abs->setReturnTypes({{numberType}});

        FunctionSignature* sign = createFunction("sign");
        sign->setArguments({numberType});
        sign->setReturnTypes({{EvaluatedType::Integer}});

        const auto floatFunctionNames = {"ceil", "floor", "round", "sqrt", "exp", "log", "log10",
                                         "sin", "cos", "tan", "cot", "asin", "acos", "atan",
                                         "degrees", "radians", "haversin"};

        for (const std::string_view name : floatFunctionNames) {
            FunctionSignature* floatFunction = createFunction(name);
            floatFunction->setArguments({numberType});
            floatFunction->setReturnTypes({{EvaluatedType::Double}});
        }
    }

    FunctionSignature* pi = createFunction("pi");
    pi->setArguments({});
    pi->setReturnTypes({{EvaluatedType::Double}});

    FunctionSignature* e = createFunction("e");
    e->setArguments({});
    e->setReturnTypes({{EvaluatedType::Double}});

    // String functions
    for (const std::string_view name : {"toUpper", "toLower", "trim", "ltrim", "rtrim", "reverse"}) {
        FunctionSignature* stringFunction = createFunction(name);
        stringFunction->setArguments({EvaluatedType::String});
        stringFunction->setReturnTypes({{EvaluatedType::String}});
    }

    for (const std::string_view name : {"toUpper", "toLower", "trim", "ltrim", "rtrim"}) {
        FunctionSignature* stringFunctionOfCell = createFunction(name);
        stringFunctionOfCell->setArguments({EvaluatedType::ListItem});
        stringFunctionOfCell->setReturnTypes({{EvaluatedType::String}});
    }

    // A cell holds a string or a list, so what its reversal is is known per row
    FunctionSignature* reverseCell = createFunction("reverse");
    reverseCell->setArguments({EvaluatedType::ListItem});
    reverseCell->setReturnTypes({{EvaluatedType::ListItem}});

    const std::vector<FunctionArgumentType> stringOrCell = {EvaluatedType::String, EvaluatedType::ListItem};

    std::vector<FunctionArgumentType> nonNullIntegerOrCell;
    for (const EvaluatedType etype : {EvaluatedType::Integer, EvaluatedType::ListItem}) {
        FunctionArgumentType integer(etype);
        integer.setRejectsNull(true);
        nonNullIntegerOrCell.push_back(integer);
    }

    std::vector<FunctionSignature*> substrings;
    createOverloads("substring", {stringOrCell, nonNullIntegerOrCell, nonNullIntegerOrCell}, substrings);

    for (FunctionSignature* substring : substrings) {
        substring->setRequiredArgCount(2);
        substring->setReturnTypes({{EvaluatedType::String}});
    }

    // coalesce answers the first of its arguments that is not null, so it takes any number
    // of them and declares none: the analyzer unifies what it is given, and the type they
    // share is what the call returns.
    FunctionSignature* coalesce = createFunction("coalesce");
    coalesce->setReturnTypes({{EvaluatedType::Null}});
    coalesce->setRequiredArgCount(1);
    coalesce->setUnifiesItsArguments(true);

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

void FunctionDecls::createOverloads(std::string_view fullName,
                                    const std::vector<std::vector<FunctionArgumentType>>& positionTypes,
                                    std::vector<FunctionSignature*>& overloads) {
    size_t overloadCount = 1;
    for (const std::vector<FunctionArgumentType>& types : positionTypes) {
        overloadCount *= types.size();
    }

    for (size_t overloadIndex = 0; overloadIndex < overloadCount; overloadIndex++) {
        FunctionSignature::ArgumentTypes arguments;

        size_t remaining = overloadIndex;
        for (const std::vector<FunctionArgumentType>& types : positionTypes) {
            arguments.push_back(types[remaining % types.size()]);
            remaining /= types.size();
        }

        FunctionSignature* overload = createFunction(fullName);
        overload->setArguments(std::move(arguments));
        overloads.push_back(overload);
    }
}

FunctionResolver::FunctionSignatureRange FunctionDecls::lookup(std::string_view fullName) const {
    const auto it = _nameMap.find(fullName);
    if (it == _nameMap.end()) {
        return FunctionSignatureRange();
    }

    const FunctionSignatures& sigs = it->second;
    return FunctionSignatureRange(sigs.data(), sigs.data() + sigs.size());
}
