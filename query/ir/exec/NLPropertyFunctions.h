#pragma once

#include <optional>
#include <string_view>
#include <vector>

#include "columns/Functions.h"
#include "list/ListBuffer.h"
#include "list/ListElementView.h"
#include "map/MapBuffer.h"
#include "map/MapEntryView.h"
#include "map/MapView.h"

#include "NLEntityProperties.h"

namespace db {

class LocalMemory;
class NLExecutionContext;

template <TypedInternalID IDT>
class NLKeysFunction {
public:
    using ArgType = IDT;
    using ResultType = ListView;

    NLKeysFunction(NLExecutionContext* context, LocalMemory* memory);
    ~NLKeysFunction();

    ResultType operator()(IDT entity);

private:
    NLEntityProperties _properties;
    QueryListBuffer* _listBuffer {nullptr};
    std::vector<std::string_view> _keys;
    std::vector<QueryListBuffer::ListItemVariant> _elements;
};

template <TypedInternalID IDT>
class NLPropertiesFunction {
public:
    using ArgType = IDT;
    using ResultType = MapView;

    NLPropertiesFunction(NLExecutionContext* context, LocalMemory* memory);
    ~NLPropertiesFunction();

    ResultType operator()(IDT entity);

private:
    NLEntityProperties _properties;
    MapBuffer<>* _mapBuffer {nullptr};
    std::vector<NLEntityProperties::Entry> _entries;
};

// The two over a value that carries its own type tag: an element of a list, which is what
// an UNWIND binds, or a value read out of a map. One holding a null answers null, one
// holding neither an entity nor a map is the row's type error
template <typename Cell>
class NLTaggedKeysFunction {
public:
    using ArgType = Cell;
    using ResultType = std::optional<ListView>;

    NLTaggedKeysFunction(NLExecutionContext* context, LocalMemory* memory);
    ~NLTaggedKeysFunction();

    ResultType operator()(Cell cell);
    ResultType operator()(const std::optional<Cell>& cell);

private:
    NLKeysFunction<NodeID> _nodeKeys;
    NLKeysFunction<EdgeID> _edgeKeys;
    MapKeysFunction _mapKeys;
};

template <typename Cell>
class NLTaggedPropertiesFunction {
public:
    using ArgType = Cell;
    using ResultType = std::optional<MapView>;

    NLTaggedPropertiesFunction(NLExecutionContext* context, LocalMemory* memory);
    ~NLTaggedPropertiesFunction();

    ResultType operator()(Cell cell);
    ResultType operator()(const std::optional<Cell>& cell);

private:
    NLPropertiesFunction<NodeID> _nodeProperties;
    NLPropertiesFunction<EdgeID> _edgeProperties;
};

}
