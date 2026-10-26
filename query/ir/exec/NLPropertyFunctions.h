#pragma once

#include <optional>
#include <string_view>
#include <vector>

#include "columns/Functions.h"
#include "list/ListBuffer.h"
#include "CellView.h"
#include "map/MapBuffer.h"
#include "map/MapView.h"

#include "NLEntityProperties.h"

namespace db {

class NLExecutionContext;
class NLStepBuffers;

template <TypedInternalID IDT>
class NLKeysFunction {
public:
    using ArgType = IDT;
    using ResultType = ListView;

    NLKeysFunction(NLExecutionContext* context, NLStepBuffers* memory);
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

    NLPropertiesFunction(NLExecutionContext* context, NLStepBuffers* memory);
    ~NLPropertiesFunction();

    ResultType operator()(IDT entity);

private:
    NLEntityProperties _properties;
    MapBuffer<>* _mapBuffer {nullptr};
    std::vector<NLEntityProperties::Entry> _entries;
};

// The four over a value that carries its own type tag: an element of a list, which is what
// an UNWIND binds, or a value read out of a map. One holding a null answers null, one
// holding neither an entity nor a map is the row's type error
class NLTaggedKeysFunction {
public:
    using ArgType = CellView;
    using ResultType = std::optional<ListView>;

    NLTaggedKeysFunction(NLExecutionContext* context, NLStepBuffers* memory);
    ~NLTaggedKeysFunction();

    ResultType operator()(CellView cell);
    ResultType operator()(const std::optional<CellView>& cell);

private:
    NLKeysFunction<NodeID> _nodeKeys;
    NLKeysFunction<EdgeID> _edgeKeys;
    MapKeysFunction _mapKeys;
};

class NLTaggedLabelsFunction {
public:
    using ArgType = CellView;
    using ResultType = std::optional<ListView>;

    NLTaggedLabelsFunction(NLExecutionContext* context, NLStepBuffers* memory);
    ~NLTaggedLabelsFunction();

    ResultType operator()(CellView cell);
    ResultType operator()(const std::optional<CellView>& cell);

private:
    LabelsFunction _labels;
};

class NLTaggedEdgeTypeFunction {
public:
    using ArgType = CellView;
    using ResultType = std::optional<types::String::Primitive>;

    NLTaggedEdgeTypeFunction(NLExecutionContext* context, NLStepBuffers* memory);
    ~NLTaggedEdgeTypeFunction();

    ResultType operator()(CellView cell);
    ResultType operator()(const std::optional<CellView>& cell);

private:
    EdgeTypesFunction _edgeType;
};

class NLTaggedPropertiesFunction {
public:
    using ArgType = CellView;
    using ResultType = std::optional<MapView>;

    NLTaggedPropertiesFunction(NLExecutionContext* context, NLStepBuffers* memory);
    ~NLTaggedPropertiesFunction();

    ResultType operator()(CellView cell);
    ResultType operator()(const std::optional<CellView>& cell);

private:
    NLPropertiesFunction<NodeID> _nodeProperties;
    NLPropertiesFunction<EdgeID> _edgeProperties;
};

}
