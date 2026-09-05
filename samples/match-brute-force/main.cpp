#include <stdlib.h>
#include <algorithm>
#include <map>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <argparse.hpp>
#include <spdlog/spdlog.h>

#include "TuringDB.h"
#include "TuringConfig.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"
#include "NLOutputSink.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "LocalMemory.h"
#include "reader/GraphReader.h"
#include "datapart/EdgeRecord.h"
#include "metadata/GraphMetadata.h"
#include "metadata/PropertyType.h"
#include "versioning/Transaction.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"
#include "ID.h"

#include "ToolInit.h"
#include "TuringException.h"

using namespace db;

namespace {

using Row = std::vector<std::string>;
using RowCounts = std::map<Row, size_t>;
using Properties = std::map<std::string, std::string, std::less<>>;

constexpr std::string_view graphName = "simpledb";

struct MatchNode {
    std::vector<std::string> _labels;
    Properties _properties;
};

struct MatchEdge {
    NodeID _source;
    NodeID _target;
    std::string _type;
    Properties _properties;
};

struct MatchGraph {
    std::vector<NodeID> _nodeIDs;
    std::vector<MatchNode> _nodes;
    std::vector<MatchEdge> _edges;
    std::vector<std::vector<EdgeID>> _outEdges;
    std::vector<std::vector<NodeID>> _outNeighbours;
};

struct Binding {
    NodeID _a;
    NodeID _b;
    NodeID _c;
    NodeID _d;
    NodeID _e;
    NodeID _f;
    NodeID _g;
    NodeID _h;
    NodeID _i;
    NodeID _j;
    NodeID _x;
    EdgeID _e1;
    EdgeID _e2;
};

using BoundNode = NodeID Binding::*;
using BoundEdge = EdgeID Binding::*;
using MatchFunction = void (*)(std::vector<Binding>&, const MatchGraph&);
using WherePredicate = bool (*)(const Binding&, const MatchGraph&);

struct ReturnItem {
    BoundNode _node {nullptr};
    BoundEdge _edge {nullptr};
    std::string_view _property;
};

struct SuiteCase {
    std::string_view _test;
    std::string_view _query;
    MatchFunction _match {nullptr};
    WherePredicate _where {nullptr};
    std::span<const ReturnItem> _returnItems;
    size_t _limit {0};
};

void valueText(std::string& text, NodeID id) {
    text = std::to_string(id.getValue());
}

void valueText(std::string& text, std::string_view value) {
    text = std::string(value);
}

void valueText(std::string& text, int64_t value) {
    text = std::to_string(value);
}

void valueText(std::string& text, CustomBool value) {
    text = value ? "true" : "false";
}

template <typename T>
void valueText(std::string& text, const std::optional<T>& value) {
    if (value) {
        valueText(text, *value);
    } else {
        text = "null";
    }
}

class TextRowSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override;

    const std::vector<Row>& getRows() const { return _rows; }

private:
    std::vector<Row> _rows;

    template <typename T>
    static bool readCell(std::string& text, const Column* chunk, size_t rowIndex);
    static void cellText(std::string& text, const Column* chunk, size_t rowIndex);
};

void TextRowSink::appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) {
    for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
        Row& row = _rows.emplace_back();
        for (const Column* chunk : chunks) {
            cellText(row.emplace_back(), chunk, rowIndex);
        }
    }
}

template <typename T>
bool TextRowSink::readCell(std::string& text, const Column* chunk, size_t rowIndex) {
    if (chunk->getKind() != ColumnVector<T>::staticKind()) {
        return false;
    }

    const ColumnVector<T>* column = static_cast<const ColumnVector<T>*>(chunk);
    valueText(text, column->getRaw()[rowIndex]);
    return true;
}

void TextRowSink::cellText(std::string& text, const Column* chunk, size_t rowIndex) {
    const bool read = readCell<NodeID>(text, chunk, rowIndex)
                   || readCell<std::string_view>(text, chunk, rowIndex)
                   || readCell<std::optional<std::string_view>>(text, chunk, rowIndex)
                   || readCell<int64_t>(text, chunk, rowIndex)
                   || readCell<std::optional<int64_t>>(text, chunk, rowIndex)
                   || readCell<CustomBool>(text, chunk, rowIndex)
                   || readCell<std::optional<CustomBool>>(text, chunk, rowIndex);

    if (!read) {
        throw TuringException("The sample cannot read a column of type " + std::string(chunk->getTypeName()));
    }
}

template <typename T>
void readNodeProperties(MatchGraph& graph, const GraphReader& reader, PropertyTypeID typeID, std::string_view name) {
    for (auto it = reader.scanNodeProperties<T>(typeID).begin(); it.isValid(); ++it) {
        MatchNode& node = graph._nodes[it.getCurrentNodeID().getValue()];
        valueText(node._properties[std::string(name)], it.get());
    }
}

template <typename T>
void readEdgeProperties(MatchGraph& graph, const GraphReader& reader, PropertyTypeID typeID, std::string_view name) {
    for (auto it = reader.scanEdgeProperties<T>(typeID).begin(); it.isValid(); ++it) {
        MatchEdge& edge = graph._edges[it.getCurrentEdgeID().getValue()];
        valueText(edge._properties[std::string(name)], it.get());
    }
}

template <typename T>
void readProperties(MatchGraph& graph, const GraphReader& reader, PropertyTypeID typeID, std::string_view name) {
    readNodeProperties<T>(graph, reader, typeID, name);
    readEdgeProperties<T>(graph, reader, typeID, name);
}

void readMatchGraph(MatchGraph& matchGraph, Graph* graph) {
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphView view = transaction.viewGraph();
    const GraphReader reader = transaction.readGraph();
    const GraphMetadata& metadata = view.metadata();

    matchGraph._nodeIDs.clear();
    uint64_t maxNodeID = 0;
    for (auto it = reader.scanNodes().begin(); it.isValid(); it.next()) {
        const NodeID node = it.get();
        matchGraph._nodeIDs.push_back(node);
        maxNodeID = std::max(maxNodeID, node.getValue());
    }

    matchGraph._nodes.assign(maxNodeID + 1, {});
    matchGraph._edges.assign(reader.getEdgeCount(), {});
    matchGraph._outEdges.assign(maxNodeID + 1, {});
    matchGraph._outNeighbours.assign(maxNodeID + 1, {});

    std::vector<LabelID> labelIDs;
    for (const NodeID node : matchGraph._nodeIDs) {
        MatchNode& matchNode = matchGraph._nodes[node.getValue()];

        labelIDs.clear();
        reader.getNodeLabelSet(node).decompose(labelIDs);
        for (const LabelID labelID : labelIDs) {
            matchNode._labels.emplace_back(metadata.labels().getName(labelID).value());
        }

        const ColumnNodeIDs source {node};
        for (const EdgeRecord& record : reader.getOutEdges(&source)) {
            MatchEdge& edge = matchGraph._edges[record._edgeID.getValue()];
            edge._source = node;
            edge._target = record._otherID;
            edge._type = std::string(metadata.edgeTypes().getName(record._edgeTypeID).value());

            matchGraph._outEdges[node.getValue()].push_back(record._edgeID);
            matchGraph._outNeighbours[node.getValue()].push_back(record._otherID);
        }
    }

    for (const PropertyTypeMap::Pair& pair : metadata.propTypes()) {
        const PropertyType& type = pair._pt;
        const std::string_view name = *pair._name;

        switch (type._valueType) {
            case ValueType::Int64:
                readProperties<types::Int64>(matchGraph, reader, type._id, name);
            break;
            case ValueType::String:
                readProperties<types::String>(matchGraph, reader, type._id, name);
            break;
            case ValueType::Bool:
                readProperties<types::Bool>(matchGraph, reader, type._id, name);
            break;
            default:
                throw TuringException("The sample cannot read properties of type " + std::string(ValueTypeName::value(type._valueType)));
            break;
        }
    }
}

const std::vector<NodeID>& outOf(const MatchGraph& graph, NodeID node) {
    return graph._outNeighbours[node.getValue()];
}

const std::vector<EdgeID>& outEdgesOf(const MatchGraph& graph, NodeID node) {
    return graph._outEdges[node.getValue()];
}

const MatchEdge& edgeOf(const MatchGraph& graph, EdgeID edge) {
    return graph._edges[edge.getValue()];
}

bool hasEdge(const MatchGraph& graph, NodeID source, NodeID target) {
    const std::vector<NodeID>& targets = outOf(graph, source);
    return std::find(targets.begin(), targets.end(), target) != targets.end();
}

bool hasLabel(const MatchGraph& graph, NodeID node, std::string_view label) {
    const std::vector<std::string>& labels = graph._nodes[node.getValue()]._labels;
    return std::find(labels.begin(), labels.end(), label) != labels.end();
}

const std::string* propertyOf(const Properties& properties, std::string_view name) {
    const auto it = properties.find(name);
    return it == properties.end() ? nullptr : &it->second;
}

const std::string* propertyOf(const MatchGraph& graph, NodeID node, std::string_view name) {
    return propertyOf(graph._nodes[node.getValue()]._properties, name);
}

const std::string* propertyOf(const MatchGraph& graph, EdgeID edge, std::string_view name) {
    return propertyOf(edgeOf(graph, edge)._properties, name);
}

// A comparison with a null property is null in Cypher, which a WHERE treats as false.
bool sameProperty(const std::string* left, const std::string* right) {
    return left && right && *left == *right;
}

bool differentProperty(const std::string* left, const std::string* right) {
    return left && right && *left != *right;
}

bool sameProperty(const MatchGraph& graph, NodeID left, NodeID right, std::string_view name) {
    return sameProperty(propertyOf(graph, left, name), propertyOf(graph, right, name));
}

bool differentProperty(const MatchGraph& graph, NodeID left, NodeID right, std::string_view name) {
    return differentProperty(propertyOf(graph, left, name), propertyOf(graph, right, name));
}

bool sameProperty(const MatchGraph& graph, EdgeID left, EdgeID right, std::string_view name) {
    return sameProperty(propertyOf(graph, left, name), propertyOf(graph, right, name));
}

// Extends every binding with one more node having an edge into its x, for a further (v)-->(x) pattern.
void addNodeIntoX(std::vector<Binding>& bindings, const MatchGraph& graph, BoundNode variable) {
    std::vector<Binding> partials;
    partials.swap(bindings);

    for (const Binding& partial : partials) {
        for (const NodeID node : graph._nodeIDs) {
            if (hasEdge(graph, node, partial._x)) {
                Binding binding = partial;
                binding.*variable = node;
                bindings.push_back(binding);
            }
        }
    }
}

// Extends every binding with one more Person, for a further (v:Person) pattern.
void addPerson(std::vector<Binding>& bindings, const MatchGraph& graph, BoundNode variable) {
    std::vector<Binding> partials;
    partials.swap(bindings);

    for (const Binding& partial : partials) {
        for (const NodeID node : graph._nodeIDs) {
            if (hasLabel(graph, node, "Person")) {
                Binding binding = partial;
                binding.*variable = node;
                bindings.push_back(binding);
            }
        }
    }
}

// Extends every binding with a (person:Person)-[edge:edgeType]->(target) hop; an empty edgeType
// accepts every edge type.
void addPersonHop(std::vector<Binding>& bindings,
                  const MatchGraph& graph,
                  BoundNode person,
                  BoundEdge edge,
                  BoundNode target,
                  std::string_view edgeType) {
    std::vector<Binding> partials;
    partials.swap(bindings);

    for (const Binding& partial : partials) {
        for (const NodeID node : graph._nodeIDs) {
            if (!hasLabel(graph, node, "Person")) {
                continue;
            }

            for (const EdgeID edgeID : outEdgesOf(graph, node)) {
                const MatchEdge& matchEdge = edgeOf(graph, edgeID);
                if (!edgeType.empty() && matchEdge._type != edgeType) {
                    continue;
                }

                Binding binding = partial;
                binding.*person = node;
                binding.*edge = edgeID;
                binding.*target = matchEdge._target;
                bindings.push_back(binding);
            }
        }
    }
}

void matchTwoIntoX(std::vector<Binding>& bindings, const MatchGraph& graph) {
    bindings.clear();

    for (const NodeID a : graph._nodeIDs) {
        for (const NodeID x : outOf(graph, a)) {
            Binding binding;
            binding._a = a;
            binding._x = x;
            bindings.push_back(binding);
        }
    }

    addNodeIntoX(bindings, graph, &Binding::_b);
}

void matchThreeIntoX(std::vector<Binding>& bindings, const MatchGraph& graph) {
    matchTwoIntoX(bindings, graph);
    addNodeIntoX(bindings, graph, &Binding::_c);
}

void matchFourIntoX(std::vector<Binding>& bindings, const MatchGraph& graph) {
    matchThreeIntoX(bindings, graph);
    addNodeIntoX(bindings, graph, &Binding::_d);
}

void matchFiveIntoX(std::vector<Binding>& bindings, const MatchGraph& graph) {
    matchFourIntoX(bindings, graph);
    addNodeIntoX(bindings, graph, &Binding::_e);
}

void matchTwoIntoXTwoOutToE(std::vector<Binding>& bindings, const MatchGraph& graph) {
    std::vector<Binding> intoX;
    matchTwoIntoX(intoX, graph);

    bindings.clear();
    for (const Binding& partial : intoX) {
        for (const NodeID c : outOf(graph, partial._x)) {
            for (const NodeID e : outOf(graph, c)) {
                for (const NodeID d : outOf(graph, partial._x)) {
                    if (hasEdge(graph, d, e)) {
                        Binding binding = partial;
                        binding._c = c;
                        binding._d = d;
                        binding._e = e;
                        bindings.push_back(binding);
                    }
                }
            }
        }
    }
}

void matchTwoHopWalk(std::vector<Binding>& bindings, const MatchGraph& graph) {
    bindings.clear();

    for (const NodeID b : graph._nodeIDs) {
        for (const NodeID c : outOf(graph, b)) {
            for (const NodeID a : graph._nodeIDs) {
                if (hasEdge(graph, a, b)) {
                    Binding binding;
                    binding._a = a;
                    binding._b = b;
                    binding._c = c;
                    bindings.push_back(binding);
                }
            }
        }
    }
}

void matchDiamond(std::vector<Binding>& bindings, const MatchGraph& graph) {
    bindings.clear();

    for (const NodeID a : graph._nodeIDs) {
        for (const NodeID b : outOf(graph, a)) {
            for (const NodeID c : graph._nodeIDs) {
                for (const NodeID d : outOf(graph, c)) {
                    for (const NodeID e : outOf(graph, d)) {
                        for (const NodeID f : outOf(graph, a)) {
                            for (const NodeID g : outOf(graph, f)) {
                                if (hasEdge(graph, c, g)) {
                                    bindings.push_back({a, b, c, d, e, f, g});
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

// The repeated (e) and (h) patterns name variables the diamond already binds, so they add no loop.
void matchDoubleDiamond(std::vector<Binding>& bindings, const MatchGraph& graph) {
    std::vector<Binding> diamonds;
    matchDiamond(diamonds, graph);

    bindings.clear();
    for (const Binding& diamond : diamonds) {
        for (const NodeID h : graph._nodeIDs) {
            for (const NodeID i : graph._nodeIDs) {
                for (const NodeID j : outOf(graph, diamond._c)) {
                    Binding binding = diamond;
                    binding._h = h;
                    binding._i = i;
                    binding._j = j;
                    bindings.push_back(binding);
                }
            }
        }
    }
}

void matchTwoPersons(std::vector<Binding>& bindings, const MatchGraph& graph) {
    bindings.assign(1, Binding {});
    addPerson(bindings, graph, &Binding::_a);
    addPerson(bindings, graph, &Binding::_b);
}

void matchPersonInterestAndPerson(std::vector<Binding>& bindings, const MatchGraph& graph) {
    bindings.assign(1, Binding {});
    addPersonHop(bindings, graph, &Binding::_a, &Binding::_e1, &Binding::_b, "INTERESTED_IN");
    std::erase_if(bindings, [&](const Binding& binding) { return !hasLabel(graph, binding._b, "Interest"); });
    addPerson(bindings, graph, &Binding::_c);
}

void matchTwoPersonHops(std::vector<Binding>& bindings, const MatchGraph& graph) {
    bindings.assign(1, Binding {});
    addPersonHop(bindings, graph, &Binding::_a, &Binding::_e1, &Binding::_b, "");
    addPersonHop(bindings, graph, &Binding::_c, &Binding::_e2, &Binding::_d, "");
}

void matchTwoPersonInterests(std::vector<Binding>& bindings, const MatchGraph& graph) {
    bindings.assign(1, Binding {});
    addPersonHop(bindings, graph, &Binding::_a, &Binding::_e1, &Binding::_b, "INTERESTED_IN");
    addPersonHop(bindings, graph, &Binding::_c, &Binding::_e2, &Binding::_d, "INTERESTED_IN");
}

bool namesPairUp(const Binding& binding, const MatchGraph& graph) {
    const bool aIsB = sameProperty(graph, binding._a, binding._b, "name");
    const bool bIsC = sameProperty(graph, binding._b, binding._c, "name");
    return aIsB || bIsC;
}

bool namesPairUpOrBIsX(const Binding& binding, const MatchGraph& graph) {
    const bool bIsX = sameProperty(graph, binding._b, binding._x, "name");
    return namesPairUp(binding, graph) || bIsX;
}

bool aNamedLikeC(const Binding& binding, const MatchGraph& graph) {
    return sameProperty(graph, binding._a, binding._c, "name");
}

bool samePhD(const Binding& binding, const MatchGraph& graph) {
    return sameProperty(graph, binding._a, binding._b, "hasPhD");
}

bool sameFrenchness(const Binding& binding, const MatchGraph& graph) {
    return sameProperty(graph, binding._a, binding._c, "isFrench");
}

bool sameDuration(const Binding& binding, const MatchGraph& graph) {
    return sameProperty(graph, binding._e1, binding._e2, "duration");
}

bool sameAge(const Binding& binding, const MatchGraph& graph) {
    return sameProperty(graph, binding._a, binding._b, "age");
}

bool sameInterestOfDifferentPeople(const Binding& binding, const MatchGraph& graph) {
    const bool sameInterest = sameProperty(graph, binding._b, binding._d, "name");
    const bool differentPeople = differentProperty(graph, binding._a, binding._c, "name");
    return sameInterest && differentPeople;
}

void filter(std::vector<Binding>& bindings, const MatchGraph& graph, WherePredicate where) {
    std::erase_if(bindings, [&](const Binding& binding) { return !where(binding, graph); });
}

void returnText(std::string& text, const Binding& binding, const ReturnItem& item, const MatchGraph& graph) {
    if (item._edge) {
        const std::string* value = propertyOf(graph, binding.*item._edge, item._property);
        text = value ? *value : "null";
    } else if (item._property.empty()) {
        valueText(text, binding.*item._node);
    } else {
        const std::string* value = propertyOf(graph, binding.*item._node, item._property);
        text = value ? *value : "null";
    }
}

void project(RowCounts& rows, std::span<const Binding> bindings, std::span<const ReturnItem> returnItems, const MatchGraph& graph) {
    rows.clear();

    Row row;
    for (const Binding& binding : bindings) {
        row.clear();
        for (const ReturnItem& item : returnItems) {
            returnText(row.emplace_back(), binding, item, graph);
        }
        rows[row]++;
    }
}

void countRows(RowCounts& counts, std::span<const Row> rows) {
    counts.clear();
    for (const Row& row : rows) {
        counts[row]++;
    }
}

size_t totalRows(const RowCounts& counts) {
    size_t total = 0;
    for (const auto& [row, count] : counts) {
        total += count;
    }
    return total;
}

void formatRow(std::string& text, const Row& row) {
    text.clear();
    for (const std::string& value : row) {
        if (!text.empty()) {
            text += ",";
        }
        text += value;
    }
}

bool runV3(std::vector<Row>& rows, QueryInterpreterV3& interpreter, LocalMemory& memory, std::string_view query) {
    TextRowSink sink;
    QueryStatus status;
    interpreter.execute(status, query, graphName, CommitHash::head(), ChangeID::head(), &memory, &sink);
    memory.clear();

    if (!status.isOk()) {
        spdlog::error("v3 failed on '{}': {}", query, status.getError());
        return false;
    }

    rows = sink.getRows();
    return true;
}

bool compareRows(const RowCounts& bruteForce, const RowCounts& v3) {
    spdlog::info("  brute force: {} rows, {} distinct", totalRows(bruteForce), bruteForce.size());
    spdlog::info("  v3:          {} rows, {} distinct", totalRows(v3), v3.size());

    bool matched = true;
    std::string text;

    for (const auto& [row, count] : bruteForce) {
        const auto it = v3.find(row);
        const size_t v3Count = it == v3.end() ? 0 : it->second;
        if (v3Count != count) {
            formatRow(text, row);
            spdlog::error("  row [{}]: brute force {} times, v3 {} times", text, count, v3Count);
            matched = false;
        }
    }

    for (const auto& [row, count] : v3) {
        if (!bruteForce.contains(row)) {
            formatRow(text, row);
            spdlog::error("  row [{}]: brute force 0 times, v3 {} times", text, count);
            matched = false;
        }
    }

    spdlog::info("  {}", matched ? "identical" : "DIFFERENT");
    return matched;
}

bool checkLimitedRows(const RowCounts& bruteForce, std::span<const Row> v3Rows, size_t limit) {
    const size_t expectedCount = std::min(limit, totalRows(bruteForce));
    bool matched = v3Rows.size() == expectedCount;
    if (!matched) {
        spdlog::error("  v3 returned {} rows, the limit admits {}", v3Rows.size(), expectedCount);
    }

    std::string text;
    for (const Row& row : v3Rows) {
        if (!bruteForce.contains(row)) {
            formatRow(text, row);
            spdlog::error("  row [{}] is not a match of the pattern", text);
            matched = false;
        }
    }

    spdlog::info("  {}", matched ? "every row is a match" : "DIFFERENT");
    return matched;
}

constexpr ReturnItem returnID(BoundNode node) {
    return {node, nullptr, {}};
}

constexpr ReturnItem returnProperty(BoundNode node, std::string_view property) {
    return {node, nullptr, property};
}

constexpr ReturnItem returnEdgeProperty(BoundEdge edge, std::string_view property) {
    return {nullptr, edge, property};
}

constexpr ReturnItem returnA[] = {returnID(&Binding::_a)};
constexpr ReturnItem returnABC[] = {returnID(&Binding::_a), returnID(&Binding::_b), returnID(&Binding::_c)};
constexpr ReturnItem returnACEG[] = {returnID(&Binding::_a),
                                     returnID(&Binding::_c),
                                     returnID(&Binding::_e),
                                     returnID(&Binding::_g)};
constexpr ReturnItem returnNamesABX[] = {returnProperty(&Binding::_a, "name"),
                                         returnProperty(&Binding::_b, "name"),
                                         returnProperty(&Binding::_x, "name")};
constexpr ReturnItem returnNamesABC[] = {returnProperty(&Binding::_a, "name"),
                                         returnProperty(&Binding::_b, "name"),
                                         returnProperty(&Binding::_c, "name")};
constexpr ReturnItem returnNamesACB[] = {returnProperty(&Binding::_a, "name"),
                                         returnProperty(&Binding::_c, "name"),
                                         returnProperty(&Binding::_b, "name")};
constexpr ReturnItem returnNamesABCX[] = {returnProperty(&Binding::_a, "name"),
                                          returnProperty(&Binding::_b, "name"),
                                          returnProperty(&Binding::_c, "name"),
                                          returnProperty(&Binding::_x, "name")};
constexpr ReturnItem returnNamesABCDX[] = {returnProperty(&Binding::_a, "name"),
                                           returnProperty(&Binding::_b, "name"),
                                           returnProperty(&Binding::_c, "name"),
                                           returnProperty(&Binding::_d, "name"),
                                           returnProperty(&Binding::_x, "name")};
constexpr ReturnItem returnNamesABCDEX[] = {returnProperty(&Binding::_a, "name"),
                                            returnProperty(&Binding::_b, "name"),
                                            returnProperty(&Binding::_c, "name"),
                                            returnProperty(&Binding::_d, "name"),
                                            returnProperty(&Binding::_e, "name"),
                                            returnProperty(&Binding::_x, "name")};
constexpr ReturnItem returnNamesABAndPhDs[] = {returnProperty(&Binding::_a, "name"),
                                               returnProperty(&Binding::_b, "name"),
                                               returnProperty(&Binding::_a, "hasPhD"),
                                               returnProperty(&Binding::_b, "hasPhD")};
constexpr ReturnItem returnNamesABCAndFrenchness[] = {returnProperty(&Binding::_a, "name"),
                                                      returnProperty(&Binding::_b, "name"),
                                                      returnProperty(&Binding::_c, "name"),
                                                      returnProperty(&Binding::_a, "isFrench")};
constexpr ReturnItem returnNamesABCDAndDuration[] = {returnProperty(&Binding::_a, "name"),
                                                     returnProperty(&Binding::_b, "name"),
                                                     returnProperty(&Binding::_c, "name"),
                                                     returnProperty(&Binding::_d, "name"),
                                                     returnEdgeProperty(&Binding::_e1, "duration")};
constexpr ReturnItem returnNamesABAndAge[] = {returnProperty(&Binding::_a, "name"),
                                              returnProperty(&Binding::_b, "name"),
                                              returnProperty(&Binding::_a, "age")};

constexpr SuiteCase suiteCases[] = {
    {"success-reads-joins-on-filters-0",
     "MATCH (a)-->(x), (b)-->(x) RETURN a.name, b.name, x.name",
     matchTwoIntoX, nullptr, returnNamesABX},

    {"success-reads-joins-on-filters-2",
     "MATCH (a)-->(x), (b)-->(x), (c)-->(x), (d)-->(x) RETURN a.name, b.name, c.name, d.name, x.name",
     matchFourIntoX, nullptr, returnNamesABCDX},

    {"success-reads-joins-on-filters-3",
     "MATCH (a)-->(x), (b)-->(x), (c)-->(x), (d)-->(x) RETURN a.name, b.name, c.name, d.name, x.name",
     matchFourIntoX, nullptr, returnNamesABCDX},

    {"success-reads-joins-on-filters-4",
     "MATCH (a)-->(x), (b)-->(x), (c)-->(x), (d)-->(x), (e)-->(x) RETURN a.name, b.name, c.name, d.name, e.name, x.name",
     matchFiveIntoX, nullptr, returnNamesABCDEX},

    {"success-reads-joins-on-filters-5",
     "MATCH (a)-->(x), (b)-->(x), (x)-->(c)-->(e), (x)-->(d)-->(e) RETURN a",
     matchTwoIntoXTwoOutToE, nullptr, returnA},

    {"success-reads-match-8",
     "MATCH (b)-->(c), (a)-->(b) RETURN a, b, c;",
     matchTwoHopWalk, nullptr, returnABC},

    {"success-reads-match-9",
     "MATCH (b)-->(c), (a)-->(b)-->(c) RETURN a, b, c;",
     matchTwoHopWalk, nullptr, returnABC},

    {"success-reads-match-where-2",
     "MATCH (a)-->(x), (b)-->(x), (c)-->(x) WHERE (a.name = b.name) OR (b.name = c.name) RETURN a.name, b.name, c.name, x.name",
     matchThreeIntoX, namesPairUp, returnNamesABCX},

    {"success-reads-match-where-3",
     "MATCH (a)-->(x), (b)-->(x), (c)-->(x) WHERE (a.name = b.name) OR (b.name = c.name) OR (b.name = x.name) RETURN a.name, b.name, c.name",
     matchThreeIntoX, namesPairUpOrBIsX, returnNamesABC},

    {"success-reads-constraint-0",
     "MATCH (b)-->(c), (a { name: c.name })-->(b) RETURN a, b, c;",
     matchTwoHopWalk, aNamedLikeC, returnABC},

    {"value-hash-join-where-0",
     "MATCH (a:Person), (b:Person) WHERE a.hasPhD = b.hasPhD RETURN a.name, b.name, a.hasPhD, b.hasPhD",
     matchTwoPersons, samePhD, returnNamesABAndPhDs},

    {"value-hash-join-where-1",
     "MATCH (a:Person)-[:INTERESTED_IN]->(b:Interest), (c:Person) WHERE a.isFrench = c.isFrench RETURN a.name, b.name, c.name, a.isFrench",
     matchPersonInterestAndPerson, sameFrenchness, returnNamesABCAndFrenchness},

    {"value-hash-join-where-2",
     "MATCH (a:Person)-[e1]->(b), (c:Person)-[e2]->(d) WHERE e1.duration = e2.duration RETURN a.name, b.name, c.name, d.name, e1.duration",
     matchTwoPersonHops, sameDuration, returnNamesABCDAndDuration},

    {"value-hash-join-where-3",
     "MATCH (a:Person), (b:Person) WHERE a.age = b.age RETURN a.name, b.name, a.age",
     matchTwoPersons, sameAge, returnNamesABAndAge},

    {"value-hash-join-where-4",
     "MATCH (a:Person)-[:INTERESTED_IN]->(b) MATCH (c:Person)-[:INTERESTED_IN]->(d) WHERE b.name = d.name AND a.name <> c.name RETURN a.name, c.name, b.name",
     matchTwoPersonInterests, sameInterestOfDifferentPeople, returnNamesACB},

    {"join-bad-keys-error",
     "MATCH (a)-->(b),(c)-->(d)-->(e),(a)-->(f)-->(g),(c)-->(g) RETURN a",
     matchDiamond, nullptr, returnA},

    {"large-double-diamond-join without its LIMIT",
     "MATCH (a)-->(b),(c)-->(d)-->(e),(a)-->(f)-->(g),(c)-->(g),(h),(i),(e),(h),(c)-->(j) RETURN a,c,e,g",
     matchDoubleDiamond, nullptr, returnACEG},

    {"large-double-diamond-join",
     "MATCH (a)-->(b),(c)-->(d)-->(e),(a)-->(f)-->(g),(c)-->(g),(h),(i),(e),(h),(c)-->(j) RETURN a,c,e,g LIMIT 10",
     matchDoubleDiamond, nullptr, returnACEG, 10},
};

}

int main(int argc, const char** argv) {
    ToolInit toolInit("match-brute-force");

    auto& argParser = toolInit.getArgParser();

    std::string turingDirArg;
    argParser.add_argument("-turing-dir")
             .metavar("path")
             .store_into(turingDirArg)
             .help("Root Turing directory (defaults to SAMPLE_DIR/.turing)");

    toolInit.init(argc, argv);

    fs::Path turingDir;
    if (!turingDirArg.empty()) {
        turingDir = fs::Path(turingDirArg);
        if (!turingDir.toAbsolute()) {
            spdlog::error("Failed to get absolute path of turing directory {}", turingDirArg);
            return EXIT_FAILURE;
        }
    } else {
        turingDir = fs::Path(SAMPLE_DIR) / ".turing";
    }

    TuringConfig config;
    config.setTuringDirectory(turingDir);
    config.setSyncedOnDisk(false);

    TuringDB db(&config);
    db.init();

    Graph* graph = nullptr;
    {
        SystemAccessor system = db.getSystemManager().accessUnique();
        graph = system.createGraph(graphName);
    }
    SimpleGraph::createSimpleGraph(graph);

    MatchGraph matchGraph;
    readMatchGraph(matchGraph, graph);

    LocalMemory memory;
    QueryInterpreterV3 interpreter(&db.getSystemManager());

    std::vector<Binding> bindings;
    RowCounts bruteForce;
    RowCounts v3;
    std::vector<Row> rows;
    bool allMatched = true;

    for (const SuiteCase& suiteCase : suiteCases) {
        suiteCase._match(bindings, matchGraph);
        if (suiteCase._where) {
            filter(bindings, matchGraph, suiteCase._where);
        }
        project(bruteForce, bindings, suiteCase._returnItems, matchGraph);

        if (!runV3(rows, interpreter, memory, suiteCase._query)) {
            return EXIT_FAILURE;
        }

        spdlog::info("{}", suiteCase._test);
        spdlog::info("  {}", suiteCase._query);

        if (suiteCase._limit == 0) {
            countRows(v3, rows);
            allMatched = compareRows(bruteForce, v3) && allMatched;
        } else {
            allMatched = checkLimitedRows(bruteForce, rows, suiteCase._limit) && allMatched;
        }
    }

    return allMatched ? EXIT_SUCCESS : EXIT_FAILURE;
}
