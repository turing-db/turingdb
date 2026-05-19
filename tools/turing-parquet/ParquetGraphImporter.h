#pragma once

#include <stddef.h>

#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>

#include "ID.h"
#include "metadata/PropertyType.h"
#include "writers/GraphWriter.h"

#include <nlohmann/json_fwd.hpp>

namespace fs {
class Path;
}

namespace db {

class Graph;
class JobSystem;
class ParquetGraphLabel;
class ParquetGraphMapping;
class ParquetGraphProperty;
class ParquetSchema;

// Loads parquet node/edge files into an embedded TuringDB graph, using the
// caller's inferred ParquetGraphMapping to drive sub-record expansion of JSON
// object/array entries inside the configured property column.
//
// Usage:
//   GraphWriter is owned internally; the caller owns the Graph, JobSystem,
//   and ParquetGraphMapping references. Call importNodeFile() once per
//   -nodes file, then importEdgeFile() once per -edges file, then finalize().
//   The external `id` column from each node row is recorded as a String
//   property on the resulting node and as a key in the in-memory id→NodeID
//   map used to resolve edge from/to references.
class ParquetGraphImporter {
public:
    ParquetGraphImporter(Graph* graph,
                         JobSystem* jobSystem,
                         const ParquetGraphMapping& propertyMapping,
                         const std::string& propertyColumn,
                         const std::string& edgeTypeColumn);
    ~ParquetGraphImporter();

    ParquetGraphImporter(const ParquetGraphImporter&) = delete;
    ParquetGraphImporter(ParquetGraphImporter&&) = delete;
    ParquetGraphImporter& operator=(const ParquetGraphImporter&) = delete;
    ParquetGraphImporter& operator=(ParquetGraphImporter&&) = delete;

    void importNodeFile(const fs::Path& path, const ParquetSchema& schema);
    void importEdgeFile(const fs::Path& path, const ParquetSchema& schema);
    void finalize();

    // Per-row callbacks invoked by ParquetNodeRowVisitor / ParquetEdgeRowVisitor
    // after each chunk is fully buffered. `undirected` is currently ignored on
    // edges; the parameter is kept so the visitor's signature mirrors the
    // parquet column set.
    void onNodeRow(std::string_view id,
                   std::string_view label,
                   std::string_view propertiesJson);
    void onEdgeRow(std::string_view fromId,
                   std::string_view toId,
                   std::string_view relation,
                   std::string_view propertiesJson,
                   bool undirected);

    size_t getNodeCount() const { return _nodeCount; }
    size_t getEdgeCount() const { return _edgeCount; }
    size_t getSubRecordCount() const { return _subRecordCount; }
    size_t getDedupedReferenceCount() const { return _dedupedReferenceCount; }
    size_t getStubNodeCount() const { return _stubNodeCount; }
    size_t getSkippedEdgeCount() const { return _skippedEdgeCount; }

private:
    GraphWriter _writer;
    const ParquetGraphMapping& _propertyMapping;
    const std::string _propertyColumn;
    const std::string _edgeTypeColumn;

    // TuringDB property-type names are graph-wide. The merged ParquetGraphMapping
    // can carry the same key name with different value types in different sub-
    // records (e.g. `strand` is a String inside `canonical_transcript` but an
    // Int64 inside `genomic_location`). Track the type bound to each name and,
    // on collision, append the value-type tag to the new name (mirrors the
    // JsonlParser pattern in import/jsonl/).
    std::string resolvePropertyName(const std::string& baseName, ValueType valueType);

    void addScalarNodeProperty(NodeID node,
                               const ParquetGraphProperty& property,
                               const nlohmann::json& value);
    void addScalarEdgeProperty(const EdgeRecord& edge,
                               const ParquetGraphProperty& property,
                               const nlohmann::json& value);
    void writeSubRecord(NodeID parent,
                        std::string_view edgeType,
                        const ParquetGraphLabel& label,
                        const nlohmann::json& value);

    NodeID resolveOrCreateStubNode(std::string_view externalId);

    std::unordered_map<std::string, NodeID> _externalIdToNodeId;
    std::unordered_map<std::string, ValueType> _propertyTypeByName;
    // Sub-record dedup: maps "<inferredLabel>:<keyValue>" to the NodeID
    // already created for that combination, so a second observation of the
    // same sub-record (same label, same `id`/`value` field) reuses the node
    // and only adds the HAS_… edge.
    std::unordered_map<std::string, NodeID> _subRecordByKey;
    size_t _nodeCount {0};
    size_t _edgeCount {0};
    size_t _subRecordCount {0};
    size_t _dedupedReferenceCount {0};
    size_t _stubNodeCount {0};
    size_t _skippedEdgeCount {0};
};

}
