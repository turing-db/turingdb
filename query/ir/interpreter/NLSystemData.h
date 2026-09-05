#pragma once

#include <stdint.h>
#include <string>
#include <string_view>
#include <vector>

#include "EmbeddingsSpec.h"
#include "Path.h"
#include "VecLibMetadata.h"

#include "columns/ColumnVector.h"
#include "metadata/PropertyType.h"
#include "versioning/ChangeID.h"

#include "NLProgram.h"

namespace db {

// Which change-management operation a change command performs. The runtime mirror
// of mlir::storage::ChangeOperation, so an NLProgram stays free of the MLIR
// dialect headers - the same split AggregateKind makes in NLProgram.h.
enum class NLChangeOperation : uint8_t {
    New = 0,
    Submit,
    Delete,
    List,
};

// Which way a transfer moves its files. The runtime mirror of
// mlir::storage::S3TransferDirection.
enum class NLS3Direction : uint8_t {
    Pull = 0,
    Push,
};

// Whether a property index covers the nodes or the edges. The runtime mirror of
// mlir::storage::IndexedEntity.
enum class NLIndexedEntity : uint8_t {
    Node = 0,
    Edge,
};

// The columns a system command reports its result table in. A name the command
// was handed, or one it read out of a structure that outlives the query, is
// emitted as a view; a name it built or was handed by value is owned.
using NLViewColumn = ColumnVector<types::String::Primitive>;
using NLStringColumn = ColumnVector<std::string>;
using NLBoolColumn = ColumnVector<types::Bool::Primitive>;
using NLCountColumn = ColumnVector<types::UInt64::Primitive>;
using NLChangeIDColumn = ColumnVector<ChangeID>;

// LOAD GRAPH / CREATE GRAPH: the graph to act on, and the single-row column the
// command names it back in.
class NLGraphCommandData : public NLFunctionData {
public:
    NLGraphCommandData(std::string_view graphName, NLViewColumn* graph)
        : _graphName(graphName),
        _graph(graph)
    {
    }

    std::string_view getGraphName() const { return _graphName; }
    NLViewColumn* getGraph() const { return _graph; }

private:
    std::string_view _graphName;
    NLViewColumn* _graph {nullptr};
};

// LOAD JSONL / LOAD GML / LOAD PARQUET: the file to import, the graph to build
// from it, the per-property embedding dimensions (JSONL alone ever carries any)
// and the statement to blame when the import fails.
class NLImportGraphData : public NLFunctionData {
public:
    NLImportGraphData(const fs::Path& path,
                      std::string_view graphName,
                      const EmbeddingsSpec& embeddings,
                      std::string_view statement,
                      NLViewColumn* graph)
        : _path(path),
        _graphName(graphName),
        _embeddings(embeddings),
        _statement(statement),
        _graph(graph)
    {
    }

    const fs::Path& getPath() const { return _path; }
    std::string_view getGraphName() const { return _graphName; }
    const EmbeddingsSpec& getEmbeddings() const { return _embeddings; }
    std::string_view getStatement() const { return _statement; }
    NLViewColumn* getGraph() const { return _graph; }

private:
    fs::Path _path;
    std::string_view _graphName;
    EmbeddingsSpec _embeddings;
    std::string_view _statement;
    NLViewColumn* _graph {nullptr};
};

// LIST GRAPH: the column the loaded graphs are named in.
class NLListGraphsData : public NLFunctionData {
public:
    explicit NLListGraphsData(NLViewColumn* graphs)
        : _graphs(graphs)
    {
    }

    NLViewColumn* getGraphs() const { return _graphs; }

private:
    NLViewColumn* _graphs {nullptr};
};

// LIST AVAILABLE GRAPHS: the three row-aligned columns each on-disk graph is
// reported in.
class NLListAvailableGraphsData : public NLFunctionData {
public:
    NLListAvailableGraphsData(NLStringColumn* graphs, NLBoolColumn* loaded, NLBoolColumn* loading)
        : _graphs(graphs),
        _loaded(loaded),
        _loading(loading)
    {
    }

    NLStringColumn* getGraphs() const { return _graphs; }
    NLBoolColumn* getLoaded() const { return _loaded; }
    NLBoolColumn* getLoading() const { return _loading; }

private:
    NLStringColumn* _graphs {nullptr};
    NLBoolColumn* _loaded {nullptr};
    NLBoolColumn* _loading {nullptr};
};

// CHANGE NEW / SUBMIT / DELETE / LIST: the operation to perform and the column
// the change IDs it acted on are reported in.
class NLChangeCommandData : public NLFunctionData {
public:
    NLChangeCommandData(NLChangeOperation operation, NLChangeIDColumn* changes)
        : _operation(operation),
        _changes(changes)
    {
    }

    NLChangeOperation getOperation() const { return _operation; }
    NLChangeIDColumn* getChanges() const { return _changes; }

private:
    NLChangeOperation _operation {NLChangeOperation::New};
    NLChangeIDColumn* _changes {nullptr};
};

// LOAD COMMIT: the hash as the query spelled it, parsed at execution.
class NLLoadCommitData : public NLFunctionData {
public:
    explicit NLLoadCommitData(std::string_view commitHash)
        : _commitHash(commitHash)
    {
    }

    std::string_view getCommitHash() const { return _commitHash; }

private:
    std::string_view _commitHash;
};

// S3 CONNECT: the credentials the client is opened with.
class NLS3ConnectData : public NLFunctionData {
public:
    NLS3ConnectData(std::string_view accessId, std::string_view secretKey, std::string_view region)
        : _accessId(accessId),
        _secretKey(secretKey),
        _region(region)
    {
    }

    std::string_view getAccessId() const { return _accessId; }
    std::string_view getSecretKey() const { return _secretKey; }
    std::string_view getRegion() const { return _region; }

private:
    std::string_view _accessId;
    std::string_view _secretKey;
    std::string_view _region;
};

// S3 PULL / S3 PUSH: the direction, the bucket, and either the single object
// (file) or the tree (prefix) to move, against a path under the data directory.
class NLS3TransferData : public NLFunctionData {
public:
    NLS3TransferData(NLS3Direction direction,
                     std::string_view bucket,
                     std::string_view prefix,
                     std::string_view file,
                     std::string_view localPath)
        : _direction(direction),
        _bucket(bucket),
        _prefix(prefix),
        _file(file),
        _localPath(localPath)
    {
    }

    NLS3Direction getDirection() const { return _direction; }
    std::string_view getBucket() const { return _bucket; }
    std::string_view getPrefix() const { return _prefix; }
    std::string_view getFile() const { return _file; }
    std::string_view getLocalPath() const { return _localPath; }

private:
    NLS3Direction _direction {NLS3Direction::Pull};
    std::string_view _bucket;
    std::string_view _prefix;
    std::string_view _file;
    std::string_view _localPath;
};

// SHOW PROCEDURES: the two row-aligned columns each procedure is reported in. A
// procedure's name outlives the query in the procedure manager, so it is emitted
// as a view; its signature is rendered per row, so it is owned.
class NLShowProceduresData : public NLFunctionData {
public:
    NLShowProceduresData(NLViewColumn* names, NLStringColumn* signatures)
        : _names(names),
        _signatures(signatures)
    {
    }

    NLViewColumn* getNames() const { return _names; }
    NLStringColumn* getSignatures() const { return _signatures; }

private:
    NLViewColumn* _names {nullptr};
    NLStringColumn* _signatures {nullptr};
};

// INSTALL <name>: the extension to install, and the single-row column the command
// names it back in.
class NLInstallExtensionData : public NLFunctionData {
public:
    NLInstallExtensionData(std::string_view extensionName, NLViewColumn* extension)
        : _extensionName(extensionName),
        _extension(extension)
    {
    }

    std::string_view getExtensionName() const { return _extensionName; }
    NLViewColumn* getExtension() const { return _extension; }

private:
    std::string_view _extensionName;
    NLViewColumn* _extension {nullptr};
};

// SHOW EXTENSIONS: the column the installed extensions are named in.
class NLShowExtensionsData : public NLFunctionData {
public:
    explicit NLShowExtensionsData(NLViewColumn* names)
        : _names(names)
    {
    }

    NLViewColumn* getNames() const { return _names; }

private:
    NLViewColumn* _names {nullptr};
};

// CREATE VECTOR INDEX: the library to create and the single-row column the
// command names it back in.
class NLCreateVectorIndexData : public NLFunctionData {
public:
    NLCreateVectorIndexData(std::string_view indexName,
                            vec::Dimension dimension,
                            vec::DistanceMetric metric,
                            vec::IndexType indexType,
                            NLViewColumn* index)
        : _indexName(indexName),
        _dimension(dimension),
        _metric(metric),
        _indexType(indexType),
        _index(index)
    {
    }

    std::string_view getIndexName() const { return _indexName; }
    vec::Dimension getDimension() const { return _dimension; }
    vec::DistanceMetric getMetric() const { return _metric; }
    vec::IndexType getIndexType() const { return _indexType; }
    NLViewColumn* getIndex() const { return _index; }

private:
    std::string_view _indexName;
    vec::Dimension _dimension {0};
    vec::DistanceMetric _metric {vec::DistanceMetric::EUCLIDEAN_DIST};
    vec::IndexType _indexType {vec::IndexType::FLAT};
    NLViewColumn* _index {nullptr};
};

// DELETE VECTOR INDEX: the library to drop and the single-row column the command
// names it back in.
class NLDeleteVectorIndexData : public NLFunctionData {
public:
    NLDeleteVectorIndexData(std::string_view indexName, NLViewColumn* index)
        : _indexName(indexName),
        _index(index)
    {
    }

    std::string_view getIndexName() const { return _indexName; }
    NLViewColumn* getIndex() const { return _index; }

private:
    std::string_view _indexName;
    NLViewColumn* _index {nullptr};
};

// SHOW VECTOR INDEXES: the two row-aligned columns each library is reported in.
class NLShowVectorIndexesData : public NLFunctionData {
public:
    NLShowVectorIndexesData(NLStringColumn* names, NLCountColumn* dimensions)
        : _names(names),
        _dimensions(dimensions)
    {
    }

    NLStringColumn* getNames() const { return _names; }
    NLCountColumn* getDimensions() const { return _dimensions; }

private:
    NLStringColumn* _names {nullptr};
    NLCountColumn* _dimensions {nullptr};
};

// LOAD VECTOR: the CSV to read, the library to fill, and the single-row column
// the command counts the added vectors in.
class NLLoadVectorData : public NLFunctionData {
public:
    NLLoadVectorData(std::string_view path, std::string_view indexName, NLCountColumn* count)
        : _path(path),
        _indexName(indexName),
        _count(count)
    {
    }

    std::string_view getPath() const { return _path; }
    std::string_view getIndexName() const { return _indexName; }
    NLCountColumn* getCount() const { return _count; }

private:
    std::string_view _path;
    std::string_view _indexName;
    NLCountColumn* _count {nullptr};
};

// LOAD EMBEDDING: the Parquet file to read, the node property to write, and the
// single-row column the command counts the written nodes in.
class NLLoadEmbeddingData : public NLFunctionData {
public:
    NLLoadEmbeddingData(std::string_view path, std::string_view propertyName, NLCountColumn* count)
        : _path(path),
        _propertyName(propertyName),
        _count(count)
    {
    }

    std::string_view getPath() const { return _path; }
    std::string_view getPropertyName() const { return _propertyName; }
    NLCountColumn* getCount() const { return _count; }

private:
    std::string_view _path;
    std::string_view _propertyName;
    NLCountColumn* _count {nullptr};
};

// CREATE INDEX: the index to declare, the property it covers, and whether it
// covers the nodes or the edges.
class NLCreatePropertyIndexData : public NLFunctionData {
public:
    NLCreatePropertyIndexData(std::string_view indexName,
                              std::string_view propertyName,
                              NLIndexedEntity entity)
        : _indexName(indexName),
        _propertyName(propertyName),
        _entity(entity)
    {
    }

    std::string_view getIndexName() const { return _indexName; }
    std::string_view getPropertyName() const { return _propertyName; }
    NLIndexedEntity getEntity() const { return _entity; }

private:
    std::string_view _indexName;
    std::string_view _propertyName;
    NLIndexedEntity _entity {NLIndexedEntity::Node};
};

// DROP INDEX: the index to drop.
class NLDropIndexData : public NLFunctionData {
public:
    explicit NLDropIndexData(std::string_view indexName)
        : _indexName(indexName)
    {
    }

    std::string_view getIndexName() const { return _indexName; }

private:
    std::string_view _indexName;
};

// EXPLAIN: the dumps the compilation of the explained query produced, and the two
// row-aligned columns they are reported in. The text is held by the module the command
// was built into, which outlives the run, so it is emitted as views.
class NLExplainData : public NLFunctionData {
public:
    NLExplainData(const std::vector<std::string_view>& stageNames,
                  const std::vector<std::string_view>& stageDumps,
                  NLViewColumn* stages,
                  NLViewColumn* dumps)
        : _stageNames(stageNames),
        _stageDumps(stageDumps),
        _stages(stages),
        _dumps(dumps)
    {
    }

    const std::vector<std::string_view>& getStageNames() const { return _stageNames; }
    const std::vector<std::string_view>& getStageDumps() const { return _stageDumps; }
    NLViewColumn* getStages() const { return _stages; }
    NLViewColumn* getDumps() const { return _dumps; }

private:
    std::vector<std::string_view> _stageNames;
    std::vector<std::string_view> _stageDumps;
    NLViewColumn* _stages {nullptr};
    NLViewColumn* _dumps {nullptr};
};

}
