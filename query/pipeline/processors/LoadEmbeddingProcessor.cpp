#include "LoadEmbeddingProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "columns/ColumnConst.h"
#include "dataframe/NamedColumn.h"
#include "metadata/PropertyType.h"
#include "reader/GraphReader.h"
#include "versioning/CommitBuilder.h"
#include "versioning/CommitWriteBuffer.h"
#include "versioning/Transaction.h"
#include "writers/MetadataBuilder.h"

#include "ParquetEmbeddingReader.h"

#include "PipelineV2.h"
#include "PipelinePort.h"
#include "ExecutionContext.h"
#include "SystemManager.h"
#include "TuringConfig.h"
#include "ID.h"
#include "PipelineException.h"

using namespace db;

namespace {

// Column names expected in the embedding Parquet file. These match the layout
// produced by the embedding converter.
constexpr std::string_view nodeIdColumn = "node_id";
constexpr std::string_view embeddingColumn = "embedding";

}

LoadEmbeddingProcessor::LoadEmbeddingProcessor(std::string_view filePath,
                                               std::string_view propertyName)
    : _filePath(filePath),
    _propertyName(propertyName)
{
}

LoadEmbeddingProcessor::~LoadEmbeddingProcessor() {
}

LoadEmbeddingProcessor* LoadEmbeddingProcessor::create(PipelineV2* pipeline,
                                                       std::string_view filePath,
                                                       std::string_view propertyName) {
    LoadEmbeddingProcessor* proc = new LoadEmbeddingProcessor(filePath, propertyName);

    PipelineOutputPort* outCount = PipelineOutputPort::create(pipeline, proc);
    proc->_outCount.setPort(outCount);
    proc->addOutput(outCount);

    proc->postCreate(pipeline);

    return proc;
}

std::string LoadEmbeddingProcessor::describe() const {
    return fmt::format("LoadEmbeddingProcessor @={}", fmt::ptr(this));
}

void LoadEmbeddingProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;

    Transaction* rawTx = ctxt->getTransaction();
    if (!rawTx) {
        throw PipelineException("LOAD EMBEDDING: missing transaction in execution context");
    }

    if (!rawTx->writingPendingCommit()) {
        throw PipelineException("LOAD EMBEDDING: cannot perform writes outside of a write transaction");
    }

    auto& tx = rawTx->get<PendingCommitWriteTx>();
    CommitBuilder* commitBuilder = tx.commitBuilder();
    bioassert(commitBuilder, "Failed to get CommitBuilder in LoadEmbeddingProcessor");

    _metadataBuilder = &commitBuilder->metadata();
    _writeBuffer = &commitBuilder->writeBuffer();

    markAsPrepared();
}

void LoadEmbeddingProcessor::reset() {
}

void LoadEmbeddingProcessor::execute() {
    // Resolve the file path relative to the data directory.
    const SystemManager* sysMan = _ctxt->getSystemManager();
    const fs::Path& dataDir = sysMan->getConfig()->getDataDir();
    const fs::Path filePath = dataDir / std::string(_filePath);

    if (!filePath.isSubDirectory(dataDir)) {
        throw PipelineException(fmt::format(
            "LOAD EMBEDDING: file path must be relative to '{}'", dataDir.get()));
    }

    ParquetEmbeddingData data;
    ParquetEmbeddingReader::read(filePath, nodeIdColumn, embeddingColumn, &data);

    const PropertyType propertyType =
        _metadataBuilder->getOrCreatePropertyType(_propertyName, ValueType::Embedding);

    // getOrCreatePropertyType returns an existing property unchanged when the
    // name is already in use, ignoring the requested ValueType. If the target
    // property already holds a non-embedding type, writing embedding values
    // under its ID produces a type-confused column that crashes on read/dump,
    // so reject the collision with a clear error instead.
    if (propertyType._valueType != ValueType::Embedding) {
        throw PipelineException(fmt::format(
            "LOAD EMBEDDING: property '{}' already exists with type {} and cannot store embeddings",
            _propertyName,
            ValueTypeName::value(propertyType._valueType)));
    }

    const PropertyTypeID propertyID = propertyType._id;

    const GraphReader reader = _ctxt->getGraphView().read();

    CommitWriteBuffer::UntypedProperty property;
    property.propertyID = propertyID;

    for (size_t row = 0; row < data._nodeIDs.size(); ++row) {
        const NodeID nodeID {static_cast<uint64_t>(data._nodeIDs[row])};
        if (!reader.graphHasNode(nodeID)) {
            throw PipelineException(fmt::format(
                "LOAD EMBEDDING: graph does not contain node with ID {}", data._nodeIDs[row]));
        }

        property.value = std::move(data._embeddings[row]);

        _writeBuffer->addNodeUpdate(nodeID, property);
    }

    auto* colCount = _outCount.getValue()->as<ColumnConst<types::UInt64::Primitive>>();
    colCount->set(data._nodeIDs.size());

    _outCount.getPort()->writeData();
    finish();
}
