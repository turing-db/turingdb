#include "Neo4jParquetImporter.h"

#include <parquet/types.h>
#include <range/v3/view/zip.hpp>

#include "ParquetNeo4jVisitor.h"
#include "ParquetReader.h"

#include "versioning/CommitBuilder.h"
#include "writers/DataPartBuilder.h"

#include "BioAssert.h"

using namespace db;
namespace rg = ranges;
namespace rv = rg::views;

void Neo4jParquetImporter::import() {
    // Opens file; populates metadata; calls @ref ParquetNeo4jVisitor::onFileStart,
    // populating column indexes
    _reader.ensureFileOpen();

    bioassert(_visitor._nodeColIdx != ParquetNeo4jVisitor::INVALID_COL_IDX,
              "failed to populate node column");

    while (_reader.nextChunk()) {
        addNodes();
    }
}

void Neo4jParquetImporter::addNodes() {
    const ParquetNeo4jVisitor::NodeIDs nodeIDs = _visitor.nodes();
    const ParquetNeo4jVisitor::Labels labels = _visitor.labels();

    bioassert(nodeIDs.data(), "Failed to get nodes");
    bioassert(labels.data(), "Failed to get nodes");
    bioassert(nodeIDs.size() == labels.size(), "Node, label mismatch");

    // DataPartBuilder& bld = _builder->getCurrentBuilder();

    // std::vector<std::string> labelBuffer;
    // for (auto [id, lbls] : rv::zip(nodeIDs, labels)) {
    // }
}
