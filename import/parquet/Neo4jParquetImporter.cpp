#include "Neo4jParquetImporter.h"

#include "ParquetReader.h"

#include "BioAssert.h"

using namespace db;

void Neo4jParquetImporter::import() {
    // Opens file; populates metadata; calls @ref ParquetNeo4jVisitor::onFileStart,
    // populating column indexes
    _reader.ensureFileOpen();

    bioassert(_visitor._nodeColIdx != ParquetNeo4jVisitor::INVALID_COL_IDX,
              "failed to populate node column");

    _reader.nextChunk();
}
