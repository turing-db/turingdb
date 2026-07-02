#include "ParquetImporter.h"

#include "ParquetEdgeVisitor.h"
#include "ParquetNodeVisitor.h"
#include "ParquetReader.h"

using namespace db;

void ParquetImporter::import() {
    {
        ParquetNodeVisitor nodeVisitor(_builder, _nodeIDs);
        ParquetReader reader(_nodeFile, nodeVisitor);
        reader.ensureFileOpen();
        while (reader.nextChunk()) {
        }
    }

    {
        ParquetEdgeVisitor edgeVisitor(_builder, _nodeIDs);
        ParquetReader reader(_edgeFile, edgeVisitor);
        reader.ensureFileOpen();
        while (reader.nextChunk()) {
        }
    }
}
