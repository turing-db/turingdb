#include "ParquetFileReading.h"

#include "ParquetReader.h"
#include "ParquetWriteSchema.h"

using namespace db;

void db::readParquetFile(const fs::Path& path,
                         ParquetSaxVisitor& visitor,
                         const ParquetWriteSchema& expectedSchema) {
    ParquetReader reader(path, visitor);
    reader.setExpectedSchema(expectedSchema);
    while (reader.nextChunk()) {
    }
}
