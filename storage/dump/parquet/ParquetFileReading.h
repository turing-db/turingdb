#pragma once

#include "Path.h"

namespace db {

class ParquetSaxVisitor;
class ParquetWriteSchema;

// Opens a Parquet file, checks it carries exactly the schema the dumpers write, and
// drains every chunk through the visitor. Throws on schema mismatch or read failure.
void readParquetFile(const fs::Path& path,
                     ParquetSaxVisitor& visitor,
                     const ParquetWriteSchema& expectedSchema);

}
