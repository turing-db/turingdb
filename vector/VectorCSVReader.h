#pragma once

namespace fs {
class Path;
}

namespace vec {

class BatchVectorCreate;

// Reads the file format LOAD VECTOR expects - one `id,dim1,...,dimN` line per
// vector - into a batch already prepared by a library write accessor, which is
// what fixes the dimension every line must match.
//
// A file that cannot be opened, a line without an ID, and a line whose value count
// disagrees with the batch's dimension are all malformed input and throw
// VectorException. Blank lines are skipped.
class VectorCSVReader {
public:
    static void read(const fs::Path& path, BatchVectorCreate& batch);
};

}
