#include "VecLibMetadataWriter.h"

#include "VecLibMetadata.h"

using namespace vec;

VecLibMetadataWriter::VecLibMetadataWriter()
{
}

VecLibMetadataWriter::~VecLibMetadataWriter() {
}

VectorResult<void> VecLibMetadataWriter::write(const VecLibMetadata& metadata) {
    if (!_writer.hasFile()) {
        return VectorError::result(VectorErrorCode::WriterNotInitialized);
    }

    if (auto res = _writer.file().clearContent(); !res) {
        return VectorError::result(VectorErrorCode::CouldNotClearMetadataFile, res.error());
    }

    _writer.write("{\n");

    // Library ID
    _writer.write("    \"id\": ");
    _writer.write(std::to_string(metadata._id));
    _writer.write(",\n");

    // Name
    _writer.write("    \"name\": \"");
    _writer.write(metadata._name);
    _writer.write("\",\n");

    // Dimension
    _writer.write("    \"dimension\": ");
    _writer.write(std::to_string(metadata._dimension));
    _writer.write(",\n");

    // Metric
    _writer.write("    \"metric\": \"");
    _writer.write(DistanceMetricName::value(metadata._metric));
    _writer.write("\",\n");

    // Precision
    _writer.write("    \"precision\": 32,\n");

    // Index type
    _writer.write("    \"index_type\": \"");
    _writer.write(IndexTypeName::value(metadata._indexType));
    _writer.write("\",\n");

    // Created timestamp
    _writer.write("    \"created_at\": ");
    _writer.write(std::to_string(metadata._createdAt));
    _writer.write(",\n");

    // Modified timestamp
    _writer.write("    \"modified_at\": ");
    _writer.write(std::to_string(metadata._modifiedAt));
    _writer.write("\n}\n");

    _writer.flush();

    if (_writer.errorOccured()) {
        return VectorError::result(VectorErrorCode::CouldNotWriteMetadataFile, _writer.error());
    }

    return {};
}

