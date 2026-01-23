#include "VecLibMetadataLoader.h"

#include <nlohmann/json.hpp>

#include "VecLibMetadata.h"

using namespace vec;

VecLibMetadataLoader::VecLibMetadataLoader()
{
}

VecLibMetadataLoader::~VecLibMetadataLoader() {
}

VectorResult<void> VecLibMetadataLoader::load(VecLibMetadata& metadata) {
    if (!_reader.hasFile()) {
        return VectorError::result<void>(VectorErrorCode::ReaderNotInitialized);
    }

    _reader.read();

    if (_reader.errorOccured()) {
        return VectorError::result<void>(VectorErrorCode::CouldNotLoadMetadataFile, _reader.error());
    }

    const fs::ByteBuffer& buffer = _reader.getBuffer();
    const std::string_view jsonText {reinterpret_cast<const char*>(buffer.data()), buffer.size()};

    try {
        const nlohmann::json json = nlohmann::json::parse(jsonText);

        // Library ID
        metadata._id = json["id"].get<uint64_t>();

        // Library name
        metadata._name = json["name"].get<std::string>();

        // Library dimension
        metadata._dimension = json["dimension"].get<uint64_t>();

        if (metadata._dimension == 0) {
            return VectorError::result<void>(VectorErrorCode::InvalidDimension);
        }

        // Library metric
        const std::string& metric = json["metric"].get<std::string>();

        if (metric == "EUCLIDEAN_DIST") {
            metadata._metric = DistanceMetric::EUCLIDEAN_DIST;
        } else if (metric == "INNER_PRODUCT") {
            metadata._metric = DistanceMetric::INNER_PRODUCT;
        } else {
            return VectorError::result<void>(VectorErrorCode::InvalidMetric);
        }

        // Library index type
        const std::string& indexType = json["index_type"].get<std::string>();

        if (indexType == "BRUTE_FORCE") {
            metadata._indexType = IndexType::BRUTE_FORCE;
        } else if (indexType == "HNSW") {
            metadata._indexType = IndexType::HNSW;
        } else {
            return VectorError::result<void>(VectorErrorCode::InvalidIndexType);
        }

        // Library timestamps
        metadata._createdAt = json["created_at"].get<uint64_t>();
        metadata._modifiedAt = json["modified_at"].get<uint64_t>();

    } catch (const nlohmann::json::exception& e) {
        fmt::println("Error parsing JSON: {}", e.what());
        return VectorError::result<void>(VectorErrorCode::InvalidMetadata);
    }

    return {};
}
