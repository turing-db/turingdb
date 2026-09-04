#include "PropertyContainerLoader.h"

#include <algorithm>

#include "DumpConfig.h"
#include "GraphDumpHelper.h"

using namespace db;

EmbeddingPropertyContainerLoader::EmbeddingPropertyContainerLoader(fs::FilePageReader& reader)
    : _reader(reader)
{
}

EmbeddingPropertyContainerLoader::~EmbeddingPropertyContainerLoader() {
}

DumpResult<std::unique_ptr<PropertyContainer>> EmbeddingPropertyContainerLoader::load() {
    Profile profile("EmbeddingPropertyContainerLoader::load");

    _reader.nextPage();

    if (_reader.errorOccured()) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS, _reader.error().value());
    }

    auto it = _reader.begin();

    if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS);
    }

    if (auto res = GraphDumpHelper::checkFileHeader(it); !res) {
        return res.get_unexpected();
    }

    [[maybe_unused]] const ValueType valueType = it.get<ValueType>();
    const uint64_t propCount = it.get<uint64_t>();
    const uint64_t dimension = it.get<uint64_t>();
    const uint64_t idPageCount = it.get<uint64_t>();
    const uint64_t floatPageCount = it.get<uint64_t>();

    auto* container = new TypedPropertyContainer<types::Embedding>(dimension);
    container->_ids.resize(propCount);

    // Loading IDs
    size_t offset = 0;
    for (size_t i = 0; i < idPageCount; i++) {
        _reader.nextPage();

        if (_reader.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS, _reader.error().value());
        }

        it = _reader.begin();

        if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS);
        }

        const size_t countInPage = it.get<uint64_t>();
        for (size_t j = 0; j < countInPage; j++) {
            container->_ids[j + offset] = it.get<EntityID::Type>();
        }
        offset += countInPage;
    }

    // Loading float data — read flat floats and alloc into the container
    std::vector<float> embeddingBuffer(dimension);
    size_t floatOffset = 0;

    for (size_t i = 0; i < floatPageCount; i++) {
        _reader.nextPage();

        if (_reader.errorOccured()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS, _reader.error().value());
        }

        it = _reader.begin();

        if (it.remainingBytes() != DumpConfig::PAGE_SIZE) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS);
        }

        const size_t countInPage = it.get<uint64_t>();
        for (size_t j = 0; j < countInPage; j++) {
            const size_t bufIdx = floatOffset % dimension;
            embeddingBuffer[bufIdx] = it.get<float>();
            floatOffset++;

            if (bufIdx == dimension - 1) {
                container->_values.alloc(embeddingBuffer);
            }
        }
    }

    // Reconstruct the entity ID -> index map
    auto& entityIndexMap = container->_entityIndexMap;
    const auto& ids = container->_ids;
    entityIndexMap.reserve(ids.size());
    for (size_t i = 0; i < ids.size(); i++) {
        entityIndexMap[ids[i]] = i;
    }

    container->_sorted = std::is_sorted(ids.begin(), ids.end());

    return {std::unique_ptr<PropertyContainer> {container}};
}
