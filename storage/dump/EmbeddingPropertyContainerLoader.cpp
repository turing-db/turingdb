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
    const uint64_t nullCount = it.get<uint64_t>();
    const uint64_t nullPageCount = it.get<uint64_t>();

    const size_t floatsPerPage = GraphDumpHelper::getCountPerPage<float>();

    uint64_t floatCount = 0;
    const bool floatCountOverflows = __builtin_mul_overflow(propCount, dimension, &floatCount);
    const bool headerInvalid = (dimension == 0)
                            || floatCountOverflows
                            || (GraphDumpHelper::getPageCountForItems(floatCount, floatsPerPage) != floatPageCount);
    if (headerInvalid) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS);
    }

    std::unique_ptr<TypedPropertyContainer<types::Embedding>> container = std::make_unique<TypedPropertyContainer<types::Embedding>>(dimension);
    container->_ids.resize(propCount);

    if (auto res = GraphDumpHelper::loadPages(_reader, idPageCount, std::span {container->_ids}, DumpErrorType::COULD_NOT_READ_PROPS); !res) {
        return res.get_unexpected();
    }

    std::vector<float> pageFloats(floatsPerPage);
    std::vector<float> partialEmbedding(dimension);
    size_t partialCount = 0;
    size_t embeddingCount = 0;

    for (size_t i = 0; i < floatPageCount; i++) {
        size_t countInPage = 0;
        if (auto res = GraphDumpHelper::loadPage(_reader, std::span {pageFloats}, countInPage, DumpErrorType::COULD_NOT_READ_PROPS); !res) {
            return res.get_unexpected();
        }

        std::span<const float> remaining = std::span<const float> {pageFloats}.first(countInPage);
        while (!remaining.empty()) {
            if (embeddingCount == propCount) {
                return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS);
            }

            const size_t taken = std::min<size_t>(dimension - partialCount, remaining.size());
            const bool wholeInPage = (partialCount == 0 && taken == dimension);

            if (wholeInPage) {
                container->_values.alloc(remaining.first(dimension));
                embeddingCount++;
            } else {
                std::copy_n(remaining.begin(), taken, partialEmbedding.begin() + partialCount);
                partialCount += taken;

                if (partialCount == dimension) {
                    container->_values.alloc(partialEmbedding);
                    partialCount = 0;
                    embeddingCount++;
                }
            }

            remaining = remaining.subspan(taken);
        }
    }

    if (embeddingCount != propCount) {
        return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS);
    }

    container->_nullIds.resize(nullCount);

    if (auto res = GraphDumpHelper::loadPages(_reader, nullPageCount, std::span {container->_nullIds}, DumpErrorType::COULD_NOT_READ_PROPS); !res) {
        return res.get_unexpected();
    }

    // Reconstruct the entity ID -> index map. Nulls go in after the values, as sort()
    // does, so a null wins for an ID carrying both
    auto& entityIndexMap = container->_entityIndexMap;
    const auto& ids = container->_ids;
    const auto& nullIds = container->_nullIds;
    entityIndexMap.reserve(ids.size() + nullIds.size());
    for (size_t i = 0; i < ids.size(); i++) {
        entityIndexMap[ids[i]] = i;
    }

    for (const EntityID id : nullIds) {
        entityIndexMap[id] = container->NULL_INDEX;
    }

    container->_sorted = std::is_sorted(ids.begin(), ids.end());

    return {std::unique_ptr<PropertyContainer> {container.release()}};
}
