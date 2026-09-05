#include "PropertyContainerLoader.h"

#include <string.h>

#include "DumpConfig.h"
#include "GraphDumpHelper.h"

#include "list/EncodedList.h"

using namespace db;

ListPropertyContainerLoader::ListPropertyContainerLoader(fs::FilePageReader& reader)
    : _reader(reader)
{
}

ListPropertyContainerLoader::~ListPropertyContainerLoader() {
}

DumpResult<std::unique_ptr<PropertyContainer>> ListPropertyContainerLoader::load() {
    Profile profile("ListPropertyContainerLoader::load");

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
    const uint64_t totalBytes = it.get<uint64_t>();
    const uint64_t idPageCount = it.get<uint64_t>();
    const uint64_t bytePageCount = it.get<uint64_t>();

    auto* container = new TypedPropertyContainer<types::List>;
    container->_ids.resize(propCount);

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

    std::vector<std::byte> stream(totalBytes);
    size_t byteOffset = 0;

    for (size_t i = 0; i < bytePageCount; i++) {
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
            stream[byteOffset + j] = std::byte {it.get<uint8_t>()};
        }
        byteOffset += countInPage;
    }

    ListContainer& values = container->_values;
    size_t recordOffset = 0;

    for (size_t i = 0; i < propCount; i++) {
        if (recordOffset + sizeof(uint64_t) > stream.size()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS);
        }

        uint64_t length = 0;
        memcpy(&length, stream.data() + recordOffset, sizeof(uint64_t));
        recordOffset += sizeof(uint64_t);

        if (recordOffset + length > stream.size()) {
            return DumpError::result(DumpErrorType::COULD_NOT_READ_PROPS);
        }

        const EncodedList encoded(std::span {stream.data() + recordOffset, length});
        values.append(encoded.decodeInto(values));

        recordOffset += length;
    }

    auto& entityIndexMap = container->_entityIndexMap;
    const auto& ids = container->_ids;
    entityIndexMap.reserve(ids.size());
    for (size_t i = 0; i < ids.size(); i++) {
        entityIndexMap[ids[i]] = i;
    }

    return {std::unique_ptr<PropertyContainer> {container}};
}
