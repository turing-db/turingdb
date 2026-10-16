#include "ParameterMapSink.h"

#include <stdint.h>
#include <string.h>

#include <spdlog/fmt/fmt.h>

#include "ParameterMap.h"
#include "list/ListBufferTypeTag.h"
#include "map/MapBufferTypeTag.h"
#include "metadata/PropertyType.h"

#include "TuringException.h"

using namespace net::proto;

namespace {

[[noreturn]] void throwUnsupportedType(std::string_view typeName) {
    throw TuringException(fmt::format("A parameter cannot hold a {} value", typeName));
}

template <typename T>
T readValue(const char* bytes) {
    T value {};
    memcpy(&value, bytes, sizeof(T));
    return value;
}

// List and map tags name their members alike but number them differently, so one reader
// serves both. The bytes are [tag][value].
template <typename Tag>
void readScalar(const char* bytes, db::ParameterValue& slot) {
    const Tag tag = readValue<Tag>(bytes);
    const char* value = bytes + sizeof(Tag);

    switch (tag) {
        case Tag::Int:
            slot.setInt64(readValue<db::types::Int64::Primitive>(value));
        break;
        case Tag::Double:
            slot.setDouble(readValue<db::types::Double::Primitive>(value));
        break;
        case Tag::Bool:
            slot.setBool(readValue<db::types::Bool::Primitive>(value));
        break;
        case Tag::Null:
            slot.setNull();
        break;
        case Tag::UInt:
            throwUnsupportedType("UInt64");
        break;
        case Tag::NodeID:
            throwUnsupportedType("NodeID");
        break;
        case Tag::EdgeID:
            throwUnsupportedType("EdgeID");
        break;
        case Tag::DateTime:
            throwUnsupportedType("DateTime");
        break;
        case Tag::Duration:
            throwUnsupportedType("Duration");
        break;
        default:
            throw TuringException(fmt::format("Invalid value tag {} in a parameter", static_cast<uint32_t>(tag)));
        break;
    }
}

}

ParameterMapSink::ParameterMapSink(db::ParameterMap* parameters)
    : _parameters(parameters)
{
}

ParameterMapSink::~ParameterMapSink() {
}

float* ParameterMapSink::allocEmbedding(size_t numFloats) {
    throwUnsupportedType("Embedding");
}

char* ParameterMapSink::allocString(size_t size) {
    ParameterFrame& frame = _frames.back();

    const bool isKey = topContainerIsMap() && !frame._keyPending;
    if (isKey) {
        frame._pendingKey.resize(size);
        frame._keyPending = true;
        return frame._pendingKey.data();
    }

    std::string& value = nextSlot().setString();
    value.resize(size);
    return value.data();
}

db::ListView ParameterMapSink::beginList(size_t elementCount, size_t byteSize) {
    throw TuringException("The parameters must be a map, not a list");
}

db::ListElementView ParameterMapSink::beginNestedList(size_t elementCount, size_t byteSize) {
    db::ParameterValue::List& list = nextSlot().setList();
    list.resize(elementCount);
    completeValue();

    _frames.push_back({&list, elementCount});
    return {};
}

db::ListElementView ParameterMapSink::writeListValue(std::string_view value) {
    completeValue();
    return {};
}

db::ListElementView ParameterMapSink::writeListValue(std::span<const float> value) {
    completeValue();
    return {};
}

db::ListElementView ParameterMapSink::writeListElementBytes(const char* bytes, size_t byteSize) {
    readScalar<db::ListBufferTypeTag>(bytes, nextSlot());
    completeValue();
    return {};
}

db::MapView ParameterMapSink::beginMap(size_t entryCount, size_t byteSize) {
    _frames.push_back({_parameters, entryCount});
    return {};
}

db::ListElementView ParameterMapSink::beginNestedMap(size_t entryCount, size_t byteSize) {
    db::ParameterValue::Map& map = nextSlot().setMap();
    completeValue();

    _frames.push_back({&map, entryCount});
    return {};
}

void ParameterMapSink::writeMapKey(std::string_view key) {
}

void ParameterMapSink::writeMapValueBytes(const char* bytes, size_t byteSize) {
    readScalar<db::MapBufferTypeTag>(bytes, nextSlot());
    completeValue();
}

bool ParameterMapSink::topContainerIsMap() const {
    return !std::holds_alternative<db::ParameterValue::List*>(_frames.back()._container);
}

bool ParameterMapSink::topContainerComplete() const {
    const ParameterFrame& frame = _frames.back();
    return frame._written == frame._expected;
}

db::ParameterValue& ParameterMapSink::nextSlot() {
    ParameterFrame& frame = _frames.back();

    if (db::ParameterValue::List* const* list = std::get_if<db::ParameterValue::List*>(&frame._container)) {
        return (**list)[frame._written];
    } else if (db::ParameterValue::Map* const* map = std::get_if<db::ParameterValue::Map*>(&frame._container)) {
        return (**map)[frame._pendingKey];
    } else {
        return std::get<db::ParameterMap*>(frame._container)->insert(frame._pendingKey);
    }
}

void ParameterMapSink::completeValue() {
    ParameterFrame& frame = _frames.back();
    frame._keyPending = false;
    frame._written++;
}
