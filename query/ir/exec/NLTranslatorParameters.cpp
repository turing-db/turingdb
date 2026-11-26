#include "NLTranslator.h"

#include <optional>
#include <vector>
#include <string>
#include <string_view>

#include <spdlog/fmt/bundled/format.h>

#include "ParameterMap.h"
#include "ParameterValue.h"
#include "StorageAttributes.h"

#include "IRValueTypes.h"
#include "NullLiteralType.h"
#include "list/ListBuffer.h"
#include "map/MapBuffer.h"
#include "map/MapWriteCursor.h"
#include "mlir/IR/BuiltinAttributes.h"
#include "mlir/IR/BuiltinTypes.h"
#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/SmallVector.h"

#include "columns/ColumnConst.h"
#include "list/ListView.h"
#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"

#include "LocalMemory.h"
#include "IRException.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

namespace {

template <typename Primitive>
Column* allocBoundConstant(LocalMemory* memory, const Primitive& held) {
    ColumnConst<Primitive>* column = memory->alloc<ColumnConst<Primitive>>();
    column->set(held);
    return column;
}

// The bytes one value of a parameter's list or map occupies as an element, tag bytes
// excluded: the same object writeParameterValue hands the cursor, a view for a container
size_t parameterValueBytes(const ParameterValue& value) {
    if (value.isNull()) {
        return sizeof(PropertyNull);
    }

    switch (value.getType()) {
        case ValueType::Bool:
            return sizeof(types::Bool::Primitive);
        break;
        case ValueType::Int64:
            return sizeof(types::Int64::Primitive);
        break;
        case ValueType::Double:
            return sizeof(types::Double::Primitive);
        break;
        case ValueType::String:
            return sizeof(types::String::Primitive);
        break;
        case ValueType::DateTime:
            return sizeof(types::DateTime::Primitive);
        break;
        case ValueType::Duration:
            return sizeof(types::Duration::Primitive);
        break;
        case ValueType::Embedding:
            return sizeof(types::Embedding::Primitive);
        break;
        case ValueType::Point:
            return sizeof(types::Point::Primitive);
        break;
        case ValueType::List:
            return sizeof(ListView);
        break;
        case ValueType::Map:
            return sizeof(MapView);
        break;
        default:
            throw IRException(fmt::format("A parameter cannot hold a {} inside a list or a map", value.getTypeName()));
        break;
    }
}

size_t parameterListValueBytes(const ParameterValue::List& elements) {
    size_t valueBytes = 0;
    for (const ParameterValue& element : elements) {
        valueBytes += parameterValueBytes(element);
    }

    return valueBytes;
}

size_t parameterMapValueBytes(const ParameterValue::Map& entries) {
    size_t valueBytes = 0;
    for (const auto& [key, value] : entries) {
        valueBytes += parameterValueBytes(value);
    }

    return valueBytes;
}

// Writes a scalar or a null of a parameter's list or map through @param cursor
template <typename Cursor>
void writeParameterValue(LocalMemory* memory, Cursor& cursor, const ParameterValue& value) {
    if (value.isNull()) {
        cursor.writeValue(CellTag::Null, PropertyNull {});
        return;
    }

    switch (value.getType()) {
        case ValueType::Bool:
            cursor.writeValue(CellTag::Bool, types::Bool::Primitive(value.getBool()));
        break;
        case ValueType::Int64:
            cursor.writeValue(CellTag::Int, value.getInt64());
        break;
        case ValueType::Double:
            cursor.writeValue(CellTag::Double, value.getDouble());
        break;
        case ValueType::String:
            cursor.writeValue(CellTag::String, memory->stringBuffer().insert(value.getString()));
        break;
        case ValueType::DateTime:
            cursor.writeValue(CellTag::DateTime, value.getDateTime());
        break;
        case ValueType::Duration:
            cursor.writeValue(CellTag::Duration, value.getDuration());
        break;
        case ValueType::Embedding:
            cursor.writeValue(CellTag::Embedding, memory->embeddingBuffer().insert(value.getEmbedding()));
        break;
        case ValueType::Point:
            cursor.writeValue(CellTag::Point, value.getPoint());
        break;
        default:
            throw IRException(fmt::format("A parameter cannot hold a {} inside a list or a map", value.getTypeName()));
        break;
    }
}

// One list or map of a parameter being written, element by element
struct ParameterContainerFrame {
    const ParameterValue::List* _list {nullptr};
    const ParameterValue::Map* _map {nullptr};
    size_t _nextElement {0};
    ParameterValue::Map::const_iterator _nextEntry;
    std::optional<ListWriteCursor> _listCursor;
    std::optional<MapWriteCursor> _mapCursor;
};

ParameterContainerFrame openParameterList(LocalMemory* memory, const ParameterValue::List& elements) {
    ParameterContainerFrame frame;
    frame._list = &elements;
    frame._listCursor = memory->listBuffer().reserveList(elements.size(), parameterListValueBytes(elements));

    return frame;
}

ParameterContainerFrame openParameterMap(LocalMemory* memory, const ParameterValue::Map& entries) {
    ParameterContainerFrame frame;
    frame._map = &entries;
    frame._nextEntry = entries.begin();
    frame._mapCursor = memory->mapBuffer().reserveMap(entries.size(), parameterMapValueBytes(entries));

    return frame;
}

template <typename Cursor>
void writeContainerView(Cursor& cursor, const ParameterContainerFrame& finished) {
    if (finished._list) {
        cursor.writeValue(CellTag::ListView, finished._listCursor->getView());
    } else {
        cursor.writeValue(CellTag::MapView, finished._mapCursor->getView());
    }
}

// Writes a parameter's list or map, and every list or map nested in it, into the query's
// buffers: one frame per open container rather than a recursion, since a parameter nests as
// deep as its sender chose. The frame of @param root comes back filled.
ParameterContainerFrame materializeParameterContainer(LocalMemory* memory, const ParameterContainerFrame& root) {
    std::vector<ParameterContainerFrame> frames {root};

    while (true) {
        ParameterContainerFrame& frame = frames.back();
        const ParameterValue* next = nullptr;

        if (frame._list && frame._nextElement < frame._list->size()) {
            next = &(*frame._list)[frame._nextElement];
            frame._nextElement++;
        } else if (frame._map && frame._nextEntry != frame._map->end()) {
            frame._mapCursor->writeKey(memory->stringBuffer().insert(frame._nextEntry->first));
            next = &frame._nextEntry->second;
            ++frame._nextEntry;
        }

        if (!next) {
            const ParameterContainerFrame finished = frame;
            frames.pop_back();
            if (frames.empty()) {
                return finished;
            }

            ParameterContainerFrame& parent = frames.back();
            if (parent._list) {
                writeContainerView(*parent._listCursor, finished);
            } else {
                writeContainerView(*parent._mapCursor, finished);
            }
        } else if (!next->isNull() && next->getType() == ValueType::List) {
            frames.push_back(openParameterList(memory, next->getList()));
        } else if (!next->isNull() && next->getType() == ValueType::Map) {
            frames.push_back(openParameterMap(memory, next->getMap()));
        } else if (frame._list) {
            writeParameterValue(memory, *frame._listCursor, *next);
        } else {
            writeParameterValue(memory, *frame._mapCursor, *next);
        }
    }
}

ListView materializeParameterList(LocalMemory* memory, const ParameterValue::List& elements) {
    const ParameterContainerFrame root = materializeParameterContainer(memory, openParameterList(memory, elements));

    return root._listCursor->getView();
}

MapView materializeParameterMap(LocalMemory* memory, const ParameterValue::Map& entries) {
    const ParameterContainerFrame root = materializeParameterContainer(memory, openParameterMap(memory, entries));

    return root._mapCursor->getView();
}

[[noreturn]] void throwParameterShapeMismatch(llvm::StringRef name, const ParameterValue& held, std::string_view reads) {
    throw IRException(fmt::format("Parameter ${} holds a {} where the program reads {}", name, held.getTypeName(), reads));
}

// Refuses a value bound to the list argument @param name whose elements the program would
// read under another type than @param listType names: a program compiled for one map is
// checked against the next. A null is one of every shape, and `any` takes every value.
void throwOnListShapeMismatch(llvm::StringRef name, mlir::Type listType, const ParameterValue& value) {
    struct Pending {
        mlir::Type _type;
        const ParameterValue* _value {nullptr};
    };

    std::vector<Pending> pending {{listType, &value}};

    while (!pending.empty()) {
        const Pending current = pending.back();
        pending.pop_back();

        const ParameterValue& held = *current._value;
        if (held.isNull() || mlir::isa<storage::AnyType>(current._type)) {
            continue;
        } else if (mlir::isa<mlir::NoneType>(current._type)) {
            throwParameterShapeMismatch(name, held, "a null");
        } else if (const auto list = mlir::dyn_cast<storage::ListType>(current._type)) {
            if (held.getType() != ValueType::List) {
                throwParameterShapeMismatch(name, held, "a list");
            }

            for (const ParameterValue& element : held.getList()) {
                pending.push_back({list.getElementType(), &element});
            }
        } else if (mlir::isa<storage::MapType>(current._type)) {
            if (held.getType() != ValueType::Map) {
                throwParameterShapeMismatch(name, held, "a map");
            }
        } else if (held.getType() != elementTypeToValueType(current._type)) {
            throwParameterShapeMismatch(name, held, ValueTypeName::value(elementTypeToValueType(current._type)));
        }
    }
}

}

void NLTranslator::bindParameterSchemaNames(mlir::storage::ParameterAttr parameter,
                                            const SchemaNameNouns& nouns,
                                            llvm::SmallVectorImpl<llvm::StringRef>& names) const {
    const llvm::StringRef name = parameter.getName();
    const ParameterValue& value = lookupParameter(name);

    if (value.isNull()) {
        throw IRException(fmt::format("Parameter ${} is null: {} is a String or a list of Strings", name, nouns._withArticle));
    }

    const ValueType type = value.getType();
    if (type == ValueType::String) {
        names.push_back(boundSchemaName(name, nouns, value.getString()));
    } else if (type == ValueType::List) {
        const ParameterValue::List& elements = value.getList();
        if (elements.empty()) {
            throw IRException(fmt::format("Parameter ${} holds no {}", name, nouns._noun));
        }

        for (size_t index = 0; index < elements.size(); index++) {
            const ParameterValue& element = elements[index];
            const bool isString = !element.isNull() && element.getType() == ValueType::String;
            if (!isString) {
                throw IRException(fmt::format("Element {} of parameter ${} has type {}: {} is a String",
                                              index,
                                              name,
                                              element.getTypeName(),
                                              nouns._withArticle));
            }

            names.push_back(boundSchemaName(name, nouns, element.getString()));
        }
    } else {
        throw IRException(fmt::format("Parameter ${} has type {}: {} is a String or a list of Strings",
                                      name,
                                      ValueTypeName::value(type),
                                      nouns._withArticle));
    }
}

void NLTranslator::bindParameterArguments(mlir::func::FuncOp function) {
    for (const mlir::BlockArgument argument : function.getArguments()) {
        const unsigned index = argument.getArgNumber();
        const mlir::StringAttr nameAttr = function.getArgAttrOfType<mlir::StringAttr>(index, storage::PARAMETER_NAME_ATTRIBUTE);
        if (!nameAttr) {
            throw IRException(fmt::format("Argument {} of main names no parameter", index));
        }

        const llvm::StringRef name = nameAttr.getValue();
        const ParameterValue& value = lookupParameter(name);
        const mlir::Type elementType = mlir::cast<nl::ChunkType>(argument.getType()).getElementType();

        _valueSlots[argument] = boundParameterColumn(name, elementType, value);
    }
}

Column* NLTranslator::boundParameterColumn(llvm::StringRef name, mlir::Type elementType, const ParameterValue& value) {
    if (mlir::isNullLiteralType(elementType)) {
        if (!value.isNull()) {
            throw IRException(fmt::format("Parameter ${} is typed as null but holds a {}", name, value.getTypeName()));
        }

        return _memory->alloc<ColumnConst<PropertyNull>>();
    }

    if (mlir::isa<storage::ListType>(elementType)) {
        if (value.isNull()) {
            throw IRException(fmt::format("Parameter ${} is typed List but holds a null", name));
        }

        throwOnListShapeMismatch(name, elementType, value);
        return allocBoundConstant<ListView>(_memory, materializeParameterList(_memory, value.getList()));
    } else if (mlir::isa<storage::MapType>(elementType)) {
        if (value.isNull() || value.getType() != ValueType::Map) {
            throwParameterShapeMismatch(name, value, "a map");
        }

        return allocBoundConstant<MapView>(_memory, materializeParameterMap(_memory, value.getMap()));
    }

    const ValueType argumentType = elementTypeToValueType(elementType);
    if (value.isNull()) {
        throw IRException(fmt::format("Parameter ${} is typed {} but holds a null", name, ValueTypeName::value(argumentType)));
    } else if (value.getType() != argumentType) {
        throw IRException(fmt::format("Parameter ${} is typed {} but holds a {}",
                                      name,
                                      ValueTypeName::value(argumentType),
                                      value.getTypeName()));
    }

    switch (argumentType) {
        case ValueType::Int64:
            return allocBoundConstant<types::Int64::Primitive>(_memory, value.getInt64());
        break;
        case ValueType::Double:
            return allocBoundConstant<types::Double::Primitive>(_memory, value.getDouble());
        break;
        case ValueType::Bool:
            return allocBoundConstant<types::Bool::Primitive>(_memory, CustomBool {value.getBool()});
        break;
        case ValueType::String:
            return allocBoundConstant<types::String::Primitive>(_memory, ownedCharacters(value.getString()));
        break;
        case ValueType::Duration:
            return allocBoundConstant<types::Duration::Primitive>(_memory, value.getDuration());
        break;
        case ValueType::DateTime:
            return allocBoundConstant<types::DateTime::Primitive>(_memory, value.getDateTime());
        break;
        case ValueType::Embedding:
            return allocBoundConstant<types::Embedding::Primitive>(_memory, _memory->embeddingBuffer().insert(value.getEmbedding()));
        break;
        case ValueType::Point:
            return allocBoundConstant<types::Point::Primitive>(_memory, value.getPoint());
        break;
        default:
            throw IRException(fmt::format("Parameter ${} is a {}, which a parameter cannot hold",
                                          name,
                                          ValueTypeName::value(argumentType)));
        break;
    }
}

const ParameterValue& NLTranslator::lookupParameter(llvm::StringRef name) const {
    const ParameterValue* value = _parameters ? _parameters->get(name) : nullptr;
    if (!value) {
        throw IRException(fmt::format("Parameter ${} is not defined", name));
    }

    return *value;
}

llvm::StringRef NLTranslator::boundSchemaName(llvm::StringRef parameterName,
                                              const SchemaNameNouns& nouns,
                                              const std::string& name) const {
    if (name.empty()) {
        throw IRException(fmt::format("Parameter ${} holds an empty {} name", parameterName, nouns._noun));
    }

    return name;
}
