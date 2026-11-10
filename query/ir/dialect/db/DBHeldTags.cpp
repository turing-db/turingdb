#include "DBHeldTags.h"

#include <stdint.h>
#include <utility>
#include <vector>

#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinAttributes.h"
#include "mlir/IR/BuiltinTypes.h"
#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/DenseSet.h"
#include "llvm/ADT/SmallVector.h"

#include "DBColumnElement.h"
#include "DBOps.h"
#include "IRLiteralList.h"
#include "NullLiteralType.h"
#include "StorageTypes.h"

using namespace db;

namespace storage = mlir::storage;

namespace {

using TagMask = uint32_t;

constexpr size_t NO_READ = 0;

TagMask tagBit(CellTag tag) {
    return TagMask {1} << static_cast<uint8_t>(tag);
}

CellTag integerTag(mlir::IntegerType integer) {
    if (integer.getWidth() == 1) {
        return CellTag::Bool;
    } else if (integer.isUnsigned()) {
        return CellTag::UInt;
    }

    return CellTag::Int;
}

mlir::Type stripNullable(mlir::Type element) {
    const storage::NullableType nullable = mlir::dyn_cast<storage::NullableType>(element);
    return nullable ? nullable.getValueType() : element;
}

void collectCarriedColumns(mlir::Value value, llvm::SmallVectorImpl<mlir::Value>& carried) {
    mlir::Operation* owner = value.getDefiningOp();
    if (const mlir::BlockArgument argument = mlir::dyn_cast<mlir::BlockArgument>(value)) {
        owner = argument.getOwner()->getParentOp();
    }

    mlir::db::ColumnCarrierOpInterface carrier = mlir::dyn_cast_or_null<mlir::db::ColumnCarrierOpInterface>(owner);
    if (carrier) {
        carrier.collectCarriedColumns(value, carried);
    }
}

mlir::IntegerAttr literalPosition(mlir::Value index) {
    mlir::db::ConstantOp constant = index.getDefiningOp<mlir::db::ConstantOp>();
    const mlir::Attribute literal = constant ? constant.getValue() : mlir::Attribute();
    const bool holdsAPosition = literal && literalTag(literal) == CellTag::Int;

    return holdsAPosition ? mlir::cast<mlir::IntegerAttr>(literal) : mlir::IntegerAttr();
}

// One read off a value: the key of a map, or an element of a list where the key is null, the
// one at the position where it is known
struct ReadStep {
    mlir::StringAttr _key;
    mlir::IntegerAttr _position;
    size_t _rest {NO_READ};
};

// Walks a column back to where its values were made, through the ops carrying it and from
// each read off it to the value read, until every value it reaches is a literal, the result
// of a typed op, or one it cannot follow. A value is reached at a path: the reads still to
// take off it to arrive at a value of the column.
class HeldTagWalk {
public:
    explicit HeldTagWalk(TagMask tags)
        : _tags(tags)
    {
        _steps.push_back(ReadStep {});
    }

    bool holdsOnlyTags(mlir::Value column) {
        reach(column, NO_READ);

        while (!_worklist.empty()) {
            const Reach next = _worklist.pop_back_val();
            const bool holds = next._value ? walkValue(next._value, next._path) : walkLiteral(next._literal, next._path);
            if (!holds) {
                return false;
            }
        }

        return true;
    }

private:
    struct Reach {
        mlir::Value _value;
        mlir::Attribute _literal;
        size_t _path {NO_READ};
    };

    using ReachKey = std::pair<const void*, size_t>;

    TagMask _tags {0};
    std::vector<ReadStep> _steps;
    llvm::DenseMap<ReachKey, size_t> _paths;
    llvm::SmallVector<Reach> _worklist;
    llvm::DenseSet<ReachKey> _reached;

    bool holds(CellTag tag) const {
        return tag == CellTag::Null || (_tags & tagBit(tag)) != 0;
    }

    size_t prepend(const ReadStep& step) {
        const mlir::Attribute read = step._key ? mlir::Attribute(step._key) : mlir::Attribute(step._position);
        const auto [entry, inserted] = _paths.try_emplace(ReachKey {read.getAsOpaquePointer(), step._rest}, _steps.size());
        if (inserted) {
            _steps.push_back(step);
        }

        return entry->second;
    }

    size_t prepend(mlir::StringAttr key, size_t rest) {
        return prepend(ReadStep {._key = key, ._rest = rest});
    }

    size_t prependElement(size_t rest) {
        return prepend(ReadStep {._rest = rest});
    }

    size_t prependElementAt(mlir::IntegerAttr position, size_t rest) {
        return prepend(ReadStep {._position = position, ._rest = rest});
    }

    // A position counts from the end where it is negative, and reads null past either end
    template <typename Elements>
    void reachElements(Elements elements, const ReadStep& step) {
        if (!step._position) {
            for (const auto element : elements) {
                reach(element, step._rest);
            }

            return;
        }

        size_t index = 0;
        if (listPositionIndex(elements.size(), step._position.getValue().getSExtValue(), index)) {
            reach(elements[index], step._rest);
        }
    }

    void reach(mlir::Value value, size_t path) {
        if (_reached.insert(ReachKey {value.getAsOpaquePointer(), path}).second) {
            _worklist.push_back(Reach {._value = value, ._path = path});
        }
    }

    void reach(mlir::Attribute literal, size_t path) {
        if (_reached.insert(ReachKey {literal.getAsOpaquePointer(), path}).second) {
            _worklist.push_back(Reach {._literal = literal, ._path = path});
        }
    }

    bool reachRegionInput(mlir::BlockArgument argument, size_t path) {
        if (const mlir::Value carriedColumn = mlir::db::carriedColumnOf(argument)) {
            reach(carriedColumn, path);
            return true;
        } else if (const mlir::Value list = mlir::db::elementSourceOf(argument)) {
            reach(list, prependElement(path));
            return true;
        }

        return false;
    }

    bool walkValue(mlir::Value value, size_t path) {
        const mlir::db::ColumnType column = mlir::dyn_cast<mlir::db::ColumnType>(value.getType());
        if (!column) {
            return false;
        } else if (mlir::isNullLiteralType(column.getType())) {
            return true;
        }

        const mlir::Type element = inferredColumnElement(value);
        const bool typed = !mlir::isa<mlir::NoneType, storage::AnyType>(element);
        if (typed && path == NO_READ) {
            return holds(elementTag(element));
        }

        const mlir::BlockArgument argument = mlir::dyn_cast<mlir::BlockArgument>(value);
        if (argument && reachRegionInput(argument, path)) {
            return true;
        }

        llvm::SmallVector<mlir::Value, 4> carried;
        collectCarriedColumns(value, carried);

        for (const mlir::Value carriedColumn : carried) {
            reach(carriedColumn, path);
        }

        if (!carried.empty()) {
            return true;
        }

        return walkMadeValue(value, element, path);
    }

    bool walkMadeValue(mlir::Value value, mlir::Type element, size_t path) {
        mlir::Operation* const made = value.getDefiningOp();

        if (mlir::db::ConstantOp constant = mlir::dyn_cast_or_null<mlir::db::ConstantOp>(made)) {
            reach(constant.getValue(), path);
        } else if (mlir::db::UnwindConst unwind = mlir::dyn_cast_or_null<mlir::db::UnwindConst>(made)) {
            for (const mlir::Attribute literal : unwind.getElements()) {
                reach(literal, path);
            }
        } else if (mlir::db::StaticMapKey keyRead = mlir::dyn_cast_or_null<mlir::db::StaticMapKey>(made)) {
            reach(keyRead.getMap(), prepend(keyRead.getKeyAttr(), path));
        } else if (mlir::db::ListIndex index = mlir::dyn_cast_or_null<mlir::db::ListIndex>(made)) {
            reach(index.getLhs(), prependElementAt(literalPosition(index.getRhs()), path));
        } else if (mlir::isa_and_nonnull<mlir::db::Head, mlir::db::Last>(made)) {
            const int64_t position = mlir::isa<mlir::db::Head>(made) ? 0 : -1;
            const mlir::IntegerAttr positionLiteral = mlir::Builder(made->getContext()).getI64IntegerAttr(position);

            reach(made->getOperand(0), prependElementAt(positionLiteral, path));
        } else if (mlir::db::Unwind unwind = mlir::dyn_cast_or_null<mlir::db::Unwind>(made)) {
            reach(unwind.getSource(), prependElement(path));
        } else if (mlir::db::UnwindComputed unwind = mlir::dyn_cast_or_null<mlir::db::UnwindComputed>(made)) {
            mlir::Block& sourceBlock = unwind.getSource().front();
            mlir::db::UnwindYield yield = mlir::cast<mlir::db::UnwindYield>(sourceBlock.getTerminator());
            reach(yield.getSource(), prependElement(path));
        } else if (mlir::db::MakeList list = mlir::dyn_cast_or_null<mlir::db::MakeList>(made)) {
            return walkMadeList(list, path);
        } else if (mlir::db::MakeMap map = mlir::dyn_cast_or_null<mlir::db::MakeMap>(made)) {
            return walkMadeMap(map, path);
        } else if (mlir::db::MergeMaps merge = mlir::dyn_cast_or_null<mlir::db::MergeMaps>(made)) {
            return walkMergedMaps(merge, path);
        } else {
            return walkType(element, path);
        }

        return true;
    }

    bool walkMadeList(mlir::db::MakeList list, size_t path) {
        if (path == NO_READ) {
            return holds(CellTag::ListView);
        }

        const ReadStep step = _steps[path];
        if (step._key) {
            return false;
        }

        reachElements(list.getElements(), step);

        return true;
    }

    // A key missing from the map reads null, and the map iterated is a list of one
    bool walkMadeMap(mlir::db::MakeMap map, size_t path) {
        if (path == NO_READ) {
            return holds(CellTag::MapView);
        }

        const ReadStep step = _steps[path];
        if (!step._key) {
            reach(map.getResult(), step._rest);
            return true;
        }

        const mlir::ArrayAttr keys = map.getKeys();
        const mlir::OperandRange values = map.getValues();

        for (size_t keyIndex = 0; keyIndex < keys.size(); keyIndex++) {
            if (keys[keyIndex] == step._key) {
                reach(values[keyIndex], step._rest);
            }
        }

        return true;
    }

    // A key of the merge reads the entry of one of the maps, or null
    bool walkMergedMaps(mlir::db::MergeMaps merge, size_t path) {
        if (path == NO_READ) {
            return holds(CellTag::MapView);
        }

        const ReadStep step = _steps[path];
        if (!step._key) {
            reach(merge.getResult(), step._rest);
            return true;
        }

        for (const mlir::Value map : merge.getMaps()) {
            reach(map, path);
        }

        return true;
    }

    // The values of a map read out of a typed column are type-erased cells, and the properties
    // of a node or an edge are not known here, so a key read off one can hold anything
    bool walkType(mlir::Type element, size_t path) {
        mlir::Type read = element;
        size_t remaining = path;

        while (remaining != NO_READ) {
            const ReadStep step = _steps[remaining];
            if (step._key) {
                return false;
            }

            const storage::ListType list = mlir::dyn_cast<storage::ListType>(read);
            if (list) {
                read = stripNullable(list.getElementType());
            }

            remaining = step._rest;
        }

        return !mlir::isa<mlir::NoneType, storage::AnyType>(read) && holds(elementTag(read));
    }

    // Anything read off null is null, a key missing from a map reads null, and a value that is
    // no list is iterated as a list of one
    bool walkLiteral(mlir::Attribute literal, size_t path) {
        if (path == NO_READ) {
            return holds(literalTag(literal));
        } else if (mlir::isa<mlir::UnitAttr>(literal)) {
            return true;
        }

        const ReadStep step = _steps[path];
        if (step._key) {
            const mlir::DictionaryAttr map = mlir::dyn_cast<mlir::DictionaryAttr>(literal);
            if (!map) {
                return false;
            }

            const mlir::Attribute entry = map.get(step._key);
            if (entry) {
                reach(entry, step._rest);
            }

            return true;
        }

        const mlir::ArrayAttr list = mlir::dyn_cast<mlir::ArrayAttr>(literal);
        if (!list) {
            reach(literal, step._rest);
            return true;
        }

        reachElements(list, step);

        return true;
    }
};

}

bool db::holdsOnlyNullOr(mlir::Value column, std::initializer_list<CellTag> tags) {
    TagMask mask = 0;
    for (const CellTag tag : tags) {
        mask |= tagBit(tag);
    }

    HeldTagWalk walk(mask);

    return walk.holdsOnlyTags(column);
}

bool db::holdsOnlyKeyedValues(mlir::Value column) {
    return holdsOnlyNullOr(column, {CellTag::MapView, CellTag::NodeID, CellTag::EdgeID});
}

bool db::holdsOnlyLists(mlir::Value column) {
    return holdsOnlyNullOr(column, {CellTag::ListView});
}

CellTag db::elementTag(mlir::Type element) {
    if (mlir::isa<storage::MapType>(element)) {
        return CellTag::MapView;
    } else if (mlir::isa<storage::ListType>(element)) {
        return CellTag::ListView;
    } else if (mlir::isa<storage::NodeIDType>(element)) {
        return CellTag::NodeID;
    } else if (mlir::isa<storage::EdgeIDType>(element)) {
        return CellTag::EdgeID;
    } else if (mlir::isa<storage::StringType>(element)) {
        return CellTag::String;
    } else if (mlir::isa<storage::BoolType>(element)) {
        return CellTag::Bool;
    } else if (const mlir::IntegerType integer = mlir::dyn_cast<mlir::IntegerType>(element)) {
        return integerTag(integer);
    } else if (mlir::isa<mlir::FloatType>(element)) {
        return CellTag::Double;
    } else if (mlir::isa<storage::DateTimeType>(element)) {
        return CellTag::DateTime;
    } else if (mlir::isa<storage::DurationType>(element)) {
        return CellTag::Duration;
    } else if (mlir::isa<storage::PointType>(element)) {
        return CellTag::Point;
    } else if (mlir::isa<storage::EmbeddingType>(element)) {
        return CellTag::Embedding;
    }

    return CellTag::INVALID;
}

// A list or a map holds a null as a unit, and a db.constant the null literal as an empty string
// of the null literal's type
CellTag db::literalTag(mlir::Attribute literal) {
    const mlir::TypedAttr typed = mlir::dyn_cast<mlir::TypedAttr>(literal);

    if (mlir::isa<mlir::UnitAttr>(literal) || (typed && mlir::isNullLiteralType(typed.getType()))) {
        return CellTag::Null;
    } else if (mlir::isa<mlir::DictionaryAttr>(literal)) {
        return CellTag::MapView;
    } else if (mlir::isa<mlir::ArrayAttr>(literal)) {
        return CellTag::ListView;
    } else if (mlir::isa<mlir::StringAttr>(literal)) {
        return CellTag::String;
    } else if (const mlir::IntegerAttr integer = mlir::dyn_cast<mlir::IntegerAttr>(literal)) {
        const mlir::IntegerType integerType = mlir::dyn_cast<mlir::IntegerType>(integer.getType());
        return integerType ? integerTag(integerType) : CellTag::INVALID;
    } else if (mlir::isa<mlir::FloatAttr>(literal)) {
        return CellTag::Double;
    }

    return CellTag::INVALID;
}
