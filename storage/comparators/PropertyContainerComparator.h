#pragma once

#include <range/v3/view/zip.hpp>

#include "properties/PropertyContainer.h"

namespace db {

template <SupportedType T>
class TypedPropertyContainerComparator {
public:
    [[nodiscard]] static bool same(const TypedPropertyContainer<T>& a,
                                   const TypedPropertyContainer<T>& b) {
        namespace rv = ranges::views;

        if (a.size() != b.size()) {
            return false;
        }

        const auto& idsA = a.ids();
        const auto& idsB = b.ids();
        const auto& valuesA = a.all();
        const auto& valuesB = b.all();

        if (idsA.size() != idsB.size()) {
            return false;
        }

        for (const auto& [idA, idB, vA, vB] : rv::zip(idsA, idsB, valuesA, valuesB)) {
            if (idA != idB || vA != vB) {
                return false;
            }
        }

        return true;
    }
};

class PropertyContainerComparator {
public:
    [[nodiscard]] static bool same(const PropertyContainer* a,
                                   const PropertyContainer* b) {
        const auto valueType = a->getValueType();

        if (valueType != b->getValueType()) {
            return false;
        }

        switch (valueType) {
            case ValueType::UInt64: {
                if (!TypedPropertyContainerComparator<types::UInt64>::same(
                        a->cast<types::UInt64>(),
                        b->cast<types::UInt64>())) {
                    return false;
                }
                break;
            }

            case ValueType::Int64: {
                if (!TypedPropertyContainerComparator<types::Int64>::same(
                        a->cast<types::Int64>(),
                        b->cast<types::Int64>())) {
                    return false;
                }
                break;
            }

            case ValueType::Double: {
                if (!TypedPropertyContainerComparator<types::Double>::same(
                        a->cast<types::Double>(),
                        b->cast<types::Double>())) {
                    return false;
                }
                break;
            }

            case ValueType::String: {
                if (!TypedPropertyContainerComparator<types::String>::same(
                        a->cast<types::String>(),
                        b->cast<types::String>())) {
                    return false;
                }
                break;
            }

            case ValueType::Bool: {
                if (!TypedPropertyContainerComparator<types::Bool>::same(
                        a->cast<types::Bool>(),
                        b->cast<types::Bool>())) {
                    return false;
                }
                break;
            }

            case ValueType::Embedding: {
                const auto& embA = a->cast<types::Embedding>();
                const auto& embB = b->cast<types::Embedding>();
                if (embA.size() != embB.size()) {
                    return false;
                }
                const auto& idsA = embA.ids();
                const auto& idsB = embB.ids();
                for (size_t i = 0; i < embA.size(); i++) {
                    if (idsA[i] != idsB[i]) {
                        return false;
                    }
                    const auto& vA = embA.get(i);
                    const auto& vB = embB.get(i);
                    if (vA.size() != vB.size()) {
                        return false;
                    }
                    if (std::memcmp(vA.data(), vB.data(), vA.size() * sizeof(float)) != 0) {
                        return false;
                    }
                }
                break;
            }

            case ValueType::Invalid:
            case ValueType::_SIZE: {
                break;
            }
        }

        return true;
    }
};

}
