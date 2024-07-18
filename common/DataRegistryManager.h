#pragma once

#include <cstdint>
#include <variant>
#include <vector>

#include "BioAssert.h"

using RegID = uint8_t;
using ContainerID = uint8_t;

template <typename... TTypes>
class DataRegistryManager {
public:
    using Variant = std::variant<std::vector<TTypes>...>;

    struct ObjectID {
        ContainerID _container {0};
        size_t _index {0};
    };

    constexpr DataRegistryManager() = default;
    constexpr DataRegistryManager(DataRegistryManager&&) = default;
    constexpr DataRegistryManager& operator=(DataRegistryManager&&) = default;
    constexpr ~DataRegistryManager() = default;

    DataRegistryManager(const DataRegistryManager&) = delete;
    DataRegistryManager& operator=(const DataRegistryManager&) = delete;

    template <typename T>
    constexpr ContainerID registerType() {
        const ContainerID id = _containers.size();
        _containers.emplace_back(std::vector<T> {});
        return id;
    }

    template <typename T>
    constexpr RegID add(ContainerID cID, T&& v) {
        msgbioassert(cID < _containers.size(),
                     "DataRegistryManager does not have this type. "
                     "You forgot to call registerType()");
        auto& c = getContainer<T>(cID);
        const RegID i = _regInfo.size();
        _regInfo.emplace_back(ObjectID {
            ._container = cID,
            ._index = c.size(),
        });
        c.emplace_back(std::forward<T>(v));
        return i;
    }

    template <typename T>
    constexpr T& get(RegID i) {
        const ObjectID& id = _regInfo.at(i);
        auto& c = getContainer<T>(id._container);
        return c.at(id._index);
    }

    template <typename T>
    constexpr const T& get(RegID i) const {
        const ObjectID& id = _regInfo.at(i);
        auto& c = getContainer<T>(id._container);
        return c.at(id._index);
    }

    template <typename T>
    constexpr const std::vector<T>& getContainer(ContainerID id) const {
        return std::get<std::vector<T>>(_containers.at(id));
    }

    template <typename T>
    constexpr std::vector<T>& getContainer(ContainerID id) {
        return std::get<std::vector<T>>(_containers.at(id));
    }

    constexpr void* getAddress(RegID regID) const {
        const auto& reg = _regInfo[regID];
        const size_t i = reg._index;
        const ContainerID cID = reg._container;
        const Variant& c = _containers.at(cID);
        return std::visit(
            [i]<typename T>(const std::vector<T>& container) {
                return (void*)&container[i];
            },
            c);
    }

    constexpr void clear() {
        for (auto& v : _containers) {
            std::visit([](auto& container) {
                container.clear();
            }, v);
        }
        _regInfo.clear();
    }

private:
    std::vector<Variant> _containers;
    std::vector<ObjectID> _regInfo;
};
