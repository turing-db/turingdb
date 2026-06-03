#pragma once

#include <string_view>
#include <string>
#include <vector>
#include <memory>
#include <shared_mutex>
#include <mutex>

#include "RWSpinLock.h"
#include "StringHashMap.h"

namespace db {

template <typename ObjectT>
class ObjectMap {
public:
    class SlotReservation;

    class ObjectSlot {
    public:
        friend SlotReservation;

        bool isFree() const {
            return _obj == nullptr;
        }

        ObjectT* getObject() const {
            return _obj.get();
        }

    private:
        std::unique_ptr<ObjectT> _obj;
    };

    class SlotReservation {
    public:
        SlotReservation() = default;

        SlotReservation(const SlotReservation&) = delete;
        SlotReservation& operator=(const SlotReservation&) = delete;

        SlotReservation(ObjectMap* map,
                        ObjectSlot* slot,
                        std::string_view name)
            : _map(map),
            _slot(slot),
            _name(name)
        {
        }

        ~SlotReservation() {
            if (_slot && _map) {
                // Slot has not been published, erase
                _map->removeSlot(_name);
            }
        }

        bool isValid() const {
            return _slot && _map;
        }

        void publish(std::unique_ptr<ObjectT>&& obj) {
            // We acquire a short lock on the spinlock to publish
            {
                std::unique_lock<RWSpinLock> lock(_map->_mapLock);
                _slot->_obj = std::move(obj);
                ++_map->_size;
            }

            _slot = nullptr;
            _map = nullptr;
        }

    private:
        ObjectMap* _map {nullptr};
        ObjectSlot* _slot {nullptr};
        std::string_view _name;
    };

    ObjectMap()
    {
    }

    ObjectMap(const ObjectMap&) = delete;
    ObjectMap& operator=(const ObjectMap&) = delete;

    ~ObjectMap() {
    }

    SlotReservation reserve(std::string_view name) {
        std::unique_lock<RWSpinLock> lock(_mapLock);

        const auto [it, inserted] = _map.try_emplace(std::string(name));
        if (!inserted) {
            return SlotReservation();
        }

        // We allocate the slot in the critical section
        // to not do an allocation for nothing if try_emplace fails
        auto slot = std::make_unique<ObjectSlot>();
        ObjectSlot* slotPtr = slot.get();
        it->second = std::move(slot);

        return SlotReservation(this, slotPtr, name);
    }

    ObjectSlot* getObject(std::string_view name) const {
        std::shared_lock<RWSpinLock> lock(_mapLock);

        const auto it = _map.find(name);
        if (it == _map.end()) {
            return nullptr;
        }

        ObjectSlot* slot = it->second.get();

        // An object slot should not be visible
        // if the slot is nullptr (being constructed) or
        // if the object contained in the slot is not published yet
        if (!slot || slot->isFree()) {
            return nullptr;
        }

        return slot;
    }

    size_t size() const {
        std::shared_lock<RWSpinLock> lock(_mapLock);
        return _size;
    }

    void listNames(std::vector<std::string_view>& names) const {
        std::shared_lock<RWSpinLock> lock(_mapLock);

        for (const auto& [name, slot] : _map) {
            if (slot && !slot->isFree()) {
                names.push_back(name);
            }
        }
    }

private:
    mutable RWSpinLock _mapLock;
    StringHashMap<std::string, std::unique_ptr<ObjectSlot>> _map;
    size_t _size {0};

    void removeSlot(std::string_view name) {
        std::unique_lock<RWSpinLock> lock(_mapLock);
        _map.erase(name);
    }
};

}
