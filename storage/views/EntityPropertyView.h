#pragma once

#include "ID.h"
#include "metadata/PropertyType.h"
#include "PropertyView.h"
#include "Panic.h"

namespace db {

class PropertyManager;
class NodeView;
class EdgeView;

class EntityPropertyView {
public:
    EntityPropertyView(const EntityPropertyView&) = default;
    EntityPropertyView(EntityPropertyView&&) noexcept = default;
    EntityPropertyView& operator=(const EntityPropertyView&) = default;
    EntityPropertyView& operator=(EntityPropertyView&&) noexcept = default;
    ~EntityPropertyView() = default;

    bool hasProperty(PropertyTypeID ptID) const {
        return std::find_if(
                   _props.begin(),
                   _props.end(),
                   [&](const auto& prop) {
                       return prop._id == ptID;
                   })
            != _props.end();
    }

    const PropertyView* tryGetProperty(PropertyTypeID propTypeID) const {
        for (const auto& v : _props) {
            if (v._id == propTypeID) {
                return &v;
            }
        }
        return nullptr;
    }

    template <SupportedType T>
    const T::Primitive* tryGetProperty(PropertyTypeID propTypeID) const {
        for (const auto& v : _props) {
            if (v._id == propTypeID) {
                return std::get<const typename T::Primitive*>(v._value);
            }
        }
        return nullptr;
    }

    const PropertyView& getProperty(PropertyTypeID propTypeID) const {
        for (const auto& v : _props) {
            if (v._id == propTypeID) {
                return v;
            }
        }
        panic("Property type {} could not be found", propTypeID);
    }

    template <SupportedType T>
    const T::Primitive& getProperty(PropertyTypeID propTypeID) const {
        for (const auto& v : _props) {
            if (v._id == propTypeID) {
                return *std::get<const typename T::Primitive*>(v._value);
            }
        }
        panic("Property type {} could not be found", propTypeID);
    }

    size_t getCount() const {
        return _props.size();
    }

    std::vector<PropertyView>::const_iterator begin() const {
        return _props.begin();
    }
    std::vector<PropertyView>::const_iterator cbegin() const {
        return _props.cbegin();
    }
    std::vector<PropertyView>::const_iterator end() const {
        return _props.end();
    }
    std::vector<PropertyView>::const_iterator cend() const {
        return _props.cend();
    }

private:
    friend PropertyManager;
    friend NodeView;
    friend EdgeView;

    std::vector<PropertyView> _props;

    EntityPropertyView() = default;
};

static_assert((size_t)ValueType::_SIZE == 6);

}
