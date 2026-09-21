#include "EntityPropertyView.h"

#include <vector>

using namespace db;

void EntityPropertyView::setProperty(PropertyTypeID propTypeID, const PropertyVariant& value) {
    for (PropertyView& prop : _props) {
        if (prop._id == propTypeID) {
            prop._value = value;
            return;
        }
    }

    PropertyView& prop = _props.emplace_back();
    prop._id = propTypeID;
    prop._value = value;
}

void EntityPropertyView::removeProperty(PropertyTypeID propTypeID) {
    const auto isProperty = [propTypeID](const PropertyView& prop) { return prop._id == propTypeID; };

    std::erase_if(_props, isProperty);
}
