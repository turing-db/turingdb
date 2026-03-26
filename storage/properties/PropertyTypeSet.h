#pragma once

#include <vector>

#include "ID.h"

namespace db {

class PropertyTypeSet {
public:
    PropertyTypeSet();
    ~PropertyTypeSet();

    PropertyTypeSet(std::vector<PropertyTypeID>&& ids);

    PropertyTypeSet(const PropertyTypeSet&);
    PropertyTypeSet(PropertyTypeSet&&) noexcept;
    PropertyTypeSet& operator=(const PropertyTypeSet&);
    PropertyTypeSet& operator=(PropertyTypeSet&&) noexcept;

    void add(PropertyTypeID ptID) {
        _ids.push_back(ptID);
    }

    bool contains(PropertyTypeID ptID) const {
        return std::find(begin(), end(), ptID) != end();
    }

    std::vector<PropertyTypeID>::const_iterator begin() const { return _ids.begin(); }
    std::vector<PropertyTypeID>::const_iterator end() const { return _ids.end(); }

    size_t size() const { return _ids.size(); }
    void clear() { _ids.clear(); }

    const std::vector<PropertyTypeID>& get() const { return _ids; }
    std::vector<PropertyTypeID>& get() { return _ids; }

private:
    std::vector<PropertyTypeID> _ids;
};

}
