#pragma once

#include <vector>

#include "ID.h"

namespace db {

class PropertyTypeSet {
public:
    using PropertyTypeIDVector = std::vector<PropertyTypeID>;

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

    PropertyTypeIDVector::const_iterator begin() const { return _ids.begin(); }
    PropertyTypeIDVector::const_iterator end() const { return _ids.end(); }

    size_t size() const { return _ids.size(); }
    void clear() { _ids.clear(); }

    const PropertyTypeIDVector& get() const { return _ids; }
    PropertyTypeIDVector& get() { return _ids; }

private:
    PropertyTypeIDVector _ids;
};

}
