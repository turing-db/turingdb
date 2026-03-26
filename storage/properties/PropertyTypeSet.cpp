#include "PropertyTypeSet.h"

using namespace db;

PropertyTypeSet::PropertyTypeSet() = default;

PropertyTypeSet::~PropertyTypeSet() = default;

PropertyTypeSet::PropertyTypeSet(std::vector<PropertyTypeID>&& ids)
    : _ids(std::move(ids))
{
}

PropertyTypeSet::PropertyTypeSet(const PropertyTypeSet&) = default;

PropertyTypeSet::PropertyTypeSet(PropertyTypeSet&&) noexcept = default;

PropertyTypeSet& PropertyTypeSet::operator=(const PropertyTypeSet&) = default;

PropertyTypeSet& PropertyTypeSet::operator=(PropertyTypeSet&&) noexcept = default;
