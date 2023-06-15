#include "Comparator.h"
#include "DBEntity.h"
#include "DBEntityType.h"

namespace db {

// Containers
using PropertyTypes = std::map<StringRef, PropertyType*>;

// Implementations
template <>
bool Comparator<DBObject>::same(const DBObject* o1, const DBObject* o2) {
    return o1->_index == o2->_index;
}

template <>
bool Comparator<DBType>::same(const DBType* t1, const DBType* t2) {
    return Comparator<DBObject>::same(t1, t2)
        && t1->_name == t2->_name;
}

template <>
bool Comparator<DBEntityType>::same(const DBEntityType* entt1, const DBEntityType* entt2) {
    bool condition1 = Comparator<DBType>::same(entt1, entt2);
    bool condition2 = Comparator<PropertyTypes>::same(&entt1->_propTypes, &entt2->_propTypes);
    return condition1 && condition2;
}

template <>
bool Comparator<DBEntity>::same(const DBEntity* e1, const DBEntity* e2) {
    return Comparator<DBObject>::same(e1, e2)
        && Comparator<DBEntityType>::same(e1->_type, e2->_type)
        && Comparator<DBEntity::Properties>::same(&e1->_properties, &e2->_properties);
}

}
