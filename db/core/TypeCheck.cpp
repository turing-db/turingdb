#include "TypeCheck.h"

#include "EdgeType.h"
#include "Node.h"
#include "PropertyType.h"

using namespace db;

TypeCheck::TypeCheck(DB* db)
    : _db(db)
{
}

bool TypeCheck::checkEdge(const EdgeType* edgeType,
                          const Node* source,
                          const Node* target) const {
    if (!edgeType || !source || !target) {
        return false;
    }

    if (!edgeType->hasSourceType(source->getType())) {
        return false;
    }

    if (!edgeType->hasTargetType(target->getType())) {
        return false;
    }

    return true;
}

bool TypeCheck::checkProperty(const Property& prop) const {
    if (!prop.isValid()) {
        return false;
    }

    return (prop.getType()->getValueType() == prop.getValue().getType());
}

bool TypeCheck::checkEntityProperty(const DBEntity* entity,
                                    const Property& prop) const {
    if (!entity) {
        return false;
    }

    if (!checkProperty(prop)) {
        return false;
    }

    if (!entity->getType()->getPropertyType(prop.getType()->getName())) {
        return false;
    }

    return true;
}
