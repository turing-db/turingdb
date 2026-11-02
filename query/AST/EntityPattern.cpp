#include "EntityPattern.h"

#include "Literal.h"

using namespace db;

EntityPattern::EntityPattern()
{
}

EntityPattern::~EntityPattern() {
}

void EntityPattern::setProperties(MapLiteral* properties) {
    if (properties) {
        properties->dropShadowedEntries();
    }

    _properties = properties;
}
