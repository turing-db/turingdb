#pragma once

#include <vector>
#include <unordered_map>

#include "StringRef.h"

namespace db {

class Property;
class EdgeType;
class Writeback;

class ComponentType {
public:
    friend Writeback;

    StringRef getName() const { return _name; }

    Property* getProperty(StringRef name) const;

private:
    StringRef _name;
    std::vector<Property*> _properties;
    std::unordered_map<StringRef, Property*> _propMap;
    std::vector<EdgeType*> _inEdgeTypes;
    std::vector<EdgeType*> _outEdgeTypes;
    std::unordered_map<StringRef, EdgeType*> _edgeMap;

    ComponentType(StringRef name);
    ~ComponentType();
    void addProperty(Property* prop);
    void addEdgeType(EdgeType* edgeType);
};

}
