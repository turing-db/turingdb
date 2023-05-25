#pragma once

namespace db {

class DB;
class EdgeType;
class Node;
class DBEntity;
class Property;

class TypeCheck {
public:
    TypeCheck(DB* db);

    bool checkEdge(const EdgeType* edgeType,
                   const Node* source,
                   const Node* target) const;
    bool checkEntityProperty(const DBEntity* entity, const Property& prop) const;
    bool checkProperty(const Property& prop) const;

private:
    DB* _db {nullptr};
};

}
