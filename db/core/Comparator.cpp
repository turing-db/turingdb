#include "Comparator.h"
#include "DB.h"
#include "Edge.h"
#include "EdgeMap.h"
#include "EdgeType.h"
#include "Network.h"
#include "Node.h"
#include "NodeType.h"
#include "Property.h"
#include "PropertyType.h"

namespace db {

// Containers
using PropertyTypes = std::map<StringRef, PropertyType*>;
using Networks = std::map<StringRef, Network*>;
using NodeTypes = std::map<StringRef, NodeType*>;
using EdgeTypes = std::map<StringRef, EdgeType*>;
using Properties = std::map<const PropertyType*, Value, DBObject::ObjectComparator>;
using Nodes = std::vector<Node*>;
using Edges = std::vector<Edge*>;
using NodeTypeSet = std::set<NodeType*, DBObject::ObjectComparator>;
using EdgeTypeSet = std::set<EdgeType*, DBObject::ObjectComparator>;

// Specialization declarations
template <>
bool Comparator<DBObject>::same(const DBObject*, const DBObject*);
template <>
bool Comparator<DBType>::same(const DBType*, const DBType*);
template <>
bool Comparator<DBEntity>::same(const DBEntity*, const DBEntity*);
template <>
bool Comparator<DBEntityType>::same(const DBEntityType*, const DBEntityType*);

template <>
bool Comparator<DB>::same(const DB*, const DB*);
template <>
bool Comparator<Network>::same(const Network*, const Network*);
template <>
bool Comparator<Property>::same(const Property*, const Property*);
template <>
bool Comparator<ValueType>::same(const ValueType*, const ValueType*);
template <>
bool Comparator<Value>::same(const Value*, const Value*);

template <>
bool Comparator<NodeType>::same(const NodeType*, const NodeType*);
template <>
bool Comparator<EdgeType>::same(const EdgeType*, const EdgeType*);
template <>
bool Comparator<PropertyType>::same(const PropertyType*, const PropertyType*);

template <>
bool Comparator<Node>::same(const Node*, const Node*);
template <>
bool Comparator<Edge>::same(const Edge*, const Edge*);

// Implementations

template <>
bool Comparator<Property>::same(const Property* p1, const Property* p2) {
    return p1->_type == p2->_type
        && Comparator<Value>::same(&p1->_value, &p2->_value);
}

template <>
bool Comparator<NodeType>::same(const NodeType* nt1, const NodeType* nt2) {
    return Comparator<DBEntityType>::same(nt1, nt2)
        && Comparator<EdgeTypeSet>::same(&nt1->_inEdgeTypes, &nt2->_inEdgeTypes)
        && Comparator<EdgeTypeSet>::same(&nt1->_outEdgeTypes, &nt2->_outEdgeTypes);
}

template <>
bool Comparator<EdgeType>::same(const EdgeType* et1, const EdgeType* et2) {
    return Comparator<DBEntityType>::same(et1, et2)
        && Comparator<NodeTypeSet>::same(&et1->_sourceTypes, &et2->_sourceTypes)
        && Comparator<NodeTypeSet>::same(&et1->_targetTypes, &et2->_targetTypes);
}

template <>
bool Comparator<ValueType>::same(const ValueType* vt1, const ValueType* vt2) {
    return vt1->_kind == vt2->_kind;
}

template <>
bool Comparator<Value>::same(const Value* vt1, const Value* vt2) {
    return vt1->_data == vt2->_data;
    return Comparator<ValueType>::same(&vt1->_type, &vt2->_type)
        && vt1->_data == vt2->_data;
}

template <>
bool Comparator<PropertyType>::same(const PropertyType* pt1,
                                    const PropertyType* pt2) {
    return Comparator<DBType>::same(pt1, pt2)
        && Comparator<DBObject>::same(pt1->_owner, pt2->_owner)
        && Comparator<ValueType>::same(&pt1->_valType, &pt2->_valType);
}


template <>
bool Comparator<DB>::same(const DB* db1, const DB* db2) {
    return Comparator<Networks>::same(&db1->_networks, &db2->_networks)
        && Comparator<NodeTypes>::same(&db1->_nodeTypes, &db2->_nodeTypes)
        && Comparator<EdgeTypes>::same(&db1->_edgeTypes, &db2->_edgeTypes);
}

}
