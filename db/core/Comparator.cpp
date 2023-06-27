#include "Comparator.h"
#include "DB.h"
#include "DBEntity.h"
#include "DBEntityType.h"
#include "Edge.h"
#include "EdgeMap.h"
#include "EdgeType.h"
#include "Network.h"
#include "Node.h"
#include "NodeType.h"
#include "Property.h"
#include "PropertyType.h"
#include "Value.h"
#include "ValueType.h"

namespace db {

template <typename T>
bool DBComparator::deepCompareMaps(const T* c1, const T* c2) {
    using U = std::remove_reference_t<decltype(*c1->begin()->second)>;

    if (c1->size() != c2->size()) {
        return false;
    }

    auto it1 = c1->cbegin();
    auto it2 = c2->cbegin();

    while (it1 != c1->cend()) {
        if (!DBComparator::same<U>(it1->second, it2->second)) {
            return false;
        }

        ++it1;
        ++it2;
    }

    return true;
}

template <typename T>
bool DBComparator::deepCompareVectors(const T* c1, const T* c2) {
    using U = std::remove_reference_t<decltype(**c1->begin())>;

    if (c1->size() != c2->size()) {
        return false;
    }

    auto it1 = c1->cbegin();
    auto it2 = c2->cbegin();

    while (it1 != c1->cend()) {
        if (!DBComparator::same<U>(*it1, *it2)) {
            return false;
        }

        ++it1;
        ++it2;
    }

    return true;
}

template <>
bool DBComparator::same<DBObject>(const DBObject* o1, const DBObject* o2) {
    return o1->_index == o2->_index;
}

template <>
bool DBComparator::same<DBType>(const DBType* t1, const DBType* t2) {
    return DBComparator::same<DBObject>(t1, t2)
        && StringRef::same(t1->_name, t2->_name);
}

template <>
bool DBComparator::same<ValueType>(const ValueType* vt1, const ValueType* vt2) {
    return vt1->_kind == vt2->_kind;
}

template <>
bool DBComparator::same<PropertyType>(const PropertyType* pt1,
                                      const PropertyType* pt2) {
    return DBComparator::same<DBType>(pt1, pt2)
        && DBComparator::same<DBObject>(pt1->_owner, pt2->_owner)
        && DBComparator::same<ValueType>(&pt1->_valType, &pt2->_valType);
}

template <>
bool DBComparator::same<DBEntityType>(const DBEntityType* entt1,
                                      const DBEntityType* entt2) {
    return DBComparator::same<DBType>(entt1, entt2)
        && deepCompareMaps(&entt1->_propTypes, &entt2->_propTypes);
}

template <>
bool DBComparator::same<Value>(const Value* vt1, const Value* vt2) {
    return DBComparator::same<ValueType>(&vt1->_type, &vt2->_type)
        && vt1->_data == vt2->_data;
}


/* Performes the following operations on two DBEntity::Properties containers:
 * - Comparison of their key (PropertyType*) indices
 * - Comparison of their values
 * */
template <>
bool DBComparator::same<DBEntity::Properties>(const DBEntity::Properties* props1,
                                              const DBEntity::Properties* props2) {
    if (props1->size() != props2->size()) {
        return false;
    }

    auto it1 = props1->cbegin();
    auto it2 = props2->cbegin();

    while (it1 != props1->cend()) {
        if (!DBComparator::same<DBObject>(it1->first, it2->first)) {
            return false;
        }

        if (!DBComparator::same<Value>(&it1->second, &it2->second)) {
            return false;
        }

        ++it1;
        ++it2;
    }

    return true;
}

template <>
bool DBComparator::same<DBEntity>(const DBEntity* e1, const DBEntity* e2) {
    return DBComparator::same<DBObject>(e1, e2)
        && DBComparator::same<DBEntityType>(e1->_type, e2->_type)
        && DBComparator::same<DBEntity::Properties>(&e1->_properties, &e2->_properties);
}

/* Performes the following operation on two EdgeType::NodeType containers:
 * - Comparison of the element indices
 * */
template <>
bool DBComparator::same<EdgeType::NodeTypes>(const EdgeType::NodeTypes* nt1,
                                             const EdgeType::NodeTypes* nt2) {
    if (nt1->size() != nt2->size()) {
        return false;
    }

    auto it1 = nt1->cbegin();
    auto it2 = nt2->cbegin();

    while (it1 != nt1->cend()) {
        if (!DBComparator::same<DBObject>(*it1, *it2)) {
            return false;
        }

        ++it1;
        ++it2;
    }

    return true;
}

/* Performes the following operation on two NodeType::EdgeType containers:
 * - Comparison of the element indices
 * */
template <>
bool DBComparator::same<NodeType::EdgeTypes>(const NodeType::EdgeTypes* et1,
                                             const NodeType::EdgeTypes* et2) {
    if (et1->size() != et2->size()) {
        return false;
    }

    auto it1 = et1->cbegin();
    auto it2 = et2->cbegin();

    while (it1 != et1->cend()) {
        if (!DBComparator::same<DBObject>(*it1, *it2)) {
            return false;
        }

        ++it1;
        ++it2;
    }

    return true;
}

/* Performes the following operations on two NodeTypes:
 * - Deep comparison of their PropertyTypes
 * - Comparison of their names
 * - Comparison of their in and out EdgeType indices
 * */
template <>
bool DBComparator::same<NodeType>(const NodeType* nt1, const NodeType* nt2) {
    return DBComparator::same<DBEntityType>(nt1, nt2)
        && DBComparator::same<NodeType::EdgeTypes>(&nt1->_inEdgeTypes, &nt2->_inEdgeTypes)
        && DBComparator::same<NodeType::EdgeTypes>(&nt1->_outEdgeTypes, &nt2->_outEdgeTypes);
}

/* Performes the following operations on two EdgeTypes:
 * - Deep comparison of their PropertyTypes
 * - Comparison of their names
 * - Comparison of their source and target NodeType indices
 * */
template <>
bool DBComparator::same<EdgeType>(const EdgeType* et1, const EdgeType* et2) {
    return DBComparator::same<DBEntityType>(et1, et2)
        && DBComparator::same<EdgeType::NodeTypes>(&et1->_sourceTypes, &et2->_sourceTypes)
        && DBComparator::same<EdgeType::NodeTypes>(&et1->_targetTypes, &et2->_targetTypes);
}

/* Performes the following operation on two EdgeMap containers:
 * - Comparion of the element indices
 * */
template <>
bool DBComparator::same<EdgeMap>(const EdgeMap* edges1, const EdgeMap* edges2) {
    if (edges1->_edges.size() != edges2->_edges.size()) {
        return false;
    }

    auto inIterator1 = edges1->_edges.cbegin();
    auto inIterator2 = edges2->_edges.cbegin();

    while (inIterator1 != edges1->_edges.cend()) {
        if (!DBComparator::same<DBObject>(inIterator1->first, inIterator2->first)) {
            return false;
        }

        ++inIterator1;
        ++inIterator2;
    }

    return true;
}

/* Performes the following operations on two Nodes:
 * - Comparison of their properties
 * - Comparison of their names
 * - Comparison of their networks indices
 * - Comparison of their in and out edge indices
 * */
template <>
bool DBComparator::same<Node>(const Node* n1, const Node* n2) {
    return DBComparator::same<DBEntity>(n1, n2)
        && StringRef::same(n1->_name, n2->_name)
        && DBComparator::same<DBObject>(n1->_net, n2->_net)
        && DBComparator::same<EdgeMap>(&n1->_inEdges, &n2->_inEdges)
        && DBComparator::same<EdgeMap>(&n1->_outEdges, &n2->_outEdges);
}

/* Performes the following operations on two Edges:
 * - Comparison of their properties
 * - Comparison of their source and target node indices
 * */
template <>
bool DBComparator::same<Edge>(const Edge* e1, const Edge* e2) {
    return DBComparator::same<DBEntity>(e1, e2)
        && DBComparator::same<DBObject>(e1->_source, e2->_source)
        && DBComparator::same<DBObject>(e1->_target, e2->_target);
}

template <>
bool DBComparator::same<Network>(const Network* net1, const Network* net2) {
    return DBComparator::same<DBObject>(net1, net2)
        && StringRef::same(net1->_name, net2->_name)
        && deepCompareVectors(&net1->_nodes, &net2->_nodes)
        && deepCompareVectors(&net1->_edges, &net2->_edges);
}


bool DBComparator::same(const DB* db1, const DB* db2) {
    return deepCompareMaps(&db1->_nodeTypes, &db2->_nodeTypes)
        && deepCompareMaps(&db1->_edgeTypes, &db2->_edgeTypes)
        && deepCompareMaps(&db1->_networks, &db2->_networks);
}

}
