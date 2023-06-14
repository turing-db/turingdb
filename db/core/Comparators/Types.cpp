#include "Comparator.h"
#include "EdgeType.h"
#include "NodeType.h"
#include "PropertyType.h"
#include "ValueType.h"

namespace db {

using NodeTypeSet = std::set<NodeType*, DBObject::ObjectComparator>;
using EdgeTypeSet = std::set<EdgeType*, DBObject::ObjectComparator>;

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
bool Comparator<PropertyType>::same(const PropertyType* pt1,
                                    const PropertyType* pt2) {
    return Comparator<DBType>::same(pt1, pt2)
        && Comparator<DBObject>::same(pt1->_owner, pt2->_owner)
        && Comparator<ValueType>::same(&pt1->_valType, &pt2->_valType);
}
}
