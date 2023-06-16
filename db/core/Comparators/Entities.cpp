#include "Comparator.h"
#include "Edge.h"
#include "Network.h"
#include "Node.h"

namespace db {

template <>
bool Comparator<Node>::same(const Node* n1, const Node* n2) {
    return Comparator<DBEntity>::same(n1, n2)
        && StringRef::same(n1->_name, n2->_name)
        && Comparator<DBObject>::same(n1->_net, n2->_net)
        && Comparator<EdgeMap>::same(&n1->_inEdges, &n2->_inEdges)
        && Comparator<EdgeMap>::same(&n1->_outEdges, &n2->_outEdges);
}

template <>
bool Comparator<Edge>::same(const Edge* e1, const Edge* e2) {
    return Comparator<DBEntity>::same(e1, e2)
        && Comparator<DBEntity>::same(e1->_source, e2->_source)
        && Comparator<DBEntity>::same(e1->_target, e2->_target);
}

}
