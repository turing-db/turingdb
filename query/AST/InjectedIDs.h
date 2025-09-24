#pragma once

#include <vector>

#include "ID.h"

namespace db {

class ASTContext;

class InjectedIDs {
public:
    friend ASTContext;

    static InjectedIDs* create(ASTContext* ctxt);

    void addID(NodeID id);
    std::vector<NodeID>& getIDs() { return _nodeIDs; };

    size_t size() { return _nodeIDs.size(); }

    bool empty() { return _nodeIDs.empty(); }

private:
    std::vector<NodeID> _nodeIDs;

    InjectedIDs();
    ~InjectedIDs();
};

}
