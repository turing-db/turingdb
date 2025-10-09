#pragma once

#include <vector>

#include "ID.h"
#include "ASTContext.h"

namespace db {

template <TypedInternalID IDT>
class DeletedIDs {
public:
    friend ASTContext;

    static DeletedIDs<IDT>* create(ASTContext* ctxt);

    size_t size() { return _deletedIDs.size(); }

    void addID(IDT id);

    std::vector<IDT>& getIDs() { return _deletedIDs; }

private:
    std::vector<IDT> _deletedIDs;

    DeletedIDs() = default;
    ~DeletedIDs() = default;
};

template <TypedInternalID IDT>
void DeletedIDs<IDT>::addID(IDT id) {
    _deletedIDs.emplace_back(id);
}

template <TypedInternalID IDT>
DeletedIDs<IDT>* DeletedIDs<IDT>::create(ASTContext* ctxt) {
    DeletedIDs* delIDs = new DeletedIDs<IDT>();
    if constexpr (std::is_same_v<IDT, NodeID>){
        ctxt->addDeletedNodes(delIDs);
    } else {
        ctxt->addDeletedEdges(delIDs);
    }
    return delIDs;
}

}
