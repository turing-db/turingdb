#include "NodeAccessor.h"

#include "Node.h"

using namespace db;

Result<bool> NodeAccessor::hasLabel(NameID label) const {
    bool deleted = false;
    bool hasLabel = false;

    // Get node data
    {
        std::shared_lock guard(_node->lock);
        deleted = _node->deleted;
        const auto& labels = _node->_labels;
        hasLabel = std::find(labels.begin(), labels.end(), label) != labels.end();
    }

    if (deleted) {
        return Error::DELETED_OBJECT;
    }

    return hasLabel;
}