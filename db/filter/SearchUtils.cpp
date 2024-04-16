#include "SearchUtils.h"

#include "DB.h"
#include "Node.h"
#include "NodeType.h"

#include "ExploratorTree.h"

#include "BioLog.h"

using namespace db;
using namespace Log;

namespace {

void printPathRec(const ExploratorTreeNode* node,
                  db::StringRef displayNameProp,
                  std::ostream& stream) {
    const ExploratorTreeNode* parent = node->getParent();
    if (parent) {
        printPathRec(parent, displayNameProp, stream);
        stream << "->";
    }

    stream << SearchUtils::getProperty(node->getNode(), displayNameProp);
}

}

std::string SearchUtils::getProperty(const db::Node* node, db::StringRef name) {
    const PropertyType* propType = node->getType()->getPropertyType(name);
    if (!propType) {
        return std::string();
    }

    const auto prop = node->getProperty(propType);
    if (prop.isValid() && prop.getValue().getType().isString()) {
        return prop.getValue().getString();
    }

    return std::string();
}

void SearchUtils::printNode(const db::Node* node, std::ostream& stream) {
    stream << "========\n";
    stream << "NodeType "+node->getType()->getName().toStdString() << "\n";
    for (const auto& [propType, value] : node->properties()) {
        if (value.getType().isString()) {
            stream << propType->getName().toStdString()+" "+value.getString() << "\n";
        }
    }
    stream << "\n";
}

void SearchUtils::printPath(const ExploratorTreeNode* node,
                            db::StringRef displayNameProp,
                            std::ostream& stream) {
    printPathRec(node, displayNameProp, stream);
    stream << "\n";
}
