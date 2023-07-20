#include "SearchUtils.h"

#include "DB.h"
#include "Node.h"
#include "NodeType.h"

using namespace db;

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

bool SearchUtils::isPublication(const db::Node* node) {
    const std::string typeName = node->getType()->getName().toStdString();
    return (typeName.find("Publication") != std::string::npos)
        || (typeName.find("Literature") != std::string::npos);
}

bool SearchUtils::isReactomeMetadata(const db::Node* node) {
    const std::string typeName = node->getType()->getName().toStdString();
    return typeName == "DatabaseObjectInstanceEdit";
}
