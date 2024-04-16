#pragma once

#include <string>
#include <ostream>

#include "StringRef.h"

namespace db {
class Node;
}

class ExploratorTreeNode;

class SearchUtils {
public:
    SearchUtils() = delete;

    static std::string getProperty(const db::Node* node, db::StringRef name);

    static void printNode(const db::Node* node, std::ostream& stream);
    static void printPath(const ExploratorTreeNode* node,
                          db::StringRef displayNameProp,
                          std::ostream& stream);
};
