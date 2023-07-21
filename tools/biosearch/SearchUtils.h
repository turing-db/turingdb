#pragma once

#include <string>

#include "StringRef.h"

namespace db {
class Node;
}

class SearchUtils {
public:
    SearchUtils() = delete;

    static std::string getProperty(const db::Node* node, db::StringRef name);

    static void printNode(const db::Node* node);

    static bool isPublication(const db::Node* node);
    static bool isReactomeMetadata(const db::Node* node);
    static bool isReactomePathway(const db::Node* node);
};
