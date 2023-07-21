#include "Explorator.h"

#include <queue>

#include "DB.h"
#include "Edge.h"
#include "NodeType.h"

#include "ExploratorTree.h"
#include "SearchUtils.h"

#include "BioLog.h"

using namespace db;
using namespace Log;

Explorator::Explorator(DB* db)
    : _db(db),
    _wb(db),
    _stIdName(db->getString("stId")),
    _displayName(db->getString("displayName")),
    _speciesName(db->getString("speciesName"))
{
    _resNet = _wb.createNetwork(_db->getString("out"));
    _subNetBuilder = std::make_unique<SubNetBuilder>(_db, _resNet);
}

Explorator::~Explorator() {
}

void Explorator::addSeeds(const std::vector<Node*>& seeds) {
    _seeds.insert(std::end(_seeds), std::begin(seeds), std::end(seeds));
}

void Explorator::run() {
    if (_tree) {
        return;
    }

    std::queue<ExploratorTreeNode*> queue;

    // Create the initial explorator tree with the seeds
    _tree = new ExploratorTree();
    for (Node* seed : _seeds) {
        ExploratorTreeNode* seedNode =  ExploratorTreeNode::create(_tree, seed, nullptr);
        queue.push(seedNode);
    }

    // Little lambda to explore the next DB node following the current one
    // during exploration
    const auto exploreNext = [this, &queue](ExploratorTreeNode* currentTreeNode, Node* next) {
        if (!shouldExplore(next)) {
            return;
        }

        auto nextTreeNode = _tree->getNode(next);
        if (!nextTreeNode) {
            // We create the next tree node if not already discovered
            nextTreeNode = ExploratorTreeNode::create(_tree, next, currentTreeNode);
            nextTreeNode->setDistance(currentTreeNode->getDistance()+1);
            queue.push(nextTreeNode);
        }
    };

    while (!queue.empty()) {
        ExploratorTreeNode* currentTreeNode = queue.front();
        queue.pop();

        visit(currentTreeNode);

        if (currentTreeNode->getDistance() >= _maxDist) {
            continue;
        }

        Node* current = currentTreeNode->getNode();
        for (Edge* edge : current->inEdges()) {
            Node* next = edge->getSource();
            exploreNext(currentTreeNode, next); 
        }

        for (Edge* edge : current->outEdges()) {
            Node* next = edge->getTarget();
            exploreNext(currentTreeNode, next);
        }
    }

    BioLog::echo("Nodes visited: "+std::to_string(_tree->getSize()));
}

void Explorator::visit(ExploratorTreeNode* treeNode) {
    Node* node = treeNode->getNode();

    if (SearchUtils::isReactomePathway(node)) {
        _pathways.insert(node);
        return;
    }

    _subNetBuilder->addNode(node);
}

bool Explorator::shouldExplore(const Node* node) {
    if (SearchUtils::isReactomeMetadata(node)) {
        return false;
    }

    // Check species name
    const std::string speciesName = SearchUtils::getProperty(node, _speciesName);
    if (!speciesName.empty() && speciesName != "Homo sapiens") {
        return false;
    }

    // We do not want publications
    if (SearchUtils::isPublication(node)) {
        return false;
    }

    return true;
}
