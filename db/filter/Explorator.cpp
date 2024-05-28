#include "Explorator.h"

#include <queue>
#include <spdlog/spdlog.h>

#include "DB.h"
#include "Edge.h"
#include "NodeType.h"

#include "SearchUtils.h"
#include "NodeSearch.h"

#include "BioAssert.h"

using namespace db;

namespace {

void buildNetworkRec(const ExploratorTreeNode* treeNode, SubNetBuilder* subNetBuilder) {
    Node* node = treeNode->getNode();
    if (subNetBuilder->getNodeInSubNet(node)) {
        return;
    }

    subNetBuilder->addNode(node);
    
    const ExploratorTreeNode* parent = treeNode->getParent();
    if (parent) {
        buildNetworkRec(parent, subNetBuilder);
    }
}

}

Explorator::Explorator(DB* db)
    : _db(db),
    _wb(db),
    _displayName(db->getString("displayName")),
    _speciesName(db->getString("speciesName")),
    _schemaClassName(db->getString("schemaClass"))
{
    _resNet = _wb.createNetwork(_db->getString("out"));
    bioassert(_resNet);
    _subNetBuilder = std::make_unique<SubNetBuilder>(_db, _resNet);

    addDefaultTargetClasses();
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

    const auto searchSeedName = _db->getString("searchSeed");

    // Create the initial explorator tree with the seeds
    _tree = new ExploratorTree();
    for (Node* seed : _seeds) {
        PropertyType* searchSeedPropType = seed->getType()->getPropertyType(searchSeedName);
        if (!searchSeedPropType) {
            searchSeedPropType = _wb.addPropertyType(seed->getType(), searchSeedName, ValueType::stringType());
        }
        bioassert(searchSeedPropType);
        const auto searchSeedProp = Property(searchSeedPropType, Value::createString("1"));
        _wb.setProperty(seed, searchSeedProp);
        ExploratorTreeNode* seedNode =  ExploratorTreeNode::create(_tree, seed, nullptr);
        queue.push(seedNode);
    }

    // Little lambda to explore the next DB node following the current one
    // during exploration
    const auto exploreNext = [this, &queue](ExploratorTreeNode* currentTreeNode,
                                            Node* next,
                                            Edge* edge) {
        if (!shouldExplore(next, edge)) {
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

        Node* node = currentTreeNode->getNode();

        if (isTarget(node)) {
            _targets.insert(currentTreeNode);
            if (!_traverseTargets) {
                continue;
            }
        }

        if (currentTreeNode->getDistance() >= _maxDist) {
            continue;
        }

        Node* current = currentTreeNode->getNode();
        for (Edge* edge : current->inEdges()) {
            Node* next = edge->getSource();
            exploreNext(currentTreeNode, next, edge); 
        }

        for (Edge* edge : current->outEdges()) {
            Node* next = edge->getTarget();
            exploreNext(currentTreeNode, next, edge);
        }
    }

    buildNetwork();

    spdlog::info("Nodes visited: {}", _tree->getSize());
}

bool Explorator::shouldExplore(const Node* node, const Edge* edge) const {
    if (_maxDegree > 0) {
        const size_t edgeCount = node->inEdges().size() + node->outEdges().size();
        if (edgeCount > _maxDegree) {
            return false;
        }
    }

    // Check species name
    const std::string speciesName = SearchUtils::getProperty(node, _speciesName);
    if (!speciesName.empty() && speciesName != "Homo sapiens") {
        return false;
    }

    // Check schema class
    const std::string schemaClass = SearchUtils::getProperty(node, _schemaClassName);
    if (_excludedClasses.find(schemaClass) != _excludedClasses.end()) {
        return false;
    }

    // Check name against exclusion list
    const std::string displayName = SearchUtils::getProperty(node, _displayName);
    if (!displayName.empty()) {
        if (_excludedNames.find(displayName) != _excludedNames.end()) {
            return false;
        }
    }

    return true;
}

void Explorator::buildNetwork() {
    for (const ExploratorTreeNode* treeNode : _targets) {
        buildNetworkRec(treeNode, _subNetBuilder.get());
    }
}

void Explorator::addExcludedName(const std::string& name) {
    _excludedNames.insert(name);
}

bool Explorator::isTarget(const db::Node* node) const {
    if (!_targetNodeNames.empty()) {
        return hasTargetName(node);
    }
    return hasTargetSchemaClass(node);
}

bool Explorator::hasTargetSchemaClass(const db::Node* node) const {
    const std::string schemaClass = SearchUtils::getProperty(node, _schemaClassName);
    return (_targetClasses.find(schemaClass) != _targetClasses.end());
}

bool Explorator::hasTargetName(const db::Node* node) const {
    const std::string displayName = SearchUtils::getProperty(node, _displayName);
    for (const auto& targetName : _targetNodeNames) {
        if (isTargetNameMatch(targetName, displayName)) {
            return true;
        }
    }

    return false;
}

bool Explorator::isTargetNameMatch(const std::string& target, const std::string& displayName) const {
    switch (_targetNameMatchType) {
        case TargetNameMatchType::EXACT:
            return target == displayName;
        case TargetNameMatchType::PREFIX:
            return NodeSearch::isPrefixMatch(displayName, target);
        case TargetNameMatchType::SUBWORD:
            return NodeSearch::isApproximateMatch(displayName, target);
        default:
            return false;
    }
}

void Explorator::setNoDefaultTargets() {
    _targetClasses.clear();
}

void Explorator::addTargetClass(const std::string& name) {
    _targetClasses.insert(name);
}

void Explorator::addTargetNodeName(const std::string& name) {
    _targetNodeNames.insert(name);
}

void Explorator::addDefaultTargetClasses() {
    _targetClasses.insert("ProteinDrug");
    _targetClasses.insert("ChemicalDrug");
}

void Explorator::addDefaultExcludedNames() {
    addExcludedName("ATP [cytosol]");
    addExcludedName("ADP [cytosol]");
    addExcludedName("AMP [cytosol]");
    addExcludedName("H2O [cytosol]");
    addExcludedName("AdoMet [cytosol]");
    addExcludedName("AdoHcy [cytosol]");
    addExcludedName("gain_of_function via non_conservative_missense_variant");
    addExcludedName("lapatinib, neratinib, afatinib, AZ5104, tesevatinib, canertinib, sapitinib, CP-724714, AEE78 [cytosol]");
}

void Explorator::addExcludedClass(const std::string& schemaClass) {
    if (_targetClasses.find(schemaClass) != _targetClasses.end()) {
        spdlog::error("Schema class {} not excluded because it is a target class",
                      schemaClass);
        return;
    }
    _excludedClasses.insert(schemaClass);
}

void Explorator::addDefaultExcludedClasses() {
    addExcludedClass("Summation");
    addExcludedClass("InstanceEdit");
    addExcludedClass("Species");
    addExcludedClass("Disease");
    addExcludedClass("Compartment");
    addExcludedClass("ReferenceDatabase");
    addExcludedClass("DatabaseIdentifier");
    addExcludedClass("EntityFunctionalStatus");

    if (!_traversePathways) {
        addExcludedClass("Pathway");
    }

    addExcludedClass("LiteratureReference");
    addExcludedClass("Publication");

    addExcludedClass("UndirectedInteraction");
    addExcludedClass("ReferenceGeneProduct");
    addExcludedClass("SimpleEntity");
    addExcludedClass("GO_MolecularFunction");

    if (!_traverseSets) {
        addExcludedClass("CandidateSet");
        addExcludedClass("DefinedSet");
    }

    if (!_traverseFailedReaction) {
        addExcludedClass("FailedReaction");
    }
}
