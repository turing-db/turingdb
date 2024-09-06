#include "Explorator.h"

#include <queue>
#include <unordered_set>
#include <spdlog/spdlog.h>

#include "DB.h"
#include "Edge.h"
#include "NodeType.h"

#include "SearchUtils.h"
#include "NodeSearch.h"

#include "BioAssert.h"

using namespace db;

namespace {

struct QueueRecord {
    QueueRecord(Node* node, ExploratorTreeNode* parent)
        : _node(node),
        _parent(parent)
    {
    }

    Node* _node {nullptr};
    ExploratorTreeNode* _parent {nullptr};
};

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

    std::queue<QueueRecord> queue;
    std::unordered_set<Node*> discovered;

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
        queue.emplace(seed, nullptr);
        discovered.insert(seed);
    }

    // Little lambda to explore the next DB node following the current one
    // during exploration
    const auto exploreNext = [this, &queue, &discovered](ExploratorTreeNode* current,
                                                         Node* next,
                                                         Edge* edge) {
        if (_debug) {
            const std::string displayName = SearchUtils::getProperty(next, _displayName);
            spdlog::info("??? Should explore node id={} displayName='{}' ???",
                         next->getIndex().getObjectID(),
                         displayName);
        }

        if (discovered.find(next) != discovered.end()) {
            if (_debug) {
                const std::string displayName = SearchUtils::getProperty(next, _displayName);
                spdlog::info("ALREADY IN QUEUE node id={} displayName='{}' ?",
                            next->getIndex().getObjectID(),
                            displayName);
            }
            return;
        }

        if (!shouldExplore(next, edge)) {
            if (_debug) {
                const std::string displayName = SearchUtils::getProperty(next, _displayName);
                spdlog::info("Should NOT explore node id={} displayName='{}'",
                            next->getIndex().getObjectID(),
                            displayName);
            }
            return;
        }

        if (_debug) {
            const std::string displayName = SearchUtils::getProperty(next, _displayName);
            spdlog::info("Add to queue node id={} displayName='{}'",
                        next->getIndex().getObjectID(),
                        displayName);
        }
        queue.emplace(next, current);
        discovered.insert(next);
    };

    while (!queue.empty()) {
        const QueueRecord current = queue.front();
        queue.pop();
        Node* node = current._node;
        ExploratorTreeNode* parent = current._parent;

        if (_debug) {
            const std::string displayName = SearchUtils::getProperty(node, _displayName);
            spdlog::info("Visit node id={} displayName={}",
                         node->getIndex().getObjectID(),
                         displayName);
        }

        // Create ExploratorTreeNode
        ExploratorTreeNode* treeNode = ExploratorTreeNode::create(_tree, node, parent);

        if (isTarget(node)) {
            if (_debug) {
                spdlog::info("Node id={} is a target node",
                             node->getIndex().getObjectID());
            }

            _targets.insert(treeNode);
            if (!_traverseTargets) {
                if (_debug) {
                    spdlog::info("Traverse targets option is false stop here");
                }
                continue;
            }
        }

        if (treeNode->getDistance() >= _maxDist) {
            if (_debug) {
                spdlog::info("Maximum distance reached, stop here");
            }
            continue;
        }

        for (Edge* edge : node->inEdges()) {
            Node* next = edge->getSource();
            exploreNext(treeNode, next, edge); 
        }

        for (Edge* edge : node->outEdges()) {
            Node* next = edge->getTarget();
            exploreNext(treeNode, next, edge);
        }
    }

    buildNetwork();

    spdlog::info("Nodes visited: {}", _tree->getSize());
}

bool Explorator::shouldExplore(const Node* node, const Edge* edge) const {
    if (_maxDegree > 0) {
        const size_t edgeCount = node->inEdges().size() + node->outEdges().size();
        if (edgeCount > _maxDegree) {
            if (_debug) {
                const std::string displayName = SearchUtils::getProperty(node, _displayName);
                spdlog::info("NODE WITH TOO MUCH DEGREE {} node id={} displayName='{}'",
                            edgeCount,
                            node->getIndex().getObjectID(),
                            displayName);
            }
            return false;
        }
    }

    // Check species name
    const std::string speciesName = SearchUtils::getProperty(node, _speciesName);
    if (!speciesName.empty() && speciesName != "Homo sapiens") {
        if (_debug) {
            const std::string displayName = SearchUtils::getProperty(node, _displayName);
            spdlog::info("NOT HUMAN SPECIES '{}' node id={} displayName='{}'",
                        speciesName,
                        node->getIndex().getObjectID(),
                        displayName);
        }
        return false;
    }

    // Check schema class
    const std::string schemaClass = SearchUtils::getProperty(node, _schemaClassName);
    if (_excludedClasses.find(schemaClass) != _excludedClasses.end()) {
        if (_debug) {
            const std::string displayName = SearchUtils::getProperty(node, _displayName);
            spdlog::info("EXCLUDED SCHEMA CLASS '{}' node id={} displayName='{}'",
                        schemaClass,
                        node->getIndex().getObjectID(),
                        displayName);
        }
        return false;
    }

    // Check name against exclusion list
    const std::string displayName = SearchUtils::getProperty(node, _displayName);
    if (!displayName.empty()) {
        if (_excludedNames.find(displayName) != _excludedNames.end()) {
            if (_debug) {
                const std::string displayName = SearchUtils::getProperty(node, _displayName);
                spdlog::info("EXCLUDED NAME node id={} displayName='{}'",
                            node->getIndex().getObjectID(),
                            displayName);
            }
            return false;
        }

        for (const auto& excludedSubWord : _excludedSubwords) {
            if (NodeSearch::isApproximateMatch(displayName, excludedSubWord)) {
                if (_debug) {
                    const std::string displayName = SearchUtils::getProperty(node, _displayName);
                    spdlog::info("EXCLUDED SUBWORD '{}' node id={} displayName='{}'",
                                excludedSubWord,
                                node->getIndex().getObjectID(),
                                displayName);
                }
                return false;
            }
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

void Explorator::addExcludedSubword(const std::string& name) {
    _excludedSubwords.emplace_back(name);
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
        spdlog::info("Schema class {} not excluded because it is a target class",
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
