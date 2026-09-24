#include "VariableDependencyGraph.h"

#include <algorithm>
#include <ranges>
#include <set>
#include <span>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "EdgePattern.h"
#include "EntityPattern.h"
#include "NodePattern.h"
#include "VariableDependencyGraphDumper.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "stmt/MatchStmt.h"
#include "stmt/UnwindStmt.h"
#include "decl/PatternData.h"
#include "decl/VarDecl.h"

#include "DependencyEdge.h"
#include "EdgeMetadata.h"
#include "VariableDependency.h"

#include "BioAssert.h"
#include "DiagnosticsManager.h"
#include "FatalException.h"

using namespace db;

static EdgeMetadata::EdgeType directionToType(EdgePattern::Direction dir) {
    switch (dir) {
        case EdgePattern::Direction::Undirected:
            return EdgeMetadata::EdgeType::GET_EDGES;
        break;
        case EdgePattern::Direction::Backward:
            return EdgeMetadata::EdgeType::GET_IN_EDGES;
        break;
        case EdgePattern::Direction::Forward:
            return EdgeMetadata::EdgeType::GET_OUT_EDGES;
        break;
    }
    throw FatalException("Invalid edge pattern direction");
}

static EdgeMetadata::EdgeType edgeTypeToNodeType(EdgeMetadata::EdgeType t) {
    if (t == EdgeMetadata::EdgeType::GET_OUT_EDGES) {
        return EdgeMetadata::EdgeType::GET_EDGE_TGT;
    } else if (t == EdgeMetadata::EdgeType::GET_IN_EDGES) {
        return EdgeMetadata::EdgeType::GET_EDGE_SRC;
    } else if (t == EdgeMetadata::EdgeType::GET_EDGES) {
        return EdgeMetadata::EdgeType::GET_EDGE_TGT;
    }
    throw FatalException(
        fmt::format("Unsure how to get node type for {}", EdgeTypeName::value(t)));
}

// The merge edges into one variable join its sources as a single variable, so a walk for
// cycles takes the first of them as an edge and leaves the others out
static bool standsForItsMerge(const DependencyEdge* edge) {
    const VariableDependency::Edges& incoming = edge->tgt()->incoming();
    const auto firstMerge = std::ranges::find_if(incoming, [](const DependencyEdge* e) { return e->isMetaEdge(); });

    return *firstMerge == edge;
}

static bool joinsOverMerge(const VariableDependency* var, const VariableDependency* other) {
    return std::ranges::any_of(var->edges(), [other](const DependencyEdge* e) {
        return e->isMetaEdge() && (e->src() == other || e->tgt() == other);
    });
}

VariableDependencyGraph::VariableDependencyGraph()
{
}

void VariableDependencyGraph::setDiagnosticsManager(const DiagnosticsManager* diagnostics) {
    _diagnostics = diagnostics;
}

void VariableDependencyGraph::throwError(std::string_view msg, const void* obj) const {
    _diagnostics->throwError(msg, obj);
}

VariableDependencyGraph::~VariableDependencyGraph() {
}

VariableDependencyGraph::VariableDependencyGraph(VariableDependencyGraph&& other) = default;

VariableDependencyGraph& VariableDependencyGraph::operator=(VariableDependencyGraph&& other) = default;

void VariableDependencyGraph::build(std::span<Stmt* const> stmts) {
    for (const Stmt* stmt : stmts) {
        if (const MatchStmt* match = dynamic_cast<const MatchStmt*>(stmt)) {
            const Pattern* pattern = match->getPattern();
            const Pattern::PatternElements& elements = pattern->elements();
            for (const PatternElement* element : elements) {
                registerPatternElement(element);
            }
        } else if (const UnwindStmt* unwind = dynamic_cast<const UnwindStmt*>(stmt)) {
            registerUnwindStmt(unwind);
        }
    }

    eliminateCycles();
}

VariableDependency* VariableDependencyGraph::registerBoundVariable(std::string_view name, const VarDecl* decl) {
    VariableDependency* boundVar = &_vars.emplace_back(name, decl);
    _boundVars.push_back(boundVar);

    return boundVar;
}

void VariableDependencyGraph::clear() {
    _vars.clear();
    _edges.clear();
    _anonymised.clear();
    _edgeIdentities.clear();
    _unwindSources.clear();
    _boundVars.clear();
}

const DependencyEdge* VariableDependencyGraph::addDirected(VariableDependency* src,
                                                           VariableDependency* tgt,
                                                           const EdgeMetadata& data) {
    DependencyEdge newEdge(src, tgt, data);

    // Exercises invariant that src outgoing and tgt incoming are synced
    const auto same = [&newEdge](DependencyEdge* other) {
        return newEdge == *other;
    };

    // If an edge between @ref src and @ref tgt with equivalent @ref data, return that
    // edge, and do not add any edges. This canonicalises patterns such as
    // `MATCH (n)-->(a), (n)-->(a)`, avoiding adding duplicate edges.
    const auto findIt = std::ranges::find_if(src->_outgoing, same);
    const bool exists = findIt != end(src->_outgoing);
    if (exists) {
        return *findIt;
    }

    DependencyEdge& placedEdge = _edges.emplace_back(std::move(newEdge));

    src->addOutgoing(&placedEdge);
    tgt->addIncoming(&placedEdge);

    return &placedEdge;
}

void VariableDependencyGraph::registerPatternElement(const PatternElement* ptn) {
    const EntityPattern* origin = ptn->getRootEntity();

    VariableDependency* originVar = getOrCreateVariable(origin);

    const auto& chain = ptn->getElementChain();

    // Two patterns sharing an edge variable are joined on identity; one element naming it
    // twice is rejected instead, as no edge is two hops of one element.
    std::vector<const VarDecl*> edgesInElement;

    VariableDependency* prev = originVar;
    for (const auto& [edge, tgtPtn] : chain) {
        const VarDecl* edgeDecl = edge->getDecl();
        bioassert(edgeDecl, "Edge pattern without declaration.");

        const bool alreadyInElement =
            std::ranges::find(edgesInElement, edgeDecl) != edgesInElement.end();
        if (alreadyInElement) {
            throwError("Re-using the same edge variable in a single pattern is not supported", edge);
        }

        edgesInElement.push_back(edgeDecl);

        VariableDependency* tgtVar = getOrCreateVariable(tgtPtn);

        const EdgePattern::Direction direction = edge->getDirection();
        const EdgeMetadata::EdgeType edgeType = directionToType(direction);
        const EdgeMetadata::EdgeType otherType = edgeTypeToNodeType(edgeType);

        VariableDependency* src {nullptr};
        VariableDependency* tgt {nullptr};

        src = prev;
        tgt = tgtVar;

        const std::string_view cypherEdgeName = edgeDecl->getName();

        // An edge the pattern leaves anonymous is one occurrence of one variable, so it
        // joins nothing: only a variable the query named gets an identity, whose
        // occurrences the code generator equates
        VariableDependency* edgeVar = nullptr;
        if (edgeDecl->isUnnamed()) {
            edgeVar = newAnonymousVariable(edgeDecl);
        } else {
            std::vector<VariableDependency*>& edgeOccurrences = _edgeIdentities[edgeDecl];
            std::string occurrenceName;
            occurrenceName += cypherEdgeName;
            occurrenceName += '\'';
            occurrenceName += std::to_string(edgeOccurrences.size());
            edgeVar = newVariable(occurrenceName);
            edgeOccurrences.push_back(edgeVar);
        }

        const EdgePatternData* edgeData = edge->getData();
        if (edgeData) {
            edgeVar->setEdgeTypeConstraint(edgeData->edgeTypeConstraints());
        }

        addDirected(src, edgeVar, EdgeMetadata {edgeType});
        addDirected(edgeVar, tgt, EdgeMetadata {otherType});

        prev = tgt;
    }
}

void VariableDependencyGraph::registerUnwindStmt(const UnwindStmt* stmt) {
    const VarDecl* decl = stmt->getDecl();
    bioassert(decl, "UNWIND without a declaration.");

    // An UNWIND over an expression evaluated per row expands the rows already in flight,
    // so its variable is bound where the statement is written rather than by a dataflow
    // of its own - it is no root, and the graph does not carry it.
    if (!stmt->unwindsLiteral()) {
        return;
    }

    // A literal list depends on no other variable and enters the graph isolated - a root
    // of its own connected component, whose dataflow the code generator opens from the
    // list. The lookup is what registerPatternElement does: two variables of one
    // declaration would leave every resolver picking one of them by container order.
    VariableDependency* unwindVar = findVariable(decl);
    if (!unwindVar) {
        unwindVar = newVariable(decl);
    }

    _unwindSources[unwindVar] = stmt;
}

VariableDependency* VariableDependencyGraph::newVariable(const VarDecl* decl) {
    return &_vars.emplace_back(decl->getName(), decl);
}

VariableDependency* VariableDependencyGraph::newVariable(std::string_view name) {
    return &_vars.emplace_back(name);
}

VariableDependency* VariableDependencyGraph::newAnonymousVariable(const VarDecl* decl) {
    std::string name;
    name += decl->getName();
    name += '\'';

    return &_vars.emplace_back(name, decl);
}

VariableDependency* VariableDependencyGraph::findVariable(const VarDecl* decl) {
    const auto match = [decl](const VariableDependency& dep) {
        return dep.getDecl() == decl;
    };
    const auto foundIt = std::ranges::find_if(_vars, match);

    return foundIt != _vars.end() ? &*foundIt : nullptr;
}

VariableDependency* VariableDependencyGraph::getOrCreateVariable(const EntityPattern* entity) {
    const VarDecl* decl = entity->getDecl();
    bioassert(decl, "Variable with null declaration.");

    // The name of an entity the pattern leaves anonymous is generated, and a Cypher alias
    // can carry that spelling too - `WITH p AS v0` beside a `MATCH (v0)-->()` - so it is
    // given a name of the graph's own, which no Cypher identifier can be
    VariableDependency* var = findVariable(decl);
    if (!var) {
        var = decl->isUnnamed() ? newAnonymousVariable(decl) : newVariable(decl);
    }

    const NodePattern* node = dynamic_cast<const NodePattern*>(entity);
    if (node) {
        const NodePatternData* data = node->getData();
        if (data) {
            var->addLabelConstraints(data->labelConstraints());
        }
    }

    return var;
}

bool VariableDependencyGraph::findCycle(Cycle& cycle) {
    struct Frame {
        VariableDependency* _var {nullptr};
        const DependencyEdge* _via {nullptr};
        VariableDependency* _from {nullptr};
    };

    std::unordered_map<const VariableDependency*, VariableDependency*> parents;
    std::unordered_map<const VariableDependency*, size_t> depths;

    std::vector<Frame> stack;

    for (VariableDependency& rootVar : _vars) {
        VariableDependency* root = &rootVar;
        if (depths.contains(root)) {
            continue;
        }

        stack.push_back({root, nullptr, nullptr});

        while (!stack.empty()) {
            const Frame frame = stack.back();
            stack.pop_back();

            VariableDependency* var = frame._var;
            if (depths.contains(var)) {
                continue;
            }

            parents[var] = frame._from;
            depths[var] = frame._from ? depths[frame._from] + 1 : 0;

            for (DependencyEdge* edge : var->edges()) {
                const bool leftOut = edge->isMetaEdge() && !standsForItsMerge(edge);
                if (leftOut || edge == frame._via) {
                    continue;
                }

                VariableDependency* other = edge->_src == var ? edge->_tgt : edge->_src;
                if (!depths.contains(other)) {
                    stack.push_back({other, edge, var});
                    continue;
                }

                // The edge joins var to a variable the tree already holds: the cycle is the
                // two tree paths up to where they meet, closed by this edge
                std::vector<VariableDependency*> fromVar;
                std::vector<VariableDependency*> fromOther;
                VariableDependency* up = var;
                VariableDependency* down = other;

                while (depths[up] > depths[down]) {
                    fromVar.push_back(up);
                    up = parents[up];
                }

                while (depths[down] > depths[up]) {
                    fromOther.push_back(down);
                    down = parents[down];
                }

                while (up != down) {
                    fromVar.push_back(up);
                    fromOther.push_back(down);
                    up = parents[up];
                    down = parents[down];
                }

                cycle.assign(fromVar.begin(), fromVar.end());
                cycle.push_back(up);
                cycle.insert(cycle.end(), fromOther.rbegin(), fromOther.rend());

                return true;
            }
        }
    }

    return false;
}

void VariableDependencyGraph::detachCycle(const Cycle& cyc) {
    if (cyc.empty()) {
        return;
    }

    bioassert(cyc.size() >= 2, "Invalid cycle.");

    // For a cycle (head, u, ..., v) with [v,head] in E :
    VariableDependency* head = cyc.front();
    VariableDependency* u = *next(begin(cyc));
    VariableDependency* v = *prev(end(cyc));

    const bool reachesUOverAHop = !joinsOverMerge(head, u);
    const bool reachesVOverAHop = !joinsOverMerge(head, v);

    // Break the cycle by subdividing with a merge edge
    if (reachesUOverAHop && reachesVOverAHop) {
        const VariableDependency* uCopy = subdivideWithMerge(u, head);
        const VariableDependency* vCopy = subdivideWithMerge(v, head);
        bioassert(uCopy && vCopy, "Cycle through {} was not detached.", head->getName());

        return;
    }

    // A merge edge is never split, so only the hop moves onto a copy, and the copy merges
    // into what the head merges into: a copy of a copy would be a merge of one source
    bioassert(reachesUOverAHop || reachesVOverAHop, "Cycle through {} has no hop at its head.", head->getName());
    VariableDependency* hopEnd = reachesUOverAHop ? u : v;

    VariableDependency* mergeTarget = head;
    for (DependencyEdge* edge = findOutgoingMerge(mergeTarget); edge; edge = findOutgoingMerge(mergeTarget)) {
        mergeTarget = edge->_tgt;
    }

    VariableDependency* copy = subdivideWithMerge(hopEnd, head);
    bioassert(copy, "Cycle through {} was not detached.", head->getName());

    if (mergeTarget != head) {
        DependencyEdge* toHead = findOutgoingMerge(copy);
        std::erase_if(copy->_outgoing, [toHead](DependencyEdge* e) { return e == toHead; });
        std::erase_if(head->_incoming, [toHead](DependencyEdge* e) { return e == toHead; });

        addDirected(copy, mergeTarget, EdgeMetadata(EdgeMetadata::EdgeType::MERGE));
    }
}

DependencyEdge* VariableDependencyGraph::findOutgoingMerge(VariableDependency* var) {
    const auto findIt = std::ranges::find_if(var->_outgoing, [](const DependencyEdge* e) { return e->isMetaEdge(); });

    return findIt == var->_outgoing.end() ? nullptr : *findIt;
}

void VariableDependencyGraph::subdivideWithMergeOutImpl(VariableDependency* s,
                                                        VariableDependency* mid,
                                                        VariableDependency* t,
                                                        DependencyEdge* e) {
    std::erase_if(s->_outgoing, [e](DependencyEdge* f) { return f == e; });
    std::erase_if(t->_incoming, [e](DependencyEdge* f) { return f == e; });

    const EdgeMetadata& data = e->data();
    addDirected(s, mid, data);
    addDirected(mid, t, EdgeMetadata(EdgeMetadata::EdgeType::MERGE));
}

void VariableDependencyGraph::subdivideWithMergeIncImpl(VariableDependency* s,
                                                        VariableDependency* mid,
                                                        VariableDependency* t,
                                                        DependencyEdge* e) {
    std::erase_if(s->_incoming, [e](DependencyEdge* f) { return f == e; });
    std::erase_if(t->_outgoing, [e](DependencyEdge* f) { return f == e; });

    const EdgeMetadata& data = e->data();
    addDirected(mid, s, data);
    addDirected(mid, t, EdgeMetadata(EdgeMetadata::EdgeType::MERGE));
}

VariableDependency* VariableDependencyGraph::subdivideWithMerge(VariableDependency* s,
                                                                VariableDependency* t) {
    std::string buf;
    { // Search out edges of @param s for an edge to @param t
        const auto findOut = std::ranges::find_if(
            s->_outgoing, [t](DependencyEdge* e) { return e->_tgt == t; });
        if (findOut != end(s->_outgoing)) {
            getNextAnonymisation(t, buf);
            VariableDependency* mid = newVariable(buf);
            subdivideWithMergeOutImpl(s, mid, t, *findOut);
            return mid;
        }
    }

    { // Search in edges of @param s for an edge from @param t
        const auto findIn = std::ranges::find_if(
            s->_incoming, [t](DependencyEdge* e) { return e->_src == t; });
        if (findIn != end(s->_incoming)) {
            getNextAnonymisation(t, buf);
            VariableDependency* mid = newVariable(buf);
            subdivideWithMergeIncImpl(s, mid, t, *findIn);
            return mid;
        }
    }

    // @param s and @param t are not connected, nothing to do

    return nullptr;
}

void VariableDependencyGraph::getNextAnonymisation(VariableDependency* v, std::string& buf) {
    const std::string_view name = v->getName();
    const int count = _anonymised[v];

    _anonymised[v]++;

    buf.clear();
    buf += name;
    buf += '\'';
    buf += std::to_string(count);
}

void VariableDependencyGraph::canonicaliseCycle(Cycle& cyc) {
    const auto isEdgeVariable = [](const VariableDependency* v) {
        return std::ranges::any_of(v->incoming(), [](const DependencyEdge* e) {
            const EdgeMetadata::EdgeType type = e->data().type();
            return type == EdgeMetadata::EdgeType::GET_OUT_EDGES
                || type == EdgeMetadata::EdgeType::GET_IN_EDGES
                || type == EdgeMetadata::EdgeType::GET_EDGES;
        });
    };
    const auto inDegree = [](const VariableDependency* v) {
        return v->incoming().size();
    };

    // A merge edge is never split into copies, so the merge target is the node with the
    // most hops around the cycle, then the higher in-degree
    const size_t cycleSize = cyc.size();
    const auto rank = [&](size_t index) {
        const VariableDependency* var = cyc[index];
        const VariableDependency* before = cyc[(index + cycleSize - 1) % cycleSize];
        const VariableDependency* after = cyc[(index + 1) % cycleSize];

        const bool isNode = !isEdgeVariable(var);
        const size_t hops = isNode ? !joinsOverMerge(var, before) + !joinsOverMerge(var, after) : 0;

        return std::tuple(hops, isNode, inDegree(var));
    };

    size_t pivotIndex = 0;
    for (size_t index = 1; index < cycleSize; index++) {
        if (rank(pivotIndex) < rank(index)) {
            pivotIndex = index;
        }
    }

    const auto pivot = cyc.begin() + pivotIndex;
    std::ranges::rotate(cyc, pivot);
}

void VariableDependencyGraph::eliminateCycles() {
    Cycle cycle;
    bool detachedACycle = false;

    // Detaching a cycle moves edges onto the merge copies it creates, so cycles sharing an
    // edge are found one at a time in the graph the previous detach left
    while (findCycle(cycle)) {
        canonicaliseCycle(cycle);
        detachCycle(cycle);
        detachedACycle = true;
    }

    if (detachedACycle) {
        cascadeMerges();
    }
}

void VariableDependencyGraph::cascadeMerges() {
    DependencyEdge* meta1 = nullptr;
    DependencyEdge* meta2 = nullptr;
    const auto getMetaPair = [&](DependencyEdge* e) {
        if (!e->isMetaEdge()) {
            return false;
        }

        if (!meta1) {
            meta1 = e;
            return true;
        }

        if (!meta2) {
            meta2 = e;
            return true;
        }
        return false;
    };

    const auto eraseFromSrc = [](DependencyEdge* toDel) {
        VariableDependency* src = toDel->_src;
        std::erase_if(src->_outgoing, [toDel](DependencyEdge* e) { return e == toDel; });
    };

    const auto isMeta = [](const DependencyEdge* e) { return e->isMetaEdge(); };

    std::string nameBuf;

    // newVariable appends to the deque, which invalidates its iterators but not its elements
    for (size_t varIndex = 0; varIndex < _vars.size(); varIndex++) {
        VariableDependency& v = _vars[varIndex];
        // For a variable which has more than 2 meta-edges, merge pairs into intermediate
        // nodes until every node has at exactly 0 or 2 incoming merge edges.
        while (std::ranges::count_if(v._incoming, isMeta) > 2) {
            meta1 = nullptr;
            meta2 = nullptr;
            // Sets @ref meta1/2 with the meta edges to merge, and removes them from
            // @ref v._incoming
            std::erase_if(v._incoming, getMetaPair);
            bioassert(meta1 && meta2, "Failed to get meta edges.");
            eraseFromSrc(meta1);
            eraseFromSrc(meta2);

            VariableDependency* src1 = meta1->_src;
            VariableDependency* src2 = meta2->_src;
            getNextAnonymisation(&v, nameBuf);
            // The sources of these two merge edges will now each have a merge edge into
            // @ref parent instead of @ref v
            VariableDependency* parent = newVariable(nameBuf);

            EdgeMetadata data(EdgeMetadata::EdgeType::MERGE);
            addDirected(src1, parent, data);
            addDirected(src2, parent, data);
            // Final edge from cascaded merge into origin
            addDirected(parent, &v, data);
        }
    }
}
