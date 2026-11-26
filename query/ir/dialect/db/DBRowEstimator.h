#pragma once

#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include <map>
#include <optional>
#include <string_view>

#include "mlir/IR/Operation.h"
#include "mlir/IR/Value.h"
#include "llvm/ADT/ArrayRef.h"
#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/DenseSet.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/STLFunctionalExtras.h"
#include "llvm/ADT/SmallVector.h"

#include "CardinalityEstimation.h"
#include "metadata/GraphMetadata.h"
#include "metadata/LabelSet.h"
#include "views/GraphView.h"

#include "DBColumnElement.h"
#include "DBOps.h"
#include "DBFilterUtils.h"
#include "DBKnownLabels.h"
#include "DBLookupPredicates.h"
#include "DBPassUtils.h"

namespace mlir::db {

struct JunctionJoins;
struct SeedPattern;
struct WalkEvent;

// The label set a scan's label names stand for, resolved against the schema, and whether
// the schema holds every one of them.
bool collectScanLabels(ArrayAttr labelNames, const ::db::GraphMetadata& metadata, ::db::LabelSet& labels);

// The edges the index keyed by one end's labels holds under these labels, which is what a
// scan of it walks, and none when the schema lacks one of them.
size_t estimateIndexedEdges(ArrayAttr labelNames,
                            bool indexedByTarget,
                            const ::db::GraphMetadata& metadata,
                            const ::db::CardinalityEstimation& estimation);

// System R's share of rows a predicate keeps (Selinger et al., 1979): the graph counts
// nodes per label set and edges per end, but no value per property.
constexpr double RANGE_SELECTIVITY = 1.0 / 3.0;
constexpr double MEMBERSHIP_SELECTIVITY_CEILING = 0.5;

// PostgreSQL's count of distinct values in a column it holds no statistics of
// (DEFAULT_NUM_DISTINCT): as many as the rows, up to this many, and two in a boolean one
constexpr double DEFAULT_DISTINCT_VALUES = 200;
constexpr double BOOLEAN_DISTINCT_VALUES = 2;

double estimateUnwoundRows(Unwind unwind);

using KnownLabels = llvm::DenseMap<Value, ::db::LabelSet>;

// The edges a hop walks as far as the estimate tells them apart: the labels known on the
// node it leaves and on the node it reaches, and the types it keeps, every type until one
// is named
struct HopEdges {
    ::db::LabelSet _near;
    ::db::LabelSet _far;
    llvm::SmallVector<::db::EdgeTypeID, 2> _types;
    bool _isTyped {false};
    bool _leavesSource {true};
    bool _walksBoth {false};
};

// The hop a walk last took at a node, and whether the node is the source of its edges
struct NodeSide {
    Operation* _hop {nullptr};
    bool _isOut {true};
};

// What an estimate walking a pattern's ops knows of its rows: the labels each node carries,
// the edges each hop walked, found from the hop and from the node it reached, and the hop
// the walk last took at each node
struct WalkKnowledge {
    KnownLabels _labels;
    llvm::DenseMap<Operation*, HopEdges> _hops;
    llvm::DenseMap<Value, Operation*> _reachingHops;
    llvm::DenseMap<Value, NodeSide> _lastSides;

    // A result an op hands on holds values of the column its rows come from, so it is
    // known as that column is: a filter keeps the labels of the nodes it reads
    void handOn(Operation* op);
    void handOn(Value rowSource, Value column);
};

// What a hop makes and reads per row it reads
struct HopEstimate {
    double _rows {0};
    double _edgesRead {0};
};

// The edge types a list of names stands for, a name the schema lacks standing for no edge
void collectEdgeTypes(ArrayAttr names, const ::db::GraphMetadata& metadata, llvm::SmallVectorImpl<::db::EdgeTypeID>& edgeTypes);

// The hop or edge scan a column of edge types was read from: its own type column, or the
// types of its edge column
Operation* findTypedHop(Value edgeTypes, llvm::function_ref<Value(Value)> nodeOf);

class RowEstimator {
public:
    explicit RowEstimator(const ::db::GraphView& view);

    double getNodeCount() const { return _nodeCount; }
    double getEdgeCount() const { return _edgeCount; }

    double getLabelledNodes(const ::db::LabelSet& labels) const;

    // The nodes a scan reads: those carrying every label @param labelNames names, every node
    // when it names none, whose labels go into @param knowledge for the column @param nodes
    double scanNodes(Value nodes, ArrayAttr labelNames, WalkKnowledge& knowledge) const;

    // The nodes a scan by property value makes: of those scanNodes reads, the ones holding
    // one of the property's distinct values
    double scanNodesByPropertyValue(ScanNodesByPropertyValue scan, WalkKnowledge& knowledge) const;

    // The rows an edge scan makes: the edges of the types @param typeNames names, every type
    // when it names none, from the nodes carrying every label @param sourceNames names to
    // those carrying every label @param targetNames names, an end with no names being any
    // node. What it walked goes into @param knowledge, as a hop out of its sources would.
    double scanEdges(Operation* scan,
                     ArrayAttr sourceNames,
                     ArrayAttr targetNames,
                     ArrayAttr typeNames,
                     WalkKnowledge& knowledge) const;

    // The rows a hop makes and the edges it reads per row it reads: the edges it walks per
    // node carrying the labels known on @param from, of its own types or else of @param
    // keptTypes, which a filter of its type column keeps, and every edge on that side of the
    // node, since a node's edges of one type are scattered through all of them. A node the
    // walk took a hop at before has the edges of the nodes that hop leads to, not the
    // average. What it walked goes into @param knowledge.
    HopEstimate walkHop(Operation* hop,
                        bool walksBackward,
                        Value from,
                        Value to,
                        ArrayAttr keptTypes,
                        WalkKnowledge& knowledge) const;

    // The share of rows a label check on @param node keeps, which then adds the labels every
    // alternative asks for to what @param knowledge knows of the node
    double keepLabelled(ArrayAttr alternatives, Value node, WalkKnowledge& knowledge) const;

    // @param nodeOf names the node a column holds, whose labels @param knowledge keeps
    double getSelectivity(Value mask, llvm::function_ref<Value(Value)> nodeOf, WalkKnowledge& knowledge) const;

    // A flat OR or AND chain is as long as the query, so the walk keeps its own stack
    double getPredicateSelectivity(Value predicate, llvm::function_ref<Value(Value)> nodeOf, const WalkKnowledge& knowledge) const;

private:
    // The edge counts and degree products asked for, read off the graph once each: the walks
    // the estimate compares ask for the same ones over and over
    using StatisticKey = llvm::SmallVector<uint64_t, 16>;

    static constexpr uint64_t EDGE_COUNT_KEY = 0;
    static constexpr uint64_t DEGREE_PRODUCTS_KEY = 1;

    const ::db::GraphMetadata& _metadata;
    ::db::CardinalityEstimation _estimation;
    double _nodeCount {0};
    double _edgeCount {0};
    mutable std::map<StatisticKey, double> _statistics;

    struct LabelCheck {
        ArrayAttr _alternatives;
        Value _nodes;
    };

    static LabelCheck readLabelCheck(Value predicate);

    static void appendLabels(const ::db::LabelSet& labels, StatisticKey& key);

    static void appendDegree(const ::db::DegreeSelection& degree, StatisticKey& key);

    double countEdges(const ::db::LabelSet& source, const HopEdges& edges, const ::db::LabelSet& target) const;

    double sumDegreeProducts(const ::db::LabelSet& labels,
                             const ::db::DegreeSelection& first,
                             const ::db::DegreeSelection& second) const;

    double countHopEdges(const HopEdges& edges) const;

    static ::db::DegreeSelection selectionOf(const HopEdges& edges, bool isOut);

    // The edges @param degree selects on the nodes carrying @param labels
    double countSelectedEdges(const ::db::LabelSet& labels, const ::db::DegreeSelection& degree) const;

    // How many times the label set's average of the edges @param next selects a node carrying
    // @param labels has when the walk took the edges @param previous selects there before:
    // a walk comes to a node with many edges more often than to one with few
    double getDegreeLift(const ::db::LabelSet& labels,
                         const ::db::DegreeSelection& previous,
                         const ::db::DegreeSelection& next) const;

    // Keeps the types of @param edges the names ask for, every one of them when the edges
    // were of any type
    void narrowTypes(ArrayAttr typeNames, HopEdges& edges) const;

    void addSharedLabels(ArrayAttr alternatives, ::db::LabelSet& labels) const;

    // The share of nodes carrying @param known that carry the labels of one alternative
    double shareLabelled(ArrayAttr alternatives, const ::db::LabelSet& known) const;

    // A node a hop reached is checked among the edges that hop walked, so a type that only
    // ever reaches nodes of a label keeps all of them; any other among the nodes it is
    // known to carry the labels of
    double shareLabelled(ArrayAttr alternatives, Value node, const WalkKnowledge& knowledge) const;

    // The share of rows a type check keeps: of the edges its hop walked, or of the graph's
    // when no hop the estimate walked made them
    double shareTyped(CheckEdgeTypeConstraint check,
                      llvm::function_ref<Value(Value)> nodeOf,
                      const WalkKnowledge& knowledge) const;

    double keepTyped(CheckEdgeTypeConstraint check,
                     llvm::function_ref<Value(Value)> nodeOf,
                     WalkKnowledge& knowledge) const;

    // With nothing to count the values by, one in DEFAULT_DISTINCT_VALUES: PostgreSQL's
    // DEFAULT_EQ_SEL
    static double getEqualityShare(double distinctValues);

    static double getPropertyDistinctValues(Type element, double nodes);

    // The distinct values a side of an equality holds: one per node in a node column, and
    // none in a value read off no node
    double getDistinctValues(Value side, llvm::function_ref<Value(Value)> nodeOf, const WalkKnowledge& knowledge) const;

    // An equality keeps one row in the distinct values of the side holding more, as
    // PostgreSQL estimates one over columns it holds no statistics of
    double getEqualitySelectivity(Value lhs, Value rhs, llvm::function_ref<Value(Value)> nodeOf, const WalkKnowledge& knowledge) const;

    double getMembershipSelectivity(InOp membership, llvm::function_ref<Value(Value)> nodeOf, const WalkKnowledge& knowledge) const;

    double getLeafSelectivity(Value predicate, llvm::function_ref<Value(Value)> nodeOf, const WalkKnowledge& knowledge) const;
};

// The rows an op seeds a factor with, and nothing at all when it seeds none - a fetch, a
// filter or a hop reads a column rather than making one. A listed set of IDs is its own
// count, a literal list one row per element, a by-label scan what the graph holds under
// those labels, and a property scan the share of those an equality keeps.
std::optional<double> estimateSourceRows(Operation* op, const RowEstimator& estimator, WalkKnowledge& knowledge);

// The ratio an op multiplies the rows it reads by, and nothing when it hands them on as
// they come. A hop makes a row per edge it walks of the nodes it reads, of the types and to
// the labels it keeps, and an undirected one walks both directions. It is a ratio rather
// than a count because a graph with fewer edges than nodes has a hop shrink its input
// rather than grow it. An unwind makes a row per element of each row's list, whose length
// is a property of the rows themselves: no statistic of the graph measures it, so it reads
// at the figure an uncountable source does - where a literal list or a range between
// literal bounds, whose elements are in the IR, is counted exactly instead.
std::optional<double> estimateRowMultiplier(Operation* op, const RowEstimator& estimator, WalkKnowledge& knowledge);

// The rows a factor makes: the product of what its sources seed it with, a factor crossing
// two of them holding a row per pair, grown or shrunk by every hop and unwind reading from
// one and cut by every filter. A join makes as many as its smaller side, each key taken as
// distinct on its side: the db level counts no distinct values. An aggregate with no
// grouping key makes one row, whatever it reads. A factor seeded from somewhere the db
// level cannot count - a procedure, a load - counts as an uncountable source's rows.
double estimateFactorRows(Region& factor, const RowEstimator& estimator);

// The work a walk does: the rows each op it emits builds, the scan of its first node, then
// each filter and each hop, and the edges each hop reads. A property equality in the first
// filter makes that scan and filter one scan by property value, which emits only the rows
// the filter keeps, and a hop emits only the edges of the types its own filter keeps, as
// the by-type hop it becomes, though it reads every edge of the nodes it leaves. Crossing a
// junction's other side in makes a row per pair, unless it is the hash join fuse_hash_join
// makes of the product and the equality right after it, which reads each side once. An
// equality of a key of each side keeps one pair in as many as the distinct keys of the side
// holding more, as PostgreSQL estimates a join of columns it holds no statistics of.
double estimateWalkCost(const SeedPattern& pattern,
                        Value start,
                        ArrayAttr startLabels,
                        llvm::ArrayRef<WalkEvent> events,
                        const JunctionJoins* joins,
                        const RowEstimator& estimator);

}
